
#include <vector>
#include <string>
#include <memory>
#include "assert.h"
#include <optional>
#include <numeric>
#include "math.h"
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <iterator>
#include <exception>
#include <thread>
#include <filesystem>
#include "sdio.h"
#include "fileio.h"

namespace dg::atomic_buffer::types{

    using path_type                         = std::filesystem::path;
    using checksum_type                     = uint32_t;
    using status_type                       = uint8_t;
    using page_id_type                      = size_t;
    using err_type                          = dg::sdio::runtime_exception::err_type; 
    using buffer_view                       = std::pair<void *, size_t>;
    using immutable_buffer_view             = std::pair<const void *, size_t>;
    using timepoint_type                    = uint32_t;
};

namespace dg::atomic_buffer::constants{

    static constexpr auto METADATA_PAGE_IDX             = size_t{0};
    static constexpr auto BIN_PAGE_OFFS                 = size_t{1};
    static constexpr const char * METADATA_NAME         = "ab_metadata";
    static constexpr const char * CONFIG_NAME           = "ab_config";
    static constexpr const char * CUR_PAGE_TRAITS       = "_curtraits";
    static constexpr const char * OLD_PAGE_TRAITS       = "_oldtraits";
    static constexpr const char * SHADOW_PAGE_TRATIS    = "_shadowtraits";
    static constexpr const char * EXTENSION             = "bin";

};

namespace dg::atomic_buffer::state{

    using namespace dg::atomic_buffer::types;

    enum status: status_type{
        anchoring,
        anchored,
        commiting,
        commited,
        rolling_back,
        rolled_back
    };

    struct Metadata{
        size_t buf_sz;
        checksum_type checksum;
        timepoint_type timestamp;
    };
    
    class Recoverable{

        public:

            virtual ~Recoverable() noexcept{}
            virtual status_type recover(status_type) = 0;
    };

    class StateControllable{

        public:

            virtual ~StateControllable() noexcept{}
            virtual const status_type& get() const noexcept = 0;
            virtual void set(status_type) = 0;
    };

    class StateSyncable{

        public:

            virtual ~StateSyncable() noexcept{}
            virtual void sync(status_type) = 0;
    };

    class StateFetchable{

        public:

            virtual ~StateFetchable() noexcept{}
            virtual status_type fetch() = 0;
    };

    class MetadataControllable{

        public:

            virtual ~MetadataControllable() noexcept{}
            virtual const Metadata& get() const noexcept = 0;
            virtual void set(Metadata) = 0;
    };
    
    class MetadataFetchable{

        public:

            virtual ~MetadataFetchable() noexcept{}
            virtual Metadata fetch() = 0;
    };

    class MetadataGeneratable{

        public:

            virtual ~MetadataGeneratable() noexcept{};
            virtual Metadata get(immutable_buffer_view) = 0;
    };

    class MetadataSerializable{

        public:

            virtual ~MetadataSerializable() noexcept{}
            virtual std::pair<std::unique_ptr<char[]>, size_t> serialize(Metadata) = 0;
            virtual Metadata deserialize(const void *) = 0;
    };

};

namespace dg::atomic_buffer::runtime_exception{

    using namespace dg::sdio::runtime_exception;
};

namespace dg::atomic_buffer::page{

    using namespace dg::atomic_buffer::types;

    class DistributionPolicy{

        public:

            virtual ~DistributionPolicy() noexcept{}
            virtual size_t hash(page_id_type) = 0;
    };

    class PathRetrievable{

        public:

            virtual ~PathRetrievable() noexcept{}
            virtual path_type to_path(page_id_type) = 0;
            virtual page_id_type to_id(const path_type&) = 0;
    };
    
    class PageInfoRetrievable{

        public:

            virtual ~PageInfoRetrievable() noexcept{}
            virtual std::vector<page_id_type> list() = 0;
            virtual size_t page_size() const noexcept = 0;
    };

};

namespace dg::atomic_buffer::page_io{

    using namespace dg::atomic_buffer::types;

    class Readable{

        public:

            virtual ~Readable() noexcept{}
            virtual void read_into(std::vector<std::pair<page_id_type, void *>>) = 0;
    };

    class Writable{

        public:

            virtual ~Writable() noexcept{}
            virtual void write(std::vector<std::pair<page_id_type, const void *>>) = 0;
    };

};

namespace dg::atomic_buffer::page_reservation{

    class Reservable{

        public:

            virtual ~Reservable() noexcept{}
            virtual void reserve(size_t) = 0;
    };

    class Shrinkable{

        public:

            virtual ~Shrinkable() noexcept{}
            virtual void shrink(size_t) = 0;
    };

};

namespace dg::atomic_buffer::page_keeper{

    using namespace dg::atomic_buffer::types;

    class PageObservable{

        public:

            virtual ~PageObservable() noexcept{}
            virtual void clear() noexcept = 0;
            virtual void add(size_t offs, size_t sz) noexcept = 0;
            virtual std::vector<page_id_type> get() = 0;
    };

};

namespace dg::atomic_buffer::page_discretizer{

    using namespace dg::atomic_buffer::types;

    class PageDiscretizable{

        public:

            using const_discretized_type    = std::pair<page_id_type, const void *>;
            using discretized_type          = std::pair<page_id_type, void *>;

            virtual ~PageDiscretizable() noexcept{}
            virtual std::vector<const_discretized_type> get(const void *, const std::vector<page_id_type>&) = 0;
            virtual std::vector<discretized_type> get(void *, const std::vector<page_id_type>&) = 0;
    };

};

namespace dg::atomic_buffer::checksum{

    using namespace dg::atomic_buffer::types;

    class Hashable{

        public:

            virtual ~Hashable() noexcept{}
            virtual checksum_type get(immutable_buffer_view) = 0;
    };
};

namespace dg::atomic_buffer::engine{
    
    using namespace dg::atomic_buffer::types;
    
    class CommitingDispatchable{

        public:

            virtual ~CommitingDispatchable() noexcept{}
            virtual void dispatch(immutable_buffer_view, std::vector<page_id_type>) = 0;
    };

    class LoadingEngine{

        public:

            virtual ~LoadingEngine() noexcept{}
            virtual size_t size() const noexcept = 0; 
            virtual std::unique_ptr<char[]> load() = 0;
            virtual void load_into(void *) = 0; 
    };

    class AnchoringEngine{

        public:

            virtual ~AnchoringEngine() noexcept{}
            virtual void anchor() = 0; //synchronization atomic anchor
    };

    class CommitingEngine{

        public:

            virtual ~CommitingEngine() noexcept{}
            virtual void clear() noexcept = 0;
            virtual void set_buf(const void *) noexcept = 0; 
            virtual void set_buf_size(size_t) noexcept = 0;
            virtual void set_buf_change(size_t offs, size_t sz) noexcept = 0;
            virtual void commit() = 0;
    };

    class RollbackEngine{

        public:
            
            virtual ~RollbackEngine() noexcept{}
            virtual void rollback() = 0; //fall back to the most recent anchored point
    };

    class Engine: public virtual LoadingEngine,
                  public virtual AnchoringEngine, 
                  public virtual CommitingEngine, 
                  public virtual RollbackEngine{};

};

//--done interface
namespace dg::atomic_buffer::utility{

    using namespace dg::atomic_buffer::types;

    struct SyncedEndiannessService{
        
        static constexpr auto is_native_big      = bool{std::endian::native == std::endian::big};
        static constexpr auto is_native_little   = bool{std::endian::native == std::endian::little};
        static constexpr auto precond            = bool{(is_native_big ^ is_native_little) != 0};
        static constexpr auto deflt              = std::endian::little; 
        static constexpr auto native_uint8       = is_native_big ? uint8_t{0} : uint8_t{1}; 

        static_assert(precond); //xor

        template <class T, std::enable_if_t<std::is_arithmetic_v<T>, bool> = true>
        static constexpr T bswap(T value){
            
            constexpr auto LOWER_BIT_MASK   = ~((char) 0u);
            constexpr auto idx_seq          = std::make_index_sequence<sizeof(T)>();
            T rs{};

            [&]<size_t ...IDX>(const std::index_sequence<IDX...>&){
                (
                    [&](size_t){

                        rs <<= CHAR_BIT;
                        rs |= value & LOWER_BIT_MASK;
                        value >>= CHAR_BIT;

                    }(IDX), ...
                );
            }(idx_seq);

            return rs;
        }

        static inline auto aligned_alloc(size_t alignment, size_t sz){

            void * buf      = std::aligned_alloc(alignment, sz);
            auto destructor = [](void * buf) noexcept{
                std::free(buf);
            };

            if (!buf){
                throw std::bad_alloc();
            }
            
            return std::unique_ptr<void, decltype(destructor)>(buf, destructor);
        }

        template <class T, std::enable_if_t<std::is_arithmetic_v<T>, bool> = true>
        static inline void dump(void * dst, T data) noexcept{    

            if constexpr(std::endian::native != deflt){
                data = bswap(data);
            }

            std::memcpy(dst, &data, sizeof(T));
        }

        template <class T, std::enable_if_t<std::is_arithmetic_v<T>, bool> = true>
        static inline T load(const void * src) noexcept{
            
            T rs{};
            std::memcpy(&rs, src, sizeof(T));

            if constexpr(std::endian::native != deflt){
                rs = bswap(rs);
            }

            return rs;
        }

        static inline const auto bswap_lambda   = []<class ...Args>(Args&& ...args){return bswap(std::forward<Args>(args)...);}; 

    };

    struct MemoryUtility{

        template <uintptr_t ALIGNMENT>
        static inline auto align(void * buf) noexcept -> void *{
            
            constexpr bool is_pow2      = (ALIGNMENT != 0) && ((ALIGNMENT & (ALIGNMENT - 1)) == 0);
            static_assert(is_pow2);

            constexpr uintptr_t MASK    = (ALIGNMENT - 1);
            constexpr uintptr_t NEG     = ~MASK;
            void * rs                   = reinterpret_cast<void *>((reinterpret_cast<uintptr_t>(buf) + MASK) & NEG);

            return rs;
        }

        static inline auto aligned_alloc(size_t alignment, size_t sz){

            void * buf      = std::aligned_alloc(alignment, sz);
            auto destructor = [](void * buf) noexcept{
                std::free(buf);
            };
            if (!buf){
                throw std::bad_alloc();
            }
            
            return std::unique_ptr<void, decltype(destructor)>(buf, destructor);
        }

        static inline auto aligned_empty_alloc(size_t alignment, size_t sz){

            auto rs = aligned_alloc(alignment, sz);
            std::memset(rs.get(), int{}, sz);
            return rs;
        } 

        static inline auto forward_shift(void * buf, size_t sz) noexcept -> void *{

            return reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(buf) + sz);
        }

        static inline auto forward_shift(const void * buf, size_t sz) noexcept -> const void *{

            return reinterpret_cast<const void *>(reinterpret_cast<uintptr_t>(buf) + sz);
        }
        
        static inline auto get_distance_vector(const void * _from, const void * _to) noexcept -> intptr_t{

            return reinterpret_cast<intptr_t>(_to) - reinterpret_cast<intptr_t>(_from); 
        } 
    };

    struct PageUtility{

        static auto relative_bin_pages_to_absolute(std::vector<page_id_type> pages) -> std::vector<page_id_type>{

            auto incrementor    = [](page_id_type page_id){return page_id + constants::BIN_PAGE_OFFS;};
            std::transform(pages.begin(), pages.end(), pages.begin(), incrementor);
            
            return pages;
        } 

        static auto absolute_bin_pages_to_relative(std::vector<page_id_type> pages) -> std::vector<page_id_type>{

            auto decrementor    = [](page_id_type page_id){return page_id - constants::BIN_PAGE_OFFS;};            
            std::transform(pages.begin(), pages.end(), pages.begin(), decrementor);

            return pages;
        }
        
        static auto add_metadata_page(std::vector<page_id_type> pages) -> std::vector<page_id_type>{
            
            pages.push_back(constants::METADATA_PAGE_IDX);
            return pages;
        }

        static auto remove_metadata_page(std::vector<page_id_type> pages) -> std::vector<page_id_type>{

            auto filterer   = [](page_id_type id){return id != constants::METADATA_PAGE_IDX;};
            auto last       = std::copy_if(pages.begin(), pages.end(), pages.begin(), filterer); 
            auto sz         = std::distance(pages.begin(), last);

            pages.resize(sz);
            return pages;
        }

        static auto shrink_pages(const std::vector<page_id_type>& pages, page_id_type sz) -> std::vector<page_id_type>{

            auto filterer       = [=](page_id_type page_id){return page_id < sz;};
            auto rs             = std::vector<page_id_type>();
            std::copy_if(pages.begin(), pages.end(), std::back_inserter(rs), filterer);

            return rs;
        }

        static constexpr auto slot(size_t buf_offs, size_t page_sz) -> page_id_type{

            return static_cast<page_id_type>(buf_offs / page_sz);
        }

        static constexpr auto size(size_t buf_sz, size_t page_sz) -> page_id_type{

            if (buf_sz == 0){
                return page_id_type{0u};
            }

            return slot(buf_sz - 1, page_sz) + 1;
        }
    };

    auto get_configuration_path(const path_type& dpath){

        return (dpath / constants::CONFIG_NAME).replace_extension(constants::EXTENSION);
    }

};

namespace dg::atomic_buffer::state{
    
    class MetadataController: public virtual MetadataControllable{

        private:

            Metadata metadata;
        
        public:

            MetadataController(Metadata metadata): metadata(std::move(metadata)){}
            
            const Metadata& get() const noexcept{
                
                return this->metadata;
            }

            void set(Metadata metadata){

                this->metadata = std::move(metadata);
            }
    };

    class MetadataFetcher: public virtual MetadataFetchable{

        private:

            std::unique_ptr<MetadataSerializable> serializer;
            const path_type src_path;

        public:

            MetadataFetcher(std::unique_ptr<MetadataSerializable> serializer,
                            path_type src_path): serializer(std::move(serializer)),
                                                 src_path(std::move(src_path)){}

            Metadata fetch(){
                
                auto rs = dg::fileio::bread(this->src_path); 
                return this->serializer->deserialize(static_cast<const void *>(rs.get()));
            }
    };

    class MetadataGenerator: public virtual MetadataGeneratable{

        private:

            std::unique_ptr<checksum::Hashable> hasher;

        public:

            MetadataGenerator(std::unique_ptr<checksum::Hashable> hasher): hasher(std::move(hasher)){}

            Metadata get(immutable_buffer_view buf_view){

                auto systime    = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock().now()).time_since_epoch().count();

                if (systime > std::numeric_limits<timepoint_type>::max() || systime < std::numeric_limits<timepoint_type>::min()){
                    std::abort(); //log required - 
                }

                Metadata rs{};
                rs.buf_sz       = buf_view.second;
                rs.checksum     = this->hasher->get(buf_view);
                rs.timestamp    = static_cast<timepoint_type>(systime);

                return rs;
            }
    };

    class StdMetadataSerializer: public virtual MetadataSerializable{ //metadata == page_sz

        private:

            const size_t page_sz;

        public:

            static_assert(std::is_trivial_v<Metadata>);

            StdMetadataSerializer(size_t page_sz): page_sz(page_sz){
                
                assert(page_sz >= sizeof(Metadata));
            }

            std::pair<std::unique_ptr<char[]>, size_t> serialize(Metadata metadata){
                
                using _MemIO    = utility::SyncedEndiannessService;
                using _MemUlt   = utility::MemoryUtility;
                
                auto buf        = std::unique_ptr<char[]>(new char[this->page_sz]);

                _MemIO::dump(buf.get(), metadata.buf_sz);
                _MemIO::dump(_MemUlt::forward_shift(buf.get(), sizeof(metadata.buf_sz)), metadata.checksum);
                _MemIO::dump(_MemUlt::forward_shift(buf.get(), sizeof(metadata.buf_sz) + sizeof(metadata.checksum)), metadata.timestamp);

                return {std::move(buf), this->page_sz};
            } 

            Metadata deserialize(const void * buf){

                using _MemIO    = utility::SyncedEndiannessService;
                using _MemUlt   = utility::MemoryUtility;

                Metadata rs{};
                
                rs.buf_sz       = _MemIO::load<decltype(rs.buf_sz)>(buf);
                rs.checksum     = _MemIO::load<decltype(rs.checksum)>(_MemUlt::forward_shift(buf, sizeof(rs.buf_sz)));
                rs.timestamp    = _MemIO::load<decltype(rs.timestamp)>(_MemUlt::forward_shift(buf, sizeof(rs.buf_sz) + sizeof(rs.checksum)));

                return rs;
            }
    };

    class StateSyncer: public virtual StateSyncable{

        private:

            const path_type src_path;

        public:

            StateSyncer(path_type src_path): src_path(std::move(src_path)){}

            void sync(status_type status){

                using _MemIO    = utility::SyncedEndiannessService;
                char buf[sizeof(status_type)];
                _MemIO::dump(static_cast<void *>(buf), status);
                dg::fileio::atomic_bwrite(this->src_path, static_cast<const void *>(buf), sizeof(status_type));
            }
    };

    class StateFetcher: public virtual StateFetchable{

        private:

            const path_type src_path;
        
        public:

            StateFetcher(path_type src_path): src_path(std::move(src_path)){}

            status_type fetch(){

                using _MemIO    = utility::SyncedEndiannessService;
                auto buf        = dg::fileio::bread(this->src_path); 
                
                return _MemIO::load<status_type>(static_cast<const void *>(buf.get()));
            }
    };

    class AnchoringRecoverer: public virtual Recoverable{

        private:

            std::shared_ptr<dg::sdio::core::DeletingDeviceInterface> deleting_device;
            std::unique_ptr<page::PageInfoRetrievable> old_page_info;
            std::unique_ptr<page::PathRetrievable> old_page_path;

        public:

            AnchoringRecoverer(std::shared_ptr<dg::sdio::core::DeletingDeviceInterface> deleting_device,
                               std::unique_ptr<page::PageInfoRetrievable> old_page_info,
                               std::unique_ptr<page::PathRetrievable> old_page_path): deleting_device(std::move(deleting_device)),
                                                                                      old_page_info(std::move(old_page_info)),
                                                                                      old_page_path(std::move(old_page_path)){}

            status_type recover(status_type status){

                if (status != state::status::anchoring){
                    std::abort();
                }

                auto old_page_ids   = this->old_page_info->list();
                auto files          = std::vector<path_type>(old_page_ids.size());

                std::transform(old_page_ids.begin(), old_page_ids.end(), files.begin(), [&](page_id_type idx){return this->old_page_path->to_path(idx);});
                this->deleting_device->del(std::move(files));

                return state::status::anchored;
            }
    };

    class AnchoredRecoverer: public virtual Recoverable{
        
        public:

            status_type recover(status_type status){

                if (status != state::anchored){
                    std::abort();
                }

                return status;
            }
    };

    class CommitingRecoverer: public virtual Recoverable{

        private:

            std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device;
            std::shared_ptr<sdio::core::DeletingDeviceInterface> del_device;
            std::unique_ptr<page::PageInfoRetrievable> old_page_info;
            std::unique_ptr<page::PageInfoRetrievable> cur_page_info;
            std::unique_ptr<page::PageInfoRetrievable> shadow_page_info;
            std::unique_ptr<page::PathRetrievable> old_page_path;
            std::unique_ptr<page::PathRetrievable> cur_page_path;
            std::unique_ptr<page::PathRetrievable> shadow_page_path;

        public:

            CommitingRecoverer(std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device,
                               std::shared_ptr<sdio::core::DeletingDeviceInterface> del_device,
                               std::unique_ptr<page::PageInfoRetrievable> old_page_info,
                               std::unique_ptr<page::PageInfoRetrievable> cur_page_info,
                               std::unique_ptr<page::PageInfoRetrievable> shadow_page_info,
                               std::unique_ptr<page::PathRetrievable> old_page_path,
                               std::unique_ptr<page::PathRetrievable> cur_page_path,
                               std::unique_ptr<page::PathRetrievable> shadow_page_path): rename_device(std::move(rename_device)),
                                                                                         del_device(std::move(del_device)),
                                                                                         old_page_info(std::move(old_page_info)),
                                                                                         cur_page_info(std::move(cur_page_info)),
                                                                                         shadow_page_info(std::move(shadow_page_info)),
                                                                                         old_page_path(std::move(old_page_path)),
                                                                                         cur_page_path(std::move(cur_page_path)),
                                                                                         shadow_page_path(std::move(shadow_page_path)){}

            status_type recover(status_type status){
                
                if (status != state::commiting){
                    std::abort();
                }
                
                auto cur_pages              = this->cur_page_info->list();
                auto old_pages              = this->old_page_info->list();
                auto shadow_pages           = this->shadow_page_info->list();
                auto old_shadow_pages       = std::vector<page_id_type>();
                auto old_cur_pages          = std::vector<page_id_type>();
                auto old_cur_shadow_pages   = std::vector<page_id_type>();

                std::sort(cur_pages.begin(), cur_pages.end());
                std::sort(old_pages.begin(), old_pages.end());
                std::sort(shadow_pages.begin(), shadow_pages.end());

                std::set_intersection(old_pages.begin(), old_pages.end(), shadow_pages.begin(), shadow_pages.end(), std::back_inserter(old_shadow_pages));
                std::set_intersection(old_pages.begin(), old_pages.end(), cur_pages.begin(), cur_pages.end(), std::back_inserter(old_cur_pages));
                std::set_intersection(old_shadow_pages.begin(), old_shadow_pages.end(), old_cur_pages.begin(), old_cur_pages.end(), std::back_inserter(old_cur_shadow_pages));

                if (old_cur_shadow_pages.size() != 0){
                    
                    this->symlink_resolve(std::move(old_cur_shadow_pages));
                    return this->recover(status);               
                }

                this->old_shadow_rollback(std::move(old_shadow_pages));
                this->old_cur_rollback(std::move(old_cur_pages));
                this->remove_shadow();

                return state::anchored;
            }

        private:

            void symlink_resolve(std::vector<page_id_type> pages){
                
                auto old_cur_res    = [&](page_id_type idx){dg::fileio::symlink_resolve(this->old_page_path->to_path(idx), this->cur_page_path->to_path(idx));}; //
                auto cur_shad_res   = [&](page_id_type idx){dg::fileio::symlink_resolve(this->cur_page_path->to_path(idx), this->shadow_page_path->to_path(idx));}; //

                std::for_each(pages.begin(), pages.end(), old_cur_res);
                std::for_each(pages.begin(), pages.end(), cur_shad_res);
            }

            void old_shadow_rollback(std::vector<page_id_type> pages){
                
                auto rename_pairs       = std::vector<std::pair<path_type, path_type>>(pages.size());
                auto transform_lambda   = [&](page_id_type idx){
                    return std::make_pair(this->old_page_path->to_path(idx), this->cur_page_path->to_path(idx));
                };
              
                std::transform(pages.begin(), pages.end(), rename_pairs.begin(), transform_lambda);
                this->rename_device->rename(std::move(rename_pairs));
            }

            void old_cur_rollback(std::vector<page_id_type> pages){
                
                auto rename_pairs       = std::vector<std::pair<path_type, path_type>>(pages.size());
                auto transform_lambda   = [&](page_id_type idx){
                    return std::make_pair(this->cur_page_path->to_path(idx), this->shadow_page_path->to_path(idx));
                };
              
                std::transform(pages.begin(), pages.end(), rename_pairs.begin(), transform_lambda);
                this->rename_device->rename(std::move(rename_pairs));
                this->old_shadow_rollback(std::move(pages));
            }

            void remove_shadow(){

                auto pages              = this->shadow_page_info->list();
                auto paths              = std::vector<path_type>(pages.size());
                auto transform_lambda   = [&](page_id_type idx){return this->shadow_page_path->to_path(idx);};

                std::transform(pages.begin(), pages.end(), paths.begin(), transform_lambda);
                this->del_device->del(std::move(paths));
            }
    };

    class CommitedRecoverer: public virtual Recoverable{

        public:

            status_type recover(status_type status){
                
                //
                if (status != state::commited){
                    std::abort();
                }

                return status;
            }
    };

    class RollingBackRecoverer: public virtual Recoverable{

        private:

            std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device;
            std::unique_ptr<page::PageInfoRetrievable> cur_page_info;
            std::unique_ptr<page::PageInfoRetrievable> old_page_info;
            std::unique_ptr<page::PathRetrievable> cur_page_path;
            std::unique_ptr<page::PathRetrievable> old_page_path;
        
        public:

            RollingBackRecoverer(std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device,
                                 std::unique_ptr<page::PageInfoRetrievable> cur_page_info,
                                 std::unique_ptr<page::PageInfoRetrievable> old_page_info,
                                 std::unique_ptr<page::PathRetrievable> cur_page_path,
                                 std::unique_ptr<page::PathRetrievable> old_page_path): rename_device(std::move(rename_device)),
                                                                                        cur_page_info(std::move(cur_page_info)),
                                                                                        old_page_info(std::move(old_page_info)),
                                                                                        cur_page_path(std::move(cur_page_path)),
                                                                                        old_page_path(std::move(old_page_path)){}

            status_type recover(status_type status){

                if (status != state::rolling_back){
                    std::abort();
                }

                auto old_pages          = this->old_page_info->list();
                auto cur_pages          = this->cur_page_info->list();
                auto old_n_cur_pages    = std::vector<page_id_type>();
                auto rename_pairs       = std::vector<std::pair<path_type, path_type>>();
                auto transform_lambda   = [&](page_id_type idx){return std::make_pair(this->old_page_path->to_path(idx), this->cur_page_path->to_path(idx));}; 
                
                std::sort(old_pages.begin(), old_pages.end());
                std::sort(cur_pages.begin(), cur_pages.end());
                std::set_intersection(old_pages.begin(), old_pages.end(), cur_pages.begin(), cur_pages.end(), std::back_inserter(old_n_cur_pages));
                std::transform(old_n_cur_pages.begin(), old_n_cur_pages.end(), std::back_inserter(rename_pairs), transform_lambda);
                
                this->rename_device->rename(std::move(rename_pairs));
                this->rm_old_files(); //avoid symlink - because rename (old, new) when equivalent == no effect

                return state::rolled_back;
            }

        private:

            void rm_old_files(){

                auto old_pages      = this->old_page_info->list();
                auto each_lambda    = [&](page_id_type idx){dg::fileio::assert_remove(this->old_page_path->to_path(idx));};
                
                std::for_each(old_pages.begin(), old_pages.end(), each_lambda);
            }        
        
    };

    class RolledBackRecoverer: public virtual Recoverable{
        
        public:

            status_type recover(status_type status){

                if (status != state::rolled_back){
                    std::abort();
                }

                return status;
            }
    };

    class StdRecoverer: public virtual Recoverable{

        private:
            
            std::unique_ptr<Recoverable> anchoring_rev;
            std::unique_ptr<Recoverable> anchored_rev;
            std::unique_ptr<Recoverable> commiting_rev;
            std::unique_ptr<Recoverable> committed_rev;
            std::unique_ptr<Recoverable> rollingback_rev;
            std::unique_ptr<Recoverable> rolledback_rev;
        
        public:

            StdRecoverer(std::unique_ptr<Recoverable> anchoring_rev,
                         std::unique_ptr<Recoverable> anchored_rev,
                         std::unique_ptr<Recoverable> commiting_rev,
                         std::unique_ptr<Recoverable> committed_rev,
                         std::unique_ptr<Recoverable> rollingback_rev,
                         std::unique_ptr<Recoverable> rolledback_rev): anchoring_rev(std::move(anchoring_rev)),
                                                                       anchored_rev(std::move(anchored_rev)),
                                                                       commiting_rev(std::move(commiting_rev)),
                                                                       committed_rev(std::move(committed_rev)),
                                                                       rollingback_rev(std::move(rollingback_rev)),
                                                                       rolledback_rev(std::move(rolledback_rev)){}

            status_type recover(status_type status){
            
                switch (status){

                    case state::anchoring:

                        return this->anchoring_rev->recover(status);
                    
                    case state::anchored:

                        return this->anchored_rev->recover(status);
                    
                    case state::commiting:

                        return this->commiting_rev->recover(status);
                    
                    case state::commited:

                        return this->committed_rev->recover(status);
                    
                    case state::rolling_back:

                        return this->rollingback_rev->recover(status);
                    
                    case state::rolled_back:

                        return this->rolledback_rev->recover(status);
                    
                    default:

                        std::abort(); //
                        break;
                };

                return {};
            }
    };

}

namespace dg::atomic_buffer::page{

    using namespace dg::atomic_buffer::types;

    class PathBase{

        protected:

            static_assert(std::is_unsigned_v<page_id_type>);

            path_type to_path(page_id_type id, const path_type& dir, const char * traits){ //precond - traits must not start with digits

                auto fname      = std::to_string(id) + traits; 
                auto fullname   = path_type(fname).replace_extension(constants::EXTENSION);
                auto fpath      = dir / fullname;

                return fpath;
            }

            page_id_type to_id(const path_type& path){
                
                using val_type          = path_type::value_type;
                constexpr val_type MMIN = '0';
                constexpr val_type MMAX = '9';

                auto nis_digit          = [=](const val_type& cur){std::clamp(cur, MMIN, MMAX) != cur;};
                auto fname              = path.filename();
                auto fdigit             = decltype(fname)();
                auto pos                = std::find(fname.begin(), fname.end(), nis_digit);

                std::copy(fname.begin(), pos, std::back_inserter(fdigit));
                return std::stoi(fdigit);
            }
    };

    class StdPathRetriever: public virtual PathRetrievable,
                            private PathBase{
        
        private:

            const path_type dir;
            const char * traits;
        
        public:

            StdPathRetriever(path_type dir, const char * traits): dir(std::move(dir)), 
                                                                  traits(traits), 
                                                                  PathBase(){}

            path_type to_path(page_id_type id){

                return PathBase::to_path(id, this->dir, this->traits);
            }

            page_id_type to_id(const path_type& path){

                return PathBase::to_id(path);
            }
    };

    class DistributedPathRetriever: public virtual PathRetrievable,
                                    private PathBase{

        private:

            std::unordered_map<size_t, std::unique_ptr<PathRetrievable>> lookup_map;
            std::unique_ptr<DistributionPolicy> distributor;
        
        public:

            DistributedPathRetriever(std::unordered_map<size_t, std::unique_ptr<PathRetrievable>> lookup_map,
                                     std::unique_ptr<DistributionPolicy> distributor): lookup_map(std::move(lookup_map)),
                                                                                       distributor(std::move(distributor)){}
            
            path_type to_path(page_id_type page_id){

                size_t bucket   = this->distributor->hash(page_id);
                return this->lookup_map[bucket]->to_path(page_id);
            }

            page_id_type to_id(const path_type& path){

                return PathBase::to_id(path);
            }
    };

    class StdPageInfoRetriever: public virtual PageInfoRetrievable,
                                private PathBase{
        
        private:

            const std::vector<path_type> dirs;
            const char * traits;
            const size_t page_sz; 
                    
        public:

            StdPageInfoRetriever(std::vector<path_type> dirs, 
                                 const char * traits,
                                 size_t page_sz): dirs(std::move(dirs)), 
                                                  traits(traits),
                                                  page_sz(page_sz),
                                                  PathBase(){}

            std::vector<page_id_type> list(){

                auto all_paths          = std::vector<std::vector<path_type>>();
                auto filtered_paths     = std::vector<path_type>();
                auto ids                = std::vector<page_id_type>();
                auto filterer           = [&](const path_type& path){return path.native().find(this->traits) != std::string::npos;};
                auto path_to_id         = [&](const path_type& path){return PathBase::to_id(path);}; 

                std::transform(this->dirs.begin(), this->dirs.end(), std::back_inserter(all_paths), fileio::list_paths<true>);
                for (const auto& dfiles: all_paths){
                    std::copy_if(dfiles.begin(), dfiles.end(), std::back_inserter(filtered_paths), filterer);
                }
                std::transform(filtered_paths.begin(), filtered_paths.end(), std::back_inserter(ids), path_to_id);

                return ids;
            }

            size_t page_size() const noexcept{

                return this->page_sz;
            }
    };

    struct Generator{

        static auto get_old_path_retriever(path_type dir) -> std::unique_ptr<PathRetrievable>{

            return std::make_unique<StdPathRetriever>(std::move(dir), constants::OLD_PAGE_TRAITS);
        }

        static auto get_cur_path_retriever(path_type dir) -> std::unique_ptr<PathRetrievable>{

            return std::make_unique<StdPathRetriever>(std::move(dir), constants::CUR_PAGE_TRAITS);
        }

        static auto get_shadow_path_retriever(path_type dir) -> std::unique_ptr<PathRetrievable>{

            return std::make_unique<StdPathRetriever>(std::move(dir), constants::SHADOW_PAGE_TRATIS);
        }

        static auto get_old_page_info_retriever(path_type dir, size_t page_sz) -> std::unique_ptr<PageInfoRetrievable>{

            return std::make_unique<StdPageInfoRetriever>(std::move(dir), constants::OLD_PAGE_TRAITS, page_sz);
        }

        static auto get_cur_page_info_retriever(path_type dir, size_t page_sz) -> std::unique_ptr<PageInfoRetrievable>{

            return std::make_unique<StdPageInfoRetriever>(std::move(dir), constants::CUR_PAGE_TRAITS, page_sz);
        }

        static auto get_shadow_page_info_retriever(path_type dir, size_t page_sz) -> std::unique_ptr<PageInfoRetrievable>{

            return std::make_unique<StdPageInfoRetriever>(std::move(dir), constants::SHADOW_PAGE_TRATIS, page_sz);
        }
    };
}

namespace dg::atomic_buffer::page_io{

    class StdPageReader: public virtual Readable{

        private:

            std::shared_ptr<dg::sdio::core::ReadingDeviceInterface> reading_device;
            std::unique_ptr<page::PathRetrievable> path_retriever;
            const size_t page_sz;

        public:

            StdPageReader(std::shared_ptr<dg::sdio::core::ReadingDeviceInterface> reading_device,
                          std::unique_ptr<page::PathRetrievable> path_retriever,
                          size_t page_sz): reading_device(std::move(reading_device)),
                                           path_retriever(std::move(path_retriever)),
                                           page_sz(page_sz){}
            

            void read_into(std::vector<std::pair<page_id_type, void *>> data){
                
                auto read_arg           = std::vector<std::tuple<path_type, void *, size_t>>(data.size());
                auto transform_lambda   = [&](const std::pair<page_id_type, void *>& cur){
                    return std::make_tuple(this->path_retriever->to_path(cur.first), cur.second, this->page_sz);
                };  

                std::transform(data.begin(), data.end(), read_arg.begin(), transform_lambda);
                this->reading_device->read_into(std::move(read_arg));
            }   
    };

    class StdPageWriter: public virtual Writable{

        private:

            std::shared_ptr<dg::sdio::core::WritingDeviceInterface> writing_device;
            std::unique_ptr<page::PathRetrievable> path_retriever;
            const size_t page_sz;

        public:

            StdPageWriter(std::shared_ptr<dg::sdio::core::WritingDeviceInterface> writing_device,
                          std::unique_ptr<page::PathRetrievable> path_retriever,
                          size_t page_sz): writing_device(std::move(writing_device)),
                                           path_retriever(std::move(path_retriever)),
                                           page_sz(page_sz){}

            void write(std::vector<std::pair<page_id_type, const void *>> data){
                
                auto write_arg          = std::vector<std::tuple<path_type, const void *, size_t>>(data.size());
                auto transform_lambda   = [&](const std::pair<page_id_type, const void *>& cur){
                    return std::make_tuple(this->path_retriever->to_path(cur.first), cur.second, this->page_sz);
                };

                std::transform(data.begin(), data.end(), write_arg.begin(), transform_lambda);
                this->writing_device->write(std::move(write_arg));
            }
    };

};

namespace dg::atomic_buffer::page_reservation{

    using namespace dg::atomic_buffer::types;

    class StdReserver: public virtual Reservable{

        private:

            std::unique_ptr<page::PageInfoRetrievable> cur_page_info;
            std::unique_ptr<page_io::Writable> page_writer;
        
        public:

            StdReserver(std::unique_ptr<page::PageInfoRetrievable> cur_page_info,
                        std::unique_ptr<page_io::Writable> page_writer): cur_page_info(std::move(cur_page_info)),
                                                                         page_writer(std::move(page_writer)){}

            void reserve(size_t page_num){

                if (page_num == 0){
                    return;
                }

                //avoid crash during reservation - leaving pages in non-continuous state (this can be solved during recovery but better to be here, single responsibility)

                using _MemUlt   = utility::MemoryUtility; 
                auto cur_pages  = this->cur_page_info->list();                
                auto seq_pages  = std::vector<page_id_type>(page_num);
                auto diff       = std::vector<page_id_type>();
                auto write_data = std::vector<std::pair<page_id_type, const void *>>();
                auto emp_buf    = _MemUlt::aligned_empty_alloc(dg::sdio::constants::STRICTEST_BLOCK_SZ, this->cur_page_info->page_size());

                std::iota(seq_pages.begin(), seq_pages.end(), 0u);
                std::sort(cur_pages.begin(), cur_pages.end());
                std::set_difference(seq_pages.begin(), seq_pages.end(), cur_pages.begin(), cur_pages.end(), std::back_inserter(diff));
                std::transform(diff.begin(), diff.end(), std::back_inserter(write_data), [&](page_id_type id){return std::make_pair(id, static_cast<const void *>(emp_buf.get()));}); //read only - no cache problem
                this->page_writer->write(std::move(write_data));
            }
    };
    
    class StdShrinker: public virtual Shrinkable{

        private:

            std::shared_ptr<dg::sdio::core::DeletingDeviceInterface> deleting_device;
            std::unique_ptr<page::PageInfoRetrievable> cur_page_info;
            std::unique_ptr<page::PathRetrievable> page_path;
        
        public:

            StdShrinker(std::shared_ptr<dg::sdio::core::DeletingDeviceInterface> deleting_device,
                        std::unique_ptr<page::PageInfoRetrievable> cur_page_info,
                        std::unique_ptr<page::PathRetrievable> page_path): deleting_device(std::move(deleting_device)),
                                                                           cur_page_info(std::move(cur_page_info)),
                                                                           page_path(std::move(page_path)){}

            void shrink(size_t page_num){
                
                auto all_pages  = this->cur_page_info->list();
                auto rm_files   = std::vector<path_type>();
                auto last       = std::copy_if(all_pages.begin(), all_pages.end(), all_pages.begin(), [=](page_id_type idx){return idx >= page_num;});
                
                std::transform(all_pages.begin(), last, std::back_inserter(rm_files), [&](page_id_type idx){return this->page_path->to_path(idx);});
                this->deleting_device->del(std::move(rm_files));
            }
    };

};

namespace dg::atomic_buffer::checksum{

    using namespace dg::atomic_buffer::types;

    class Hasher: public virtual Hashable{

        public:

            checksum_type checksum(const void * buf, size_t buf_sz){
                
                using _MemIO    = utility::SyncedEndiannessService; 
                using _MemUlt   = utility::MemoryUtility;

                const auto MOD  = std::numeric_limits<checksum_type>::max() >> 1;
                auto ibuf       = buf;
                auto rs         = checksum_type{};
                auto tmp        = checksum_type{};
                auto cycles     = static_cast<size_t>(buf_sz / sizeof(checksum_type));

                for (size_t i = 0; i < cycles; ++i){
                    
                    tmp = _MemIO::load<checksum_type>(ibuf);
                    tmp %= MOD;
                    rs  += tmp;
                    rs  %= MOD;
                    ibuf = _MemUlt::forward_shift(ibuf, sizeof(checksum_type));
                }

                return rs;
            }
    };

};

namespace dg::atomic_buffer::page_keeper{

    class StdPageObserver: public virtual PageObservable{

        private:

            std::vector<bool> injective_bloom_filter;
            const size_t page_sz;
        
        public:

            StdPageObserver(std::vector<bool> injective_bloom_filter,
                            size_t page_sz): injective_bloom_filter(std::move(injective_bloom_filter)), 
                                             page_sz(page_sz){
                
                assert(page_sz != 0);
            }

            void clear() noexcept{

                for (size_t i = 0; i < this->injective_bloom_filter.size(); ++i){
                    this->injective_bloom_filter[i] = false;
                }
            }

            void add(size_t offset, size_t sz) noexcept{
                
                //
                if (sz == 0){
                    return;
                }
                
                size_t bob_page_idx = offset / this->page_sz;
                size_t eob_page_idx = (offset + sz - 1) / this->page_sz;

                for (size_t i = bob_page_idx; i <= eob_page_idx; ++i){
                    this->injective_bloom_filter[i] = true;
                }
            }

            std::vector<page_id_type> get(){

                auto rs = std::vector<page_id_type>();

                for (size_t i = 0; i < this->injective_bloom_filter.size(); ++i){
                    if (this->injective_bloom_filter[i]){
                        rs.push_back(i); 
                    }
                }

                return rs;
            }
    };

};

namespace dg::atomic_buffer::page_discretizer{

    class StdPageDiscretizer: public virtual PageDiscretizable{

        private:

            const size_t page_size;
        
        public:

            using const_discretized_type    = PageDiscretizable::const_discretized_type;
            using discretized_type          = PageDiscretizable::discretized_type;

            StdPageDiscretizer(size_t page_size): page_size(page_size){

                assert(page_size != 0);
            }

            std::vector<const_discretized_type> get(const void * buf, const std::vector<page_id_type>& page_ids){

                using _MemUlt       = utility::MemoryUtility;
                auto idx_set        = std::unordered_set<page_id_type>(page_ids.begin(), page_ids.end());
                auto rs             = std::vector<const_discretized_type>(idx_set.size());
                auto discretizer    = [&](page_id_type idx) -> const_discretized_type{return {idx, _MemUlt::forward_shift(buf, this->page_size * idx)};};

                std::transform(idx_set.begin(), idx_set.end(), rs.begin(), discretizer);

                return rs;
            }

            std::vector<discretized_type> get(void * buf, const std::vector<page_id_type>& page_ids){

                using _MemUlt       = utility::MemoryUtility;
                auto idx_set        = std::unordered_set<page_id_type>(page_ids.begin(), page_ids.end());
                auto rs             = std::vector<discretized_type>(idx_set.size());
                auto discretizer    = [&](page_id_type idx) -> discretized_type{return {idx, _MemUlt::forward_shift(buf, this->page_size * idx)};};

                std::transform(idx_set.begin(), idx_set.end(), rs.begin(), discretizer);

                return rs; 
            }
    };

};

namespace dg::atomic_buffer::engine::anchor{

    class StdAnchoringEngine: public virtual AnchoringEngine{

        private:

            std::shared_ptr<state::StateControllable> state_controller;
            std::shared_ptr<state::MetadataControllable> metadata_controller;
            std::unique_ptr<state::StateSyncable> state_syncer;
            std::unique_ptr<state::Recoverable> recoverer;
            std::unique_ptr<state::MetadataFetchable> metadata_fetcher;

        public:

            StdAnchoringEngine(std::shared_ptr<state::StateControllable> state_controller,
                               std::shared_ptr<state::MetadataControllable> metadata_controller,
                               std::unique_ptr<state::StateSyncable> state_syncer,
                               std::unique_ptr<state::Recoverable> recoverer,
                               std::unique_ptr<state::MetadataFetchable> metadata_fetcher): state_controller(std::move(state_controller)),
                                                                                            metadata_controller(std::move(metadata_controller)),
                                                                                            state_syncer(std::move(state_syncer)),
                                                                                            recoverer(std::move(recoverer)),
                                                                                            metadata_fetcher(std::move(metadata_fetcher)){}

            void anchor(){
                
                switch (this->state_controller->get()){
                    
                    case state::anchoring:
                        std::abort(); //invalid (invoked pre-recovered)
                        return; 

                    case state::anchored:
                        return;
                    
                    case state::commiting:
                        std::abort(); ////invalid (invoked pre-recovered)
                        return;
                    
                    case state::commited:
                        this->start_anchoring_process();
                        return;
                    
                    case state::rolling_back:
                        std::abort(); //invalid (invoked pre-recovered)
                        return;
                    
                    case state::rolled_back:
                        this->start_anchoring_process();
                        return; 
                    
                    default:
                        std::abort(); //invalid state
                        return;
                }
            }
        
        private:

            void start_anchoring_process(){
                
                this->state_syncer->sync(state::anchoring);
                auto new_state = this->recoverer->recover(state::anchoring);
                this->state_syncer->sync(new_state);
                this->state_controller->set(new_state);
                this->metadata_controller->set(this->metadata_fetcher->fetch());
            }
    };

}

namespace dg::atomic_buffer::engine::load{

    class StdLoadingEngine: public virtual LoadingEngine{

        private:

            std::shared_ptr<state::MetadataControllable> metadata;
            std::unique_ptr<checksum::Hashable> hasher;
            std::unique_ptr<page_discretizer::PageDiscretizable> page_discretizer;
            std::unique_ptr<page::PageInfoRetrievable> page_info;
            std::unique_ptr<page_io::Readable> page_reader;

        public:

            StdLoadingEngine(std::shared_ptr<state::MetadataControllable> metadata,
                             std::unique_ptr<checksum::Hashable> hasher,
                             std::unique_ptr<page_discretizer::PageDiscretizable> page_discretizer,
                             std::unique_ptr<page::PageInfoRetrievable> page_info,
                             std::unique_ptr<page_io::Readable> page_reader): metadata(std::move(metadata)),
                                                                              hasher(std::move(hasher)),
                                                                              page_discretizer(std::move(page_discretizer)),
                                                                              page_info(std::move(page_info)),
                                                                              page_reader(std::move(page_reader)){}
            
            size_t size() const noexcept{
                
                return this->metadata->get().buf_sz;
            }

            std::unique_ptr<char[]> load(){

                auto rs = std::unique_ptr<char[]>(new char[this->size()]);
                this->load_into(static_cast<void *>(rs.get()));

                return rs;
            }

            void load_into(void * buf){
                
                if (this->size() % this->page_info->page_size() != 0){
                    std::abort(); //segfault
                }

                using _PageUlt          = utility::PageUtility;
                auto view               = buffer_view{buf, this->size()};
                auto pages              = this->page_info->list();
                auto bin_pages          = _PageUlt::absolute_bin_pages_to_relative(_PageUlt::remove_metadata_page(std::move(pages)));
                auto discretized        = this->page_discretizer->get(buf, bin_pages);

                this->page_reader->read_into(std::move(discretized));

                if (this->hasher->get(view) != this->metadata->get().checksum){
                    throw runtime_exception::CorruptedError();
                }
            }
        
    };

}

namespace dg::atomic_buffer::engine::rollback{

    class StdRollbackEngine: public virtual RollbackEngine{

        private:

            std::shared_ptr<state::StateControllable> state_controller;
            std::shared_ptr<state::MetadataControllable> metadata_controller;
            std::unique_ptr<state::StateSyncable> state_syncer;
            std::unique_ptr<state::Recoverable> recoverer;
            std::unique_ptr<state::MetadataFetchable> metadata_fetcher;

        public:

            StdRollbackEngine(std::shared_ptr<state::StateControllable> state_controller,
                              std::shared_ptr<state::MetadataControllable> metadata_controller,
                              std::unique_ptr<state::StateSyncable> state_syncer,
                              std::unique_ptr<state::Recoverable> recoverer,
                              std::unique_ptr<state::MetadataFetchable> metadata_fetcher): state_controller(std::move(state_controller)),
                                                                                           metadata_controller(std::move(metadata_controller)),
                                                                                           state_syncer(std::move(state_syncer)),
                                                                                           recoverer(std::move(recoverer)),
                                                                                           metadata_fetcher(std::move(metadata_fetcher)){}

            void rollback(){
                
                switch (this->state_controller->get()){
                    
                    case state::anchoring:
                        std::abort(); //invalid (invoked pre-recovered)
                        return; 

                    case state::anchored:
                        return;
                    
                    case state::commiting:
                        std::abort(); ////invalid (invoked pre-recovered)
                        return;
                    
                    case state::commited:
                        this->start_rolling_back_process();
                        return;
                    
                    case state::rolling_back:
                        std::abort(); //invalid (invoked pre-recovered)
                        return;
                    
                    case state::rolled_back:
                        return; 
                    
                    default:
                        std::abort(); //invalid state
                        return;
                }
            }
        
        private:

            void start_rolling_back_process(){

                this->state_syncer->sync(state::rolling_back);
                auto new_state  = this->recoverer->recover(state::rolling_back);
                this->state_syncer->sync(new_state);
                this->state_controller->set(new_state);
                this->metadata_controller->set(this->metadata_fetcher->fetch());
            }
    };  

};

namespace dg::atomic_buffer::engine::commit{

    class CommitingDispatcher: public virtual CommitingDispatchable{

        private:

            const size_t page_sz;
            std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device;
            std::unique_ptr<page_io::StdPageWriter> page_writer;
            std::unique_ptr<state::MetadataGenerator> metadata_gen;
            std::unique_ptr<state::MetadataSerializable> metadata_serializer;
            std::unique_ptr<page_discretizer::PageDiscretizable> discretizer;
            std::unique_ptr<page_reservation::Reservable> page_reserver;
            std::unique_ptr<page::PathRetrievable> old_page_path;
            std::unique_ptr<page::PathRetrievable> cur_page_path;
            std::unique_ptr<page::PathRetrievable> shadow_page_path;

        public:

            CommitingDispatcher(size_t page_sz,
                                std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device,
                                std::unique_ptr<page_io::StdPageWriter> page_writer,
                                std::unique_ptr<state::MetadataGenerator> metadata_gen,
                                std::unique_ptr<state::MetadataSerializable> metadata_serializer,
                                std::unique_ptr<page_discretizer::PageDiscretizable> discretizer,
                                std::unique_ptr<page_reservation::Reservable> page_reserver,
                                std::unique_ptr<page::PathRetrievable> old_page_path,
                                std::unique_ptr<page::PathRetrievable> cur_page_path,
                                std::unique_ptr<page::PathRetrievable> shadow_page_path): page_sz(page_sz),
                                                                                          rename_device(std::move(rename_device)),
                                                                                          page_writer(std::move(page_writer)),
                                                                                          metadata_gen(std::move(metadata_gen)),
                                                                                          metadata_serializer(std::move(metadata_serializer)),
                                                                                          discretizer(std::move(discretizer)),
                                                                                          page_reserver(std::move(page_reserver)),
                                                                                          old_page_path(std::move(old_page_path)),
                                                                                          cur_page_path(std::move(cur_page_path)),
                                                                                          shadow_page_path(std::move(shadow_page_path)){}

            void dispatch(immutable_buffer_view buf, std::vector<page_id_type> pages){
                
                if (buf.second % this->page_sz != 0){
                    std::abort(); //SEG_FAULT
                }

                using PageUlt       = utility::PageUtility;
    
                auto page_count     = PageUlt::size(buf.second, this->page_sz);
                auto all_page_count = page_count + constants::BIN_PAGE_OFFS;
                auto b_pages        = PageUlt::shrink_pages(pages, page_count);
                auto all_pages      = PageUlt::add_metadata_page(PageUlt::relative_bin_pages_to_absolute(b_pages)); //...
                auto metadata       = this->get_serialized_metadata(buf); //neither is this
                auto discretized    = this->discretizer->get(buf.first, b_pages);
                auto write_data     = this->get_write_data(static_cast<const void *>(metadata.get()), std::move(discretized)); //not this class resposibility - future refactoring 

                this->page_reserver->reserve(all_page_count);
                this->page_writer->write(std::move(write_data));
                this->forward_snap(all_pages);
            }

        private:

            auto get_write_data(const void * metadata_buf, std::vector<std::pair<page_id_type, const void *>> discretized_bin) -> std::vector<std::pair<page_id_type, const void *>>{
                
                for (auto& e: discretized_bin){
                    e.first += constants::BIN_PAGE_OFFS; //make_room for metadata_page
                }

                discretized_bin.push_back({constants::METADATA_PAGE_IDX, metadata_buf});
                return discretized_bin;
            } 

            auto get_serialized_metadata(immutable_buffer_view buf) -> std::shared_ptr<void>{
                
                using _MemUlt       = utility::MemoryUtility;
                auto metadata       = this->metadata_gen->get(buf);
                auto serialized     = this->metadata_serializer->serialize(metadata);
                auto rs             = _MemUlt::aligned_alloc(sdio::constants::STRICTEST_BLOCK_SZ, serialized.second);
                std::memcpy(rs.get(), serialized.first.get(), serialized.second);

                return rs;
            }

            void forward_snap(const std::vector<page_id_type>& pages){
                
                auto cur_old_transform      = [&](page_id_type page_id){return std::make_pair(this->cur_page_path->to_path(page_id), this->old_page_path->to_path(page_id));};
                auto shad_cur_transform     = [&](page_id_type page_id){return std::make_pair(this->shadow_page_path->to_path(page_id), this->cur_page_path->to_path(page_id));};
                auto cur_old_pairs          = std::vector<std::pair<path_type, path_type>>(pages.size());
                auto shad_cur_pairs         = std::vector<std::pair<path_type, path_type>>(pages.size());

                std::transform(pages.begin(), pages.end(), cur_old_pairs.begin(), cur_old_transform);
                std::transform(pages.begin(), pages.end(), shad_cur_pairs.begin(), shad_cur_transform);
                this->rename_device->rename(std::move(cur_old_pairs));
                this->rename_device->rename(std::move(shad_cur_pairs));
            }
    };

    class StdCommittingEngine: public virtual CommitingEngine{

        private:

            std::shared_ptr<state::StateControllable> state_controller;
            std::shared_ptr<state::MetadataControllable> metadata_controller;
            std::unique_ptr<state::MetadataFetchable> metadata_fetcher;
            std::unique_ptr<state::StateSyncable> state_syncer;
            std::unique_ptr<page_keeper::PageObservable> page_observer;
            std::unique_ptr<CommitingDispatchable> dispatcher;
            immutable_buffer_view buf_view;

        public:

            StdCommittingEngine(std::shared_ptr<state::StateControllable> state_controller,
                                std::shared_ptr<state::MetadataControllable> metadata_controller,
                                std::unique_ptr<state::MetadataFetchable> metadata_fetcher,
                                std::unique_ptr<state::StateSyncable> state_syncer,
                                std::unique_ptr<page_keeper::PageObservable> page_observer,
                                std::unique_ptr<CommitingDispatchable> dispatcher,
                                immutable_buffer_view buf_view): state_controller(std::move(state_controller)),
                                                                 metadata_controller(std::move(metadata_controller)),
                                                                 metadata_fetcher(std::move(metadata_fetcher)),
                                                                 state_syncer(std::move(state_syncer)),
                                                                 page_observer(std::move(page_observer)),
                                                                 dispatcher(std::move(dispatcher)),
                                                                 buf_view(std::move(buf_view)){}

            void clear() noexcept{

                this->page_observer->clear();
            }

            void set_buf(const void * buf) noexcept{

                this->buf_view.first = buf;
            }

            void set_buf_size(size_t sz) noexcept{

                this->buf_view.second = sz;
            }

            void set_buf_change(size_t offs, size_t sz) noexcept{

                this->page_observer->add(offs, sz);
            }

            void commit(){
                
                if (this->state_controller->get() != state::anchored){
                    std::abort();
                }

                this->state_syncer->sync(state::commiting);
                this->dispatcher->dispatch(this->buf_view, this->page_observer->get());
                this->state_syncer->sync(state::commited);

                this->metadata_controller->set(this->metadata_fetcher->fetch());
                this->state_controller->set(state::commited);
            }
    };

}

namespace dg::atomic_buffer::engine{

    class EngineInstance: public virtual engine::Engine{

        private:

            std::unique_ptr<LoadingEngine> loader;
            std::unique_ptr<CommitingEngine> commiter;
            std::unique_ptr<RollbackEngine> rollbacker;
            std::unique_ptr<AnchoringEngine> anchorer;
        
        public:

            EngineInstance(std::unique_ptr<LoadingEngine> loader, 
                           std::unique_ptr<CommitingEngine> commiter,
                           std::unique_ptr<RollbackEngine> rollbacker,
                           std::unique_ptr<AnchoringEngine> anchorer): loader(std::move(loader)),
                                                                       commiter(std::move(commiter)),
                                                                       rollbacker(std::move(rollbacker)),
                                                                       anchorer(std::move(anchorer)){}
            
            size_t size() const noexcept{
                
                return this->loader->size();
            }

            std::unique_ptr<char[]> load(){

                return this->loader->load();
            }

            void load_into(void * buf){

                this->loader->load_into(buf);
            }

            void anchor(){

                this->anchorer->anchor();
            }

            void clear() noexcept{

                this->commiter->clear();
            }

            void set_buf(const void * buf) noexcept{

                this->commiter->set_buf(buf);
            }

            void set_buf_size(size_t sz) noexcept{

                this->commiter->set_buf_size(sz);
            }

            void set_buf_change(size_t offs, size_t sz) noexcept{

                this->commiter->set_buf_change(offs, sz);
            }

            void commit(){

                this->commiter->commit();
            }

            void rollback(){

                this->rollbacker->rollback();
            }
    };

};

namespace dg::atomic_buffer::user_interface{

    using namespace dg::atomic_buffer::types;

    struct Config{
        path_type config_dir;
        std::vector<path_type> bin_dirs;
        std::vector<double> distribution_policy;
        size_t page_sz;
        //filesys-type 
    };

    struct IODevices{
        std::shared_ptr<sdio::core::ReadingDeviceInterface> reading_device;
        std::shared_ptr<sdio::core::WritingDeviceInterface> writing_device;
        std::shared_ptr<sdio::core::DeletingDeviceInterface> del_device;
        std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device;
    };

    extern void mount(Config){

    }

    extern auto spawn(path_type config_dir, IODevices io_devices) -> std::unique_ptr<engine::Engine>{ //(required) reinstantiate on exception to recover and do state-correction (OS-bound, impossible to have noexcept reverter(destructor))

    }

};