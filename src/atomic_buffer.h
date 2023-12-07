
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

    static constexpr const char * STATE_NAME            = "ab_state";
    static constexpr const char * CONFIG_NAME           = "ab_config";
    static constexpr const char * CUR_PAGE_TRAITS       = "_curtraits";
    static constexpr const char * OLD_PAGE_TRAITS       = "_oldtraits";
    static constexpr const char * SHADOW_PAGE_TRATIS    = "_shadowtraits";
    static constexpr const char * METADATA_NAME         = "0_curtraits";
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

    class DistributorInterface{

        public:

            virtual ~DistributorInterface() noexcept{}
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
            virtual void read_into(const std::vector<std::pair<page_id_type, void *>>&) = 0;
    };

    class Writable{

        public:

            virtual ~Writable() noexcept{}
            virtual void write(const std::vector<std::pair<page_id_type, const void *>>&) = 0;
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
            virtual void dispatch(immutable_buffer_view, const std::vector<page_id_type>&) = 0;
    };

    class LoadingEngine{

        public:

            virtual ~LoadingEngine() noexcept{}
            virtual size_t size() = 0; 
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

            auto filterer   = [](page_id_type page_id){return page_id != constants::METADATA_PAGE_IDX;};
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

        static auto get_iota_pages(size_t page_count) -> std::vector<page_id_type>{

            auto rs             = std::vector<page_id_type>(page_count);
            std::iota(rs.begin(), rs.end(), 0u);

            return rs;
        }

        static constexpr auto slot(size_t buf_offs, size_t page_sz) -> page_id_type{

            return static_cast<page_id_type>(buf_offs / page_sz);
        }

        static constexpr auto size(size_t buf_sz, size_t page_sz) -> page_id_type{

            if (buf_sz == 0u){
                return page_id_type{0u};
            }

            return slot(buf_sz - 1, page_sz) + 1;
        }
    };

};

namespace dg::atomic_buffer::state{
    
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

            Metadata deserialize(const void * buf){ //REVIEW: deserialization should be g(f(x)) - to perform necessary integrity check
                
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
                this->deleting_device->del(files);

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
                    
                    this->rename_resolve(old_cur_shadow_pages);
                    return this->recover(status);               
                }

                this->old_shadow_rollback(old_shadow_pages);
                this->old_cur_rollback(old_cur_pages);
                this->remove_shadow();

                return state::anchored;
            }

        private:

            void rename_resolve(const std::vector<page_id_type>& pages){
                
                auto old_cur_res    = [&](page_id_type idx){dg::fileio::rename_resolve(this->old_page_path->to_path(idx), this->cur_page_path->to_path(idx));}; //
                auto cur_shad_res   = [&](page_id_type idx){dg::fileio::rename_resolve(this->cur_page_path->to_path(idx), this->shadow_page_path->to_path(idx));}; //

                std::for_each(pages.begin(), pages.end(), old_cur_res);
                std::for_each(pages.begin(), pages.end(), cur_shad_res);
            }

            void old_shadow_rollback(const std::vector<page_id_type>& pages){
                
                auto rename_pairs       = std::vector<std::pair<path_type, path_type>>(pages.size());
                auto transform_lambda   = [&](page_id_type idx){
                    return std::make_pair(this->old_page_path->to_path(idx), this->cur_page_path->to_path(idx));
                };
              
                std::transform(pages.begin(), pages.end(), rename_pairs.begin(), transform_lambda);
                this->rename_device->rename(rename_pairs);
            }

            void old_cur_rollback(const std::vector<page_id_type>& pages){
                
                auto rename_pairs       = std::vector<std::pair<path_type, path_type>>(pages.size());
                auto transform_lambda   = [&](page_id_type idx){
                    return std::make_pair(this->cur_page_path->to_path(idx), this->shadow_page_path->to_path(idx));
                };
              
                std::transform(pages.begin(), pages.end(), rename_pairs.begin(), transform_lambda);
                this->rename_device->rename(rename_pairs);
                this->old_shadow_rollback(pages);
            }

            void remove_shadow(){

                auto pages              = this->shadow_page_info->list();
                auto paths              = std::vector<path_type>(pages.size());
                auto transform_lambda   = [&](page_id_type idx){return this->shadow_page_path->to_path(idx);};

                std::transform(pages.begin(), pages.end(), paths.begin(), transform_lambda);
                this->del_device->del(paths);
            }
    };

    class CommitedRecoverer: public virtual Recoverable{

        public:

            status_type recover(status_type status){
                
                if (status != state::commited){
                    std::abort();
                }

                return status;
            }
    };

    class RollingBackRecoverer: public virtual Recoverable{

        private:

            std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device;
            std::unique_ptr<page::PageInfoRetrievable> old_page_info;
            std::unique_ptr<page::PathRetrievable> cur_page_path;
            std::unique_ptr<page::PathRetrievable> old_page_path;
        
        public:

            RollingBackRecoverer(std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device,
                                 std::unique_ptr<page::PageInfoRetrievable> old_page_info,
                                 std::unique_ptr<page::PathRetrievable> cur_page_path,
                                 std::unique_ptr<page::PathRetrievable> old_page_path): rename_device(std::move(rename_device)),
                                                                                        old_page_info(std::move(old_page_info)),
                                                                                        cur_page_path(std::move(cur_page_path)),
                                                                                        old_page_path(std::move(old_page_path)){}

            status_type recover(status_type status){

                if (status != state::rolling_back){
                    std::abort();
                }

                auto old_pages          = this->old_page_info->list();
                auto rename_pairs       = std::vector<std::pair<path_type, path_type>>(old_pages.size());
                auto transform_lambda   = [&](page_id_type idx){return std::make_pair(this->old_page_path->to_path(idx), this->cur_page_path->to_path(idx));}; 
                
                std::transform(old_pages.begin(), old_pages.end(), rename_pairs.begin(), transform_lambda); //
                this->rename_device->rename(rename_pairs);
                this->rm_old_files(); //avoid same link - because rename (old, new) when equivalent == no effect

                return state::rolled_back;
            }

        private:

            void rm_old_files(){

                auto old_pages      = this->old_page_info->list();
                auto each_lambda    = [&](page_id_type idx){dg::fileio::assert_rm(this->old_page_path->to_path(idx));};
                
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
            
            std::unique_ptr<Recoverable> anchoring_rcvr;
            std::unique_ptr<Recoverable> anchored_rcvr;
            std::unique_ptr<Recoverable> commiting_rcvr;
            std::unique_ptr<Recoverable> committed_rcvr;
            std::unique_ptr<Recoverable> rollingback_rcvr;
            std::unique_ptr<Recoverable> rolledback_rcvr;
        
        public:

            StdRecoverer(std::unique_ptr<Recoverable> anchoring_rcvr,
                         std::unique_ptr<Recoverable> anchored_rcvr,
                         std::unique_ptr<Recoverable> commiting_rcvr,
                         std::unique_ptr<Recoverable> committed_rcvr,
                         std::unique_ptr<Recoverable> rollingback_rcvr,
                         std::unique_ptr<Recoverable> rolledback_rcvr): anchoring_rcvr(std::move(anchoring_rcvr)),
                                                                        anchored_rcvr(std::move(anchored_rcvr)),
                                                                        commiting_rcvr(std::move(commiting_rcvr)),
                                                                        committed_rcvr(std::move(committed_rcvr)),
                                                                        rollingback_rcvr(std::move(rollingback_rcvr)),
                                                                        rolledback_rcvr(std::move(rolledback_rcvr)){}

            status_type recover(status_type status){
            
                switch (status){

                    case state::anchoring:
                        return this->anchoring_rcvr->recover(status);

                    case state::anchored:
                        return this->anchored_rcvr->recover(status);
                    
                    case state::commiting:
                        return this->commiting_rcvr->recover(status);
                    
                    case state::commited:
                        return this->committed_rcvr->recover(status);
                    
                    case state::rolling_back:
                        return this->rollingback_rcvr->recover(status);
                    
                    case state::rolled_back:
                        return this->rolledback_rcvr->recover(status);
                    
                    default:
                        std::abort(); //
                        break;
                };

                return {};
            }
    };

    struct ComponentFactory{
        
        static auto get_metadata_fetcher(std::unique_ptr<MetadataSerializable> metadata_serializer, path_type metadata_path) -> std::unique_ptr<MetadataFetchable>{

            return std::make_unique<MetadataFetcher>(std::move(metadata_serializer), std::move(metadata_path));
        }

        static auto get_metadata_generator(std::unique_ptr<checksum::Hashable> hasher) -> std::unique_ptr<MetadataGeneratable>{

            return std::make_unique<MetadataGenerator>(std::move(hasher));
        } 

        static auto get_metadata_serializer(const size_t page_sz) -> std::unique_ptr<MetadataSerializable>{

            return std::make_unique<StdMetadataSerializer>(page_sz);
        }

        static auto get_state_syncer(path_type state_path) -> std::unique_ptr<StateSyncable>{

            return std::make_unique<StateSyncer>(std::move(state_path));
        } 

        static auto get_state_fetcher(path_type state_path) -> std::unique_ptr<StateFetchable>{

            return std::make_unique<StateFetcher>(std::move(state_path));
        }

        static auto get_anchoring_recoverer(std::shared_ptr<sdio::core::DeletingDeviceInterface> deleting_device, 
                                            std::unique_ptr<page::PageInfoRetrievable> old_page_info,
                                            std::unique_ptr<page::PathRetrievable> old_page_path) -> std::unique_ptr<Recoverable>{
            
            return std::make_unique<AnchoringRecoverer>(std::move(deleting_device), std::move(old_page_info), std::move(old_page_path));
        }

        static auto get_anchorerd_recoverer() -> std::unique_ptr<Recoverable>{

            return std::make_unique<AnchoredRecoverer>();
        }

        static auto get_commiting_recoverer(std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device,
                                            std::shared_ptr<sdio::core::DeletingDeviceInterface> del_device,
                                            std::unique_ptr<page::PageInfoRetrievable> old_page_info,
                                            std::unique_ptr<page::PageInfoRetrievable> cur_page_info,
                                            std::unique_ptr<page::PageInfoRetrievable> shadow_page_info,
                                            std::unique_ptr<page::PathRetrievable> old_page_path,
                                            std::unique_ptr<page::PathRetrievable> cur_page_path,
                                            std::unique_ptr<page::PathRetrievable> shadow_page_path) -> std::unique_ptr<Recoverable>{
            
            return std::make_unique<CommitingRecoverer>(std::move(rename_device), std::move(del_device), std::move(old_page_info), 
                                                        std::move(cur_page_info), std::move(shadow_page_info), std::move(old_page_path),
                                                        std::move(cur_page_path), std::move(shadow_page_path));
        }

        static auto get_commited_recoverer() -> std::unique_ptr<Recoverable>{

            return std::make_unique<CommitedRecoverer>();
        }

        static auto get_rollingback_recoverer(std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device,
                                              std::unique_ptr<page::PageInfoRetrievable> old_page_info,
                                              std::unique_ptr<page::PathRetrievable> cur_page_path,
                                              std::unique_ptr<page::PathRetrievable> old_page_path) -> std::unique_ptr<Recoverable>{
            
            return std::make_unique<RollingBackRecoverer>(std::move(rename_device), std::move(old_page_info), std::move(cur_page_path), std::move(old_page_path));
        }

        static auto get_rolledback_recoverer() -> std::unique_ptr<Recoverable>{

            return std::make_unique<RolledBackRecoverer>();
        }

        static auto get_std_recoverer(std::unique_ptr<Recoverable> anchoring_rcvr,
                                      std::unique_ptr<Recoverable> anchored_rcvr,
                                      std::unique_ptr<Recoverable> commiting_rcvr,
                                      std::unique_ptr<Recoverable> commited_rcvr, 
                                      std::unique_ptr<Recoverable> rollingback_rcvr,
                                      std::unique_ptr<Recoverable> rolledback_rcvr) -> std::unique_ptr<Recoverable>{
            
            return std::make_unique<StdRecoverer>(std::move(anchoring_rcvr), std::move(anchored_rcvr), 
                                                 std::move(commiting_rcvr), std::move(commited_rcvr),
                                                 std::move(rollingback_rcvr), std::move(rolledback_rcvr));
        }
    };

}

namespace dg::atomic_buffer::page{

    using namespace dg::atomic_buffer::types;

    class PathBase{

        protected:

            static_assert(std::is_unsigned_v<page_id_type>);

            path_type to_path(const path_type& dir, page_id_type id, const char * traits){ //precond - traits must not start with digits

                auto fname      = std::to_string(id) + traits; 
                auto fullname   = path_type(fname).replace_extension(constants::EXTENSION);
                auto fpath      = dir / fullname;

                return fpath;
            }

            page_id_type to_id(const path_type& path){
                
                using val_type          = path_type::value_type;
                constexpr val_type MMIN = '0';
                constexpr val_type MMAX = '9';

                auto nis_digit          = [=](const val_type& cur){return std::clamp(cur, MMIN, MMAX) != cur;};
                auto fname              = path.filename().native();
                auto fdigit             = decltype(fname)();
                auto pos                = std::find_if(fname.begin(), fname.end(), nis_digit);

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

                return PathBase::to_path(this->dir, id, this->traits);
            }

            page_id_type to_id(const path_type& path){

                return PathBase::to_id(path);
            }
    };

    class DistributedPathRetriever: public virtual PathRetrievable,
                                    private PathBase{

        private:

            std::unordered_map<size_t, std::unique_ptr<PathRetrievable>> lookup_map;
            std::unique_ptr<DistributorInterface> distributor;
        
        public:

            DistributedPathRetriever(std::unordered_map<size_t, std::unique_ptr<PathRetrievable>> lookup_map,
                                     std::unique_ptr<DistributorInterface> distributor): lookup_map(std::move(lookup_map)),
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
                auto filterer           = [&](const path_type& path){return path.native().find(this->traits) != std::string::npos;}; //
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

    struct ComponentFactory{

        static auto get_std_path_retriever(path_type dir, const char * traits) -> std::unique_ptr<PathRetrievable>{

            return std::make_unique<StdPathRetriever>(std::move(dir), traits);
        }

        static auto get_distributed_path_retriever(std::unordered_map<size_t, std::unique_ptr<PathRetrievable>> lookup_map,
                                                   std::unique_ptr<DistributorInterface> distributor) -> std::unique_ptr<PathRetrievable>{
            
            return std::make_unique<DistributedPathRetriever>(std::move(lookup_map), std::move(distributor));
        }

        static auto get_std_page_info_retriever(std::vector<path_type> dirs, 
                                                const char * traits, 
                                                size_t page_sz) -> std::unique_ptr<PageInfoRetrievable>{
            
            return std::make_unique<StdPageInfoRetriever>(std::move(dirs), traits, page_sz);
        }

    };

    struct DistributedPathRetrieverMaker{

        static auto make(std::vector<path_type> dirs, const char * traits, std::vector<double> distribution) -> std::unique_ptr<PathRetrievable>{ //

            return {};
        }
    };

}

namespace dg::atomic_buffer::page_io{

    class StdPageReader: public virtual Readable{

        private:

            std::shared_ptr<sdio::core::ReadingDeviceInterface> reading_device;
            std::unique_ptr<page::PathRetrievable> path_retriever;
            const size_t page_sz;

        public:

            StdPageReader(std::shared_ptr<sdio::core::ReadingDeviceInterface> reading_device,
                          std::unique_ptr<page::PathRetrievable> path_retriever,
                          size_t page_sz): reading_device(std::move(reading_device)),
                                           path_retriever(std::move(path_retriever)),
                                           page_sz(page_sz){}
            

            void read_into(const std::vector<std::pair<page_id_type, void *>>& data){
                
                auto read_arg           = std::vector<std::tuple<path_type, void *, size_t>>(data.size());
                auto transform_lambda   = [&](const std::pair<page_id_type, void *>& cur){
                    return std::make_tuple(this->path_retriever->to_path(cur.first), cur.second, this->page_sz);
                };  

                std::transform(data.begin(), data.end(), read_arg.begin(), transform_lambda);
                this->reading_device->read_into(read_arg); 
            }   
    };

    class StdPageWriter: public virtual Writable{

        private:

            std::shared_ptr<sdio::core::WritingDeviceInterface> writing_device;
            std::unique_ptr<page::PathRetrievable> path_retriever;
            const size_t page_sz;

        public:

            StdPageWriter(std::shared_ptr<sdio::core::WritingDeviceInterface> writing_device,
                          std::unique_ptr<page::PathRetrievable> path_retriever,
                          size_t page_sz): writing_device(std::move(writing_device)),
                                           path_retriever(std::move(path_retriever)),
                                           page_sz(page_sz){}

            void write(const std::vector<std::pair<page_id_type, const void *>>& data){
                
                auto write_arg          = std::vector<std::tuple<path_type, const void *, size_t>>(data.size());
                auto transform_lambda   = [&](const std::pair<page_id_type, const void *>& cur){
                    return std::make_tuple(this->path_retriever->to_path(cur.first), cur.second, this->page_sz);
                };

                std::transform(data.begin(), data.end(), write_arg.begin(), transform_lambda);
                this->writing_device->write(write_arg);
            }
    };

    struct ComponentFactory{

        static auto get_std_page_reader(std::shared_ptr<sdio::core::ReadingDeviceInterface> reading_device, 
                                        std::unique_ptr<page::PathRetrievable> path_retriever,
                                        size_t page_sz) -> std::unique_ptr<Readable>{
            
            return std::make_unique<StdPageReader>(std::move(reading_device), std::move(path_retriever), page_sz);
        }

        static auto get_std_page_writer(std::shared_ptr<sdio::core::WritingDeviceInterface> writing_device, 
                                        std::unique_ptr<page::PathRetrievable> path_retriever,
                                        size_t page_sz) -> std::unique_ptr<Writable>{
            
            return std::make_unique<StdPageWriter>(std::move(writing_device), std::move(path_retriever), page_sz);
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

            void reserve(size_t page_count){

                using _MemUlt   = utility::MemoryUtility; 

                auto cur_pages  = this->cur_page_info->list();                
                auto seq_pages  = std::vector<page_id_type>(page_count);
                auto diff       = std::vector<page_id_type>();
                auto write_data = std::vector<std::pair<page_id_type, const void *>>();
                auto emp_buf    = _MemUlt::aligned_empty_alloc(dg::sdio::constants::STRICTEST_BLOCK_SZ, this->cur_page_info->page_size());

                std::iota(seq_pages.begin(), seq_pages.end(), 0u);
                std::sort(cur_pages.begin(), cur_pages.end());
                std::set_difference(seq_pages.begin(), seq_pages.end(), cur_pages.begin(), cur_pages.end(), std::back_inserter(diff));
                std::transform(diff.begin(), diff.end(), std::back_inserter(write_data), [&](page_id_type id){return std::make_pair(id, static_cast<const void *>(emp_buf.get()));}); //read only - no cache problem
                this->page_writer->write(write_data);
            }
    };
    
    class StdShrinker: public virtual Shrinkable{

        private:

            std::shared_ptr<sdio::core::DeletingDeviceInterface> deleting_device;
            std::unique_ptr<page::PageInfoRetrievable> cur_page_info;
            std::unique_ptr<page::PathRetrievable> page_path;
        
        public:

            StdShrinker(std::shared_ptr<sdio::core::DeletingDeviceInterface> deleting_device,
                        std::unique_ptr<page::PageInfoRetrievable> cur_page_info,
                        std::unique_ptr<page::PathRetrievable> page_path): deleting_device(std::move(deleting_device)),
                                                                           cur_page_info(std::move(cur_page_info)),
                                                                           page_path(std::move(page_path)){}

            void shrink(size_t page_count){
                
                auto all_pages  = this->cur_page_info->list();
                auto rm_files   = std::vector<path_type>();
                auto last       = std::copy_if(all_pages.begin(), all_pages.end(), all_pages.begin(), [=](page_id_type idx){return idx >= page_count;});
                
                std::transform(all_pages.begin(), last, std::back_inserter(rm_files), [&](page_id_type idx){return this->page_path->to_path(idx);});
                this->deleting_device->del(rm_files);
            }
    };

    struct ComponentFactory{

        static auto get_std_reserver(std::unique_ptr<page::PageInfoRetrievable> cur_page_info, 
                                     std::unique_ptr<page_io::Writable> page_writer) -> std::unique_ptr<Reservable>{
            
            return std::make_unique<StdReserver>(std::move(cur_page_info), std::move(page_writer));
        } 

        static auto get_std_shrinker(std::shared_ptr<sdio::core::DeletingDeviceInterface> deleting_device,
                                     std::unique_ptr<page::PageInfoRetrievable> cur_page_info,
                                     std::unique_ptr<page::PathRetrievable> cur_page_path) -> std::unique_ptr<Shrinkable>{
            
            return std::make_unique<StdShrinker>(std::move(deleting_device), std::move(cur_page_info), std::move(cur_page_path));
        }
    };

};

namespace dg::atomic_buffer::checksum{

    using namespace dg::atomic_buffer::types;

    class Hasher: public virtual Hashable{

        public:

            checksum_type get(immutable_buffer_view buf){
                
                using _MemIO    = utility::SyncedEndiannessService; 
                using _MemUlt   = utility::MemoryUtility;

                const auto MOD  = std::numeric_limits<checksum_type>::max() >> 1;
                auto ibuf       = buf.first;
                auto rs         = checksum_type{};
                auto tmp        = checksum_type{};
                auto cycles     = static_cast<size_t>(buf.second / sizeof(checksum_type));

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

    struct ComponentFactory{

        static auto get_std_hasher() -> std::unique_ptr<Hashable>{

            return std::make_unique<Hasher>();
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
                
                if (sz == 0u){
                    return;
                }

                using _PageUlt  = utility::PageUtility;
                size_t beg      = _PageUlt::slot(offset, this->page_sz);
                size_t last     = _PageUlt::size(offset + sz, this->page_sz);

                for (size_t i = beg; i < last; ++i){
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

    struct ComponentFactory{

        static auto get_std_page_observer(size_t max_page_count, size_t page_sz) -> std::unique_ptr<PageObservable>{

            auto bfilter    = std::vector<bool>();
            bfilter.resize(max_page_count, false);          

            return std::make_unique<StdPageObserver>(std::move(bfilter), page_sz);
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
                auto rs             = std::vector<const_discretized_type>(page_ids.size());
                auto discretizer    = [&](page_id_type idx) -> const_discretized_type{return {idx, _MemUlt::forward_shift(buf, this->page_size * idx)};};

                std::transform(page_ids.begin(), page_ids.end(), rs.begin(), discretizer);

                return rs;
            }

            std::vector<discretized_type> get(void * buf, const std::vector<page_id_type>& page_ids){

                using _MemUlt       = utility::MemoryUtility;
                auto rs             = std::vector<discretized_type>(page_ids.size());
                auto discretizer    = [&](page_id_type idx) -> discretized_type{return {idx, _MemUlt::forward_shift(buf, this->page_size * idx)};};

                std::transform(page_ids.begin(), page_ids.end(), rs.begin(), discretizer);

                return rs; 
            }
    };

    struct ComponentFactory{

        static auto get_std_page_discretizer(size_t page_sz) -> std::unique_ptr<PageDiscretizable>{

            return std::make_unique<StdPageDiscretizer>(page_sz);
        }
    };

};

namespace dg::atomic_buffer::engine{

    class StdAnchoringEngine: public virtual AnchoringEngine{

        private:

            std::unique_ptr<state::StateSyncable> state_syncer;
            std::unique_ptr<state::Recoverable> recoverer;
            std::unique_ptr<state::StateFetchable> state_fetcher;

        public:

            StdAnchoringEngine(std::unique_ptr<state::StateSyncable> state_syncer,
                               std::unique_ptr<state::Recoverable> recoverer,
                               std::unique_ptr<state::StateFetchable> state_fetcher): state_syncer(std::move(state_syncer)),
                                                                                      recoverer(std::move(recoverer)),
                                                                                      state_fetcher(std::move(state_fetcher)){}

            void anchor(){
                
                switch (this->state_fetcher->fetch()){
                    
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
            }
    };

    class StdLoadingEngine: public virtual LoadingEngine{

        private:

            std::unique_ptr<state::MetadataFetchable> metadata_fetcher;
            std::unique_ptr<checksum::Hashable> hasher;
            std::unique_ptr<page_discretizer::PageDiscretizable> page_discretizer;
            std::unique_ptr<page::PageInfoRetrievable> page_info_retriever; //
            std::unique_ptr<page_io::Readable> page_reader;

        public:

            StdLoadingEngine(std::unique_ptr<state::MetadataFetchable> metadata_fetcher,
                             std::unique_ptr<checksum::Hashable> hasher,
                             std::unique_ptr<page_discretizer::PageDiscretizable> page_discretizer,
                             std::unique_ptr<page::PageInfoRetrievable> page_info_retriever,
                             std::unique_ptr<page_io::Readable> page_reader): metadata_fetcher(std::move(metadata_fetcher)),
                                                                              hasher(std::move(hasher)),
                                                                              page_discretizer(std::move(page_discretizer)),
                                                                              page_info_retriever(std::move(page_info_retriever)),
                                                                              page_reader(std::move(page_reader)){}
            
            size_t size(){
                
                return this->metadata_fetcher->fetch().buf_sz;
            }

            std::unique_ptr<char[]> load(){

                auto rs = std::unique_ptr<char[]>(new char[this->size()]);
                this->load_into(static_cast<void *>(rs.get()));

                return rs;
            }

            void load_into(void * buf){
                
                if (this->size() % this->page_info_retriever->page_size() != 0){
                    std::abort(); //segfault
                }

                using _PageUlt          = utility::PageUtility;
                auto view               = buffer_view{buf, this->size()};
                auto page_count         = _PageUlt::size(size(), this->page_info_retriever->page_size());
                auto bin_pages          = _PageUlt::get_iota_pages(page_count);
                auto discretized        = this->page_discretizer->get(buf, bin_pages);

                this->page_reader->read_into(this->get_read_arg(std::move(discretized))); //

                if (this->hasher->get(view) != this->metadata_fetcher->fetch().checksum){
                    throw runtime_exception::CorruptedError();
                }
            }
        
        private:

            auto get_read_arg(std::vector<std::pair<size_t, void *>> data) -> std::vector<std::pair<size_t, void *>>{

                for (auto& e: data){
                    e.first += constants::BIN_PAGE_OFFS;
                }

                return data;
            }
    };

    class StdRollbackEngine: public virtual RollbackEngine{

        private:

            std::unique_ptr<state::StateFetchable> state_fetcher;
            std::unique_ptr<state::StateSyncable> state_syncer;
            std::unique_ptr<state::Recoverable> recoverer;

        public:

            StdRollbackEngine(std::unique_ptr<state::StateFetchable> state_fetcher,
                              std::unique_ptr<state::StateSyncable> state_syncer,
                              std::unique_ptr<state::Recoverable> recoverer): state_fetcher(std::move(state_fetcher)),
                                                                              state_syncer(std::move(state_syncer)),
                                                                              recoverer(std::move(recoverer)){}

            void rollback(){
                
                switch (this->state_fetcher->fetch()){
                    
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
            }
    }; 

    class CommitingDispatcher: public virtual CommitingDispatchable{

        private:

            const size_t page_sz;
            std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device;
            std::unique_ptr<state::MetadataFetchable> metadata_fetcher;
            std::unique_ptr<page_io::Writable> page_writer;
            std::unique_ptr<state::MetadataGeneratable> metadata_gen;
            std::unique_ptr<state::MetadataSerializable> metadata_serializer;
            std::unique_ptr<page_discretizer::PageDiscretizable> discretizer;
            std::unique_ptr<page_reservation::Reservable> reserver;
            std::unique_ptr<page::PathRetrievable> old_path_retriever;
            std::unique_ptr<page::PathRetrievable> cur_path_retriever;
            std::unique_ptr<page::PathRetrievable> shadow_path_retriever;

        public:

            CommitingDispatcher(size_t page_sz,
                                std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device,
                                std::unique_ptr<state::MetadataFetchable> metadata_fetcher,
                                std::unique_ptr<page_io::Writable> page_writer,
                                std::unique_ptr<state::MetadataGeneratable> metadata_gen,
                                std::unique_ptr<state::MetadataSerializable> metadata_serializer,
                                std::unique_ptr<page_discretizer::PageDiscretizable> discretizer,
                                std::unique_ptr<page_reservation::Reservable> reserver,
                                std::unique_ptr<page::PathRetrievable> old_path_retriever,
                                std::unique_ptr<page::PathRetrievable> cur_path_retriever,
                                std::unique_ptr<page::PathRetrievable> shadow_path_retriever): page_sz(page_sz),
                                                                                               rename_device(std::move(rename_device)),
                                                                                               metadata_fetcher(std::move(metadata_fetcher)),
                                                                                               page_writer(std::move(page_writer)),
                                                                                               metadata_gen(std::move(metadata_gen)),
                                                                                               metadata_serializer(std::move(metadata_serializer)),
                                                                                               discretizer(std::move(discretizer)),
                                                                                               reserver(std::move(reserver)),
                                                                                               old_path_retriever(std::move(old_path_retriever)),
                                                                                               cur_path_retriever(std::move(cur_path_retriever)),
                                                                                               shadow_path_retriever(std::move(shadow_path_retriever)){}

            void dispatch(immutable_buffer_view buf, const std::vector<page_id_type>& pages){
                
                if (buf.second % this->page_sz != 0u){
                    std::abort(); //SEG_FAULT
                }

                using PageUlt       = utility::PageUtility;
                using _MemUlt       = utility::MemoryUtility;

                auto org_buf_sz     = this->metadata_fetcher->fetch().buf_sz; 
                auto empty_buf      = _MemUlt::aligned_empty_alloc(sdio::constants::STRICTEST_BLOCK_SZ, this->page_sz);
                auto page_count     = PageUlt::size(buf.second, this->page_sz);
                auto all_page_count = page_count + constants::BIN_PAGE_OFFS;
                auto b_pages        = PageUlt::shrink_pages(pages, page_count);
                auto metadata       = this->get_serialized_metadata(buf); 
                auto discretized    = this->discretizer->get(buf.first, b_pages);
                auto wo_pages       = this->get_whiteouted_pages(buf.second, org_buf_sz);
                auto _discretized   = this->add_empty_pages(std::move(discretized), wo_pages, static_cast<const void *>(empty_buf.get())); 
                auto write_data     = this->get_write_data(static_cast<const void *>(metadata.get()), std::move(_discretized));

                auto snap_pages     = b_pages;
                snap_pages.insert(snap_pages.end(), wo_pages.begin(), wo_pages.end());
                snap_pages          = PageUlt::add_metadata_page(PageUlt::relative_bin_pages_to_absolute(std::move(snap_pages)));

                this->reserver->reserve(all_page_count);
                this->page_writer->write(write_data);
                this->forward_snap(snap_pages); 
            }

        private:

            auto get_whiteouted_pages(size_t new_buf_sz, size_t old_buf_sz) -> std::vector<page_id_type>{

                using PageUlt           = utility::PageUtility;
                size_t new_page_count   = PageUlt::size(new_buf_sz, this->page_sz);
                size_t old_page_count   = PageUlt::size(old_buf_sz, this->page_sz);

                if (new_page_count >= old_page_count){
                    return {};
                } 

                size_t delta    = old_page_count - new_page_count;
                auto rs         = std::vector<page_id_type>(delta);
                std::iota(rs.begin(), rs.end(), new_page_count);

                return rs;
            }

            auto add_empty_pages(std::vector<std::pair<page_id_type, const void *>> discretized, const std::vector<page_id_type>& wo_pages, const void * empty_buf) -> std::vector<std::pair<page_id_type, const void *>>{
                
                auto transform_lambda    = [=](page_id_type page_id){return std::make_pair(page_id, empty_buf);};
                std::transform(wo_pages.begin(), wo_pages.end(), std::back_inserter(discretized), transform_lambda);

                return discretized;
            } 

            auto get_write_data(const void * metadata_buf, std::vector<std::pair<page_id_type, const void *>> discretized_bin) -> std::vector<std::pair<page_id_type, const void *>>{
                
                for (auto& e: discretized_bin){
                    e.first += constants::BIN_PAGE_OFFS; //make_room for metadata_page
                }

                auto metadata_bucket    = std::pair<page_id_type, const void *>{constants::METADATA_PAGE_IDX, metadata_buf};
                discretized_bin.push_back(metadata_bucket);
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
                
                auto cur_old_transform      = [&](page_id_type page_id){return std::make_pair(this->cur_path_retriever->to_path(page_id), this->old_path_retriever->to_path(page_id));};
                auto shad_cur_transform     = [&](page_id_type page_id){return std::make_pair(this->shadow_path_retriever->to_path(page_id), this->cur_path_retriever->to_path(page_id));};
                auto cur_old_pairs          = std::vector<std::pair<path_type, path_type>>(pages.size());
                auto shad_cur_pairs         = std::vector<std::pair<path_type, path_type>>(pages.size());

                std::transform(pages.begin(), pages.end(), cur_old_pairs.begin(), cur_old_transform);
                std::transform(pages.begin(), pages.end(), shad_cur_pairs.begin(), shad_cur_transform);
                this->rename_device->rename(cur_old_pairs);
                this->rename_device->rename(shad_cur_pairs);
            }
    };

    class StdCommittingEngine: public virtual CommitingEngine{

        private:

            std::unique_ptr<state::StateFetchable> state_fetcher;
            std::unique_ptr<state::StateSyncable> state_syncer;
            std::unique_ptr<page_keeper::PageObservable> page_observer;
            std::unique_ptr<CommitingDispatchable> dispatcher;
            immutable_buffer_view buf_view;

        public:

            StdCommittingEngine(std::unique_ptr<state::StateFetchable> state_fetcher,
                                std::unique_ptr<state::StateSyncable> state_syncer,
                                std::unique_ptr<page_keeper::PageObservable> page_observer,
                                std::unique_ptr<CommitingDispatchable> dispatcher,
                                immutable_buffer_view buf_view): state_fetcher(std::move(state_fetcher)),
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
                
                if (this->state_fetcher->fetch() != state::anchored){
                    std::abort(); //log
                }

                this->state_syncer->sync(state::commiting);
                this->dispatcher->dispatch(this->buf_view, this->page_observer->get());
                this->state_syncer->sync(state::commited);
            }
    };

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
            
            size_t size(){
                
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

    struct ComponentFactory{

        static auto get_std_anchoring_engine(std::unique_ptr<state::StateSyncable> state_syncer,
                                             std::unique_ptr<state::Recoverable> recoverer,
                                             std::unique_ptr<state::StateFetchable> state_fetcher) -> std::unique_ptr<AnchoringEngine>{
            
            return std::make_unique<StdAnchoringEngine>(std::move(state_syncer), std::move(recoverer), std::move(state_fetcher));
        }

        static auto get_std_loading_engine(std::unique_ptr<state::MetadataFetchable> metadata_fetcher,
                                           std::unique_ptr<checksum::Hashable> hasher,
                                           std::unique_ptr<page_discretizer::PageDiscretizable> page_discretizer,
                                           std::unique_ptr<page::PageInfoRetrievable> page_info_retriever,
                                           std::unique_ptr<page_io::Readable> page_reader) -> std::unique_ptr<LoadingEngine>{
            
            return std::make_unique<StdLoadingEngine>(std::move(metadata_fetcher), std::move(hasher), std::move(page_discretizer), 
                                                      std::move(page_info_retriever), std::move(page_reader));
        }

        static auto get_std_rollback_engine(std::unique_ptr<state::StateFetchable> state_fetcher,
                                            std::unique_ptr<state::StateSyncable> state_syncer, 
                                            std::unique_ptr<state::Recoverable> recoverer) -> std::unique_ptr<RollbackEngine>{
            
            return std::make_unique<StdRollbackEngine>(std::move(state_fetcher), std::move(state_syncer), std::move(recoverer));
        }

        static auto get_commiting_dispatcher(size_t page_sz,
                                             std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device,
                                             std::unique_ptr<state::MetadataFetchable> metadata_fetcher,
                                             std::unique_ptr<page_io::Writable> page_writer,
                                             std::unique_ptr<state::MetadataGeneratable> metadata_gen,
                                             std::unique_ptr<state::MetadataSerializable> metadata_serializer,
                                             std::unique_ptr<page_discretizer::PageDiscretizable> discretizer,
                                             std::unique_ptr<page_reservation::Reservable> reserver,
                                             std::unique_ptr<page::PathRetrievable> old_path_retriever,
                                             std::unique_ptr<page::PathRetrievable> cur_path_retriever,
                                             std::unique_ptr<page::PathRetrievable> shadow_path_retriever) -> std::unique_ptr<CommitingDispatchable>{
            
            return std::make_unique<CommitingDispatcher>(page_sz, std::move(rename_device), std::move(metadata_fetcher), 
                                                         std::move(page_writer), std::move(metadata_gen), std::move(metadata_serializer), 
                                                         std::move(discretizer), std::move(reserver), std::move(old_path_retriever),
                                                         std::move(cur_path_retriever), std::move(shadow_path_retriever));
        }

        static auto get_std_commiting_engine(std::unique_ptr<state::StateFetchable> state_fetcher,
                                             std::unique_ptr<state::StateSyncable> state_syncer,
                                             std::unique_ptr<page_keeper::PageObservable> page_observer,
                                             std::unique_ptr<CommitingDispatchable> dispatcher) -> std::unique_ptr<CommitingEngine>{
            
            return std::make_unique<StdCommittingEngine>(std::move(state_fetcher), std::move(state_syncer),
                                                         std::move(page_observer), std::move(dispatcher), immutable_buffer_view{});
        }

        static auto get_engine(std::unique_ptr<LoadingEngine> loader, 
                               std::unique_ptr<CommitingEngine> commiter, 
                               std::unique_ptr<RollbackEngine> rollbacker,
                               std::unique_ptr<AnchoringEngine> anchorer) -> std::unique_ptr<Engine>{
            
            return std::make_unique<EngineInstance>(std::move(loader), std::move(commiter), std::move(rollbacker), std::move(anchorer));
        }
    };

}

namespace dg::atomic_buffer::resources{

    using namespace dg::atomic_buffer::types;

    struct BaseConfig{
        path_type dir;
        size_t page_sz;
        size_t max_page_count;
    };

    struct IODevices{
        std::shared_ptr<sdio::core::ReadingDeviceInterface> reading_device;
        std::shared_ptr<sdio::core::WritingDeviceInterface> writing_device;
        std::shared_ptr<sdio::core::DeletingDeviceInterface> del_device;
        std::shared_ptr<sdio::core::RenamingDeviceInterface> rename_device;
    };

    struct Config: BaseConfig, IODevices{};

    struct Utility{

        static auto get_config_path(const path_type& dir){

            return (dir / constants::CONFIG_NAME).replace_extension(constants::EXTENSION);
        }

        static auto get_state_path(const path_type& dir){

            return (dir / constants::STATE_NAME).replace_extension(constants::EXTENSION);;
        }   

        static auto get_metadata_path(const path_type& dir){

            return (dir / constants::METADATA_NAME).replace_extension(constants::EXTENSION);
        }
    };
    
    struct StateMounter{

        static void mount_state(BaseConfig config){
            
            state::ComponentFactory::get_state_syncer(Utility::get_state_path(config.dir))->sync(state::anchored);
        }
        
        static void mount_metadata(BaseConfig config){

            //bad -- 
            auto metadata_gen           = state::ComponentFactory::get_metadata_generator(checksum::ComponentFactory::get_std_hasher()); //REVIEW: assumption use std_hasher 
            auto metadata_serializer    = state::ComponentFactory::get_metadata_serializer(config.page_sz); //REVIEW: assumption --
            auto metadata               = metadata_gen->get(immutable_buffer_view{});
            auto serialized             = metadata_serializer->serialize(metadata);

            dg::fileio::atomic_bwrite(Utility::get_metadata_path(config.dir), static_cast<const void *>(serialized.first.get()), serialized.second); //wrong - 
        }

    };

    struct ResourceInitializer{

        static void clear_n_mkdirs(BaseConfig config){

            auto dirs   = std::vector<path_type>();
            // auto dirs   = config.bin_dirs;
            dirs.push_back(config.dir);

            std::for_each(dirs.begin(), dirs.end(), dg::fileio::mkdir);
            std::for_each(dirs.begin(), dirs.end(), static_cast<uintmax_t (*)(const path_type&)>(std::filesystem::remove_all));
            std::for_each(dirs.begin(), dirs.end(), dg::fileio::mkdir);
        }

        static void initialize(BaseConfig config){

            StateMounter::mount_metadata(config);
            StateMounter::mount_state(config);
        } 

        static void defaultize(BaseConfig config){

            clear_n_mkdirs(config);
            initialize(config);
        }
    };

    struct ResourceSpawner{

        using _Ult  = Utility;
 
        static auto get_hasher(Config config) -> std::unique_ptr<checksum::Hashable>{

            return checksum::ComponentFactory::get_std_hasher();
        }

        static auto get_metadata_generator(Config config) -> std::unique_ptr<state::MetadataGeneratable>{

            return state::ComponentFactory::get_metadata_generator(get_hasher(config));
        } 

        static auto get_state_syncer(Config config) -> std::unique_ptr<state::StateSyncable>{

            return state::ComponentFactory::get_state_syncer(_Ult::get_state_path(config.dir));
        }

        static auto get_state_fetcher(Config config) -> std::unique_ptr<state::StateFetchable>{
            
            return state::ComponentFactory::get_state_fetcher(_Ult::get_state_path(config.dir)); 
        } 

        static auto get_metadata_serializer(Config config) -> std::unique_ptr<state::MetadataSerializable>{

            return state::ComponentFactory::get_metadata_serializer(config.page_sz);
        } 

        static auto get_metadata_fetcher(Config config) -> std::unique_ptr<state::MetadataFetchable>{

            return state::ComponentFactory::get_metadata_fetcher(get_metadata_serializer(config), 
                                                                 _Ult::get_metadata_path(config.dir));
        }

        static auto get_old_info_retriever(Config config) -> std::unique_ptr<page::PageInfoRetrievable>{

            return page::ComponentFactory::get_std_page_info_retriever({config.dir}, constants::OLD_PAGE_TRAITS, config.page_sz);
        } 

        static auto get_cur_info_retriever(Config config) -> std::unique_ptr<page::PageInfoRetrievable>{

            return page::ComponentFactory::get_std_page_info_retriever({config.dir}, constants::CUR_PAGE_TRAITS, config.page_sz);
        }

        static auto get_shadow_info_retriever(Config config) -> std::unique_ptr<page::PageInfoRetrievable>{

            return page::ComponentFactory::get_std_page_info_retriever({config.dir}, constants::SHADOW_PAGE_TRATIS, config.page_sz);
        } 

        static auto get_old_path_retriever(Config config) -> std::unique_ptr<page::PathRetrievable>{
            
            return page::ComponentFactory::get_std_path_retriever(config.dir, constants::OLD_PAGE_TRAITS);
        }

        static auto get_cur_path_retriever(Config config) -> std::unique_ptr<page::PathRetrievable>{

            return page::ComponentFactory::get_std_path_retriever(config.dir, constants::CUR_PAGE_TRAITS);
        }

        static auto get_shadow_path_retriever(Config config) -> std::unique_ptr<page::PathRetrievable>{

            return page::ComponentFactory::get_std_path_retriever(config.dir, constants::SHADOW_PAGE_TRATIS);
        }

        static auto get_anchoring_recoverer(Config config) -> std::unique_ptr<state::Recoverable>{

            return state::ComponentFactory::get_anchoring_recoverer(config.del_device, get_old_info_retriever(config), get_old_path_retriever(config));
        }

        static auto get_anchorerd_recoverer(Config config) -> std::unique_ptr<state::Recoverable>{

            return state::ComponentFactory::get_anchorerd_recoverer();
        }

        static auto get_commiting_recoverer(Config config) -> std::unique_ptr<state::Recoverable>{

            return state::ComponentFactory::get_commiting_recoverer(config.rename_device, config.del_device, get_old_info_retriever(config), 
                                                                    get_cur_info_retriever(config), get_shadow_info_retriever(config),
                                                                    get_old_path_retriever(config), get_cur_path_retriever(config),
                                                                    get_shadow_path_retriever(config));
        }

        static auto get_commited_recoverer(Config config) -> std::unique_ptr<state::Recoverable>{

            return state::ComponentFactory::get_commited_recoverer();
        } 

        static auto get_rollingback_recoverer(Config config) -> std::unique_ptr<state::Recoverable>{

            return state::ComponentFactory::get_rollingback_recoverer(config.rename_device, get_old_info_retriever(config), get_cur_path_retriever(config), 
                                                                      get_old_path_retriever(config));
        }

        static auto get_rolledback_recoverer(Config config) -> std::unique_ptr<state::Recoverable>{

            return state::ComponentFactory::get_rolledback_recoverer();
        } 

        static auto get_std_recoverer(Config config) -> std::unique_ptr<state::Recoverable>{

            return state::ComponentFactory::get_std_recoverer(get_anchoring_recoverer(config), get_anchorerd_recoverer(config),
                                                              get_commiting_recoverer(config), get_commited_recoverer(config), 
                                                              get_rollingback_recoverer(config), get_rolledback_recoverer(config));    
        }
        
        static auto get_page_reader(Config config) -> std::unique_ptr<page_io::Readable>{

            return page_io::ComponentFactory::get_std_page_reader(config.reading_device, get_cur_path_retriever(config), config.page_sz);
        }

        static auto get_shadow_page_writer(Config config) -> std::unique_ptr<page_io::Writable>{

            return page_io::ComponentFactory::get_std_page_writer(config.writing_device, get_shadow_path_retriever(config), config.page_sz);
        }

        static auto get_cur_page_writer(Config config) -> std::unique_ptr<page_io::Writable>{

            return page_io::ComponentFactory::get_std_page_writer(config.writing_device, get_cur_path_retriever(config), config.page_sz);
        }

        static auto get_page_discretizer(Config config) -> std::unique_ptr<page_discretizer::PageDiscretizable>{

            return page_discretizer::ComponentFactory::get_std_page_discretizer(config.page_sz);
        } 

        static auto get_page_observer(Config config) -> std::unique_ptr<page_keeper::PageObservable>{

            return page_keeper::ComponentFactory::get_std_page_observer(config.max_page_count, config.page_sz);
        }

        static auto get_page_reserver(Config config) -> std::unique_ptr<page_reservation::Reservable>{

            return page_reservation::ComponentFactory::get_std_reserver(get_cur_info_retriever(config), get_cur_page_writer(config));
        }

        static auto get_page_shrinker(Config config) -> std::unique_ptr<page_reservation::Shrinkable>{

            return page_reservation::ComponentFactory::get_std_shrinker(config.del_device, get_cur_info_retriever(config), get_cur_path_retriever(config));
        }

        static auto get_commiting_dispatcher(Config config) -> std::unique_ptr<engine::CommitingDispatchable>{

            return engine::ComponentFactory::get_commiting_dispatcher(config.page_sz, config.rename_device, get_metadata_fetcher(config),
                                                                      get_shadow_page_writer(config), get_metadata_generator(config),
                                                                      get_metadata_serializer(config), get_page_discretizer(config),
                                                                      get_page_reserver(config), get_old_path_retriever(config), 
                                                                      get_cur_path_retriever(config), get_shadow_path_retriever(config));
        }

        static auto get_loading_engine(Config config) -> std::unique_ptr<engine::LoadingEngine>{

            return engine::ComponentFactory::get_std_loading_engine(get_metadata_fetcher(config), get_hasher(config),
                                                                    get_page_discretizer(config), get_cur_info_retriever(config),
                                                                    get_page_reader(config));
        }

        static auto get_commiting_engine(Config config) -> std::unique_ptr<engine::CommitingEngine>{

            return engine::ComponentFactory::get_std_commiting_engine(get_state_fetcher(config), get_state_syncer(config), 
                                                                      get_page_observer(config), get_commiting_dispatcher(config));
        } 

        static auto get_rollback_engine(Config config) -> std::unique_ptr<engine::RollbackEngine>{

            return engine::ComponentFactory::get_std_rollback_engine(get_state_fetcher(config), get_state_syncer(config), 
                                                                     get_rollingback_recoverer(config));
        }   
        
        static auto get_anchoring_engine(Config config) -> std::unique_ptr<engine::AnchoringEngine>{
            
            return engine::ComponentFactory::get_std_anchoring_engine(get_state_syncer(config), get_anchoring_recoverer(config), 
                                                                      get_state_fetcher(config));
        } 

        static auto get_engine(Config config) -> std::unique_ptr<engine::Engine>{

            return engine::ComponentFactory::get_engine(get_loading_engine(config), get_commiting_engine(config),
                                                        get_rollback_engine(config), get_anchoring_engine(config)); 
        } 
    };

    struct ResourceController{

        static void defaultize(BaseConfig config){  

            ResourceInitializer::defaultize(config);
        }

        static auto recover(BaseConfig base_config, IODevices io_devices){

            Config config{};
            static_cast<BaseConfig&>(config)    = base_config;
            static_cast<IODevices&>(config)     = io_devices;

            dg::fileio::dskchk(config.dir);          
            ResourceSpawner::get_state_syncer(config)->sync(ResourceSpawner::get_std_recoverer(config)->recover(ResourceSpawner::get_state_fetcher(config)->fetch())); // bad
            auto metadata   = ResourceSpawner::get_metadata_fetcher(config)->fetch(); // bad
            auto page_count = utility::PageUtility::size(metadata.buf_sz, config.page_sz); // bad
            ResourceSpawner::get_page_shrinker(config)->shrink(page_count + constants::BIN_PAGE_OFFS); //bad
        }

        static auto spawn(BaseConfig base_config, IODevices io_devices) -> std::unique_ptr<engine::Engine>{

            Config config{};
            static_cast<BaseConfig&>(config)    = base_config;
            static_cast<IODevices&>(config)     = io_devices;

            return ResourceSpawner::get_engine(config);
        }
    };

}

namespace dg::atomic_buffer::user_interface{

    using namespace dg::atomic_buffer::types;
    using Config    = resources::BaseConfig;
    using IODevices = resources::IODevices; 

    extern void mount(Config config){

        resources::ResourceController::defaultize(config);
    }

    extern void recover(Config config, IODevices io_devices){ //to be invoked on exception (to avoid infinite recursion)

        resources::ResourceController::recover(config, io_devices);   
    }

    extern auto spawn(Config config, IODevices io_devices) -> std::unique_ptr<engine::Engine>{ //detaching IODevices for performance tuning and, most importantly, testings 

        return resources::ResourceController::spawn(config, io_devices);
    }

};