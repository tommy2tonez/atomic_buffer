#ifndef __FILE_IO_H__
#define __FILE_IO_H__

#include <filesystem>
#include <memory>
#include <stdio.h>
#include <algorithm>
#include <numeric>
#include <optional>
#include <cstring>
#include <string> 
#include <functional>
#include "expected.h"
#include <unistd.h>
#include <climits>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace dg::fileio::types{

    using err_type          = uint8_t;
    using path_type         = std::filesystem::path; 
}

namespace dg::fileio::runtime_exception{

    using err_type  = fileio::types::err_type; 

    struct OSError: std::exception{}; 
    struct FNFError: std::exception{};
    struct BOFError: std::exception{};
    struct CorruptedError: std::exception{};

    static constexpr err_type SUCCESS_CONST         = 0b000000;
    static constexpr err_type FNF_CONST             = 0b000001;
    static constexpr err_type BUFOVERFLOW_CONST     = 0b000010;
    static constexpr err_type OSERROR_CONST         = 0b000100;
    static constexpr err_type CORRUPTED_CONST       = 0b001000;  
    static constexpr err_type MEM_EXHAUSTION_CONST  = 0b010000;
    static constexpr err_type OTHER_CONST           = 0b100000;

    static void throw_err(const err_type err_code){

        switch (err_code){
            case SUCCESS_CONST:
                break;
            case FNF_CONST:
                throw FNFError();
            case BUFOVERFLOW_CONST:
                throw BOFError();
            case OSERROR_CONST:
                throw OSError();
            case CORRUPTED_CONST:
                throw CorruptedError();
            case MEM_EXHAUSTION_CONST:
                throw std::bad_alloc();
            case OTHER_CONST:
                throw std::exception();
            default:
                std::abort();
                break;
        }
    }
}

namespace dg::fileio::constants{

    static constexpr auto READ_DIR_FLAG             = O_DIRECTORY | O_RDONLY;
    static constexpr auto WRITE_FLAG                = O_WRONLY | O_CREAT | O_TRUNC;
    static constexpr auto DIRECT_WRITE_FLAG         = WRITE_FLAG | O_DIRECT;
    static constexpr auto CREATE_EMPTY_FLAG         = O_CREAT | O_TRUNC;
    static constexpr auto READ_FLAG                 = O_RDONLY;
    static constexpr auto DIRECT_READ_FLAG          = READ_FLAG | O_DIRECT;
    static constexpr auto STRICTEST_BLOCK_SZ        = size_t{1} << 15; //not to be changed (compatibility)
    static constexpr auto DFLT_MODE                 = S_IRWXU;
    static constexpr const char * atomic_traits     = "_atomictraits"; 

}

namespace dg::fileio::utility{

    using namespace dg::fileio::types;   

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

}

namespace dg::fileio::fd_services{

    using namespace fileio::runtime_exception;
    using namespace fileio::types;

    auto open_file(const path_type& path, int flags) -> std::shared_ptr<int>{ //shared_ptr prereq - RTTI support (runtime-verify this)

        auto int_space      = std::unique_ptr<int>(new int{});
        auto fpath          = path.native();
        int fd              = open(fpath.c_str(), flags, constants::DFLT_MODE);

        auto fclose_lambda  = [](int * fd_arg) noexcept{
            
            if (close(*fd_arg) == -1){
                std::terminate(); //better to terminate the program at this point
            }
            delete fd_arg;
        };

        if (fd == -1){
            return {};
        }

        *int_space          = fd;
        auto rs             = std::unique_ptr<int, decltype(fclose_lambda)>(int_space.get(), fclose_lambda);
        int_space.release();

        return rs;
    } 

    auto assert_open_file(const path_type& path, int flags) -> std::shared_ptr<int>{

        auto rs = open_file(path, flags);
        if (!rs){
            throw OSError();
        }
        return rs;
    }

    template <class ...Args>
    auto seekg(Args&& ...args){

        auto rs = lseek(std::forward<Args>(args)...);
        if (rs == -1){
            throw OSError();
        }
        return rs;
    }

    template <class ...Args>
    auto tellg(int fd){
    
        return seekg(fd, 0L, SEEK_CUR);
    }

    auto _fsync(int fd){

        if (fsync(fd) != 0){
            throw OSError();
        }
    }

    auto get_device_block_size(int fd) -> size_t{
        
        struct stat stx; //REVIEW: initialization
        
        if (fstat(fd, &stx) == -1){
            throw OSError();
        }

        if (stx.st_blksize <= 0){
            throw OSError();
        }

        return static_cast<size_t>(stx.st_blksize);
    }

    void direct_fwrite(int fd, const void * buf, size_t sz){
        
        const size_t BLOCK_SZ   = get_device_block_size(fd);
        uintptr_t buf_addr      = reinterpret_cast<uintptr_t>(buf); 
                
        if (sz == 0u){
            return;
        }

        if (sz % BLOCK_SZ != 0){
            std::abort(); //SEGFAULT equivalent
        } 

        if (buf_addr % BLOCK_SZ != 0){
            std::abort(); //SEGFAULT equivalent
        }

        if (write(fd, buf, sz) != sz){ //REVIEW: benchmark required (is discretization required?)
            throw OSError();
        }
    }

    void direct_fread(int fd, void * buf, size_t sz){

        const size_t BLOCK_SZ   = get_device_block_size(fd);
        uintptr_t buf_addr      = reinterpret_cast<uintptr_t>(buf);

        if (sz == 0u){
            return;
        }

        if (sz % BLOCK_SZ != 0){
            std::abort(); //SEGFAULT equivalent
        }

        if (buf_addr % BLOCK_SZ != 0){
            std::abort(); //SEGFAULT equivalent
        }

        if (read(fd, buf, sz) != sz){ //REVIEW: benchmark required (is discretization required?)
            throw OSError();
        }
    }

    void norm_fwrite(int fd, const void * buf, size_t sz){

        if (write(fd, buf, sz) != sz){
            throw OSError();
        }
    }

    void norm_fread(int fd, void * buf, size_t sz){

        if (read(fd, buf, sz) != sz){
            throw OSError();
        }
    }

}

namespace dg::fileio::atomic_services{

    using namespace fileio::types;

    auto add_atomic_traits(const path_type& path) -> path_type{

        auto ext        = path.extension().native();
        auto fname      = path_type{path}.replace_extension("").filename().native();
        auto new_fname  = fname + constants::atomic_traits;
        auto new_path   = path_type{path}.replace_filename(new_fname).replace_extension(ext);

        return new_path;
    }

    auto has_atomic_traits(const path_type& path) -> bool{

        return path.native().find(constants::atomic_traits) != std::string::npos;
    }
}

namespace dg::fileio::core{

    using namespace fileio::runtime_exception;
    using namespace fileio::types;

    template <bool IS_DIRECT>
    auto get_read_mode(const std::integral_constant<bool, IS_DIRECT>&){

        if constexpr(IS_DIRECT){
            return constants::DIRECT_READ_FLAG;
        } else{
            return constants::READ_FLAG;
        }
    }

    template <bool IS_DIRECT>
    auto get_write_mode(const std::integral_constant<bool, IS_DIRECT>&){

        if constexpr(IS_DIRECT){
            return constants::DIRECT_WRITE_FLAG;
        } else{
            return constants::WRITE_FLAG;
        }
    }

    template <bool IS_DIRECT = false>
    auto bread_into(const path_type& path, void * buf, size_t sz, const std::integral_constant<bool, IS_DIRECT>& IDIC = std::integral_constant<bool, IS_DIRECT>{}) -> std::pair<void *, size_t>{

        auto fptr       = fd_services::assert_open_file(path, get_read_mode(IDIC));
        auto true_sz    = fd_services::seekg(*fptr, 0L, SEEK_END);

        if (sz < true_sz){
            throw BOFError();
        }
        
        fd_services::seekg(*fptr, 0L, SEEK_SET);

        if constexpr(IS_DIRECT){
            fd_services::direct_fread(*fptr, buf, true_sz);
        } else{
            fd_services::norm_fread(*fptr, buf, true_sz);
        }

        return {buf, true_sz};
    }

    template <bool IS_DIRECT, bool HAS_FSYNC>
    void bwrite(const path_type& path, const void * buf, size_t sz, const std::integral_constant<bool, IS_DIRECT>& IDIC, const std::integral_constant<bool, HAS_FSYNC>&){
        
        auto fptr   = fd_services::assert_open_file(path, get_write_mode(IDIC)); 
        fd_services::seekg(*fptr, 0L, SEEK_SET);

        if constexpr(IS_DIRECT){
            fd_services::direct_fwrite(*fptr, buf, sz);
        } else{
            fd_services::norm_fwrite(*fptr, buf, sz);
        }

        if constexpr(HAS_FSYNC){
            fd_services::_fsync(*fptr);
        }
    }

}

namespace dg::fileio{

    using namespace fileio::runtime_exception;
    using namespace fileio::types;

    template <class Functor, class ...Args, std::enable_if_t<std::is_same_v<decltype(std::declval<Functor>()(std::declval<Args>()...)), void>, bool> = true>
    auto noexcept_wrap(Functor&& functor, Args&& ...args) noexcept -> err_type{

        try{
            functor(std::forward<Args>(args)...);
            return SUCCESS_CONST;
        } catch (OSError& e){
            return OSERROR_CONST;
        } catch (FNFError& e){
            return FNF_CONST;
        } catch (BOFError& e){
            return BUFOVERFLOW_CONST;
        } catch (CorruptedError& e){
            return CORRUPTED_CONST;
        } catch (std::bad_alloc& e){
            return MEM_EXHAUSTION_CONST;
        } catch (std::exception& e){
            return OTHER_CONST;
        }

        std::abort();
        return {};
    }

    template <class Functor, class ...Args, std::enable_if_t<!std::is_same_v<decltype(std::declval<Functor>()(std::declval<Args>()...)), void>, bool> = true>
    auto noexcept_wrap(Functor&& functor, Args&& ...args) noexcept -> dg::expected<decltype(functor(std::forward<Args>(args)...)), err_type>{

        try{
            return functor(std::forward<Args>(args)...);;
        } catch (OSError& e){
            return dg::unexpected(OSERROR_CONST);
        } catch (FNFError& e){
            return dg::unexpected(FNF_CONST);
        } catch (BOFError& e){
            return dg::unexpected(BUFOVERFLOW_CONST);
        } catch (CorruptedError& e){
            return dg::unexpected(CORRUPTED_CONST);
        } catch (std::bad_alloc& e){
            return dg::unexpected(MEM_EXHAUSTION_CONST);
        } catch (std::exception& e){
            return dg::unexpected(OTHER_CONST);
        }

        std::abort();
        return dg::unexpected(OTHER_CONST); //unreachable
    }

    auto get_device_block_size(const path_type& path) -> size_t{

        auto fptr   = fd_services::assert_open_file(path, O_RDONLY);
        return fd_services::get_device_block_size(*fptr);
    }

    auto size(const path_type& path) -> size_t{
        
        auto fptr   = fd_services::assert_open_file(path, O_RDONLY); 
        return static_cast<size_t>(fd_services::seekg(*fptr, 0L, SEEK_END));
    } 

    auto bread_into(const path_type& path, void * buf, size_t sz) -> std::pair<void *, size_t>{

        return core::bread_into(path, buf, sz, std::integral_constant<bool, false>{});
    } 

    auto direct_bread_into(const path_type& path, void * buf, size_t sz) -> std::pair<void *, size_t>{

        return core::bread_into(path, buf, sz, std::integral_constant<bool, true>{});
    }

    auto bread(const path_type& path) -> std::pair<std::unique_ptr<char[]>, size_t>{

        auto sz     = size(path);
        auto rs     = std::unique_ptr<char[]>(new char[sz]); 
        bread_into(path, static_cast<void *>(rs.get()), sz);

        return std::make_pair(std::move(rs), sz);
    }

    auto direct_bread(const path_type& path){

        using _MemUlt       = utility::MemoryUtility;
        auto sz             = size(path);
        auto rs             = _MemUlt::aligned_alloc(constants::STRICTEST_BLOCK_SZ, sz); 
        direct_bread_into(path, static_cast<void *>(rs.get()), sz);

        return std::make_pair(std::move(rs), sz);
    }

    void ffsync(const path_type& path){

        auto fptr   = fd_services::assert_open_file(path, constants::READ_FLAG); 
        fd_services::_fsync(*fptr);
    }

    void dsync(const path_type& path){

        auto dptr   = fd_services::assert_open_file(path, constants::READ_DIR_FLAG);
        fd_services::_fsync(*dptr);
    }

    void pdsync(const path_type& path){
        
        dsync(path.parent_path());
    }

    void mkdir(const path_type& path){
        
        std::filesystem::create_directory(path); //
        dsync(path);
    } 

    void bwrite(const path_type& path, const void * buf, size_t sz){
        
        core::bwrite(path, buf, sz, std::integral_constant<bool, false>{}, std::integral_constant<bool, false>{});
    }

    void sync_bwrite(const path_type& path, const void * buf, size_t sz){

        core::bwrite(path, buf, sz, std::integral_constant<bool, false>{}, std::integral_constant<bool, true>{});
        pdsync(path);
    }

    void direct_bwrite(const path_type& path, const void * buf, size_t sz){

        core::bwrite(path, buf, sz, std::integral_constant<bool, true>{}, std::integral_constant<bool, false>{});
    } 

    void direct_sync_bwrite(const path_type& path, const void * buf, size_t sz){

        core::bwrite(path, buf, sz, std::integral_constant<bool, true>{}, std::integral_constant<bool, true>{});
        pdsync(path);
    }

    void rm(const path_type& path){

        if (!std::filesystem::remove(path)){
            throw OSError();
        }
    }

    void rm_if_exists(const path_type& path){

        if (std::filesystem::exists(path)){
            rm(path);
        }
    }

    void assert_rm(const path_type& path){

        rm(path);
        pdsync(path);
    }

    void rename_resolve(const path_type& path1, const path_type& path2){

        bool precond    =   std::filesystem::exists(path1) && std::filesystem::exists(path2) && std::filesystem::equivalent(path1, path2);

        if (precond){
            assert_rm(path1);
        }
    }

    auto is_empty_file(const path_type& path) -> bool{

        return size(path) == 0u;
    }

    void sync_emptify(const path_type& path){

        {
            auto fptr   = fd_services::assert_open_file(path, constants::CREATE_EMPTY_FLAG);
            fd_services::_fsync(*fptr);
        }
        pdsync(path);
    }

    void sync_remove(const path_type& path){
        
        sync_emptify(path);
        rm(path);
    } 

    void sync_rename(const path_type& old_path, const path_type& new_path){ //sync new_path

        std::filesystem::rename(old_path, new_path);
        ffsync(new_path);
        pdsync(new_path);
    }

    void dual_sync_rename(const path_type& old_path, const path_type& new_path){ //sync both endpoints

        sync_rename(old_path, new_path);
        rm_if_exists(old_path);
        sync_emptify(old_path);
        rm(old_path);
    }

    void atomic_bwrite(const path_type& path, const void * buf, size_t sz){
        
        auto atomic_path = atomic_services::add_atomic_traits(path);
        rm_if_exists(atomic_path);
        sync_bwrite(atomic_path, buf, sz);
        sync_rename(atomic_path, path);
    }

    void atomic_direct_bwrite(const path_type& path, const void * buf, size_t sz){

        auto atomic_path = atomic_services::add_atomic_traits(path);
        rm_if_exists(atomic_path);
        direct_sync_bwrite(atomic_path, buf, sz);
        sync_rename(atomic_path, path);
    }

    template <bool IS_RECURSIVE = false>
    auto list_paths(const path_type& dir) -> std::vector<path_type>{

        auto dir_iter_gen = [=]{
            if constexpr(!IS_RECURSIVE){
                return std::filesystem::directory_iterator(dir);
            } else{
                return std::filesystem::recursive_directory_iterator(dir);
            }
        };
        
        auto iter   = dir_iter_gen();
        auto paths  = std::vector<path_type>();

        for (const std::filesystem::directory_entry& de: iter){
            paths.push_back(de.path());
        }

        return paths;
    }

    template <bool IS_RECURSIVE = false>
    void atomic_clean(const path_type& dir){
        
        auto paths  = list_paths<IS_RECURSIVE>(dir);
        auto last   = std::copy_if(paths.begin(), paths.end(), paths.begin(), atomic_services::has_atomic_traits);
        
        std::for_each(paths.begin(), last, rm);
    }

    template <bool IS_RECURSIVE = false>
    void empty_clean(const path_type& dir){

        auto paths  = list_paths<IS_RECURSIVE>(dir);
        auto last   = std::copy_if(paths.begin(), paths.end(), paths.begin(), is_empty_file);

        std::for_each(paths.begin(), last, rm);
    }

    template <bool IS_RECURSIVE = false>
    void dskchk(const path_type& dir){

        atomic_clean<IS_RECURSIVE>(dir);
        empty_clean<IS_RECURSIVE>(dir);
        dsync(dir); //
    }

}
#endif