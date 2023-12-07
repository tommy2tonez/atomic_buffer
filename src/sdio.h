#ifndef __SDIO_H__
#define __SDIO_H__

#include <filesystem>
#include <tuple>
#include "fileio.h"
#include <unordered_map>
#include <unordered_set>

namespace dg::sdio::types{

    using path_type                     = std::filesystem::path;
    using err_type                      = dg::fileio::err_type; 
    using io_operation_type             = uint8_t;
    using io_policy_type                = uint8_t;
    static constexpr auto phasher       = [](const path_type& path){return std::hash<std::string>{}(path.string());}; 
    using write_map_type                = std::unordered_map<path_type, std::pair<const void *, size_t>, decltype(phasher)>;
    using read_map_type                 = std::unordered_map<path_type, std::pair<void *, size_t>, decltype(phasher)>;  

}

namespace dg::sdio::constants{

    static constexpr auto STRICTEST_BLOCK_SZ  = dg::fileio::constants::STRICTEST_BLOCK_SZ;
}

namespace dg::sdio::runtime_exception{

    using namespace dg::fileio::runtime_exception; 
}

namespace dg::sdio::basic_device{

    using namespace dg::sdio::types;

    class ReadingDeviceInterface{

        public:

            virtual ~ReadingDeviceInterface() noexcept{}
            virtual std::pair<void *, size_t> read_into(const path_type&, void *, size_t) = 0;
    };

    class WritingDeviceInterface{

        public:

            virtual ~WritingDeviceInterface() noexcept{}
            virtual void write(const path_type&, const void *, size_t) = 0;
    };

    class DeletingDeviceInterface{

        public:

            virtual ~DeletingDeviceInterface() noexcept{}
            virtual void del(const path_type&) = 0;
    };

    class RenamingDeviceInterface{

        public:

            virtual ~RenamingDeviceInterface() noexcept{}
            virtual void rename(const path_type&, const path_type&) = 0;
    };

}

namespace dg::sdio::synchronizer{

    using namespace dg::sdio::types;

    class Synchronizable{

        public:

            virtual ~Synchronizable() noexcept{} 
            virtual void sync(const std::vector<path_type>&) = 0;
    };
};

namespace dg::sdio::core{

    using namespace dg::sdio::types;

    class WritingDeviceInterface{

        public:

            virtual ~WritingDeviceInterface() noexcept{}
            virtual void write(const std::vector<std::tuple<path_type, const void *, size_t>>&) = 0;
    };

    class ReadingDeviceInterface{

        public:

            virtual ~ReadingDeviceInterface() noexcept{}
            virtual void read_into(const std::vector<std::tuple<path_type, void *, size_t>>&) = 0;
    };

    class DeletingDeviceInterface{

        public:

            virtual ~DeletingDeviceInterface() noexcept{}
            virtual void del(const std::vector<path_type>&) = 0;
    };

    class RenamingDeviceInterface{

        public:

            virtual ~RenamingDeviceInterface() noexcept{}
            virtual void rename(const std::vector<std::pair<path_type, path_type>>&) = 0; //user-configurable - depends on durability requirements
    };

}

// ----

namespace dg::sdio::utility{

    using namespace sdio::types;

    auto pathify(const std::vector<std::tuple<path_type, const void *, size_t>>& inp) -> std::vector<path_type>{
        
        auto rs     = std::vector<path_type>(inp.size());
        std::transform(inp.begin(), inp.end(), rs.begin(), [](const auto& cur){return std::get<0>(cur);});

        return rs;
    }

    auto pathify(const std::vector<std::pair<path_type, path_type>>& inp) -> std::vector<path_type>{

        auto rs     = std::vector<path_type>();

        for (const auto& e: inp){
            rs.push_back(e.first);
            rs.push_back(e.second);
        }

        return rs;
    }

    auto setify(const std::vector<path_type>& paths) -> std::vector<path_type>{

        const auto hasher   = [](const path_type& path){return std::hash<std::string>{}(path.string());};
        auto path_set       = std::unordered_set<path_type, decltype(hasher)>(paths.begin(), paths.end(), paths.size(), hasher);
        auto rs             = std::vector<path_type>(path_set.begin(), path_set.end());

        return rs;
    } 
}

namespace dg::sdio::basic_device{

    struct StdFileReader: virtual ReadingDeviceInterface{

        std::pair<void *, size_t> read_into(const path_type& path, void * buf, size_t sz){

            return fileio::bread_into(path, buf, sz);
        }
    };

    struct DirectFileReader: virtual ReadingDeviceInterface{

        std::pair<void *, size_t> read_into(const path_type& path, void * buf, size_t sz){

            return fileio::direct_bread_into(path, buf, sz);
        }
    };

    struct StdFileWriter: virtual WritingDeviceInterface{

        void write(const path_type& path, const void * buf, size_t sz){

            fileio::core::bwrite(path, buf, sz, std::integral_constant<bool, false>{}, std::integral_constant<bool, false>{});
        }
    };

    struct DirectFileWriter: virtual WritingDeviceInterface{

        void write(const path_type& path, const void * buf, size_t sz){

            fileio::core::bwrite(path, buf, sz, std::integral_constant<bool, true>{}, std::integral_constant<bool, false>{});
        }
    };

    struct AtomicFileWriter: virtual WritingDeviceInterface{

        void write(const path_type& path, const void * buf, size_t sz){

            fileio::atomic_bwrite(path, buf, sz);
        }
    };

    struct AtomicDirectFileWriter: virtual WritingDeviceInterface{

        void write(const path_type& path, const void * buf, size_t sz){

            fileio::atomic_direct_bwrite(path, buf, sz);
        }
    };

    struct StdRenamer: virtual RenamingDeviceInterface{
        
        void rename(const path_type& o_path, const path_type& n_path){

            std::filesystem::rename(o_path, n_path);
        }
    };

    struct SyncRenamer: virtual RenamingDeviceInterface{

        void rename(const path_type& o_path, const path_type& n_path){

            fileio::dual_sync_rename(o_path, n_path);
        }
    };

    struct StdDeleter: virtual DeletingDeviceInterface{

        void del(const path_type& path){

            fileio::rm(path);
        }
    };

    struct SyncDeleter: virtual DeletingDeviceInterface{

        void del(const path_type& path){

            fileio::sync_remove(path);
        }
    };  

    struct ComponentFactory{
        
        static auto get_std_file_reader() -> std::unique_ptr<ReadingDeviceInterface>{

            return std::make_unique<StdFileReader>();
        } 

        static auto get_direct_file_reader() -> std::unique_ptr<ReadingDeviceInterface>{

            return std::make_unique<DirectFileReader>();
        }

        static auto get_std_file_writer() -> std::unique_ptr<WritingDeviceInterface>{

            return std::make_unique<StdFileWriter>();
        }

        static auto get_direct_file_writer() -> std::unique_ptr<WritingDeviceInterface>{

            return std::make_unique<DirectFileWriter>();
        }

        static auto get_atomic_file_writer() -> std::unique_ptr<WritingDeviceInterface>{

            return std::make_unique<AtomicFileWriter>();
        }

        static auto get_atomic_direct_file_writer() -> std::unique_ptr<WritingDeviceInterface>{

            return std::make_unique<AtomicDirectFileWriter>();
        }

        static auto get_std_renamer() -> std::unique_ptr<RenamingDeviceInterface>{

            return std::make_unique<StdRenamer>();
        }

        static auto get_sync_renamer() -> std::unique_ptr<RenamingDeviceInterface>{

            return std::make_unique<SyncRenamer>();
        }

        static auto get_std_deleter() -> std::unique_ptr<DeletingDeviceInterface>{

            return std::make_unique<StdDeleter>();
        }

        static auto get_sync_deleter() -> std::unique_ptr<DeletingDeviceInterface>{

            return std::make_unique<SyncDeleter>();
        }
    };
}

namespace dg::sdio::synchronizer{
    
    struct ParentDirectorySynchronizer: virtual Synchronizable{

        void sync(const std::vector<path_type>& paths){
            
            auto pd_transform   = [](const path_type& path){return path.parent_path();};
            auto pds            = std::vector<path_type>();

            std::transform(paths.begin(), paths.end(), std::back_inserter(pds), pd_transform);
            pds = utility::setify(pds);
            std::for_each(pds.begin(), pds.end(), dg::fileio::dsync);
        }
    };

    struct FileSynchronizer: virtual Synchronizable{

        void sync(const std::vector<path_type>& paths){

            std::for_each(paths.begin(), paths.end(), dg::fileio::ffsync);
            ParentDirectorySynchronizer().sync(paths);
        }
    };

    struct EmptySynchronizer: virtual Synchronizable{

        void sync(const std::vector<path_type>&){}
    };

    struct ComponentFactory{

        static auto get_parent_dir_syncer() -> std::unique_ptr<Synchronizable>{

            return std::make_unique<ParentDirectorySynchronizer>();
        }

        static auto get_file_syncer() -> std::unique_ptr<Synchronizable>{

            return std::make_unique<FileSynchronizer>();
        }

        static auto get_empty_syncer() -> std::unique_ptr<Synchronizable>{

            return std::make_unique<EmptySynchronizer>();
        }
    };
}

namespace dg::sdio::core{
 
    class StdReadingDevice: public virtual ReadingDeviceInterface{

        private:

            std::unique_ptr<basic_device::ReadingDeviceInterface> file_reader;

        public:

            StdReadingDevice(std::unique_ptr<basic_device::ReadingDeviceInterface> file_reader): file_reader(std::move(file_reader)){}

            void read_into(const std::vector<std::tuple<path_type, void *, size_t>>& read_data){

                auto feach_lambda   = [&](const auto& e){this->file_reader->read_into(std::get<0>(e), std::get<1>(e), std::get<2>(e));};
                std::for_each(read_data.begin(), read_data.end(), feach_lambda);
            }
    };

    class StdWritingDevice: public virtual WritingDeviceInterface{

        private:

            std::unique_ptr<basic_device::WritingDeviceInterface> file_writer;
            std::unique_ptr<synchronizer::Synchronizable> syncer;

        public:

            StdWritingDevice(std::unique_ptr<basic_device::WritingDeviceInterface> file_writer,
                             std::unique_ptr<synchronizer::Synchronizable> syncer): file_writer(std::move(file_writer)),
                                                                                    syncer(std::move(syncer)){}

            void write(const std::vector<std::tuple<path_type, const void *, size_t>>& write_data){

                auto feach_lambda   = [&](const auto& e){this->file_writer->write(std::get<0>(e), std::get<1>(e), std::get<2>(e));};
                std::for_each(write_data.begin(), write_data.end(), feach_lambda);
                this->syncer->sync(utility::pathify(write_data));
            }
    };

    class StdRenamingDevice: public virtual RenamingDeviceInterface{

        private:

            std::unique_ptr<basic_device::RenamingDeviceInterface> file_renamer;
            std::unique_ptr<synchronizer::Synchronizable> syncer;

        public:

            StdRenamingDevice(std::unique_ptr<basic_device::RenamingDeviceInterface> file_renamer,
                              std::unique_ptr<synchronizer::Synchronizable> syncer): file_renamer(std::move(file_renamer)),
                                                                                     syncer(std::move(syncer)){}

            void rename(const std::vector<std::pair<path_type, path_type>>& rename_data){

                auto feach_lambda   = [&](const auto& e){this->file_renamer->rename(e.first, e.second);};
                std::for_each(rename_data.begin(), rename_data.end(), feach_lambda);
                this->syncer->sync(utility::pathify(rename_data));
            }
    };

    class StdDelDevice: public virtual DeletingDeviceInterface{

        private:

            std::unique_ptr<basic_device::DeletingDeviceInterface> deleter;
            std::unique_ptr<synchronizer::Synchronizable> syncer;

        public:

            StdDelDevice(std::unique_ptr<basic_device::DeletingDeviceInterface> deleter,
                         std::unique_ptr<synchronizer::Synchronizable> syncer): deleter(std::move(deleter)),
                                                                                syncer(std::move(syncer)){}

            void del(const std::vector<path_type>& paths){

                auto feach_lambda   = [&](const auto& e){this->deleter->del(e);};
                std::for_each(paths.begin(), paths.end(), feach_lambda);
                this->syncer->sync(paths);
            }
    };

    struct ComponentFactory{

        static auto get_std_reading_device(std::unique_ptr<basic_device::ReadingDeviceInterface> file_reader) -> std::unique_ptr<ReadingDeviceInterface>{

            return std::make_unique<StdReadingDevice>(std::move(file_reader));
        }

        static auto get_std_writing_device(std::unique_ptr<basic_device::WritingDeviceInterface> file_writer, 
                                           std::unique_ptr<synchronizer::Synchronizable> syncer) -> std::unique_ptr<WritingDeviceInterface>{
            
            return std::make_unique<StdWritingDevice>(std::move(file_writer), std::move(syncer));
        }

        static auto get_std_renaming_device(std::unique_ptr<basic_device::RenamingDeviceInterface> file_renamer,
                                            std::unique_ptr<synchronizer::Synchronizable> syncer) -> std::unique_ptr<RenamingDeviceInterface>{
            
            return std::make_unique<StdRenamingDevice>(std::move(file_renamer), std::move(syncer));
        }

        static auto get_std_del_device(std::unique_ptr<basic_device::DeletingDeviceInterface> deleter, 
                                       std::unique_ptr<synchronizer::Synchronizable> syncer){
            
            return std::make_unique<StdDelDevice>(std::move(deleter), std::move(syncer));
        }
    };
};

namespace dg::sdio::user_interface{

    extern auto get_reading_device() -> std::unique_ptr<core::ReadingDeviceInterface>{

        return core::ComponentFactory::get_std_reading_device(basic_device::ComponentFactory::get_std_file_reader());
    }

    extern auto get_direct_reading_device() -> std::unique_ptr<core::ReadingDeviceInterface>{

        return core::ComponentFactory::get_std_reading_device(basic_device::ComponentFactory::get_direct_file_reader());
    }

    extern auto get_writing_device() -> std::unique_ptr<core::WritingDeviceInterface>{

        return core::ComponentFactory::get_std_writing_device(basic_device::ComponentFactory::get_std_file_writer(),
                                                              synchronizer::ComponentFactory::get_file_syncer());
    }

    extern auto get_direct_writing_device() -> std::unique_ptr<core::WritingDeviceInterface>{

        return core::ComponentFactory::get_std_writing_device(basic_device::ComponentFactory::get_direct_file_writer(), 
                                                              synchronizer::ComponentFactory::get_file_syncer());
    }

    extern auto get_deleting_device() -> std::unique_ptr<core::DeletingDeviceInterface>{
        
        return core::ComponentFactory::get_std_del_device(basic_device::ComponentFactory::get_std_deleter(),
                                                          synchronizer::ComponentFactory::get_parent_dir_syncer());
    }

    extern auto get_renaming_device() -> std::unique_ptr<core::RenamingDeviceInterface>{

        return core::ComponentFactory::get_std_renaming_device(basic_device::ComponentFactory::get_std_renamer(), 
                                                               synchronizer::ComponentFactory::get_parent_dir_syncer());
    }
};

#endif