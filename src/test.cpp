#include <chrono>
#include "atomic_buffer.h"
#include "sdio.h"
#include <iostream>
#include <memory>
#include <unordered_set>
#include <algorithm>
#include <vector>
#include "fileio.h"
#include <iostream>
#include <random>

using path_type     = std::filesystem::path;

class DefectiveRenamingDevice: public dg::sdio::core::RenamingDeviceInterface{
    
    public:

        void rename(const std::vector<std::pair<path_type, path_type>>& path_pairs){

            static auto rand_dev    = std::bind(std::uniform_int_distribution<size_t>{}, std::mt19937{});

            for (const auto& path_pair: path_pairs){

                if (rand_dev() % 30 == 0){
                    // dg::fileio::rm(path_pair.second);
                    std::filesystem::remove(path_pair.second);
                    std::filesystem::create_hard_link(path_pair.first, path_pair.second);
                    throw std::exception();
                    // std::terminate();
                } else{
                    dg::fileio::dual_sync_rename(path_pair.first, path_pair.second);
                }
            }
        }
};

class DefectiveWritingDevice: public dg::sdio::core::WritingDeviceInterface{

    public:

        void write(const std::vector<std::tuple<path_type, const void *, size_t>>& write_data){

            static auto rand_dev    = std::bind(std::uniform_int_distribution<size_t>{}, std::mt19937{});

            for (const auto& e: write_data){

                if (rand_dev() % 30 == 0){
                    dg::fileio::sync_emptify(std::get<0>(e));
                    throw std::exception();
                } else{
                    dg::fileio::atomic_bwrite(std::get<0>(e), std::get<1>(e), std::get<2>(e));
                }
            }
        }
};

class DefectiveDeletingDevice: public dg::sdio::core::DeletingDeviceInterface{
    
    public:

        void del(const std::vector<path_type>& paths){

            static auto rand_dev    = std::bind(std::uniform_int_distribution<size_t>{}, std::mt19937{});

            for (const auto& path: paths){

                if (rand_dev() % 30 == 0){
                    throw std::exception();
                } else{
                    dg::fileio::assert_rm(path);
                }
            }
        }

};

void verify(const void * buf, size_t sz, const void * bbuf, size_t ssz){

    if (sz != ssz){
        std::cout << "mayday sz" << "<>" << sz << "<>" << ssz << std::endl;
        std::abort();
        return;
    }

    if (std::memcmp(buf, bbuf, sz) != 0){
        std::cout << "mayday" << std::endl;
        std::abort();
        return;
    }

    std::cout << "passed" << std::endl;
}

auto get_random_buf(const size_t PAGE_SZ, const size_t RANDOM_SPAN) -> std::pair<std::unique_ptr<char[]>, size_t>{

    static auto random_device   = std::bind(std::uniform_int_distribution<size_t>{}, std::mt19937{});
    static auto buf_rdevice     = std::bind(std::uniform_int_distribution<char>{}, std::mt19937{});
    auto span                   = random_device() % RANDOM_SPAN + 1;
    auto buf_sz                 = PAGE_SZ * span;
    
    if (buf_sz == 0u){
        return {nullptr, size_t{}};
    }

    auto buf                    = std::unique_ptr<char[]>(new char[buf_sz]);
    std::generate(buf.get(), buf.get() + buf_sz, buf_rdevice);
    
    return {std::move(buf), buf_sz};
}

void recover_until_noexcept(auto config, auto reading_dev){

    try{
        dg::atomic_buffer::user_interface::recover(config, reading_dev);
    } catch (std::exception& e){
        recover_until_noexcept(config, reading_dev);
    }
}

void anchor_no_except(auto config, auto devs){

    try{
        auto ins    =  dg::atomic_buffer::user_interface::spawn(config, devs);
        ins->anchor();
    } catch (std::exception& e){
        recover_until_noexcept(config, devs);
        anchor_no_except(config, devs);
    }
}

void rollback_no_except(auto config, auto devs){

    try{

        auto ins    = dg::atomic_buffer::user_interface::spawn(config, devs);
        ins->rollback();
    } catch (std::exception& e){

        recover_until_noexcept(config, devs);
        rollback_no_except(config, devs);
    }
}

int main(){

    size_t i                    = 0u;
    const size_t PAGE_SZ        = 100;
    const size_t RANDOM_SPAN    = 10;
    const size_t MAX_PAGES      = 10;
    const char * path           = "atomictest";

    auto config                 = dg::atomic_buffer::user_interface::Config{path, PAGE_SZ, MAX_PAGES};    
    dg::atomic_buffer::user_interface::mount(config);
    auto stable_state           = std::make_pair(std::unique_ptr<char[]>(new char[1]), size_t{0u}); 
    auto devs                   = dg::atomic_buffer::user_interface::IODevices{dg::sdio::user_interface::get_reading_device(), 
                                                                               std::make_unique<DefectiveWritingDevice>(),
                                                                               std::make_unique<DefectiveDeletingDevice>(),
                                                                               std::make_unique<DefectiveRenamingDevice>()};

    while (true){

        auto ins                = dg::atomic_buffer::user_interface::spawn(config, devs);
        auto random_buf         = get_random_buf(PAGE_SZ, RANDOM_SPAN);
        anchor_no_except(config, devs);

        try{

            ins->set_buf(random_buf.first.get());
            ins->set_buf_size(random_buf.second);
            ins->set_buf_change(0u, random_buf.second);
            ins->commit();

            auto loaded     = ins->load(); 
            auto ssz        = ins->size();
            verify(random_buf.first.get(), random_buf.second, loaded.get(), ssz);

            if (i % 2 == 0){
                anchor_no_except(config, devs);
                stable_state    = std::make_pair(std::move(random_buf.first), random_buf.second); 
            } else{
                throw std::exception();
            }
        } catch (std::exception& e){
            recover_until_noexcept(config, devs);
            rollback_no_except(config, devs);
            auto sstable_state      = std::make_pair(ins->load(), ins->size());
            verify(stable_state.first.get(), stable_state.second, sstable_state.first.get(), sstable_state.second);
        }
        
        i++;
    }
}