#ifndef __DG_ASYNC_H__
#define __DG_ASYNC_H__

#include <memory>
#include <thread>
#include <mutex>
#include <vector>
#include <atomic>
#include <queue>
#include <sched.h>
#include <pthread.h>

namespace dg::async{

    using priority_type = uint8_t; 
    using task_type     = uint8_t;

    static constexpr priority_type LO_PRIO   = 0;
    static constexpr priority_type MID_PRIO  = 1;
    static constexpr priority_type HI_PRIO   = 2; 

    enum task_options: task_type{
        fio,
        cpu,
        network
    };

    class Synchronizable{

        public:

            virtual ~Synchronizable() noexcept{}
            virtual bool operator()() noexcept = 0;
    };

    class ExceptSynchronizable{

        public:

            virtual ~ExceptSynchronizable() noexcept{}
            virtual bool operator()()  = 0;
    };

    class Breakable{ //busy break

        public:

            virtual ~Breakable() noexcept{}
            virtual void enter_break() noexcept = 0; 
    };

    class ExceptBreakable{

        public:

            virtual ~ExceptBreakable() noexcept{}
            virtual void enter_break() = 0;
    };

    class Waitable{ //busy wait

        public:

            virtual ~Waitable() noexcept{}
            virtual void wait(Synchronizable&) noexcept = 0; 
    }; 

    class ExceptWaitable{

        public:

            virtual ~ExceptWaitable() noexcept{}
            virtual void wait(ExceptSynchronizable&) = 0;
    }; 

    class WorkOrder{

        public:

            virtual ~WorkOrder() noexcept{}
            virtual priority_type get_priority() const noexcept = 0;
            virtual void operator()() noexcept = 0; //important 
    };

    class WorkOrderContainable{

        public:

            virtual ~WorkOrderContainable() noexcept{}
            virtual void add(std::unique_ptr<WorkOrder>) = 0;
            virtual std::unique_ptr<WorkOrder> pop() noexcept = 0;
    };

    class Workable{

        public:

            virtual ~Workable() noexcept{}
            virtual void set_distributor(WorkOrderContainable *) noexcept = 0;
    };
    
    class WorkerSpawnable{

        public:

            virtual ~WorkerSpawnable() noexcept{}
            virtual std::unique_ptr<Workable> get() = 0;
    };

    class Executable{

        public:

            virtual ~Executable() noexcept{}
            virtual void add(std::unique_ptr<WorkOrder>) = 0;
    };

    class ThreadPoolable: public virtual Executable{

        public:

            virtual ~ThreadPoolable() noexcept{}
            virtual void spawn(size_t) = 0;
            virtual void terminate() noexcept = 0;
    };

};

namespace dg::async::v1{

    template <class T>
    class FunctorWrappedSynchronizer: public virtual Synchronizable{

        private:

            T functor;
        
        public:

            static_assert(std::is_nothrow_destructible_v<T>);

            FunctorWrappedSynchronizer(T functor): functor(std::move(functor)){}

            bool operator()() noexcept{

                static_assert(noexcept(this->functor()));
                return this->functor();
            }
    };
    
    template <class T>
    class FunctorWrappedWorkOrder: public virtual WorkOrder{

        private:

            T functor;
            priority_type prio_val;

        public:

            static_assert(std::is_nothrow_destructible_v<T>);

            FunctorWrappedWorkOrder(T functor, priority_type prio_val): functor(std::move(functor)), 
                                                                        prio_val(prio_val){}

            priority_type get_priority() const noexcept{

                return this->prio_val;
            }

            void operator()() noexcept{

                static_assert(noexcept(this->functor()));
                this->functor();
            }
    };

    class BusyWaiter: public virtual Waitable{

        public:

            void wait(Synchronizable& synchronizer) noexcept{
                while (!synchronizer()){
                    std::this_thread::yield();
                }
            }
    };

    class BusyBreaker: public virtual Breakable{

        public:

            void enter_break() noexcept{
                // using namespace std::chrono_literals;
                std::this_thread::yield();
                // std::this_thread::sleep_for(1000ms);
            }
    };
    
    class ThreadWorker: public virtual Workable{

        private:

            std::unique_ptr<Breakable> breaker;
            std::shared_ptr<std::atomic<bool>> poison_pill;
            std::atomic<WorkOrderContainable *> container;   
            
        public:

            ThreadWorker(std::unique_ptr<Breakable> breaker,
                         std::shared_ptr<std::atomic<bool>> poison_pill): breaker(std::move(breaker)),
                                                                          poison_pill(std::move(poison_pill)),
                                                                          container(){}

            void set_distributor(WorkOrderContainable * arg) noexcept{

                this->container.exchange(arg, std::memory_order_release);
            }

            void operator()() noexcept{

                while (true){

                    if (this->poison_pill->load(std::memory_order_acquire)){
                        break;
                    }

                    auto src    = this->container.load(std::memory_order_acquire);
                    auto wo     = bool{src} ? src->pop(): std::unique_ptr<WorkOrder>{};

                    if (wo){
                        (*wo)();
                    } else{
                        this->breaker->enter_break();
                    }
                }
            }
    };

    class ThreadSupervisor: public virtual Workable{

        private:

            std::unique_ptr<std::thread> thread_ins;
            std::shared_ptr<std::atomic<bool>> poison_pill;
            std::unique_ptr<Workable> worker; 

        public:

            ThreadSupervisor(std::unique_ptr<std::thread> thread_ins,
                             std::shared_ptr<std::atomic<bool>> poison_pill,
                             std::unique_ptr<Workable> worker): thread_ins(std::move(thread_ins)),
                                                                poison_pill(std::move(poison_pill)),
                                                                worker(std::move(worker)){}

            ~ThreadSupervisor() noexcept{
                
                this->poison_pill->exchange(true, std::memory_order_release);
                this->thread_ins->join();
            }

            void set_distributor(WorkOrderContainable * container) noexcept{

                this->worker->set_distributor(container);
            }
    };

    class StdContainer: public virtual WorkOrderContainable{

        private:

            std::vector<std::unique_ptr<WorkOrder>> orders;
            std::mutex mtx;
        
        public:

            void add(std::unique_ptr<WorkOrder> order){

                std::lock_guard<std::mutex> guard(this->mtx);
                this->orders.push_back(std::move(order)); 
            }

            std::unique_ptr<WorkOrder> pop() noexcept{

                std::lock_guard<std::mutex> guard(this->mtx); 

                if (this->orders.empty()){
                    return {};
                }

                auto rs = std::move(this->orders.back());
                this->orders.pop_back();

                return rs;
            }
    };

    class PriorityContainer: public virtual WorkOrderContainable{

        private:

            std::vector<std::unique_ptr<WorkOrder>> orders;
            std::mutex mtx;
        
        public:

            PriorityContainer(): orders(), mtx(){}

            void add(std::unique_ptr<WorkOrder> order){

                std::lock_guard<std::mutex> guard(this->mtx);
                this->orders.push_back(std::move(order));
                std::push_heap(this->orders.begin(), this->orders.end(), [](const auto& lhs, const auto& rhs){return lhs->get_priority() < rhs->get_priority();}); 
            }

            std::unique_ptr<WorkOrder> pop() noexcept{

                std::lock_guard<std::mutex> guard(this->mtx); 

                if (this->orders.empty()){
                    return {};
                }

                std::pop_heap(this->orders.begin(), this->orders.end(), [](const auto& lhs, const auto& rhs){return lhs->get_priority() < rhs->get_priority();});
                auto rs = std::move(this->orders.back());
                this->orders.pop_back();

                return rs;
            }
        
    };

    class StdAsyncWorkerSpawner: public virtual WorkerSpawnable{

        public:

            std::unique_ptr<Workable> get(){
                    
                auto poison_pill        = std::make_shared<std::atomic<bool>>(bool{false});
                auto breaker            = std::make_unique<BusyBreaker>();
                auto worker             = std::make_unique<ThreadWorker>(std::move(breaker), poison_pill);
                auto executable         = [worker_ptr = worker.get()]() noexcept{
                    worker_ptr->operator()();
                };
                auto dynamic_thread_ins = std::make_unique<std::thread>(std::move(executable));
                auto rs_worker          = std::make_unique<ThreadSupervisor>(std::move(dynamic_thread_ins), poison_pill, std::move(worker)); 

                return rs_worker;
            }
    };

    // class SameAffinityWorkerSpawner: public virtual WorkerSpawnable{

    //     public:

    //         std::unique_ptr<Workable> get(){
                
    //             static int i = 0;

    //             auto poison_pill        = std::make_shared<std::atomic<bool>>(bool{false});
    //             auto breaker            = std::make_unique<BusyBreaker>();
    //             auto worker             = std::make_unique<ThreadWorker>(std::move(breaker), poison_pill);
    //             auto executable         = [worker_ptr = worker.get()]() noexcept{
    //                 worker_ptr->operator()();
    //             };
    //             auto thread_ins         = std::make_unique<std::thread>(std::move(executable));
                
    //             cpu_set_t cpuset;
    //             CPU_ZERO(&cpuset);
    //             CPU_SET((i++) % std::thread::hardware_concurrency(), &cpuset);
    //             int rc                  = pthread_setaffinity_np(thread_ins->native_handle(), sizeof(cpu_set_t), &cpuset);
                
    //             if (rc != 0){
    //                 throw std::exception{};
    //             }

    //             auto rs_worker          = std::make_unique<ThreadSupervisor>(std::move(thread_ins), poison_pill, std::move(worker)); 
    //             return rs_worker;
    //         }
    // };
    

    class StdThreadPooler: public virtual ThreadPoolable{

        private:

            std::vector<std::unique_ptr<Workable>> workers;
            std::unique_ptr<WorkerSpawnable> spawner;
            std::unique_ptr<WorkOrderContainable> container;

        public:

            StdThreadPooler(std::unique_ptr<WorkerSpawnable> spawner,
                            std::unique_ptr<WorkOrderContainable> container): workers(),
                                                                              spawner(std::move(spawner)),
                                                                              container(std::move(container)){}

            ~StdThreadPooler() noexcept{

                this->terminate();
            }
            
            void spawn(size_t thread_num){
                
                this->terminate();

                for (size_t i = 0; i < thread_num; ++i){
                    
                    std::unique_ptr<Workable> worker = this->spawner->get();
                    worker->set_distributor(this->container.get());
                    this->workers.push_back(std::move(worker));
                }
            }

            void add(std::unique_ptr<WorkOrder> work_order){

                this->container->add(std::move(work_order));
            }

            void terminate() noexcept{

                this->workers.clear();
            }
    };
 
    auto get_thread_pool() -> std::unique_ptr<ThreadPoolable>{
        
        auto worker_spawner     = std::make_unique<StdAsyncWorkerSpawner>();
        auto wo_container       = std::make_unique<PriorityContainer>();

        return std::make_unique<StdThreadPooler>(std::move(worker_spawner), std::move(wo_container));
    }

    // auto get_same_affinity_thread_pool() -> std::unique_ptr<ThreadPoolable>{

    //     auto worker_spawner     = std::make_unique<SameAffinityWorkerSpawner>();
    //     auto wo_container       = std::make_unique<PriorityContainer>();

    //     return std::make_unique<StdThreadPooler>(std::move(worker_spawner), std::move(wo_container));
    // }

    template <class Executable>
    auto get_work_order(Executable executable, task_type task_type, const priority_type prio = LO_PRIO) -> std::unique_ptr<WorkOrder>{

        return std::make_unique<FunctorWrappedWorkOrder<Executable>>(std::move(executable), prio);
    }

}


#endif