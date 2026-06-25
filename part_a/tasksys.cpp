#include "tasksys.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
using namespace std;

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_=num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::threadRun(IRunnable* runnable,int threadID, int num_total_tasks){
    for(int counter=threadID;counter<num_total_tasks;counter+=num_threads_){
        runnable->runTask(counter,num_total_tasks);
    }
}
void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::vector<std::thread> threads;
    for (int i = 0; i < std::min(num_threads_,num_total_tasks); i++) {
        std::thread worker = std::thread([=](int num_threads_) {
            int counter =i;
            while (counter<num_total_tasks) {
                runnable->runTask(counter,num_total_tasks);
                counter+=num_threads_;
            }
        },num_threads_);
        threads.push_back(std::move(worker));
          
    }
    // for (int i = 0; i < std::min(num_threads_,num_total_tasks); i++) {
    //     std::thread worker = std::thread(&TaskSystemParallelSpawn::threadRun,this,runnable,i,num_total_tasks);
    //     threads.push_back(std::move(worker));
    // }
    for(int i=0;i<std::min(num_threads_,num_total_tasks);i++){
        threads[i].join();
    }
}


TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}


TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    work_exists_=false;
    num_threads_=num_threads;
    task_id_=0;
    thread_pool_.resize(num_threads);
    exit_work=false;
    for(int i=0;i<num_threads;i++){
        thread_pool_[i]=std::thread(&TaskSystemParallelThreadPoolSpinning::startThreadSpinLoop,this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    exit_work=true;
    for(int i=0;i<num_threads_;i++){
        if(thread_pool_[i].joinable()){
            thread_pool_[i].join();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::startThreadSpinLoop(){
    int local_task_id;
    while(!exit_work){    
        std::this_thread::sleep_for(std::chrono::microseconds(10));
        if (work_exists_){
            while(work_exists_){
                
                task_assignment_mutex_.lock();
                local_task_id=task_id_;
                if (local_task_id==num_total_tasks_){
                    task_assignment_mutex_.unlock();
                    break;
                }
                task_id_=task_id_+1;

                task_assignment_mutex_.unlock();
                runnable_->runTask(local_task_id,num_total_tasks_);
                task_completion_mutex_.lock();
                tasks_completed_++;
                if (tasks_completed_==num_total_tasks_){
                    work_exists_=false;
                    task_completion_mutex_.unlock();
                    break;
                }
                task_completion_mutex_.unlock();
            }
        }
    }
    return ;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    runnable_ = runnable;
    num_total_tasks_ = num_total_tasks;
    task_id_ = 0;
    work_exists_ = true;
    tasks_completed_=0;
        
    while(work_exists_){
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

void TaskSystemParallelThreadPoolSleeping::runThreadEventLoop(){

    int local_task_id;
    while(true){
        std::unique_lock<std::mutex> start_work_lock(work);
        start_work.wait(start_work_lock,[=]{return stop_spinning || enter_loop_<num_threads_;});
        if (stop_spinning){ 
            break;
        }
        enter_loop_++;
        start_work_lock.unlock();

        while (true) {
            task_assignment_mutex_.lock();
            local_task_id = task_id_;
            if (local_task_id == num_total_tasks_) {
                task_assignment_mutex_.unlock();
                break;
            }
            task_id_ = task_id_ + 1;
            task_assignment_mutex_.unlock();

            runnable_->runTask(local_task_id, num_total_tasks_);

            task_completion_mutex_.lock();
            tasks_completed_++;
            if (tasks_completed_ == num_total_tasks_) {
                task_completion_mutex_.unlock();
                std::unique_lock<std::mutex> lock(stop_work);
                tasks_done_ = true;
                work_complete.notify_one();
                

                
            } else {
                task_completion_mutex_.unlock();
            }
        }
    }
    return ;
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_=num_threads;
    num_total_tasks_=0;
    stop_spinning=false;
    tasks_done_=false;
    for(int i=0;i<num_threads_;i++){
        thread_pool_.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::runThreadEventLoop,this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    stop_spinning=true;
    start_work.notify_all();
    for(int i=0;i<num_threads_;i++){
        thread_pool_[i].join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    if (num_total_tasks == 0) {
        return;
    }

    {
        std::unique_lock<std::mutex> lk(work);
        runnable_ = runnable;
        num_total_tasks_ = num_total_tasks;
        task_id_ = 0;
        tasks_completed_ = 0;
        tasks_done_ = false;
        enter_loop_ = 0;
        start_work.notify_all();
    }

    std::unique_lock<std::mutex> lock(stop_work);
    work_complete.wait(lock, [&]{ return tasks_done_; });

}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
