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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
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
