#ifndef BLOCKQUEUE_H
#define BLOCKQUEUE_H

#include<mutex>
#include<deque>
#include<condition_variable>
#include<sys/time.h>

template<class T>
class BlockDeque{
public:
    explicit BlockDeque(size_t MaxCapacity = 1000);
    
    ~BlockDeque();

    void clear();

    bool empty();

    bool full();

    void Close();

    size_t size() const;

    size_t capacity() const;

    T front();

    T back();

    void push_back(const T &item);

    void push_front(const T &itme);

    bool pop(T &item);

    bool pop(T &item,int timeout);

    void flush();

private:
    std::deque<T> deq_;

    size_t capacity_;

    std::mutex mtx_;

    bool isClose_;

    std::condition_variable condConsumer_;

    std::condition_variable condProducer_;
};

template<class T>
BlockDeque<T>::BlockDeque(size_t MaxCapacity):capacity_(MaxCapacity){
    //assert(MaxCapacity > 0);
    isClose_ = false;
}

template<class T>
BlockDeque<T>::~BlockDeque(){
    Close();
}

template<class T>
void BlockDeque<T>::Close(){
    {
        std::lock_guard<std::mutex> locker(mtx_);
        deq_.clear();
        isClose_ = true;
    }
    condProducer_.notify_all();
    condConsumer_.notify_all();
}

template<class T>
void BlockDeque<T>::flush(){
    condConsumer_.notify_one();
}

template<class T>
void BlockDeque<T>::clear() {
    std::lock_guard<std::mutex> locker(mtx_);
    deq_.clear();
}

template<class T>
T BlockDeque<T>::front() {
    std::lock_guard<std::mutex> locker(mtx_);
    return deq_.front();
}

template<class T>
T BlockDeque<T>::back() {
    std::lock_guard<std::mutex> locker(mtx_);
    return deq_.back();
}

template<class T>
size_t BlockDeque<T>::size() const {
    std::lock_guard<std::mutex> locker(mtx_);
    return deq_.size();
}

template<class T>
size_t BlockDeque<T>::capacity() const{
    std::lock_guard<std::mutex> locker(mtx_);
    return capacity_;
}

template<class T>
void BlockDeque<T>::push_back(const T &item){
    std::unique_lock<std::mutex> locker(mtx_);
    // 如果队列已满，生产者将阻塞
    while (deq_.size() >= capacity_)
    {
        condProducer_.wait(locker);
    }
    // 队列未满，则入队，并通知消费者
    deq_.push_back(item);
    std::cout << "deque push" << std::endl;
    condConsumer_.notify_one();
    
}

template<class T>
void BlockDeque<T>::push_front(const T &item) {
    std::unique_lock<std::mutex> locker(mtx_);
    while(deq_.size() >= capacity_) {
        condProducer_.wait(locker);
    }
    deq_.push_front(item);
    condConsumer_.notify_one();
}

template<class T>
bool BlockDeque<T>::empty() {
    std::lock_guard<std::mutex> locker(mtx_);
    return deq_.empty();
}

template<class T>
bool BlockDeque<T>::full(){
    std::lock_guard<std::mutex> locker(mtx_);
    return deq_.size() >= capacity_;
}

template<class T>
bool BlockDeque<T>::pop(T &item){
    std::unique_lock<std::mutex> locker(mtx_);
    while (deq_.empty())
    {
        //如果队列为空，消费者将阻塞
        condConsumer_.wait(locker);
        if(isClose_){
            return false;
        }
    }
    item = deq_.front();
    deq_.pop_front();
    std::cout << "deque pop" << std::endl;
    // 通知生产者
    condProducer_.notify_one();
    return true;
}

//超时处理，避免等待线程永久地阻塞在条件变量上
template<class T>
bool BlockDeque<T>::pop(T &item,int timeout){
    std::unique_lock<std::mutex> locker(mtx_);
    while (deq_.empty())
    {
        if(condConsumer_.wait_for(locker,std::chrono::seconds(timeout)) == std::cv_status::timeout){
            return false;
        }
        if(isClose_){
            return false;
        }
    }
    item = deq_.front();
    deq_.pop_front();
    condProducer_.notify_one();
    return true;
}


#endif // BLOCKQUEUE_H