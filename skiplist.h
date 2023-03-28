#ifndef SkipList_H
#define SkipList_H
#include <iostream> 
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <mutex>
#include <fstream>
#include "log/log.h"
#define STORE_FILE "store/dumpFile"

std::mutex mtx;
std::string delimiter = ":";

template<typename K, typename V>
class Node{
public:
    Node(){}

    Node(K k, V v, int);
    
    ~Node();

    K get_key() const;

    V get_value() const;

    void set_value(V);

    Node<K,V> **forward;

    int node_level;

private:
    K key;
    V value;
};

template<typename K, typename V>
Node<K,V>::Node(const K k, const V v, int level){
    this->key = k;
    this->value = v;
    this->node_level = level;
    
      // level + 1, because array index is from 0 - level
    this->forward = new Node<K, V>*[level+1];

    // Fill forward array with 0(NULL)
    memset(this->forward,0,sizeof(Node<K,V>*)*(level+1));
};

template<typename K,typename V>
Node<K,V>::~Node(){
    delete []forward;
}

template<typename K, typename V>
void Node<K,V>::set_value(V value){
    this->value = value;
}

template<typename K, typename V>
K Node<K,V>::get_key() const{
    return key;
}

template<typename K,typename V>
V Node<K,V>::get_value() const{
    return value;
}


// Class template for Skip list
template <typename K, typename V>
class SkipList{
public:
    SkipList(int);
    ~SkipList();
    int get_random_level();
    Node<K,V>* create_node(K,V,int);
    int insert_element(K,V);
    void display_list();
    bool search_element(K);
    void delete_element(K);
    void dump_file();
    void load_file();
    int size();

private:
    void get_key_value_from_string(const std::string& str, std::string* key, std::string* value);
    bool is_valid_string(const std::string& str);
private:
    // Maximum level of the skip list 
    int _max_level;
    
    // current level of skip list 
    int _skip_list_level;
    
    // pointer to header node 
    Node<K,V> *_header;

    //file operator
    std::ofstream _file_writer;
    std::ifstream _file_reader;

    // skiplist current element count
    int _element_count;
};

// construct skip list
template<typename K, typename V> 
SkipList<K, V>::SkipList(int max_level) {

    this->_max_level = max_level;
    this->_skip_list_level = 0;
    this->_element_count = 0;

    // create header node and initialize key and value to null
    K k;
    V v;
    this->_header = new Node<K, V>(k, v, _max_level);
};

template<typename K, typename V> 
SkipList<K, V>::~SkipList() {

    if (_file_writer.is_open()) {
        _file_writer.close();
    }
    if (_file_reader.is_open()) {
        _file_reader.close();
    }
    delete _header;
}

template<typename K, typename V>
int SkipList<K, V>::get_random_level(){

    int k = 1;
    while (rand() % 2) {
        k++;
    }
    k = (k < _max_level) ? k : _max_level;
    return k;
};

//create new node 
template<typename K,typename V>
Node<K,V>* SkipList<K,V>::create_node(const K k, const V v, int level){
    Node<K,V>* node = new Node<K,V>(k,v,level);
    return node;
};

// Search for element in skip list 
/*
                           +------------+
                           |  select 60 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |
level 3         1+-------->10+------------------>50+           70       100
                                                   |
                                                   |
level 2         1          10         30         50|           70       100
                                                   |
                                                   |
level 1         1    4     10         30         50|           70       100
                                                   |
                                                   |
level 0         1    4   9 10         30   40    50+-->60      70       100
  每一个节点都有一个forward，forward是一个二级指针（存放指针的数组），例如 图中的10的forward为[&50,&30,&30] 
  而图中的40的forward为[&50].可以理解为forward是单链表next的数组
*/

template<typename K, typename V>
bool SkipList<K,V>::search_element(K key){
    std::cout << "search_element-----------------" << std::endl;
    Node<K,V> *current = _header;
     // for循环高度遍历，while循环水平遍历每一层。
    for (int i = _skip_list_level; i>=0; i--)
    {
       while (current->forward[i] && current->forward[i]->get_key() < key )
       {
            current = current->forward[i];
       }
    }
    
    current = current->forward[0];

    if (current && current->get_key() == key)
    {
        std::cout << "Found key: " << key << ", value: " << current->get_value() << std::endl;
        LOG_INFO("%s","Successfully Found");
        return true;
    }

    std::cout << "Not Found Key:" << key << std::endl;
    LOG_ERROR("%s","Failed Found");
    return false;
    
}



// Insert given key and value in skip list 
// return 1 means element exists  
// return 0 means insert successfully
/* 
                           +------------+
                           |  insert 50 |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |                      insert +----+
level 3         1+-------->10+---------------> | 50 |          70       100
                                               |    |
                                               |    |
level 2         1          10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 1         1    4     10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 0         1    4   9 10         30   40  | 50 |  60      70       100
                                               +----+
*/

template<typename K, typename V>
int SkipList<K,V>::insert_element(const K key, const V value){
    mtx.lock();
    Node<K,V> *current = this->_header;
     // create update array and initialize it 
    // update 是个节点数组，用于后续插入位置的前向链接 和 后向链接
    Node<K,V> *update[_max_level+1];
    memset(update,0,sizeof(Node<K, V>*)*(_max_level+1)); 
     // for循环高度遍历，while循环水平遍历每一层。
    for(int i = _skip_list_level;i>=0;i--){
        while (current->forward[i] != NULL && current->forward[i]->get_key() < key)
        {
            current = current->forward[i];
        }
        update[i] = current;        
    }
     //  定位到 == key 或者 > key 的 节点
    current = current->forward[0];
     // 如果current节点存在且和key相等，提示，并解锁
    if(current != NULL && current->get_key() == key){
        std::cout << "key: " << key <<  ", exists" << std::endl;
        LOG_ERROR("%s","Failed inserted");
        mtx.unlock();
        return 1;
    }

    // 否则，需要在update[0]和current node节点之间插入 [节点]
    if (current == NULL || current->get_key() != key)
    {
         // 随机生成节点的高度
        int random_level = get_random_level();

        // 如果随机高度比当前的跳表的高度大，update数组在多余高出来的部分保存头节点指针
        if (random_level > _skip_list_level)
        {
            for(int i = _skip_list_level +1; i < random_level + 1;i++){
                update[i] = _header;
            }
            _skip_list_level = random_level;
        }
        // 用随机生成的高度 创建 insert node
        Node<K,V>* inserted_node = create_node(key,value,random_level);

        // node节点 forword 指向 所在位置后面的节点指针
        // node节点 所在位置前面的节点 指向node
        //每一层都需要插入
        for(int i = 0; i<=random_level;i++){
            inserted_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = inserted_node;
        }
        

        std::cout << "Successfully inserted key:" << key << ", value:" << value << std::endl;
        
        LOG_INFO("%s","Successfully inserted");
        
        _element_count ++;
    }
    mtx.unlock();
    return 0;
}

// Display skip list 
template<typename K, typename V>
void SkipList<K,V>::display_list(){
    std::cout << "\n*****Skip List*****"<<"\n"; 
    for (int i = 0; i <= _skip_list_level; i++)
    {
        Node<K,V> *node = this->_header->forward[i];
        std::cout << "Level " << i <<": ";
        while (node != NULL)
        {
            std::cout << node->get_key() << ":" << node->get_value() << ";";
            node = node->forward[i];
        }
        std::cout << std::endl;
    }
    
}


// Dump data in memory to file
template<typename K, typename V>
void SkipList<K,V>::dump_file(){
    std::cout << "dump_file-----------------" << std::endl;
    _file_writer.open(STORE_FILE);
    Node<K,V> *node = this->_header->forward[0];

    while (node != NULL)
    {
        _file_writer << node->get_key() << ":" << node->get_value() << "\n";
        std::cout << node->get_key() << ":" << node->get_value() << ";\n";
        node = node->forward[0];
    }
    _file_writer.flush();
    _file_writer.close();
    return;
}

// Load data from disk
template<typename K, typename V> 
void SkipList<K, V>::load_file() {

    _file_reader.open(STORE_FILE);
    std::cout << "load_file-----------------" << std::endl;
    std::string line;
    std::string* key = new std::string();
    std::string* value = new std::string();
    while (getline(_file_reader, line)) {
        get_key_value_from_string(line, key, value);
        if (key->empty() || value->empty()) {
            continue;
        }
        insert_element(*key, *value);
        std::cout << "key:" << *key << "value:" << *value << std::endl;
    }
    _file_reader.close();
}

// Get current SkipList size 
template<typename K, typename V> 
int SkipList<K, V>::size() { 
    return _element_count;
}

template<typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string& str, std::string* key, std::string* value) {

    if(!is_valid_string(str)) {
        return;
    }
    *key = str.substr(0, str.find(delimiter));
    *value = str.substr(str.find(delimiter)+1, str.length());
}

template<typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string& str) {

    if (str.empty()) {
        return false;
    }
    if (str.find(delimiter) == std::string::npos) {
        return false;
    }
    return true;
}

template<typename K, typename V>
void SkipList<K,V>::delete_element(K key){
    mtx.lock();
    Node<K,V> *current = this->_header;
    Node<K,V> *update[_max_level+1];
    memset(update,0,sizeof(Node<K, V>*)*(_max_level+1));

    for (int i = _skip_list_level; i >= 0; i--)
    {
        while (current->forward[i]!=NULL && current->forward[i]->get_key() < key)
        {
            current = current->forward[i];
        }
        update[i] = current;
    }
    current = current->forward[0];
    if (current != NULL && current->get_key() == key)
    {
         // 从低级别开始删除每层的节点，
        for (int i = 0; i <= _skip_list_level; i++)
        {
            if(update[i]->forward[i] !=current)
                break;

             // 如果是要删除的节点 重新设置指针的指向，将其指向下一个节点的位置    
            update[i]->forward[i] = current->forward[i];
        }
        // 减少没有元素的层，更新跳表的level
        while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0)
        {
            _skip_list_level --;
        }
        
        std::cout <<  "Successfully deleted key "<< key << std::endl;
        _element_count --;
    }
    mtx.unlock();
    return;
    
};

#endif 