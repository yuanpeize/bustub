//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {


template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  //判断某一个key是否在某一个bucket中
  bool flag = false;  // 标志位：是否找到，初始化为没有找到fasle

  // #define BUCKET_ARRAY_SIZE (PAGE_SIZE / (sizeof(MappingType) + 0.25))
  // 0.25是从哪里来的？
  // 每一个slot有两个标志位，标记曾经是否放过数据，现在里面有没有数据。每一个标记位用unsigned char（8位）中的一位表示 0.25byte
  uint32_t array_size = BUCKET_ARRAY_SIZE;  // the number of (key, value) pairs that can be stored in an extendible hashing bucket page.
  for (uint32_t i = 0; i < array_size; i++) {
    // slot槽中有数据且key值相同，则返回true
    if (IsReadable(i) && cmp(array_[i].first, key) == 0) {
      result->emplace_back(array_[i].second);
      flag = true;
    } else if (!IsOccupied(i)) {  // 提前结束寻找 因为这个位置从来都没有放过数据，那么从这个slot往后的slot必然都是空的，没有必要再遍历
      break;
    }
  }
  return flag;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  uint32_t array_size = BUCKET_ARRAY_SIZE;
  // pos的范围是0-array_size-1，表示可以插入的slot的下标
  // 初始化pos的值是一个非法的位置 -1/array_size都可
  uint32_t pos = array_size;
  // 注意：如果bucket_page里已经有这个(key,value)，不能重复插入可以允许有相同的key，但不允许有相同的(key，value)
  // 注意：当你找到一个空slot时，你可以把这个slot的index记录下来，但是你仍然需要遍历完所有的slot,以确保该条数据没有被插入过    

  for (uint32_t i = 0; i < array_size; i++) {
    // 如果待插入的(key,value)已经存在，返回false
    if (IsReadable(i) && cmp(array_[i].first, key) == 0 && array_[i].second == value) {  
      return false;
    }
    if (!IsReadable(i)) { // 找到了空slot
      // 遍历整个slot的过程中，空闲的槽可能不止一个，我们只需要记录下第一个可插入的slot的index即可
      if (pos == array_size) {  
        pos = i;  // 把首个空槽的index记录下来
      }
      if (!IsOccupied(i)) {  // 提前结束
        break;
      }
    }
  }

  // 以上过程只是遍历了所有的slot，不一定找到了一个空槽
  // 如果bucket已经满了，暂时不能插入，返回fasle
  // 返回false后，会触发扩容机制，属于可动态扩展哈希范畴，具体处理过程见extendible_hash_table.cpp
  // 我们这里只是原先的bucket里能不能插入的问题
  if (pos == array_size) {  
    return false;
  }

  // 此时说明找到了空槽
  // 插入(key,value)
  array_[pos].first = key;
  array_[pos].second = value;
  // 插入的同时要修改两个标志位
  SetOccupied(pos);
  SetReadable(pos);
  return true;
}



template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  
  // 这个函数只是删除了某一条记录
  // 至于删除后，该bucket是否为空，需不需要合并，这是动态可扩展哈希的内容，具体见extendible_hash_table.cpp
  uint32_t array_size = BUCKET_ARRAY_SIZE;
  for (uint32_t i = 0; i < array_size; i++) {
    if (IsReadable(i) && cmp(array_[i].first, key) == 0 && array_[i].second == value) {
      //如果找到了该条记录，修改标志位即可
      //逻辑上删除
      SetUnreadable(i);  
      return true;
    }
    if (!IsOccupied(i)) {  // 提前结束
      break;
    }
  }
  return false;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  //获取bucket中下标为index的槽中的数据的key
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].first;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  // 获取bucket中下标为index的槽中的数据的value
  if (IsReadable(bucket_idx)) {
    return array_[bucket_idx].second;
  }
  return {};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // 删除bucket中下标为index的槽中的数据
  SetUnreadable(bucket_idx); //修改标志位即可
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  // 判断bucket中下标为index的槽是否曾经放过数据
  // char occupied_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1]; //向上取整
  // unsiged char(8bit)记录了8个slot的标志位0/1
  uint32_t index = bucket_idx / 8; // char数组的下标
  uint32_t offset = bucket_idx % 8; // offset是char的index，最低位offset为0
  return static_cast<bool>(occupied_[index] & (1 << offset));
}


template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  //eg:offset=3
  //   00001000
  occupied_[index] |= 1 << offset;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  return static_cast<bool>(readable_[index] & (1 << offset));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  readable_[index] |= 1 << offset;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnreadable(uint32_t bucket_idx) {
  uint32_t index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  //eg:offset=3  00001000 111101111
  readable_[index] &= ~(1 << offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  //判断这个bucket是否已满

  uint32_t exact_div_size = BUCKET_ARRAY_SIZE / 8;  // 整除的部分应该全部为ff
  for (uint32_t i = 0; i < exact_div_size; i++) {
    // readable_[i]类型为unsigned char，故可以直接与0xff比较
    if (readable_[i] != 0xff) { //说明至少某一位为0
      return false;
    }
  }

  uint32_t rest = BUCKET_ARRAY_SIZE - BUCKET_ARRAY_SIZE / 8 * 8;  // 只有rest个位为1
  unsigned char expect_value = (1 << rest) - 1;
  //1. rest=0，返回true
  //2. rest!=0 
  //2.1 readable_[(BUCKET_ARRAY_SIZE - 1) / 8] == expect_value，返回true
  //2.2 readable_[(BUCKET_ARRAY_SIZE - 1) / 8] != expect_value，返回false
  return !(rest != 0 && readable_[(BUCKET_ARRAY_SIZE - 1) / 8] != expect_value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  //统计bucket中现在有多少个slot存放着数据
  uint32_t cnt = 0;
  uint32_t array_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1; //向上取整
  unsigned char n;
  for (uint32_t i = 0; i < array_size; i++) {
    n = readable_[i];
    //统计unsiged char(8 bit)中1的个数
    while (n != 0) {
      n &= n - 1; //每次消去最低位的1
      cnt++;
    }
  }
  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Size() -> uint32_t {  
  //统计bucket中slot的个数
  return BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  //判断bucket是否为空
  // 如果为空,可读标记位全为0
  uint32_t array_size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;//向上取整
  for (uint32_t i = 0; i < array_size; i++) {
    if (readable_[i] != 0) { 
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
