//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    pages_num_ = num_pages;
}

LRUReplacer::~LRUReplacer(){
    map_frames_.clear();
    list_unpinned_.clear();
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (map_frames_.empty()) {
    return false;
  }

  *frame_id = list_unpinned_.back();
  list_unpinned_.pop_back();
  map_frames_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (map_frames_.count(frame_id) == 0) {
    return;
  }
  auto p = map_frames_[frame_id];
  list_unpinned_.erase(p);
  map_frames_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(latch_);
  if (map_frames_.count(frame_id) != 0 ) {
    return;
  }
  list_unpinned_.push_front(frame_id);
  map_frames_.emplace(frame_id, list_unpinned_.begin());
}

auto LRUReplacer::Size() -> size_t {
     return map_frames_.size(); 
}

}  // namespace bustub




//*************************************************************************

//                     方法二：时间戳

//************************************************************************

// #include "buffer/lru_replacer.h"
// #include <cassert>

// namespace bustub {

// LRUReplacer::LRUReplacer(size_t num_pages) : timestamp(1) {}

// LRUReplacer::~LRUReplacer() = default;

// bool LRUReplacer::Victim(frame_id_t *frame_id) {
//   if (frame_map.empty()) {
//     *frame_id = -1;
//     return false;
//   }

//   replacer_mutex.lock();

//   // initialize the first is the answer
//   int64_t min_timestamp = frame_map.begin()->second;
//   frame_id_t min_frame_id = frame_map.begin()->first;

  
//   // find the min_frame_id
//   for (const auto &p : frame_map) {
//     if (p.second < min_timestamp) {
//       min_timestamp = p.second;
//       min_frame_id = p.first;
//     }
//   }

//   auto ret = frame_map.erase(min_frame_id);
//   assert(ret > 0);

//   *frame_id = min_frame_id;

//   replacer_mutex.unlock();

//   return true;
// }

// void LRUReplacer::Pin(frame_id_t frame_id) {
//   replacer_mutex.lock();

//   frame_map.erase(frame_id);

//   replacer_mutex.unlock();
// }

// void LRUReplacer::Unpin(frame_id_t frame_id) {
//   replacer_mutex.lock();

//   frame_map.insert({frame_id, timestamp++});

//   replacer_mutex.unlock();
// }

// size_t LRUReplacer::Size() { return frame_map.size(); }

// }  // namespace bustub
