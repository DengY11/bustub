//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  is_accessible_.resize(num_frames + 1);
  current_size_ = 0;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  *frame_id = 0;
  if (history_map_.empty() && cache_map_.empty()) {
    return false;
  }

  auto it = history_list_.end();
  while (it != history_list_.begin()) {
    it--;
    if (!is_accessible_[*it]) {
      continue;
    }
    history_map_.erase(*it);
    *frame_id = *it;
    use_count_[*it] = 0;
    is_accessible_[*it] = false;
    current_size_--;
    history_list_.erase(it);
    return true;
  }
  it = cache_list_.end();
  while (it != cache_list_.begin()) {
    it--;
    if (!is_accessible_[*it]) {
      continue;
    }
    *frame_id = *it;
    use_count_[*it] = 0;
    current_size_--;
    is_accessible_[*it] = false;
    cache_map_.erase(*it);
    cache_list_.erase(it);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  use_count_[frame_id]++;
  if (use_count_[frame_id] == k_) {
    if (history_map_.count(frame_id) != 0U) {
      auto it = history_map_[frame_id];
      history_list_.erase(it);
    }
    history_map_.erase(frame_id);
    // 2. add frame_id into cache list
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else if (use_count_[frame_id] > k_) {
    if (cache_map_.count(frame_id) != 0U) {
      auto it = cache_map_[frame_id];
      cache_list_.erase(it);
    }
    // 2. add frame_id into cache list
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else {
    if (history_map_.count(frame_id) == 0U) {
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  if (use_count_[frame_id] == 0) {
    return;
  }
  if (!is_accessible_[frame_id] && set_evictable) {
    current_size_++;
  }
  if (is_accessible_[frame_id] && !set_evictable) {
    current_size_--;
  }
  is_accessible_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (!is_accessible_[frame_id]) {
    return;
  }
  if (frame_id > static_cast<int>(replacer_size_)) {
    return;
  }
  if (use_count_[frame_id] < k_) {
    auto it = history_map_[frame_id];
    history_list_.erase(it);
    history_map_.erase(frame_id);
  } else {
    auto it = cache_map_[frame_id];
    cache_list_.erase(it);
    cache_map_.erase(frame_id);
  }
  use_count_[frame_id] = 0;
  is_accessible_[frame_id] = false;
  current_size_--;
}

auto LRUKReplacer::Size() -> size_t { return current_size_; }

}  // namespace bustub
