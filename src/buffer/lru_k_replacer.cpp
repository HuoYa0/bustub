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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

// 从内存中淘汰某个帧
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);
  LRUKNode *targetNode;
  int max_time = this.current_timestamp_;
  // 遍历not_k_map找最早进入的node进行返回
  for (const auto &pair : this.not_k_map) {
    if (pair.second.evictable == false) continue;
    if (pair.second.init_timestamp < max_time) {
      targetNode = &pair.second;
      max_time = pair.second.init_timestamp;
    }
  }
  if (targetNode) {
    frame_id_t id = targetNode.frame_id;
    this.not_k_map.erase(id);
    return id;
  }
  // 遍历k_map找最早进入的node进行返回
  for (const auto &pair : this.k_map) {
    if (pair.second.init_timestamp < max_time) {
      targetNode = &pair.second;
      max_time = pair.second.init_timestamp;
    }
  }
  if (targetNode) {
    frame_id_t id = targetNode.frame_id;
    this.k_map.erase(id);
    return id;
  }
  // not_k_map和k_map都为空 返回null
  else
    return std::nullopt;
}

// 通过frame_id访问某个帧，并进行记录。frame_id为新时需要创建新node
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  this.current_timestamp_++;
  // If frame id is invalid  throw an exception.
  if (frame_id >= this.replacer_size_ || frame_id < 0) throw -1;
  // 在k_map中
  if (this.k_map.find(frame_id) != this.k_map.end()) {
    auto node = this.k_map.find(frame_id)->second node.last_visit_timestamp = this.current_timestamp_;
    node.access_time++;
  } else if (this.not_k_map.find(frame_id) != this.not_k_map.end()) {
    // 在not_k_map中
    auto node = this.not_k_map.find(frame_id)->second;
    node.last_visit_timestamp = this.current_timestamp_;
    node.access_time++;
    if (node.access_time >= this.k_) {
      this.k_map.insert({frame_id, node});
      this.not_k_map.erase(frame_id);
    }
  }
  // 新node ，没有空间，删一个
  else if (TotalSize() >= this.replacer_size_)
    Evict();
  this.k_map.insert({frame_id, LRUKNode(frame_id, this.current_timestamp_)});
}

// 将一个帧设置为指定的evictable，即pin住
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id >= this.replacer_size_ || frame_id < 0) throw -1;
  if (this.k_map.find(frame_id) != this.k_map.end()) {
    auto node = this.k_map.find(frame_id)->second;
    node.evictable = set_evictable;
  } else if (this.not_k_map.find(frame_id) != this.not_k_map.end()) {
    auto node = this.not_k_map.find(frame_id)->second;
    node.evictable = set_evictable;
  }
}

// 删除指定frame，但它得是能删的
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id >= this.replacer_size_ || frame_id < 0) throw -1;
  if (this.k_map.find(frame_id) != this.k_map.end()) {
    auto node = this.k_map.find(frame_id)->second;
    if (node.evictable == false)
      throw -1;
    else
      this.k_map.erase(frame_id);

  } else if (this.not_k_map.find(frame_id) != this.not_k_map.end()) {
    auto node = this.not_k_map.find(frame_id)->second;
    if (node.evictable == false)
      throw -1;
    else
      this.not_k_map.erase(frame_id);
  }
}

// 可淘汰帧的数量
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  size_t size = 0;
  for (const auto &pair : this.not_k_map) {
    if (pair.second.evictable) size++
  }
  for (const auto &pair : this.k_map) {
    if (pair.second.evictable) size++
  }
  return size;
}

// 所有帧的数量
auto LRUKReplacer::TotalSize() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return this.not_k_map.size() + this.k_map.size();
}

}  // namespace bustub
