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
#include <iostream>
#include "common/exception.h"
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k), replacer_size_(num_frames) {}

// 从内存中淘汰某个帧
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  this->current_timestamp_++;
  std::lock_guard<std::mutex> lock(latch_);
  bustub::LRUKNode *target_node = nullptr;
  size_t max_time = this->current_timestamp_;
  // 遍历not_k_map_找最早进入的node进行返回
  for (const auto &pair : this->not_k_map_) {
    if (!pair.second.evictable) {
      continue;
    }
    if (pair.second.init_timestamp <= max_time) {
      target_node = const_cast<bustub::LRUKNode *>(&pair.second);
      max_time = pair.second.init_timestamp;
    }
  }
  if (target_node != nullptr) {
    frame_id_t id = target_node->frame_id;
    this->not_k_map_.erase(id);
    // std::cout<<"not_k_map_ size: "<<this->not_k_map_.size()<<std::endl;
    return id;
  }
  // 遍历k_map找最晚访问进入的node进行返回
  for (const auto &pair : this->k_map_) {
    if (!pair.second.evictable) {
      continue;
    }
    if (pair.second.last_visit_timestamp < max_time) {
      target_node = const_cast<bustub::LRUKNode *>(&pair.second);
      max_time = pair.second.last_visit_timestamp;
    }
  }
  if (target_node != nullptr) {
    frame_id_t id = target_node->frame_id;
    this->k_map_.erase(id);
    // std::cout<<"k_map_ size: "<<this->k_map_.size()<<std::endl;
    return id;
  }
  // not_k_map_和k_map都为空 返回null
  return std::nullopt;
}

// 通过frame_id访问某个帧，并进行记录。frame_id为新时需要创建新node
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
        // std::cout<<"begin:"<<frame_id<<std::endl;
        // std::cout<<"not_k_map_ size: "<<this->not_k_map_.size()<<std::endl;
        // std::cout<<"k_map_ size:"<<this->k_map_.size()<<std::endl;
  std::lock_guard<std::mutex> lock(latch_);
  this->current_timestamp_++;
  // If frame id is invalid  throw an exception.
  if (static_cast<size_t>(frame_id) >= this->replacer_size_ || frame_id < 0) {
    throw -1;
  }
  // 在k_map中
  if (this->k_map_.find(frame_id) != this->k_map_.end()) {
    this->k_map_[frame_id].last_visit_timestamp = this->current_timestamp_;
    this->k_map_[frame_id].access_time++;
  } else if (this->not_k_map_.find(frame_id) != this->not_k_map_.end()) {
    // 在not_k_map_中
    this->not_k_map_[frame_id].last_visit_timestamp = this->current_timestamp_;
    this->not_k_map_[frame_id].access_time++;
    if (this->not_k_map_[frame_id].access_time >= this->k_) {
      LRUKNode node = this->not_k_map_[frame_id];
      this->k_map_.insert({frame_id, node});
      this->not_k_map_.erase(frame_id);
    }
  }
  // 新node ，没有空间，删一个
  else if (TotalSize() >= this->replacer_size_) {
    Evict();
  }
  else {
    this->not_k_map_.insert({frame_id, LRUKNode(frame_id, this->current_timestamp_)});
  }
        // std::cout<<"end"<<std::endl;
        // std::cout<<"not_k_map_ size: "<<this->not_k_map_.size()<<std::endl;
        // std::cout<<"k_map_ size: "<<this->k_map_.size()<<std::endl;
        // std::cout<<"----------"<<std::endl;

}

// 将一个帧设置为指定的evictable，即pin住
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= this->replacer_size_ || frame_id < 0) {
    throw -1;
  }
  if (this->k_map_.find(frame_id) != this->k_map_.end()) {
     this->k_map_[frame_id].evictable=set_evictable;
  } else if (this->not_k_map_.find(frame_id) != this->not_k_map_.end()) {
    this->not_k_map_[frame_id].evictable=set_evictable;
  }
}

// 删除指定frame，但它得是能删的
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= this->replacer_size_ || frame_id < 0) {
    throw -1;
  }
  if (this->k_map_.find(frame_id) != this->k_map_.end()) {
    auto node = this->k_map_.find(frame_id)->second;
    if (!node.evictable) {
      throw -1;
    }
    this->k_map_.erase(frame_id);
  } else if (this->not_k_map_.find(frame_id) != this->not_k_map_.end()) {
    auto node = this->not_k_map_.find(frame_id)->second;
    if (!node.evictable) {
      throw -1;
    }
    this->not_k_map_.erase(frame_id);
  }
}

// 可淘汰帧的数量
auto LRUKReplacer::Size() -> size_t {
  int size = 0;
  for (const auto &pair : this->not_k_map_) {
    if (pair.second.evictable) {
      size++;
    }
  }
  for (const auto &pair : this->k_map_) {
    if (pair.second.evictable) {
      size++;
    }
  }
  return size;
}

// 所有帧的数量
auto LRUKReplacer::TotalSize() -> size_t { return this->not_k_map_.size() + this->k_map_.size(); }

}// namespace bustub