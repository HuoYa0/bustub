//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_.value().join();
  }
}

// 接收请求并放入请求队列
void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::optional<DiskRequest>(std::move(r))); }

// 从请求队列中获取新请求，并根据请求类型调用磁盘读写函数
void DiskScheduler::StartWorkerThread() {
  while (true) {
    std::optional<DiskRequest> request = request_queue_.Get();
    // std::nullopt in the queue means to exit the loop
    if (!request.has_value()) {
      break;
    }
    bustub::DiskRequest &request_val = request.value();
    if (request_val.is_write_) {
      disk_manager_->WritePage(request_val.page_id_, request_val.data_);
    } else {
      disk_manager_->ReadPage(request->page_id_, request->data_);
    }
    // 请求已处理完成，值设为true
    request->callback_.set_value(true);
  }
}

}  // namespace bustub
