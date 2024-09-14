//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "catalog/catalog.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_oid_t tid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(tid);
  iterator_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iterator_->IsEnd()) {
    auto [meta, tuple_] = iterator_->GetTuple();
    if (meta.is_deleted_) {
      ++(*iterator_);
      continue;
    }
    *tuple = iterator_->GetTuple().second;
    *rid = iterator_->GetRID();
    ++(*iterator_);
    return true;
  }
  return false;
}

}  // namespace bustub
