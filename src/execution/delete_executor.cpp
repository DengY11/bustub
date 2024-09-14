//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  int32_t delete_count = 0;
  RID delete_rid;
  Tuple delete_tuple{};
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    TupleMeta meta = table_info_->table_->GetTupleMeta(delete_rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, delete_rid);
    ++delete_count;
    for (auto index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      auto index_key =
          delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(index_key, delete_rid, exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
