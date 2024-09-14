//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())) {}

void InsertExecutor::Init() {
  child_->Init();
  // c++的多态机制，允许指向基类的指针调用派生类的override的该函数
  // 虽然 child_ 是一个指向 AbstractExecutor 类型的指针（std::unique_ptr<AbstractExecutor>），但实际运行时它可以指向
  // AbstractExecutor 的派生类对象。
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  [[maybe_unused]] int32_t insert_count = 0;
  RID emit_rid;
  Tuple to_insert_tuple{};
  TupleMeta meta{};
  while (child_->Next(&to_insert_tuple, &emit_rid)) {
    std::optional<RID> new_rid = table_info_->table_->InsertTuple(meta, to_insert_tuple, exec_ctx_->GetLockManager(),
                                                                  exec_ctx_->GetTransaction(), table_info_->oid_);
    if (!new_rid) {
      continue;
    }
    ++insert_count;
    for (auto index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      auto index_key = to_insert_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                    index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(index_key, new_rid.value(), exec_ctx_->GetTransaction());
    }
  }
  std::vector<Value> values{};
  values.emplace_back(TypeId::INTEGER, insert_count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
