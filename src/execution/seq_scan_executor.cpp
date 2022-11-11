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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iter_(nullptr, RID(), nullptr) {}

auto SeqScanExecutor::SchemaEqual(const Schema *table_schema, const Schema *output_schema) -> bool {
  auto table_colums = table_schema->GetColumns();
  auto output_colums = output_schema->GetColumns();
  
  if (table_colums.size() != output_colums.size()) {
    return false;
  }

  int col_size = table_colums.size();
  uint32_t offset1;
  uint32_t offset2;
  std::string name1;
  std::string name2;
  
  for (int i = 0; i < col_size; i++) {
    offset1 = table_colums[i].GetOffset();
    offset2 = output_colums[i].GetOffset();
    name1 = table_colums[i].GetName();
    name2 = output_colums[i].GetName();
    if (name1 != name2 || offset1 != offset2) {
      return false;
    }
  }
  return true;
}


void SeqScanExecutor::TupleSchemaTranformUseEvaluate(const Tuple *table_tuple, const Schema *table_schema,
                                                     Tuple *dest_tuple, const Schema *dest_schema) {

  auto columns = dest_schema->GetColumns(); 
  std::vector<Value> dest_value;
  dest_value.reserve(columns.size());
  
  for (const auto &col : columns) {
    dest_value.emplace_back(col.GetExpr()->Evaluate(table_tuple, table_schema));
  }
  
  *dest_tuple = Tuple(dest_value, dest_schema);
}


void SeqScanExecutor::Init() {

  auto table_oid = plan_->GetTableOid(); //获取待全表扫描的页的唯一标识table_id
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid); //获取catalog=>获取table_info
  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); //获取table_heap=>获取table_iter

  auto table_schema = table_info_->schema_; //获取原表的schema
  auto output_schema = plan_->OutputSchema(); //获取输出schema

  //判断输出模式与原表模式是否相等
  is_same_schema_ = SchemaEqual(&table_schema, output_schema);
  
  auto transaction = exec_ctx_->GetTransaction(); //The transaction executing the query
  auto lockmanager = exec_ctx_->GetLockManager(); //The lock manager that the executor uses
  
  if (transaction->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    //给所有的元组加读锁，当事务提交后再解锁
    auto iter = table_info_->table_->Begin(exec_ctx_->GetTransaction());
    while (iter != table_info_->table_->End()) {
      lockmanager->LockShared(transaction, iter->GetRid());
      ++iter;
    }
  }
}


auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  
  auto predicate = plan_->GetPredicate(); //这个predicate用于筛选记录
  auto table_schema = table_info_->schema_;
  auto output_schema = plan_->OutputSchema();
  auto transaction = exec_ctx_->GetTransaction();
  auto lockmanager = exec_ctx_->GetLockManager();
  
  bool res;

  while (table_iter_ != table_info_->table_->End()) {
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lockmanager->LockShared(transaction, table_iter_->GetRid());
    }

    auto p_tuple = &(*table_iter_);  // 获取指向元组的指针
    res = true;
    if (predicate != nullptr) {
      res = predicate->Evaluate(p_tuple, &table_schema).GetAs<bool>();
    }

    if (res) {
      if (!is_same_schema_) {
        TupleSchemaTranformUseEvaluate(p_tuple, &table_schema, tuple, output_schema);
      } else {
        *tuple = *p_tuple;
      }
      *rid = p_tuple->GetRid();  // 返回表元组的ID
    }

    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lockmanager->Unlock(transaction, table_iter_->GetRid());
    }

    ++table_iter_;  // 指向下一位置后再返回
    
    if (res) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
