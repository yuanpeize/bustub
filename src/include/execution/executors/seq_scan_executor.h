//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);


  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); }

 private:

  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;  
  // 通过plan->GetTableOid()获取table_oid 
  // 通过exec_ctx->GetCatalog()->GetTable(table_oid)获取TableInfo
  TableInfo *table_info_;
  // TableInfo里有一个成员变量 std::unique_ptr<TableHeap> table_
  // TableHeap里有两个成员函数：
  // -Begin() -> TableIterator;
  // -End() -> TableIterator;
  TableIterator table_iter_;  

  // 在进行全表扫描时，可能存在的情况
  // table_A
  // Sno Sname Sage Ssex Saddr
  //  1   Lily  20   1  shanghai
  //  ......

  // eg: select * from table_A （此时输入模式与输出模式一致）
  // eg: select Sname,Saddr from table_A （此时的输入模式与输出模式不一致）
  // is_same_schema_标志位：原表的schema与输出的schema是否一致
  // 如何获取原表的schema？ 从TableInfo中获取Schema
  bool is_same_schema_;  
  // 该函数用于判断原表模式与输出模式是否一致
  auto SchemaEqual(const Schema *table_schema, const Schema *output_schema) -> bool;
  // 如果原表模式与输出模式不同，则需要根据输出模式进行转换
  void TupleSchemaTranformUseEvaluate(const Tuple *table_tuple, const Schema *table_schema, Tuple *dest_tuple,
                                      const Schema *dest_schema);
};
}  // namespace bustub
