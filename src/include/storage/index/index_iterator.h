//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *bpm, const B_PLUS_TREE_LEAF_PAGE_TYPE *page, int index, ReadPageGuard page_guard);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return (itr).page_ == page_ && (itr).index_ == index_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return !((itr).page_ == page_ && (itr).index_ == index_); }
  ReadPageGuard page_guard_;

 private:
  // add your own private member variables here
  const B_PLUS_TREE_LEAF_PAGE_TYPE *page_{nullptr};
  int index_{INVALID_PAGE_ID};
  BufferPoolManager *bpm_{nullptr};
};

}  // namespace bustub
