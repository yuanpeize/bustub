//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <stack>

#include "common/exception.h"
#include "common/rwlatch.h"

namespace bustub {

class TrieNode
{
public:
    explicit TrieNode(char key_char) : key_char_(key_char) {}

    TrieNode(TrieNode &&other_trie_node) noexcept
    {
        key_char_ = other_trie_node.key_char_;
        is_end_ = other_trie_node.is_end_;
        children_ = std::move(other_trie_node.children_);
    }

    virtual ~TrieNode() = default;

    bool HasChild(char key_char) const { 
        return this->children_.find(key_char) != this->children_.end(); 
    }

    bool HasChildren() const { 
        return !this->children_.empty(); 
    }

    bool IsEndNode() const { 
        return this->is_end_; 
    }

    char GetKeyChar() const { 
        return key_char_; 
    }

    std::unordered_map<char, std::unique_ptr<TrieNode>> &GetChildren() { 
        return children_; 
    }

    std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child)
    {
        if (this->children_.count(key_char) != 0 || child->GetKeyChar() != key_char)
        {
            return nullptr;
        }
        this->children_[key_char] = std::move(child);
        return &this->children_[key_char];
    }

    std::unique_ptr<TrieNode> *GetChildNode(char key_char)
    {
        if (this->children_.count(key_char) == 0)
        {
            return nullptr;
        }
        return &this->children_[key_char];
    }

    void RemoveChildNode(char key_char)
    {
        if (this->children_.count(key_char) == 0)
        {
            return;
        }
        this->children_.erase(key_char);
    }

    void SetEndNode(bool is_end) { 
        this->is_end_ = is_end; 
    }

protected:
    char key_char_;
    bool is_end_{false};
    std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};


template <typename T>
class TrieNodeWithValue : public TrieNode
{
private:
    T value_;

public:
    TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::move(trieNode))
    {
        this->value_ = value;
        this->is_end_ = true;
    }

    TrieNodeWithValue(char key_char, T value) : TrieNode(key_char)
    {
        this->value_ = value;
        this->is_end_ = true;
    }

    ~TrieNodeWithValue() override = default;

    T GetValue() const { return value_; }
};


class Trie
{
private:
    std::unique_ptr<TrieNode> root_;

public:
    Trie() 
    { 
        this->root_ = std::make_unique<TrieNode>('\0'); 
    }

    template <typename T>
    bool Insert(const std::string &key, T value)
    {
        if (key.empty())
        {
            return false;
        }
        const size_t key_size = key.size();
        auto matched_node = &this->root_;
        
        for (size_t i = 0; i < key_size; i++)
        {
            if (matched_node->get()->GetChildNode(key[i]) == nullptr)
            {
                matched_node->get()->InsertChildNode(key[i], std::make_unique<TrieNode>(key[i]));
            }
            auto father_node = matched_node;
            matched_node = matched_node->get()->GetChildNode(key[i]);
            
            if (i == key_size - 1)
            {
                if (matched_node->get()->IsEndNode())
                {
                    break;
                }
                auto tmp_node = std::move(*matched_node);
                father_node->get()->RemoveChildNode(key[i]);
                auto new_node = std::make_unique<TrieNodeWithValue<T>>(std::move(*tmp_node), value);
                father_node->get()->InsertChildNode(key[i], std::move(new_node));
                return true;
            }
        }
        return false;
    }

    bool Remove(const std::string &key)
    {
        size_t key_size = key.size();
        auto node_stack = std::stack<std::unique_ptr<TrieNode> *>();
        auto matched_node = &this->root_;
        for (size_t i = 0; i < key_size; i++)
        {
            char key_char = key[i];
            if (matched_node->get()->GetChildNode(key_char) == nullptr)
            {
                return false;
            }
            node_stack.push(matched_node);
            matched_node = matched_node->get()->GetChildNode(key_char);
            
            if (i == key_size - 1)
            {
                if (!matched_node->get()->IsEndNode())
                {
                    break;
                }
                if (matched_node->get()->HasChildren())
                {
                    return true;
                }
                node_stack.top()->get()->RemoveChildNode(key_char);
                while (node_stack.size() > 1)
                {
                    auto node = node_stack.top();
                    if (node->get()->HasChildren() || node->get()->IsEndNode())
                    {
                        break;
                    }
                    node_stack.pop();
                    node_stack.top()->get()->RemoveChildNode(node->get()->GetKeyChar());
                }
                return true;
            }
        }
        return false;
    }


    template <typename T>
    T GetValue(const std::string &key, bool *success)
    {
        auto cur_node = &this->root_;
        size_t key_size = key.size();
        for (size_t i = 0; i < key_size; i++)
        {
            char cur_char = key[i];
            if (cur_node->get()->GetChildNode(cur_char) == nullptr)
            {
                break;
            }
            cur_node = cur_node->get()->GetChildNode(cur_char);
            
            if (i == key_size - 1)
            {
                if (cur_node->get()->IsEndNode())
                {
                    auto tmp_node = dynamic_cast<TrieNodeWithValue<T> *>(cur_node->get());
                    if (tmp_node != nullptr)
                    {
                        *success = true;
                        return tmp_node->GetValue();
                    }
                }
            }
        }
        *success = false;
        return T();
    }
};

}  // namespace bustub