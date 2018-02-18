/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.path;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.HashMap;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

class TrieNode<T>{
    
    private transient String key;
    private transient T value;
    private boolean isWildcard;
    private final String wildcard;

    private transient String namedWildcard;

    private Map<String, TrieNode<T>> children;

    public TrieNode(String key, T value, String wildcard) {
        this.key = key;
        this.wildcard = wildcard;
        this.isWildcard = (key.equals(wildcard));
        this.value = value;
        this.children = emptyMap();
        if (isNamedWildcard(key)) {
            namedWildcard = key.substring(key.indexOf('{') + 1, key.indexOf('}'));
        } else {
            namedWildcard = null;
        }
    }

    public void updateKeyWithNamedWildcard(String key) {
        this.key = key;
        namedWildcard = key.substring(key.indexOf('{') + 1, key.indexOf('}'));
    }

    public boolean isWildcard() {
        return isWildcard;
    }

    public synchronized void addChild(TrieNode<T> child) {
        addInnerChild(child.key, child);
    }

    private void addInnerChild(String key, TrieNode<T> child) {
        Map<String, TrieNode<T>> newChildren = new HashMap<>(children);
        newChildren.put(key, child);
        children = unmodifiableMap(newChildren);
    }

    public TrieNode<T> getChild(String key) {
        return children.get(key);
    }

    public synchronized void insert(String[] path, int index, T value) {
        if (index >= path.length)
            return;

        String token = path[index];
        String key = token;
        if (isNamedWildcard(token)) {
            key = wildcard;
        }
        TrieNode<T> node = children.get(key);
        if (node == null) {
            T nodeValue = index == path.length - 1 ? value : null;
            node = new TrieNode(token, nodeValue, wildcard);
            addInnerChild(key, node);
        } else {
            if (isNamedWildcard(token)) {
                node.updateKeyWithNamedWildcard(token);
            }
            /*
             * If the target node already exists, but is without a value,
             *  then the value should be updated.
             */
            if (index == (path.length - 1)) {
                if (node.value != null) {
                    throw new IllegalArgumentException("Path [" + String.join("/", path)+ "] already has a value ["
                            + node.value + "]");
                } else {
                    node.value = value;
                }
            }
        }

        node.insert(path, index + 1, value);
    }

    public synchronized void insertOrUpdate(String[] path, int index, T value, BiFunction<T, T, T> updater) {
        if (index >= path.length)
            return;

        String token = path[index];
        String key = token;
        if (isNamedWildcard(token)) {
            key = wildcard;
        }
        TrieNode<T> node = children.get(key);
        if (node == null) {
            T nodeValue = index == path.length - 1 ? value : null;
            node = new TrieNode(token, nodeValue, wildcard);
            addInnerChild(key, node);
        } else {
            if (isNamedWildcard(token)) {
                node.updateKeyWithNamedWildcard(token);
            }
            /*
             * If the target node already exists, but is without a value,
             *  then the value should be updated.
             */
            if (index == (path.length - 1)) {
                if (node.value != null) {
                    node.value = updater.apply(node.value, value);
                } else {
                    node.value = value;
                }
            }
        }

        node.insertOrUpdate(path, index + 1, value, updater);
    }

    private boolean isNamedWildcard(String key) {
        return key.indexOf('{') != -1 && key.indexOf('}') != -1;
    }

    private String namedWildcard() {
        return namedWildcard;
    }

    private boolean isNamedWildcard() {
        return namedWildcard != null;
    }

    public T retrieve(String[] path, int index, Map<String, String> params, PathTrie.TrieMatchingMode trieMatchingMode, PathTrieBuilder.Decoder decoder) {
        if (index >= path.length)
            return null;

        String token = path[index];
        TrieNode<T> node = children.get(token);
        boolean usedWildcard;

        if (node == null) {
            if (trieMatchingMode == PathTrie.TrieMatchingMode.WILDCARD_NODES_ALLOWED) {
                node = children.get(wildcard);
                if (node == null) {
                    return null;
                }
                usedWildcard = true;
            } else if (trieMatchingMode == PathTrie.TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED && index == 1) {
                /*
                 * Allow root node wildcard matches.
                 */
                node = children.get(wildcard);
                if (node == null) {
                    return null;
                }
                usedWildcard = true;
            } else if (trieMatchingMode == PathTrie.TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED && index + 1 == path.length) {
                /*
                 * Allow leaf node wildcard matches.
                 */
                node = children.get(wildcard);
                if (node == null) {
                    return null;
                }
                usedWildcard = true;
            } else {
                return null;
            }
        } else {
            if (index + 1 == path.length && node.value == null && children.get(wildcard) != null
                    && PathTrie.EXPLICIT_OR_ROOT_WILDCARD.contains(trieMatchingMode) == false) {
                /*
                 * If we are at the end of the path, the current node does not have a value but
                 * there is a child wildcard node, use the child wildcard node.
                 */
                node = children.get(wildcard);
                usedWildcard = true;
            } else if (index == 1 && node.value == null && children.get(wildcard) != null
                    && trieMatchingMode == PathTrie.TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED) {
                /*
                 * If we are at the root, and root wildcards are allowed, use the child wildcard
                 * node.
                 */
                node = children.get(wildcard);
                usedWildcard = true;
            } else {
                usedWildcard = token.equals(wildcard);
            }
        }

        put(params, node, token, decoder);

        if (index == (path.length - 1)) {
            return (T) node.value;
        }

        T nodeValue = node.retrieve(path, index + 1, params, trieMatchingMode, decoder);
        if (nodeValue == null && !usedWildcard && trieMatchingMode != PathTrie.TrieMatchingMode.EXPLICIT_NODES_ONLY) {
            node = children.get(wildcard);
            if (node != null) {
                put(params, node, token, decoder);
                nodeValue = node.retrieve(path, index + 1, params, trieMatchingMode, decoder);
            }
        }

        return nodeValue;
    }

    private void put(Map<String, String> params, TrieNode<T> node, String value, PathTrieBuilder.Decoder decoder) {
        if (params != null && node.isNamedWildcard()) {
            params.put(node.namedWildcard(), decoder.decode(value));
        }
    }

    @Override
    public String toString() {
        return key;
    }
}