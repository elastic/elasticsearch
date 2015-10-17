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

import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 *
 */
public class PathTrie<T> {

    public static interface Decoder {
        String decode(String value);
    }

    public static final Decoder NO_DECODER = new Decoder() {
        @Override
        public String decode(String value) {
            return value;
        }
    };

    private final Decoder decoder;
    private final TrieNode root;
    private final char separator;
    private T rootValue;

    public PathTrie() {
        this('/', "*", NO_DECODER);
    }

    public PathTrie(Decoder decoder) {
        this('/', "*", decoder);
    }

    public PathTrie(char separator, String wildcard, Decoder decoder) {
        this.decoder = decoder;
        this.separator = separator;
        root = new TrieNode(new String(new char[]{separator}), null, wildcard);
    }

    public class TrieNode {
        private transient String key;
        private transient T value;
        private boolean isWildcard;
        private final String wildcard;

        private transient String namedWildcard;

        private Map<String, TrieNode> children;

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

        public synchronized void addChild(TrieNode child) {
            addInnerChild(child.key, child);
        }

        private void addInnerChild(String key, TrieNode child) {
            Map<String, TrieNode> newChildren = new HashMap<>(children);
            newChildren.put(key, child);
            children = unmodifiableMap(newChildren);
        }

        public TrieNode getChild(String key) {
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
            TrieNode node = children.get(key);
            if (node == null) {
                T nodeValue = index == path.length - 1 ? value : null;
                node = new TrieNode(token, nodeValue, wildcard);
                addInnerChild(key, node);
            } else {
                if (isNamedWildcard(token)) {
                    node.updateKeyWithNamedWildcard(token);
                }

                // in case the target(last) node already exist but without a value
                // than the value should be updated.
                if (index == (path.length - 1)) {
                    assert (node.value == null || node.value == value);
                    if (node.value == null) {
                        node.value = value;
                    }
                }
            }

            node.insert(path, index + 1, value);
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

        public T retrieve(String[] path, int index, Map<String, String> params) {
            if (index >= path.length)
                return null;

            String token = path[index];
            TrieNode node = children.get(token);
            boolean usedWildcard;
            if (node == null) {
                node = children.get(wildcard);
                if (node == null) {
                    return null;
                }
                usedWildcard = true;
            } else {
                // If we are at the end of the path, the current node does not have a value but there
                // is a child wildcard node, use the child wildcard node
                if (index + 1 == path.length && node.value == null && children.get(wildcard) != null) {
                    node = children.get(wildcard);
                    usedWildcard = true;
                } else {
                    usedWildcard = token.equals(wildcard);
                }
            }

            put(params, node, token);

            if (index == (path.length - 1)) {
                return node.value;
            }

            T res = node.retrieve(path, index + 1, params);
            if (res == null && !usedWildcard) {
                node = children.get(wildcard);
                if (node != null) {
                    put(params, node, token);
                    res = node.retrieve(path, index + 1, params);
                }
            }

            return res;
        }

        private void put(Map<String, String> params, TrieNode node, String value) {
            if (params != null && node.isNamedWildcard()) {
                params.put(node.namedWildcard(), value);
            }
        }

        @Override
        public String toString() {
            return key;
        }
    }

    public void insert(String path, T value) {
        String[] strings = Strings.splitStringToArray(path, separator);
        if (strings.length == 0) {
            rootValue = value;
            return;
        }
        int index = 0;
        // supports initial delimiter.
        if (strings.length > 0 && strings[0].isEmpty()) {
            index = 1;
        }
        root.insert(strings, index, value);
    }

    public T retrieve(String path) {
        return retrieve(path, null);
    }

    public T retrieve(String path, Map<String, String> params) {
        if (path.length() == 0) {
            return rootValue;
        }
        String[] strings = splitPath(decoder.decode(path));
        if (strings.length == 0) {
            return rootValue;
        }
        int index = 0;
        // supports initial delimiter.
        if (strings.length > 0 && strings[0].isEmpty()) {
            index = 1;
        }
        return root.retrieve(strings, index, params);
    }

    /*
      Splits up the url path up by '/' and is aware of
      index name expressions that appear between '<' and '>'.
     */
    String[] splitPath(final String path) {
        if (path == null || path.length() == 0) {
            return Strings.EMPTY_ARRAY;
        }
        int count = 1;
        boolean splitAllowed = true;
        for (int i = 0; i < path.length(); i++) {
            final char currentC = path.charAt(i);
            if ('<' == currentC) {
                splitAllowed = false;
            } else if (currentC == '>') {
                splitAllowed = true;
            } else if (splitAllowed && currentC == separator) {
                count++;
            }
        }

        final List<String> result = new ArrayList<>(count);
        final StringBuilder builder = new StringBuilder();

        splitAllowed = true;
        for (int i = 0; i < path.length(); i++) {
            final char currentC = path.charAt(i);
            if ('<' == currentC) {
                splitAllowed = false;
            } else if (currentC == '>') {
                splitAllowed = true;
            } else  if (splitAllowed && currentC == separator) {
                if (builder.length() > 0) {
                    result.add(builder.toString());
                    builder.setLength(0);
                }
                continue;
            }
            builder.append(currentC);
        }
        if (builder.length() > 0) {
            result.add(builder.toString());
        }
        return result.toArray(new String[result.size()]);
    }
}
