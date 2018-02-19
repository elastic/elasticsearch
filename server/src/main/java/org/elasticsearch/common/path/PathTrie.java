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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

import java.util.Map;

public class PathTrie<T> {

    static enum TrieMatchingMode {
        /*
         * Retrieve only explicitly mapped nodes, no wildcards are
         * matched.
         */
        EXPLICIT_NODES_ONLY,
        /*
         * Retrieve only explicitly mapped nodes, with wildcards
         * allowed as root nodes.
         */
        WILDCARD_ROOT_NODES_ALLOWED,
        /*
         * Retrieve only explicitly mapped nodes, with wildcards
         * allowed as leaf nodes.
         */
        WILDCARD_LEAF_NODES_ALLOWED,
        /*
         * Retrieve both explicitly mapped and wildcard nodes.
         */
        WILDCARD_NODES_ALLOWED
    }

    static EnumSet<TrieMatchingMode> EXPLICIT_OR_ROOT_WILDCARD =
            EnumSet.of(TrieMatchingMode.EXPLICIT_NODES_ONLY, TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED);
    
    public interface Decoder {
        String decode(String value);
    }

    private final Decoder decoder;
    private final TrieNode<T> root;
    private byte[] rootValue;


	private FSTRepresentation<T> representation;

    static final String SEPARATOR = "/";
    static final String WILDCARD = "*";

    //Changed constructor here, only decoder and it created new root from scratch
    private PathTrie(Decoder decoder, TrieNode<T> root, byte[] rootValue2) {
        this.decoder = decoder;
        this.root = root;
        this.rootValue = rootValue2;
        this.representation = new FSTRepresentation<T>();
    }

    //TrieNode class used to be here (and was also public)
    
    private static class TrieNode<T>{

        private transient String key;
        private transient T value;
        private boolean isWildcard;
        private final String wildcard;

        private transient String namedWildcard;

        private Map<String, TrieNode<T>> children;

        private TrieNode(String key, T value, String wildcard) {
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

        /**
         * Update the key and the nameWildcard
         * @param key the new key
         */
        private void updateKeyWithNamedWildcard(String key) {
            this.key = key;
            namedWildcard = key.substring(key.indexOf('{') + 1, key.indexOf('}'));
        }

        /**
         * Check if this node is a wildcard
         * @return true if it is a wildcard, false otherwise
         */
        private boolean isWildcard() {
            return isWildcard;
        }

        /**
         * Add a child
         * @param child the new node to be added as a child
         */
        private synchronized void addChild(TrieNode<T> child) {
            addInnerChild(child.key, child);
        }

        /**
         * Add a child
         * @param key the key of the new node
         * @param child the new node to be added as a child
         */
        private void addInnerChild(String key, TrieNode<T> child) {
            Map<String, TrieNode<T>> newChildren = new HashMap<>(children);
            newChildren.put(key, child);
            children = unmodifiableMap(newChildren);
        }

        /**
         * A getter for a child
         * @param key the key corresponding to the child we want to get
         * @return the corresponding TrieNode
         */
        private TrieNode<T> getChild(String key) {
            return children.get(key);
        }

        /**
         * Insert a new value. If the target node already exists, but is without a value,
         * then the value should be inserted, if it already has a value, throws an exception
         * @param path
         * @param index
         * @param value
         * @throws this method can throw IllegalArgumentException
         */
        private synchronized void insert(String[] path, int index, T value) {
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
                node = new TrieNode<T>(token, nodeValue, wildcard);
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

        /**
         * Insert a new value. If the target node already exists, but is without a value,
         * then the value should be inserted, if it already has a value, update it.
         * @param path
         * @param index
         * @param value
         * @param updater
         */
        private synchronized void insertOrUpdate(String[] path, int index, T value, BiFunction<T, T, T> updater) {
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
                node = new TrieNode<T>(token, nodeValue, wildcard);
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

        /**
         * Check if the key is namedWildcard
         * @param key
         * @return true if it is, false otherwise
         */
        private boolean isNamedWildcard(String key) {
            return key.indexOf('{') != -1 && key.indexOf('}') != -1;
        }

        /**
         * A getter for namedWildcart
         * @return namedWildcard
         */
        private String namedWildcard() {
            return namedWildcard;
        }

        /**
         * Check if it is namedWildcard
         * @return true if it is, false otherwise
         */
        private boolean isNamedWildcard() {
            return namedWildcard != null;
        }

        /**
         * Retrieve the value of the node that we want
         * @param path
         * @param index
         * @param params
         * @param trieMatchingMode
         * @param decoder
         * @return the value of the corresponding node
         */
        private T retrieve(String[] path, int index, Map<String, String> params, PathTrie.TrieMatchingMode trieMatchingMode, Decoder decoder) {
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

        /**
         * Put a new entry in params, if the node is not namedWildcard do nothing
         * @param params
         * @param node
         * @param value
         * @param decoder
         */
        private void put(Map<String, String> params, TrieNode<T> node, String value, Decoder decoder) {
            if (params != null && node.isNamedWildcard()) {
                params.put(node.namedWildcard(), decoder.decode(value));
            }
        }

        @Override
        public String toString() {
            return key;
        }
    }

    /**
     * Return the value of the node corresponding to the path
     * @param path we want the value of the node corresponding to this path
     * @return the value or null if not found
     */
    public T retrieve(String path) {
        return retrieve(path, null, TrieMatchingMode.WILDCARD_NODES_ALLOWED);
    }

    /**
     * Return the value of the node corresponding to the path
     * @param path we want the value of the node corresponding to this path
     * @param params the map of params
     * @return the value or null if not found
     */
    public T retrieve(String path, Map<String, String> params) {
        return retrieve(path, params, TrieMatchingMode.WILDCARD_NODES_ALLOWED);
    }

    /**
     * Return the value of the node corresponding to the path
     * @param path we want the value of the node corresponding to this path
     * @param params the map of params
     * @param trieMatchingMode the matching mode we want to use
     * @return the value or null if not found
     */
    public T retrieve(String path, Map<String, String> params, TrieMatchingMode trieMatchingMode) {
        if (path.length() == 0) {
            return representation.fromBytes(rootValue);
        }
        String[] strings = path.split(SEPARATOR);
        if (strings.length == 0) {
            return representation.fromBytes(rootValue);
        }
        int index = 0;

        // Supports initial delimiter.
        if (strings.length > 0 && strings[0].isEmpty()) {
            index = 1;
        }

        return (T) root.retrieve(strings, index, params, trieMatchingMode, decoder);
    }

    /**
     * Returns an iterator of the objects stored in the {@code PathTrie}, using
     * all possible {@code TrieMatchingMode} modes. The {@code paramSupplier}
     * is called between each invocation of {@code next()} to supply a new map
     * of parameters.
     * @param path we want the value of the node corresponding to this path
     * @param paramSupplier it is called between each invocation next() to supply a new map
     * of parameters.
     */
    public Iterator<T> retrieveAll(String path, Supplier<Map<String, String>> paramSupplier) {
        return new PathTrieIterator<>(this, path, paramSupplier);
    }

    @SuppressWarnings("hiding")
	class PathTrieIterator<T> implements Iterator<T> {

        private final List<TrieMatchingMode> modes;
        private final Supplier<Map<String, String>> paramSupplier;
        private final PathTrie<T> trie;
        private final String path;

        PathTrieIterator(PathTrie<T> trie, String path, Supplier<Map<String, String>> paramSupplier) {
            this.path = path;
            this.trie = trie;
            this.paramSupplier = paramSupplier;
            this.modes = new ArrayList<>(Arrays.asList(TrieMatchingMode.EXPLICIT_NODES_ONLY,
                TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED,
                TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED,
                TrieMatchingMode.WILDCARD_NODES_ALLOWED));
            assert TrieMatchingMode.values().length == 4 : "missing trie matching mode";
        }

        /**
         * @return true if there is a next element, false otherwise
         */
        @Override
        public boolean hasNext() {
            return modes.isEmpty() == false;
        }

        /**
         * Return the value of the element corresponding to path using the first mode and remove this mode
         * @throws this method can throw NoSuchElementException
         * @return the value of the element corresponding to path using the first mode
         */
        @Override
        public T next() {
            if (modes.isEmpty()) {
                throw new NoSuchElementException("called next() without validating hasNext()! no more modes available");
            }
            TrieMatchingMode mode = modes.remove(0);
            Map<String, String> params = paramSupplier.get();
            return trie.retrieve(path, params, mode);
        }
    }
    
    public static class PathTrieBuilder<T> {

        private final Decoder decoder;
        private final TrieNode<T> root;
        private byte[] rootValue;
        private FSTRepresentation<T> representation;

        public PathTrieBuilder(Decoder decoder){
            this.representation = new FSTRepresentation<T>();
            this.decoder = decoder;
            root = new TrieNode<T>(PathTrie.SEPARATOR, null, PathTrie.WILDCARD);
        }

        public void insert(String path, T value) {
            String[] strings = path.split(PathTrie.SEPARATOR);
            if (strings.length == 0) {
                if (rootValue != null) {
                    throw new IllegalArgumentException("Path [/] already has a value [" + rootValue + "]");
                }
                rootValue = representation.toBytes(value);
                return;
            }
            int index = 0;
            // Supports initial delimiter.
            if (strings.length > 0 && strings[0].isEmpty()) {
                index = 1;
            }
            root.insert(strings, index, value);
        }

        /**
         * Insert a value for the given path. If the path already exists, replace the value with:
         * <pre>
         * value = updater.apply(oldValue, newValue);
         * </pre>
         * allowing the value to be updated if desired.
         */
        public void insertOrUpdate(String path, T value, BiFunction<T, T, T> updater) {
            String[] strings = path.split(PathTrie.SEPARATOR);
            if (strings.length == 0) {
                if (rootValue != null) {
                    T currentValue = representation.fromBytes(rootValue);
                    T updatedValue = updater.apply(currentValue, value);
                    rootValue = representation.toBytes(updatedValue);
                } else {
                    rootValue = representation.toBytes(value);
                }
                return;
            }
            int index = 0;
            // Supports initial delimiter.
            if (strings.length > 0 && strings[0].isEmpty()) {
                index = 1;
            }
            root.insertOrUpdate(strings, index, value, updater);
        }


        /**
         * Creates a PathTrie object based
         *
        **/
        public PathTrie<T> createpathTrie(){
            return new PathTrie<T>(decoder, root, rootValue);
        }
    }
}
