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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
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


    private final PathTrieBuilder.Decoder decoder;
    private final TrieNode<T> root;
    private byte[] rootValue;


	private FSTRepresentation<T> representation;

    static final String SEPARATOR = "/";
    static final String WILDCARD = "*";

    //Changed constructor here, only decoder and it created new root from scratch
    public PathTrie(PathTrieBuilder.Decoder decoder, TrieNode<T> root, byte[] rootValue2) {
        this.decoder = decoder;
        this.root = root;
        this.rootValue = rootValue2;
        this.representation = new FSTRepresentation<T>();
    }

    //TrieNode class used to be here (and was also public)

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
}
