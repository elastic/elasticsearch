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

package org.elasticsearch.common.collect;

import org.apache.lucene.util.mutable.MutableValueInt;

import java.lang.reflect.Array;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;

/**
 * An immutable map whose writes result in a new copy of the map to be created.
 *
 * This is essentially a hash array mapped trie: inner nodes use a bitmap in
 * order to map hashes to slots by counting ones. In case of a collision (two
 * values having the same 32-bits hash), a leaf node is created which stores
 * and searches for values sequentially.
 *
 * Reads and writes both perform in logarithmic time. Null keys and values are
 * not supported.
 *
 * This structure might need to perform several object creations per write so
 * it is better suited for work-loads that are not too write-intensive.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Hash_array_mapped_trie">the wikipedia page</a>
 */
public final class CopyOnWriteHashMap<K, V> extends AbstractMap<K, V> {

    private static final int TOTAL_HASH_BITS = 32;
    private static final Object[] EMPTY_ARRAY = new Object[0];

    private static final int HASH_BITS = 6;
    private static final int HASH_MASK = 0x3F;

    /**
     * Return a copy of the provided map.
     */
    public static <K, V> CopyOnWriteHashMap<K, V> copyOf(Map<? extends K, ? extends V> map) {
        if (map instanceof CopyOnWriteHashMap) {
            // no need to copy in that case
            @SuppressWarnings("unchecked")
            final CopyOnWriteHashMap<K, V> cowMap = (CopyOnWriteHashMap<K, V>) map;
            return cowMap;
        } else {
            return new CopyOnWriteHashMap<K, V>().copyAndPutAll(map);
        }
    }

    /**
     * Abstraction of a node, implemented by both inner and leaf nodes.
     */
    private abstract static class Node<K, V> {

        /**
         * Recursively get the key with the given hash.
         */
        abstract V get(Object key, int hash);

        /**
         * Recursively add a new entry to this node. <code>hashBits</code> is
         * the number of bits that are still set in the hash. When this value
         * reaches a number that is less than or equal to <tt>0</tt>, a leaf
         * node needs to be created since it means that a collision occurred
         * on the 32 bits of the hash.
         */
        abstract Node<K, V> put(K key, int hash, int hashBits, V value, MutableValueInt newValue);

        /**
         * Recursively remove an entry from this node.
         */
        abstract Node<K, V> remove(Object key, int hash);

        /**
         * For the current node only, append entries that are stored on this
         * node to <code>entries</code> and sub nodes to <code>nodes</code>.
         */
        abstract void visit(Deque<Map.Entry<K, V>> entries, Deque<Node<K, V>> nodes);

        /**
         * Whether this node stores nothing under it.
         */
        abstract boolean isEmpty();

    }

    /**
     * A leaf of the tree where all hashes are equal. Values are added and retrieved in linear time.
     */
    private static class Leaf<K, V> extends Node<K, V> {

        private final K[] keys;
        private final V[] values;

        Leaf(K[] keys, V[] values) {
            this.keys = keys;
            this.values = values;
        }

        @SuppressWarnings("unchecked")
        Leaf() {
            this((K[]) EMPTY_ARRAY, (V[]) EMPTY_ARRAY);
        }

        @Override
        boolean isEmpty() {
            return keys.length == 0;
        }

        @Override
        void visit(Deque<Map.Entry<K, V>> entries, Deque<Node<K, V>> nodes) {
            for (int i = 0; i < keys.length; ++i) {
                entries.add(new AbstractMap.SimpleImmutableEntry<>(keys[i], values[i]));
            }
        }

        @Override
        V get(Object key, int hash) {
            for (int i = 0; i < keys.length; i++) {
                if (key.equals(keys[i])) {
                    return values[i];
                }
            }
            return null;

        }

        private static <T> T[] replace(T[] array, int index, T value) {
            final T[] copy = Arrays.copyOf(array, array.length);
            copy[index] = value;
            return copy;
        }

        @Override
        Leaf<K, V> put(K key, int hash, int hashBits, V value, MutableValueInt newValue) {
            assert hashBits <= 0 : hashBits;
            int slot = -1;
            for (int i = 0; i < keys.length; i++) {
                if (key.equals(keys[i])) {
                    slot = i;
                    break;
                }
            }

            final K[] keys2;
            final V[] values2;

            if (slot < 0) {
                keys2 = appendElement(keys, key);
                values2 = appendElement(values, value);
                newValue.value = 1;
            } else {
                keys2 = replace(keys, slot, key);
                values2 = replace(values, slot, value);
            }

            return new Leaf<>(keys2, values2);
        }

        @Override
        Leaf<K, V> remove(Object key, int hash) {
            int slot = -1;
            for (int i = 0; i < keys.length; i++) {
                if (key.equals(keys[i])) {
                    slot = i;
                    break;
                }
            }
            if (slot < 0) {
                return this;
            }
            final K[] keys2 = removeArrayElement(keys, slot);
            final V[] values2 = removeArrayElement(values, slot);
            return new Leaf<>(keys2, values2);
        }
    }

    private static <T> T[] removeArrayElement(T[] array, int index) {
        final Object result = Array.newInstance(array.getClass().getComponentType(), array.length - 1);
        System.arraycopy(array, 0, result, 0, index);
        if (index < array.length - 1) {
            System.arraycopy(array, index + 1, result, index, array.length - index - 1);
        }

        return (T[]) result;
    }

    public static <T> T[] appendElement(final T[] array, final T element) {
        final T[] newArray = Arrays.copyOf(array, array.length + 1);
        newArray[newArray.length - 1] = element;
        return newArray;
    }

    public static <T> T[] insertElement(final T[] array, final T element, final int index) {
        final T[] result = Arrays.copyOf(array, array.length + 1);
        System.arraycopy(array, 0, result, 0, index);
        result[index] = element;
        if (index < array.length) {
            System.arraycopy(array, index, result, index + 1, array.length - index);
        }
        return result;
    }


    /**
     * An inner node in this trie. Inner nodes store up to 64 key-value pairs
     * and use a bitmap in order to associate hashes to them. For example, if
     * an inner node contains 5 values, then 5 bits will be set in the bitmap
     * and the ordinal of the bit set in this bit map will be the slot number.
     *
     * As a consequence, the number of slots in an inner node is equal to the
     * number of one bits in the bitmap.
     */
    private static class InnerNode<K, V> extends Node<K, V> {

        private final long mask; // the bitmap
        private final K[] keys;
        final Object[] subNodes; // subNodes[slot] is either a value or a sub node in case of a hash collision

        InnerNode(long mask, K[] keys, Object[] subNodes) {
            this.mask = mask;
            this.keys = keys;
            this.subNodes = subNodes;
            assert consistent();
        }

        // only used in assert
        private boolean consistent() {
            assert Long.bitCount(mask) == keys.length;
            assert Long.bitCount(mask) == subNodes.length;
            for (int i = 0; i < keys.length; ++i) {
                if (subNodes[i] instanceof Node) {
                    assert keys[i] == null;
                } else {
                    assert keys[i] != null;
                }
            }
            return true;
        }

        @Override
        boolean isEmpty() {
            return mask == 0;
        }

        @SuppressWarnings("unchecked")
        InnerNode() {
            this(0, (K[]) EMPTY_ARRAY, EMPTY_ARRAY);
        }

        @Override
        void visit(Deque<Map.Entry<K, V>> entries, Deque<Node<K, V>> nodes) {
            for (int i = 0; i < keys.length; ++i) {
                final Object sub = subNodes[i];
                if (sub instanceof Node) {
                    @SuppressWarnings("unchecked")
                    final Node<K, V> subNode = (Node<K, V>) sub;
                    assert keys[i] == null;
                    nodes.add(subNode);
                } else {
                    @SuppressWarnings("unchecked")
                    final V value = (V) sub;
                    entries.add(new AbstractMap.SimpleImmutableEntry<>(keys[i], value));
                }
            }
        }

        /**
         * For a given hash on 6 bits, its value is set if the bitmap has a one
         * at the corresponding index.
         */
        private boolean exists(int hash6) {
            return (mask & (1L << hash6)) != 0;
        }

        /**
         * For a given hash on 6 bits, the slot number is the number of one
         * bits on the right of the <code>hash6</code>-th bit.
         */
        private int slot(int hash6) {
            return Long.bitCount(mask & ((1L << hash6) - 1));
        }

        @Override
        V get(Object key, int hash) {
            final int hash6 = hash & HASH_MASK;
            if (!exists(hash6)) {
                return null;
            }
            final int slot = slot(hash6);
            final Object sub = subNodes[slot];
            assert sub != null;
            if (sub instanceof Node) {
                assert keys[slot] == null; // keys don't make sense on inner nodes
                @SuppressWarnings("unchecked")
                final Node<K, V> subNode = (Node<K, V>) sub;
                return subNode.get(key, hash >>> HASH_BITS);
            } else {
                if (keys[slot].equals(key)) {
                    @SuppressWarnings("unchecked")
                    final V v = (V) sub;
                    return v;
                } else {
                    // we have an entry for this hash, but the value is different
                    return null;
                }
            }
        }

        private Node<K, V> newSubNode(int hashBits) {
            if (hashBits <= 0) {
                return new Leaf<K, V>();
            } else {
                return new InnerNode<K, V>();
            }
        }

        private InnerNode<K, V> putExisting(K key, int hash, int hashBits, int slot, V value, MutableValueInt newValue) {
            final K[] keys2 = Arrays.copyOf(keys, keys.length);
            final Object[] subNodes2 = Arrays.copyOf(subNodes, subNodes.length);

            final Object previousValue = subNodes2[slot];
            if (previousValue instanceof Node) {
                // insert recursively
                assert keys[slot] == null;
                subNodes2[slot] = ((Node<K, V>) previousValue).put(key, hash, hashBits, value, newValue);
            } else if (keys[slot].equals(key)) {
                // replace the existing entry
                subNodes2[slot] = value;
            } else {
                // hash collision
                final K previousKey = keys[slot];
                final int previousHash = previousKey.hashCode() >>> (TOTAL_HASH_BITS - hashBits);
                Node<K, V> subNode = newSubNode(hashBits);
                subNode = subNode.put(previousKey, previousHash, hashBits, (V) previousValue, newValue);
                subNode = subNode.put(key, hash, hashBits, value, newValue);
                keys2[slot] = null;
                subNodes2[slot] = subNode;
            }
            return new InnerNode<>(mask, keys2, subNodes2);
        }

        private InnerNode<K, V> putNew(K key, int hash6, int slot, V value) {
            final long mask2 = mask | (1L << hash6);
            final K[] keys2 = insertElement(keys, key, slot);
            final Object[] subNodes2 = insertElement(subNodes, value, slot);
            return new InnerNode<>(mask2, keys2, subNodes2);
        }

        @Override
        InnerNode<K, V> put(K key, int hash, int hashBits, V value, MutableValueInt newValue) {
            final int hash6 = hash & HASH_MASK;
            final int slot = slot(hash6);

            if (exists(hash6)) {
                hash >>>= HASH_BITS;
                hashBits -= HASH_BITS;
                return putExisting(key, hash, hashBits, slot, value, newValue);
            } else {
                newValue.value = 1;
                return putNew(key, hash6, slot, value);
            }
        }

        private InnerNode<K, V> removeSlot(int hash6, int slot) {
            final long mask2 = mask  & ~(1L << hash6);
            final K[] keys2 = removeArrayElement(keys, slot);
            final Object[] subNodes2 = removeArrayElement(subNodes, slot);
            return new InnerNode<>(mask2, keys2, subNodes2);
        }

        @Override
        InnerNode<K, V> remove(Object key, int hash) {
            final int hash6 = hash & HASH_MASK;
            if (!exists(hash6)) {
                return this;
            }
            final int slot = slot(hash6);
            final Object previousValue = subNodes[slot];
            if (previousValue instanceof Node) {
                @SuppressWarnings("unchecked")
                final Node<K, V> subNode = (Node<K, V>) previousValue;
                final Node<K, V> removed = subNode.remove(key, hash >>> HASH_BITS);
                if (removed == subNode) {
                    // not in sub-nodes
                    return this;
                }
                if (removed.isEmpty()) {
                    return removeSlot(hash6, slot);
                }
                final K[] keys2 = Arrays.copyOf(keys, keys.length);
                final Object[] subNodes2 = Arrays.copyOf(subNodes, subNodes.length);
                subNodes2[slot] = removed;
                return new InnerNode<>(mask, keys2, subNodes2);
            } else if (keys[slot].equals(key)) {
                // remove entry
                return removeSlot(hash6, slot);
            } else {
                // hash collision, nothing to remove
                return this;
            }
        }

    }

    private static class EntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {

        private final Deque<Map.Entry<K, V>> entries;
        private final Deque<Node<K, V>> nodes;

        EntryIterator(Node<K, V> node) {
            entries = new ArrayDeque<>();
            nodes = new ArrayDeque<>();
            node.visit(entries, nodes);
        }

        @Override
        public boolean hasNext() {
            return !entries.isEmpty() || !nodes.isEmpty();
        }

        @Override
        public Map.Entry<K, V> next() {
            while (entries.isEmpty()) {
                if (nodes.isEmpty()) {
                    throw new NoSuchElementException();
                }
                final Node<K, V> nextNode = nodes.pop();
                nextNode.visit(entries, nodes);
            }
            return entries.pop();
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private final InnerNode<K, V> root;
    private final int size;

    /**
     * Create a new empty map.
     */
    public CopyOnWriteHashMap() {
        this(new InnerNode<K, V>(), 0);
    }

    private CopyOnWriteHashMap(InnerNode<K, V> root, int size) {
        this.root = root;
        this.size = size;
    }

    @Override
    public boolean containsKey(Object key) {
        // works fine since null values are not supported
        return get(key) != null;
    }

    @Override
    public V get(Object key) {
        if (key == null) {
            throw new IllegalArgumentException("null keys are not supported");
        }
        final int hash = key.hashCode();
        return root.get(key, hash);
    }

    @Override
    public int size() {
        assert size != 0 || root.isEmpty();
        return size;
    }

    /**
     * Associate <code>key</code> with <code>value</code> and return a new copy
     * of the hash table. The current hash table is not modified.
     */
    public CopyOnWriteHashMap<K, V> copyAndPut(K key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("null keys are not supported");
        }
        if (value == null) {
            throw new IllegalArgumentException("null values are not supported");
        }
        final int hash = key.hashCode();
        final MutableValueInt newValue = new MutableValueInt();
        final InnerNode<K, V> newRoot = root.put(key, hash, TOTAL_HASH_BITS, value, newValue);
        final int newSize = size + newValue.value;
        return new CopyOnWriteHashMap<>(newRoot, newSize);
    }

    /**
     * Same as {@link #copyAndPut(Object, Object)} but for an arbitrary number of entries.
     */
    public CopyOnWriteHashMap<K, V> copyAndPutAll(Map<? extends K, ? extends V> other) {
        return copyAndPutAll(other.entrySet());
    }

    public <K1 extends K, V1 extends V> CopyOnWriteHashMap<K, V> copyAndPutAll(Iterable<Entry<K1, V1>> entries) {
        CopyOnWriteHashMap<K, V> result = this;
        for (Entry<K1, V1> entry : entries) {
            result = result.copyAndPut(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public <K1 extends K, V1 extends V> CopyOnWriteHashMap<K, V> copyAndPutAll(Stream<Entry<K1, V1>> entries) {
        return copyAndPutAll(entries::iterator);
    }

    /**
     * Remove the given key from this map. The current hash table is not modified.
     */
    public CopyOnWriteHashMap<K, V> copyAndRemove(Object key) {
        if (key == null) {
            throw new IllegalArgumentException("null keys are not supported");
        }
        final int hash = key.hashCode();
        final InnerNode<K, V> newRoot = root.remove(key, hash);
        if (root == newRoot) {
            return this;
        } else {
            return new CopyOnWriteHashMap<>(newRoot, size - 1);
        }
    }

    /**
     * Same as {@link #copyAndRemove(Object)} but for an arbitrary number of entries.
     */
    public CopyOnWriteHashMap<K, V> copyAndRemoveAll(Collection<?> keys) {
        CopyOnWriteHashMap<K, V> result = this;
        for (Object key : keys) {
            result = result.copyAndRemove(key);
        }
        return result;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<Map.Entry<K, V>>() {

            @Override
            public Iterator<java.util.Map.Entry<K, V>> iterator() {
                return new EntryIterator<>(root);
            }

            @Override
            public boolean contains(Object o) {
                if (o == null || !(o instanceof Map.Entry)) {
                    return false;
                }
                Map.Entry<?, ?> entry = (java.util.Map.Entry<?, ?>) o;
                return entry.getValue().equals(CopyOnWriteHashMap.this.get(entry.getKey()));
            }

            @Override
            public int size() {
                return CopyOnWriteHashMap.this.size();
            }
        };
    }

}
