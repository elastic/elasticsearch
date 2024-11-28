/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.core.Releasables;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A hash table from objects to objects. This implementation resolves collisions
 *  with open addressing and linear probing and does not support null values.
 *  This class is not thread-safe.
 *
 *  Note that this class does not track either the actual keys or values. It is responsibility of
 *  the caller to release those objects if necessary.
 */
public final class ObjectObjectPagedHashMap<K, V> extends AbstractPagedHashMap implements Iterable<ObjectObjectPagedHashMap.Cursor<K, V>> {

    private ObjectArray<K> keys;
    private ObjectArray<V> values;

    public ObjectObjectPagedHashMap(long capacity, BigArrays bigArrays) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    public ObjectObjectPagedHashMap(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        keys = bigArrays.newObjectArray(capacity());
        boolean success = false;
        try {
            values = bigArrays.newObjectArray(capacity());
            success = true;
        } finally {
            if (false == success) {
                close();
            }
        }
    }

    /**
     * Get the value that is associated with <code>key</code> or null if <code>key</code>
     * was not present in the hash table.
     */
    public V get(K key) {
        final long slot = slot(key.hashCode(), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final V value = values.get(index);
            if (value == null) {
                return null;
            } else if (keys.get(index).equals(key)) {
                return value;
            }
        }
    }

    /**
     * Put this new (key, value) pair into this hash table and return the value
     * that was previously associated with <code>key</code> or null in case of
     * an insertion.
     */
    public V put(K key, V value) {
        assert value != null : "Null values are not supported";
        if (size >= maxSize) {
            assert size == maxSize;
            grow();
        }
        assert size < maxSize;
        return set(key, key.hashCode(), value);
    }

    /**
     * Remove the entry which has this key in the hash table and return the
     * associated value or null if there was no entry associated with this key.
     */
    public V remove(K key) {
        final long slot = slot(key.hashCode(), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final V previous = values.getAndSet(index, null);
            if (previous == null) {
                return null;
            } else if (keys.get(index).equals(key)) {
                --size;
                for (long j = nextSlot(index, mask); used(j); j = nextSlot(j, mask)) {
                    removeAndAdd(j);
                }
                return previous;
            } else {
                // repair and continue
                values.set(index, previous);
            }
        }
    }

    private V set(K key, int code, V value) {
        assert key.hashCode() == code;
        assert size < maxSize;
        final long slot = slot(code, mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final V previous = values.getAndSet(index, value);
            if (previous == null) {
                // slot was free
                keys.set(index, key);
                ++size;
                return null;
            } else if (key.equals(keys.get(index))) {
                // we just updated the value
                return previous;
            } else {
                // not the right key, repair and continue
                values.set(index, previous);
            }
        }
    }

    @Override
    public Iterator<Cursor<K, V>> iterator() {
        return new Iterator<>() {

            boolean cached;
            final Cursor<K, V> cursor;
            {
                cursor = new Cursor<>();
                cursor.index = -1;
                cached = false;
            }

            @Override
            public boolean hasNext() {
                if (cached == false) {
                    while (true) {
                        ++cursor.index;
                        if (cursor.index >= capacity()) {
                            break;
                        } else if (used(cursor.index)) {
                            cursor.key = keys.get(cursor.index);
                            cursor.value = values.get(cursor.index);
                            break;
                        }
                    }
                    cached = true;
                }
                return cursor.index < capacity();
            }

            @Override
            public Cursor<K, V> next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                cached = false;
                return cursor;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    @Override
    public void close() {
        Releasables.close(keys, values);
    }

    @Override
    protected void resize(long capacity) {
        keys = bigArrays.resize(keys, capacity);
        values = bigArrays.resize(values, capacity);
    }

    @Override
    protected boolean used(long bucket) {
        return values.get(bucket) != null;
    }

    @Override
    protected void removeAndAdd(long index) {
        final K key = keys.get(index);
        final V value = values.getAndSet(index, null);
        reset(key, value);
    }

    private void reset(K key, V value) {
        final ObjectArray<V> values = this.values;
        final long mask = this.mask;
        final long slot = slot(key.hashCode(), mask);
        for (long index = slot;; index = nextSlot(index, mask)) {
            final V previous = values.get(index);
            if (previous == null) {
                // slot was free
                values.set(index, value);
                keys.set(index, key);
                break;
            }
        }
    }

    public static final class Cursor<K, V> {
        public long index;
        public K key;
        public V value;
    }
}
