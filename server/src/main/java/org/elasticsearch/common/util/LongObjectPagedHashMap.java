/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.core.Releasables;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A hash table from native longs to objects. This implementation resolves collisions
 * using open-addressing and does not support null values. This class is not thread-safe.
 */
public final class LongObjectPagedHashMap<T> extends AbstractPagedHashMap implements Iterable<LongObjectPagedHashMap.Cursor<T>> {

    private LongArray keys;
    private ObjectArray<T> values;

    public LongObjectPagedHashMap(long capacity, BigArrays bigArrays) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    public LongObjectPagedHashMap(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        boolean success = false;
        try {
            // `super` allocates a big array so we have to `close` if we fail here or we'll leak it.
            keys = bigArrays.newLongArray(capacity(), false);
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
    public T get(long key) {
        for (long i = slot(hash(key), mask);; i = nextSlot(i, mask)) {
            final T value = values.get(i);
            if (value == null) {
                return null;
            } else if (keys.get(i) == key) {
                return value;
            }
        }
    }

    /**
     * Put this new (key, value) pair into this hash table and return the value
     * that was previously associated with <code>key</code> or null in case of
     * an insertion.
     */
    public T put(long key, T value) {
        if (size >= maxSize) {
            assert size == maxSize;
            grow();
        }
        assert size < maxSize;
        return set(key, value);
    }

    /**
     * Remove the entry which has this key in the hash table and return the
     * associated value or null if there was no entry associated with this key.
     */
    public T remove(long key) {
        for (long i = slot(hash(key), mask);; i = nextSlot(i, mask)) {
            final T previous = values.set(i, null);
            if (previous == null) {
                return null;
            } else if (keys.get(i) == key) {
                --size;
                for (long j = nextSlot(i, mask); used(j); j = nextSlot(j, mask)) {
                    removeAndAdd(j);
                }
                return previous;
            } else {
                // repair and continue
                values.set(i, previous);
            }
        }
    }

    private T set(long key, T value) {
        if (value == null) {
            throw new IllegalArgumentException("Null values are not supported");
        }
        for (long i = slot(hash(key), mask);; i = nextSlot(i, mask)) {
            final T previous = values.set(i, value);
            if (previous == null) {
                // slot was free
                keys.set(i, key);
                ++size;
                return null;
            } else if (key == keys.get(i)) {
                // we just updated the value
                return previous;
            } else {
                // not the right key, repair and continue
                values.set(i, previous);
            }
        }
    }

    @Override
    public Iterator<Cursor<T>> iterator() {
        return new Iterator<Cursor<T>>() {

            boolean cached;
            final Cursor<T> cursor;
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
            public Cursor<T> next() {
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
        final long key = keys.get(index);
        final T value = values.set(index, null);
        --size;
        final T removed = set(key, value);
        assert removed == null;
    }

    public static final class Cursor<T> {
        public long index;
        public long key;
        public T value;
    }

}
