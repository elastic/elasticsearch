/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import java.util.Iterator;

public final class HppcMaps {

    private HppcMaps() {
    }

    /**
     * Returns a new map with the given number of expected elements.
     *
     * @param expectedElements
     *          The expected number of elements guaranteed not to cause buffer
     *          expansion (inclusive).
     */
    public static <K, V> ObjectObjectHashMap<K, V> newMap(int expectedElements) {
        return new ObjectObjectHashMap<>(expectedElements);
    }

    /**
     * Returns a map like {@link #newMap} that does not accept <code>null</code> keys
     */
    public static <K, V> ObjectObjectHashMap<K, V> newNoNullKeysMap() {
        return newNoNullKeysMap(16);
    }

    /**
     * Returns a map like {@link #newMap(int)} that does not accept <code>null</code> keys
     *
     * @param expectedElements
     *          The expected number of elements guaranteed not to cause buffer
     *          expansion (inclusive).
     */
    public static <K, V> ObjectObjectHashMap<K, V> newNoNullKeysMap(int expectedElements) {
        return new ObjectObjectHashMap<K, V>(expectedElements) {
            @Override
            public V put(K key, V value) {
                if (key == null) {
                    throw new IllegalArgumentException("Map key must not be null");
                }
                return super.put(key, value);
            }
        };
    }

    /**
     * @return an intersection view over the two specified containers (which can be KeyContainer or ObjectHashSet).
     */
    // Hppc has forEach, but this means we need to build an intermediate set, with this method we just iterate
    // over each unique value without creating a third set.
    public static <T> Iterable<T> intersection(ObjectLookupContainer<T> container1, final ObjectLookupContainer<T> container2) {
        assert container1 != null && container2 != null;
        final Iterator<ObjectCursor<T>> iterator = container1.iterator();
        final Iterator<T> intersection = new Iterator<T>() {

            T current;

            @Override
            public boolean hasNext() {
                if (iterator.hasNext()) {
                    do {
                        T next = iterator.next().value;
                        if (container2.contains(next)) {
                            current = next;
                            return true;
                        }
                    } while (iterator.hasNext());
                }
                return false;
            }

            @Override
            public T next() {
                return current;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return intersection;
            }
        };
    }

    public static final class Object {
        public static final class Integer {
            public static <V> ObjectIntHashMap<V> ensureNoNullKeys(int capacity, float loadFactor) {
                return new ObjectIntHashMap<V>(capacity, loadFactor) {
                    @Override
                    public int put(V key, int value) {
                        if (key == null) {
                            throw new IllegalArgumentException("Map key must not be null");
                        }
                        return super.put(key, value);
                    }
                };
            }
        }
    }
}
