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

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.util.Iterator;

/**
 */
public final class HppcMaps {

    private HppcMaps() {
    }

    /**
     * Returns a new map with the given initial capacity
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newMap(int capacity) {
        return new ObjectObjectOpenHashMap<>(capacity);
    }

    /**
     * Returns a new map with a default initial capacity of
     * {@value com.carrotsearch.hppc.HashContainerUtils#DEFAULT_CAPACITY}
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newMap() {
        return newMap(16);
    }

    /**
     * Returns a map like {@link #newMap()} that does not accept <code>null</code> keys
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newNoNullKeysMap() {
        return ensureNoNullKeys(16);
    }

    /**
     * Returns a map like {@link #newMap(int)} that does not accept <code>null</code> keys
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> newNoNullKeysMap(int capacity) {
        return ensureNoNullKeys(capacity);
    }

    /**
     * Wraps the given map and prevent adding of <code>null</code> keys.
     */
    public static <K, V> ObjectObjectOpenHashMap<K, V> ensureNoNullKeys(int capacity) {
        return new ObjectObjectOpenHashMap<K, V>(capacity) {

            @Override
            public V put(K key, V value) {
                if (key == null) {
                    throw new ElasticsearchIllegalArgumentException("Map key must not be null");
                }
                return super.put(key, value);
            }

        };
    }

    /**
     * @return an intersection view over the two specified containers (which can be KeyContainer or ObjectOpenHashSet).
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

    public final static class Object {

        public final static class Integer {

            public static <V> ObjectIntOpenHashMap<V> ensureNoNullKeys(int capacity, float loadFactor) {
                return new ObjectIntOpenHashMap<V>(capacity, loadFactor) {

                    @Override
                    public int put(V key, int value) {
                        if (key == null) {
                            throw new ElasticsearchIllegalArgumentException("Map key must not be null");
                        }
                        return super.put(key, value);
                    }
                };
            }

        }

    }

}
