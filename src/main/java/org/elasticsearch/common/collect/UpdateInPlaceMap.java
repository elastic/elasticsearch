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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.Iterables;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A map that exposes only read only methods, and can be mutated using a {@link #mutator()}. It
 * allows for a cutoff switch between {@link ImmutableOpenMap} and {@link ConcurrentMap}, based on size, since as
 * the size grows bigger, cloning the immutable map cost gets bigger and bigger, and might as well move to CHM.
 * <p/>
 * Note, its important to understand the semantics of the class and its mutator, its not an update in place, when
 * CHM is used, changes to the mutator will be reflected in the existing maps!. This class should be used as if
 * its a regular, mutable concurrent map, mutation can affect the existing map.
 * <p/>
 * This class only allows for a single concurrent mutator to execute at the same time.
 */
public final class UpdateInPlaceMap<K, V> {

    final int switchSize;
    final AtomicBoolean mutating = new AtomicBoolean();
    volatile ImmutableOpenMap<K, V> immutableMap;
    volatile ConcurrentMap<K, V> concurrentMap;

    UpdateInPlaceMap(int switchSize) {
        this.switchSize = switchSize;
        if (switchSize == 0) {
            this.concurrentMap = ConcurrentCollections.newConcurrentMap();
            this.immutableMap = null;
        } else {
            this.concurrentMap = null;
            this.immutableMap = ImmutableOpenMap.of();
        }
    }

    /**
     * Returns if the map is empty or not.
     */
    public boolean isEmpty() {
        final ImmutableOpenMap<K, V> immutableMap = this.immutableMap;
        final ConcurrentMap<K, V> concurrentMap = this.concurrentMap;
        return immutableMap != null ? immutableMap.isEmpty() : concurrentMap.isEmpty();
    }

    /**
     * Returns the value matching a key, or null if not matched.
     */
    public V get(K key) {
        final ImmutableOpenMap<K, V> immutableMap = this.immutableMap;
        final ConcurrentMap<K, V> concurrentMap = this.concurrentMap;
        return immutableMap != null ? immutableMap.get(key) : concurrentMap.get(key);
    }

    /**
     * Returns all the values in the map, on going mutator changes might or might not be reflected
     * in the values.
     */
    public Iterable<V> values() {
        return new Iterable<V>() {
            @Override
            public Iterator<V> iterator() {
                final ImmutableOpenMap<K, V> immutableMap = UpdateInPlaceMap.this.immutableMap;
                final ConcurrentMap<K, V> concurrentMap = UpdateInPlaceMap.this.concurrentMap;
                if (immutableMap != null) {
                    return immutableMap.valuesIt();
                } else {
                    return Iterables.unmodifiableIterable(concurrentMap.values()).iterator();
                }
            }
        };
    }

    /**
     * Opens a mutator allowing to mutate this map. Note, only one mutator is allowed to execute.
     */
    public Mutator mutator() {
        if (!mutating.compareAndSet(false, true)) {
            throw new ElasticsearchIllegalStateException("map is already mutating, can't have another mutator on it");
        }
        return new Mutator();
    }

    public static <K, V> UpdateInPlaceMap<K, V> of(int switchSize) {
        return new UpdateInPlaceMap<>(switchSize);
    }

    public final class Mutator implements Releasable {

        private ImmutableOpenMap.Builder<K, V> immutableBuilder;

        private Mutator() {
            if (immutableMap != null) {
                immutableBuilder = ImmutableOpenMap.builder(immutableMap);
            } else {
                immutableBuilder = null;
            }
        }

        public V get(K key) {
            if (immutableBuilder != null) {
                return immutableBuilder.get(key);
            }
            return concurrentMap.get(key);
        }

        public V put(K key, V value) {
            if (immutableBuilder != null) {
                V v = immutableBuilder.put(key, value);
                switchIfNeeded();
                return v;
            } else {
                return concurrentMap.put(key, value);
            }
        }

        public Mutator putAll(Map<K, V> map) {
            for (Map.Entry<K, V> entry : map.entrySet()) {
                put(entry.getKey(), entry.getValue());
            }
            return this;
        }

        public V remove(K key) {
            return immutableBuilder != null ? immutableBuilder.remove(key) : concurrentMap.remove(key);
        }

        private void switchIfNeeded() {
            if (concurrentMap != null) {
                assert immutableBuilder == null;
                return;
            }
            if (immutableBuilder.size() <= switchSize) {
                return;
            }
            concurrentMap = ConcurrentCollections.newConcurrentMap();
            for (ObjectObjectCursor<K, V> cursor : immutableBuilder) {
                concurrentMap.put(cursor.key, cursor.value);
            }
            immutableBuilder = null;
            immutableMap = null;
        }

        public void close() {
            if (immutableBuilder != null) {
                immutableMap = immutableBuilder.build();
            }
            assert (immutableBuilder != null && concurrentMap == null) || (immutableBuilder == null && concurrentMap != null);
            mutating.set(false);
        }
    }
}
