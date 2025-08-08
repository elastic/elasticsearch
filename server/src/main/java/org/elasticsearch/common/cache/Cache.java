/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.ToLongBiFunction;

/**
 * Interface for cache implementations
 * <p>
 * Implementations are expected to notify through a {@link RemovalListener} but the interface does not feature a method for registering a
 * listener, as how it's left to the implementation. This leaves the option open for it to be registered through a constructor. If an
 * implementation supplies the option of modifying the listener(s) after the cache is created, it should specify its guarantees of delivery
 * of notifications. If the implementation features an eviction strategy, and events are evicted, a removal notification with
 * {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} EVICTED should be emitted. If {@link #invalidate(Key)},
 * {@link #invalidate(Key, Value)}, or {@link #invalidateAll()} is used, a removal notification with
 * {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED should be emitted.
 * </p>
 * <p>
 * Similarly, the implementation doesn't specify a means of supplying a weigher in the form of a {@link ToLongBiFunction}. The function
 * will be supplied the Kay Value pair as arguments and should compute and return the weight of the entry.
 * If values have mutable components which affect their weight, if any recomputation happens, and when, is left to the implementation.
 * </p>
 *
 * @param <Key> Type of keys to lookup values
 * @param <Value> Type of values stored in the cache
 */
public interface Cache<Key, Value> {
    /**
     * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or null if this map contains no mapping for the key
     */
    Value get(Key key);

    /**
     * Associates the specified value with the specified key in this map. If the map previously contained a mapping for
     * the key, the old value is replaced.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     */
    void put(Key key, Value value);

    /**
     * If the specified key is not already associated with a value (or is mapped to null), attempts to compute its
     * value using the given mapping function and enters it into this map unless null. The load method for a given key
     * will be invoked at most once.
     *
     * Use of different {@link CacheLoader} implementations on the same key concurrently may result in only the first
     * loader function being called and the second will be returned the result provided by the first including any exceptions
     * thrown during the execution of the first.
     *
     * @param key    the key whose associated value is to be returned or computed for if non-existent
     * @param loader the function to compute a value given a key
     * @return the current (existing or computed) non-null value associated with the specified key
     * @throws ExecutionException thrown if loader throws an exception or returns a null value
     */
    Value computeIfAbsent(Key key, CacheLoader<Key, Value> loader) throws ExecutionException;

    /**
     * Invalidate the association for the specified key. A removal notification will be issued for invalidated
     * entries with {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     *
     * @param key the key whose mapping is to be invalidated from the cache
     */
    void invalidate(Key key);

    /**
     * Invalidate the entry for the specified key and value. If the value provided is not equal to the value in
     * the cache, no removal will occur. A removal notification will be issued for invalidated
     * entries with {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     *
     * @param key the key whose mapping is to be invalidated from the cache
     * @param value the expected value that should be associated with the key
     */
    void invalidate(Key key, Value value);

    /**
     * Invalidate all cache entries. A removal notification will be issued for invalidated entries with
     * {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     */
    void invalidateAll();

    /**
     * Blocking call that evaluates all entries if they meet the requirements and evicts the entries based on the eviction strategy if
     * provided / supported by the implementation, no-op otherwise.
     * A removal notification will be issued for evicted entries with
     * {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} EVICTED
     */
    default void refresh() {}

    /**
     * The number of entries in the cache.
     *
     * @return the number of entries in the cache
     */
    int count();

    /**
     * The total weight of all entries in the cache. This interface does not specify the unit or what gives an entry weight,
     * implementations are expected to allow the user to specify an external weigher in the form of a {@link ToLongBiFunction}
     *
     * @return the weight of all the entries in the cache
     */
    long weight();

    /**
     * An Iterable that allows to transverse all keys in the cache. Modifications might be visible and no guarantee is made on ordering of
     * these modifications as new entries might end up in the already transversed part of the cache. So while transversing; if A and B are
     * added to the cache in that order, we might only observe B. Implementations should allow the cache to be modified while transversing
     * but only {@link Iterator#remove()} is guaranteed to not affect further transversal.
     * Implementations might guarantee a specific sequencing or other stronger guarantees.
     * {@link Iterator#remove()} issues a removal notification with
     * {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     *
     * @return an {@link Iterable} over the keys in the cache
     */
    Iterable<Key> keys();

    /**
     * An Iterable that allows to transverse all keys in the cache. Modifications might be visible and no guarantee is made on ordering of
     * these modifications as new entries might end up in the already transversed part of the cache. So while transversing; if A and B are
     * added to the cache, and in that order, we might only observe B. Implementations should allow the cache to be modified while
     * transversing but only {@link Iterator#remove()} is guaranteed to not affect further transversal.
     * Implementations might guarantee a specific sequencing or other stronger guarantees.
     * {@link Iterator#remove()} issues a removal notification with
     * {@link org.elasticsearch.common.cache.RemovalNotification.RemovalReason} INVALIDATED.
     *
     * @return an {@link Iterable} over the values in the cache
     */
    Iterable<Value> values();

    /**
     * The cache statistics tracking hits, misses and evictions. These are captured on a best-effort basis.
     *
     * @return the current cache statistics
     */
    Stats stats();

    /**
     * Performs an action for each cache entry in the cache. While iterating over the cache entries this method might use locks. As such,
     * the specified consumer should not try to modify the cache. Visibility of modifications might or might not be seen by the consumer.
     *
     * @param consumer the {@link BiConsumer}
     */
    void forEach(BiConsumer<Key, Value> consumer);

    /**
     * Point in time capture of cache statistics
     * @param hits number of times a cached value was hit
     * @param misses number of times no cached value could be found
     * @param evictions number of entries that have been evicted
     */
    record Stats(long hits, long misses, long evictions) {

        public long getHits() {
            return hits;
        }

        public long getMisses() {
            return misses;
        }

        public long getEvictions() {
            return evictions;
        }
    }
}
