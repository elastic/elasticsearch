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

/**
 * Interface for cache implementations, currently still quite tied to {@link LRUCache}
 *
 * @param <Key> Type used for keys to lookup values
 * @param <Value> Type used for values stored in the cache
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
     * Force any outstanding evictions to occur
     */
    void refresh();

    /**
     * The number of entries in the cache.
     *
     * @return the number of entries in the cache
     */
    int count();

    /**
     * The weight of the entries in the cache.
     *
     * @return the weight of the entries in the cache
     */
    long weight();

    /**
     * An undefined sequencing of the keys in the cache that supports removal. Implementations might guarantee a specific sequencing.
     * The returned sequence is not protected from mutations to the cache (except for {@link Iterator#remove()}. The result of
     * iteration under any other mutation is undefined.
     *
     * @return an {@link Iterable} over the keys in the cache
     */
    Iterable<Key> keys();

    /**
     * An undefined sequencing of the values in the cache that supports removal. Implementations might guarantee a specific sequencing.
     * The returned sequence is not protected from mutations to the cache (except for {@link Iterator#remove()}. The result of
     * iteration under any other mutation is undefined.
     *
     * @return an {@link Iterable} over the values in the cache
     */
    Iterable<Value> values();

    /**
     * The cache statistics tracking hits, misses and evictions. These are taken on a best-effort basis meaning that
     * they could be out-of-date mid-flight.
     *
     * @return the current cache statistics
     */
    CacheStats stats();

    /**
     * Point in time capture of stats
     * @param hits number of times a cached value was hit
     * @param misses number of times no cached value could be found
     * @param evictions number of entries that have been evicted
     */
    record CacheStats(long hits, long misses, long evictions) {

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
