/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.index.store.remote.utils.cache;


import org.elasticsearch.index.store.remote.utils.cache.stats.CacheStats;

import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Custom Cache which support typical cache operations (put, get, ...) and it support reference counting per individual key which might
 * change eviction behavior
 * @param <K> type of the key
 * @param <V> type of the value
 *
 * @opensearch.internal
 */
public interface RefCountedCache<K, V> {

    /**
     * Returns the value associated with {@code key} in this cache, or {@code null} if there is no
     * cached value for {@code key}. Retrieving an item automatically increases its reference
     * count.
     */
    V get(K key);

    /**
     * Associates {@code value} with {@code key} in this cache. If the cache previously contained a
     * value associated with {@code key}, the old value is replaced by {@code value}.
     */
    V put(K key, V value);

    /**
     * If the specified key is already associated with a value, attempts to update its value using the given mapping
     * function and enters the new value. If the mapping function returns null the item is removed from the
     * cache, regardless of its reference count. If the mapping function returns non-null the value is updated.
     * The new entry will have the reference count of the previous entry plus one, as this method automatically
     * increases the reference count by one when it returns the newly mapped value.
     * <p>
     * If the specified key is NOT already associated with a value, then the value of the remapping function
     * will be associated with the given key, and its reference count will be set to one. If the remapping function
     * returns null then nothing is done.
     * <p>
     * The remappingFunction method for a given key will be invoked at most once.
     */
    V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

    /**
     * Discards any cached value for key {@code key}, regardless of reference count.
     */
    void remove(K key);

    /**
     * Discards all entries in the cache, regardless of reference count.
     */
    void clear();

    /**
     * Returns the approximate number of entries in this cache.
     */
    long size();

    /**
     * increment references count for key {@code key}.
     */
    void incRef(K key);

    /**
     * decrement references count for key {@code key}.
     */
    void decRef(K key);

    /**
     * Removes all cache entries with a reference count of zero, regardless of current capacity.
     *
     * @return The total weight of all removed entries.
     */
    default long prune() {
        return prune(key -> true);
    }

    /**
     * Removes the cache entries which match the predicate criteria for the key
     * and have a reference count of zero, regardless of current capacity.
     *
     * @return The total weight of all removed entries.
     */
    long prune(Predicate<K> keyPredicate);

    /**
     * Returns the weighted usage of this cache.
     *
     * @return the combined weight of the values in this cache
     */
    CacheUsage usage();

    /**
     * Returns a current snapshot of this cache's cumulative statistics. All statistics are
     * initialized to zero, and are monotonically increasing over the lifetime of the cache.
     * <p>
     * Due to the performance penalty of maintaining statistics, some implementations may not record
     * the usage history immediately or at all.
     *
     * @return the current snapshot of the statistics of this cache
     */
    CacheStats stats();
}
