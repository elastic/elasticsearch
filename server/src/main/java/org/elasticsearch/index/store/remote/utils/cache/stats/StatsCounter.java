/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.index.store.remote.utils.cache.stats;

import org.elasticsearch.index.store.remote.utils.cache.RefCountedCache;

import java.util.function.BiFunction;

/**
 * Accumulates statistics during the operation of a {@link RefCountedCache} for presentation by
 * {@link RefCountedCache#stats}. This is solely intended for consumption by {@code Cache} implementors.
 *
 * @opensearch.internal
 */
public interface StatsCounter<K> {

    /**
     * Records cache hits. This should be called when a cache request returns a cached value.
     *
     * @param count the number of hits to record
     */
    void recordHits(K key, int count);

    /**
     * Records cache misses. This should be called when a cache request returns a value that was not
     * found in the cache. This method should be called by the loading thread, as well as by threads
     * blocking on the load. Multiple concurrent calls to {@link RefCountedCache} lookup methods with the same
     * key on an absent value should result in a single call to either {@code recordLoadSuccess} or
     * {@code recordLoadFailure} and multiple calls to this method, despite all being served by the
     * results of a single load operation.
     *
     * @param count the number of misses to record
     */
    void recordMisses(K key, int count);

    /**
     * Records the explicit removal of an entry from the cache. This should only been called when an entry is
     * removed as a result of manual
     * {@link RefCountedCache#remove(Object)}
     * {@link RefCountedCache#compute(Object, BiFunction)}
     *
     * @param weight the weight of the removed entry
     */
    void recordRemoval(long weight);

    /**
     * Records the replacement of an entry from the cache. This should only been called when an entry is
     * replaced as a result of manual
     * {@link RefCountedCache#put(Object, Object)}
     * {@link RefCountedCache#compute(Object, BiFunction)}
     */
    void recordReplacement();

    /**
     * Records the eviction of an entry from the cache. This should only been called when an entry is
     * evicted due to the cache's eviction strategy, and not as a result of manual
     * {@link RefCountedCache#remove(Object)}  removals}.
     *
     * @param weight the weight of the evicted entry
     */
    void recordEviction(long weight);

    /**
     * Returns a snapshot of this counter's values. Note that this may be an inconsistent view, as it
     * may be interleaved with update operations.
     *
     * @return a snapshot of this counter's values
     */

    CacheStats snapshot();
}
