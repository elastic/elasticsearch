/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.index.store.remote.utils.cache.stats;


/**
 * A non thread-safe {@link StatsCounter} implementation.
 *
 * @opensearch.internal
 */
public class DefaultStatsCounter<K> implements StatsCounter<K> {
    private long hitCount;
    private long missCount;
    private long removeCount;
    private long removeWeight;
    private long replaceCount;
    private long evictionCount;
    private long evictionWeight;

    public DefaultStatsCounter() {
        this.hitCount = 0L;
        this.missCount = 0L;
        this.removeCount = 0L;
        this.removeWeight = 0L;
        this.replaceCount = 0L;
        this.evictionCount = 0L;
        this.evictionWeight = 0L;
    }

    @Override
    public void recordHits(K key, int count) {
        hitCount += count;
    }

    @Override
    public void recordMisses(K key, int count) {
        missCount += count;
    }

    @Override
    public void recordRemoval(long weight) {
        removeCount++;
        removeWeight += weight;
    }

    @Override
    public void recordReplacement() {
        replaceCount++;
    }

    @Override
    public void recordEviction(long weight) {
        evictionCount++;
        evictionWeight += weight;
    }

    @Override
    public CacheStats snapshot() {
        return new CacheStats(hitCount, missCount, removeCount, removeWeight, replaceCount, evictionCount, evictionWeight);
    }

    @Override
    public String toString() {
        return snapshot().toString();
    }
}
