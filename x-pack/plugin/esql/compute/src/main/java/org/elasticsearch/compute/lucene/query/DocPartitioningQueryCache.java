/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.IndicesQueryCache;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A query cache for doc partitioning that tries to prevent multiple threads from populating the cache for the same segment.
 * Other threads pause at the operator level (via {@link SubscribableListener}) until caching completes, then use the cached result.
 * This is best-effort as other threads might also fall back to an uncached scorer.
 */
final class DocPartitioningQueryCache implements QueryCache {
    private final Map<Object, SubscribableListener<Void>> cachingListeners = ConcurrentCollections.newConcurrentMap();
    private final QueryCache actual;

    DocPartitioningQueryCache(QueryCache actual) {
        this.actual = actual;
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        if (weight instanceof CachingWeightWrapper) {
            return weight;
        }
        return new CachingWeightWrapper(actual.doCache(new DocPartitioningWeight(weight), policy));
    }

    /**
     * Returns a listener that completes when all cache population finishes for the given leaf,
     * or {@code null} if no caching is in progress for this leaf.
     */
    SubscribableListener<Void> blockedOnCaching(LeafReaderContext leaf) {
        SubscribableListener<Void> listener = cachingListeners.get(leaf.id());
        if (listener == null || listener.isDone()) {
            return null;
        }
        return listener;
    }

    private static SubscribableListener<Void> combine(SubscribableListener<Void> first, SubscribableListener<Void> second) {
        SubscribableListener<Void> combined = new SubscribableListener<>();
        AtomicInteger counter = new AtomicInteger(2);
        ActionListener<Void> onComplete = ActionListener.running(() -> {
            if (counter.decrementAndGet() == 0) {
                combined.onResponse(null);
            }
        });
        first.addListener(onComplete);
        second.addListener(onComplete);
        return combined;
    }

    private class DocPartitioningWeight extends IndicesQueryCache.OptionalCachingWeight {
        private final Set<Object> cached = ConcurrentCollections.newConcurrentSet();

        DocPartitioningWeight(Weight weight) {
            super(weight);
        }

        private void maybeRemoveCachingListener(LeafReaderContext leaf) {
            cachingListeners.compute(leaf.id(), (k, curr) -> curr == null || curr.isDone() ? null : curr);
        }

        @Override
        public Releasable startCaching(LeafReaderContext leaf) {
            final SubscribableListener<Void> listener = new SubscribableListener<>();
            cachingListeners.compute(leaf.id(), (k, curr) -> curr == null || curr.isDone() ? listener : combine(curr, listener));
            if (cached.add(leaf.id())) {
                return () -> {
                    listener.onResponse(null);
                    maybeRemoveCachingListener(leaf);
                };
            }
            listener.onResponse(null);
            maybeRemoveCachingListener(leaf);
            return null;
        }
    }

    /**
     * Marker to prevent double-wrapping a {@link Weight} that already passed through {@link #doCache}.
     * Queries like {@link org.apache.lucene.search.ConstantScoreQuery} can invoke {@code doCache(doCache(w))}.
     * See CachingWeightWrapper in {@link IndicesQueryCache} or {@link org.apache.lucene.search.LRUQueryCache}
     */
    private static class CachingWeightWrapper extends FilterWeight {
        CachingWeightWrapper(Weight weight) {
            super(weight);
        }
    }
}
