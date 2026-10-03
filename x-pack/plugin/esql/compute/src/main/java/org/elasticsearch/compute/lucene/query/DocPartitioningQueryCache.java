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
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.IndicesQueryCache;

import java.util.Map;
import java.util.Set;

/**
 * A query cache for doc partitioning that tries to prevent multiple threads from populating the cache for the same segment.
 * Other threads pause at the operator level (via {@link SubscribableListener}) until caching completes, then use the cached result.
 * This is best-effort as other threads might also fall back to an uncached scorer.
 */
final class DocPartitioningQueryCache implements QueryCache {

    /**
     * How many instances are populating the cache for a leaf.
     */
    private static class PendingCaching {
        final SubscribableListener<Void> completion = new SubscribableListener<>();
        final RefCounted tasks = AbstractRefCounted.of(() -> completion.onResponse(null));
    }

    private final Map<Object, PendingCaching> pendingCachingPerLeaf = ConcurrentCollections.newConcurrentMap();
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
        var pendingTask = pendingCachingPerLeaf.get(leaf.id());
        if (pendingTask == null) {
            return null;
        }
        SubscribableListener<Void> completion = pendingTask.completion;
        if (completion.isDone()) {
            return null;
        }
        return completion;
    }

    private class DocPartitioningWeight extends IndicesQueryCache.OptionalCachingWeight {
        private final Set<Object> cached = ConcurrentCollections.newConcurrentSet();

        DocPartitioningWeight(Weight weight) {
            super(weight);
        }

        private void maybeRemoveCachingListener(LeafReaderContext leaf) {
            pendingCachingPerLeaf.compute(leaf.id(), (k, curr) -> curr == null || curr.tasks.hasReferences() == false ? null : curr);
        }

        @Override
        public Releasable startCaching(LeafReaderContext leaf) {
            var pending = pendingCachingPerLeaf.compute(
                leaf.id(),
                (k, curr) -> curr == null || curr.tasks.tryIncRef() == false ? new PendingCaching() : curr
            );
            if (cached.add(leaf.id())) {
                return () -> {
                    pending.tasks.decRef();
                    maybeRemoveCachingListener(leaf);
                };
            } else {
                pending.tasks.decRef();
                maybeRemoveCachingListener(leaf);
                return null;
            }
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
