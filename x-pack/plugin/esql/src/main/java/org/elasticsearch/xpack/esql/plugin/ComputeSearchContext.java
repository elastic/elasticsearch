/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.DefaultShardContext;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;

/**
 * Search and shard context used as entries in {@link org.elasticsearch.compute.lucene.IndexedByShardId}. These are shared by both the data
 * and node-reduce drivers, such that each data driver will have its own (disjoint) set of contexts, but the node-reduce driver will have
 * access to all contexts created for the data drivers.
 * <p>
 * This class is ref-counted rather than {@link org.elasticsearch.core.Releasable} because it has multiple concurrent owners (the holder
 * that created it and, optionally, a lazily-created {@link ShardContext}). Callers release ownership via {@link #decRef()}, and the
 * underlying {@link SearchContext} is closed only when the count reaches zero.
 */
class ComputeSearchContext {
    private final int index;
    private final SearchContext searchContext;
    private final SetOnce<ShardContext> shardContext = new SetOnce<>();
    /**
     * Tracks ownership of the underlying {@link SearchContext}. Starts at 1 (for the holder that created this context); incremented
     * again if a {@link ShardContext} is lazily created (since it holds a back-reference via {@code this::decRef}). The search context
     * is closed when the count reaches zero. This is intentionally separate from the operator-level ref counting in
     * {@link ShardContext}, which tracks how many Lucene operators are actively using the shard context.
     */
    private final RefCounted refs;

    ComputeSearchContext(int index, SearchContext searchContext) {
        this.index = index;
        this.searchContext = searchContext;
        this.refs = AbstractRefCounted.of(() -> Releasables.close(searchContext));
    }

    public int index() {
        return index;
    }

    ShardContext shardContext() {
        if (shardContext.get() == null) {
            synchronized (this) {
                if (shardContext.get() == null) {
                    refs.incRef();
                    shardContext.set(createShardContext());
                }
            }
        }
        return shardContext.get();
    }

    public SearchContext searchContext() {
        return searchContext;
    }

    void incRef() {
        refs.incRef();
    }

    private ShardContext createShardContext() {
        SearchExecutionContext searchExecutionContext = new SearchExecutionContext(searchContext.getSearchExecutionContext()) {
            @Override
            public SourceProvider createSourceProvider(SourceFilter sourceFilter) {
                return new ReinitializingSourceProvider(super::createSourceProvider);
            }
        };
        searchContext.addReleasable(searchExecutionContext::releaseQueryConstructionMemory);
        return new DefaultShardContext(index, this::decRef, searchExecutionContext, searchContext.request().getAliasFilter());
    }

    boolean decRef() {
        return refs.decRef();
    }
}
