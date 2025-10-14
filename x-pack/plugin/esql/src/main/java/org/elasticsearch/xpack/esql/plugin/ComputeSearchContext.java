/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.core.Releasable;
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
 *
 * Closing this will only close the underlying search context, and doing so <i>directly</i> is generally only done during error handling. In
 * happy-path execution, this class will be closed when the reference count in the {@link ShardContext} returned by {@link #shardContext()}
 * reaches 0.
 */
class ComputeSearchContext implements Releasable {
    private final int index;
    private final SearchContext searchContext;
    private final SetOnce<ShardContext> shardContext = new SetOnce<>();

    ComputeSearchContext(int index, SearchContext searchContext) {
        this.index = index;
        this.searchContext = searchContext;
    }

    public int index() {
        return index;
    }

    ShardContext shardContext() {
        if (shardContext.get() == null) {
            shardContext.set(createShardContext());
        }
        return shardContext.get();
    }

    public SearchContext searchContext() {
        return searchContext;
    }

    private ShardContext createShardContext() {
        SearchExecutionContext searchExecutionContext = new SearchExecutionContext(searchContext.getSearchExecutionContext()) {
            @Override
            public SourceProvider createSourceProvider(SourceFilter sourceFilter) {
                return new ReinitializingSourceProvider(super::createSourceProvider);
            }
        };
        return new DefaultShardContext(index, this, searchExecutionContext, searchContext.request().getAliasFilter());
    }

    @Override
    public void close() {
        Releasables.close(searchContext);
    }
}
