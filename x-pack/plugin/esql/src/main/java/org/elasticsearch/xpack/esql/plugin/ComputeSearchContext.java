/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.DefaultShardContext;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;

/**
 * Search and shard context used as entries in {@link org.elasticsearch.compute.lucene.IndexedByShardId}. These are shared by both the data
 * and node-reduce drivers, such that each data driver will have its own (disjoint) set of contexts, but the node-reduce driver will have
 * access to all contexts created for the data drivers.
 * <p>
 * Closing this will only close the underlying search context, and doing so <i>directly</i> is generally only done during error handling. In
 * happy-path execution, this class will be closed when the reference count in the {@link ShardContext} returned by
 * {@link #shardContext(QueryWarnings)} reaches 0.
 * <p>
 * Two flavors of {@link ShardContext} creation are provided:
 * <ul>
 *     <li>{@link #shardContext(QueryWarnings)} — the normal path. The returned {@link ShardContext} holds a releasable back-reference to
 *     this context,
 *     so when operators finish and the shard context's ref count reaches zero, the underlying {@link SearchContext} is closed. This is
 *     cached via {@link SetOnce} since the same shard context is shared across data and node-reduce drivers.</li>
 *     <li>{@link #newDetachedShardContext()} — for the retained-contexts path (remote fetch). Creates a <i>fresh, independent</i>
 *     {@link ShardContext} whose close is a no-op with respect to the underlying {@link SearchContext}. Use this when the
 *     {@link SearchContext} lifecycle is managed externally (e.g., by a {@link RetainedSearchContextsRegistry} entry) and must outlive any
 *     single set of operators.</li>
 * </ul>
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

    ShardContext shardContext(QueryWarnings queryWarnings) {
        if (shardContext.get() == null) {
            shardContext.set(createShardContext(this, queryWarnings));
        }
        return shardContext.get();
    }

    public SearchContext searchContext() {
        return searchContext;
    }

    /**
     * Creates a fresh, independent {@link ShardContext} whose close does <i>not</i> release the underlying {@link SearchContext}. Use this
     * when the search context lifecycle is managed externally (e.g., by a {@link RetainedSearchContextsRegistry} entry) and must survive
     * across multiple sets of operators. Each call returns a new instance with its own ref count.
     * <p>
     * This detached mode is a temporary design: it exists to support retained-context remote fetch while the broader lifecycle model is
     * being finalized. Expect it to be replaced or folded into a unified context ownership scheme in a follow-up.
     */
    ShardContext newDetachedShardContext() {
        return createShardContext(() -> {}, QueryWarnings.NOOP);
    }

    private ShardContext createShardContext(Releasable releasable, QueryWarnings queryWarnings) {
        EsqlSearchExecutionContext searchExecutionContext = new EsqlSearchExecutionContext(
            searchContext.getSearchExecutionContext(),
            queryWarnings
        );
        // Registered unconditionally; for detached shard contexts this is a no-op since the remote fetch path does not construct
        // Lucene queries and the counter stays at zero.
        searchContext.addReleasable(searchExecutionContext::releaseQueryConstructionMemory);
        return new DefaultShardContext(index, releasable, searchExecutionContext, searchContext.request().getAliasFilter());
    }

    @Override
    public void close() {
        Releasables.close(searchContext);
    }
}
