/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.esql.fetch.lifetime.DocRefOriginResolver;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.DefaultShardContext;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;

import java.util.Objects;

/**
 * Search and shard context used as entries in {@link org.elasticsearch.compute.lucene.IndexedByShardId}. These are shared by both the data
 * and node-reduce drivers, such that each data driver will have its own (disjoint) set of contexts, but the node-reduce driver will have
 * access to all contexts created for the data drivers.
 * <p>
 * Closing this will only close the underlying search context, and doing so <i>directly</i> is generally only done during error handling. In
 * happy-path execution, this class will be closed when the reference count in the {@link ShardContext} returned by
 * {@link #shardContext(QueryWarnings)} reaches 0.
 */
class ComputeSearchContext implements Releasable {
    private final int index;
    @Nullable
    private final SearchContext searchContext;
    @Nullable
    private final DocRefOriginResolver originResolver;
    private final SetOnce<ShardContext> shardContext = new SetOnce<>();

    ComputeSearchContext(int index, SearchContext searchContext) {
        this(index, searchContext, DocRefOriginResolver.NONE);
    }

    /**
     * @param originResolver names the reader of this shard for rows that leave the node as document references
     */
    ComputeSearchContext(int index, SearchContext searchContext, DocRefOriginResolver originResolver) {
        this.index = index;
        this.searchContext = Objects.requireNonNull(searchContext);
        this.originResolver = Objects.requireNonNull(originResolver);
    }

    private ComputeSearchContext(int index) {
        this.index = index;
        this.searchContext = null;
        this.originResolver = null;
    }

    /** Tombstone with no {@link SearchContext}, so the closed search context can be GC'd. */
    ComputeSearchContext tombstone() {
        return new ComputeSearchContext(index);
    }

    private void ensureNotTombstone() {
        if (searchContext == null) {
            assert false : "ComputeSearchContext for index [" + index + "] was already closed";
            throw new AlreadyClosedException("ComputeSearchContext for index [" + index + "] was already closed");
        }
    }

    public int index() {
        return index;
    }

    ShardContext shardContext(QueryWarnings queryWarnings) {
        if (shardContext.get() == null) {
            shardContext.set(createShardContext(queryWarnings));
        }
        return shardContext.get();
    }

    public SearchContext searchContext() {
        ensureNotTombstone();
        return searchContext;
    }

    private ShardContext createShardContext(QueryWarnings queryWarnings) {
        ensureNotTombstone();
        EsqlSearchExecutionContext searchExecutionContext = new EsqlSearchExecutionContext(
            searchContext.getSearchExecutionContext(),
            queryWarnings
        );
        searchContext.addReleasable(searchExecutionContext::releaseQueryConstructionMemory);
        return new DefaultShardContext(
            index,
            this,
            searchExecutionContext,
            searchContext.request().getAliasFilter(),
            CachedSupplier.wrap(() -> originResolver.originOf(searchContext))
        );
    }

    @Override
    public void close() {
        Releasables.close(searchContext);
    }
}
