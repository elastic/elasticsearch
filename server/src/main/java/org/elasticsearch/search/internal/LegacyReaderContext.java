/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.util.Objects;

public final class LegacyReaderContext extends ReaderContext {
    private final ShardSearchRequest shardSearchRequest;
    private final ScrollContext scrollContext;
    private final Engine.Searcher searcher;

    private AggregatedDfs aggregatedDfs;
    private RescoreDocIds rescoreDocIds;

    public LegacyReaderContext(
        ShardSearchContextId id,
        IndexService indexService,
        IndexShard indexShard,
        Engine.SearcherSupplier reader,
        ShardSearchRequest shardSearchRequest,
        long keepAliveInMillis
    ) {
        super(id, indexService, indexShard, reader, keepAliveInMillis, false);
        assert shardSearchRequest.readerId() == null;
        assert shardSearchRequest.keepAlive() == null;
        assert id.getSearcherId() == null : "Legacy reader context must not have searcher id";
        this.shardSearchRequest = Objects.requireNonNull(shardSearchRequest, "ShardSearchRequest must be provided");
        if (shardSearchRequest.scroll() != null) {
            // Search scroll requests are special, they don't hold indices names so we have
            // to reuse the searcher created on the request that initialized the scroll.
            // This ensures that we wrap the searcher's reader with the user's permissions
            // when they are available.
            final Engine.Searcher delegate = searcherSupplier.acquireSearcher("search");
            addOnClose(delegate);
            // wrap the searcher so that closing is a noop, the actual closing happens when this context is closed
            this.searcher = new Engine.Searcher(
                delegate.source(),
                delegate.getDirectoryReader(),
                delegate.getSimilarity(),
                delegate.getQueryCache(),
                delegate.getQueryCachingPolicy(),
                () -> {}
            );
            this.scrollContext = new ScrollContext();
        } else {
            this.scrollContext = null;
            this.searcher = null;
        }
    }

    @Override
    public Engine.Searcher acquireSearcher(String source) {
        if (scrollContext != null) {
            assert Engine.SEARCH_SOURCE.equals(source) : "scroll context should not acquire searcher for " + source;
            return searcher;
        }
        return super.acquireSearcher(source);
    }

    @Override
    public ShardSearchRequest getShardSearchRequest(ShardSearchRequest other) {
        if (other != null) {
            // The top level knn search modifies the source after the DFS phase.
            // so we need to update the source stored in the context.
            shardSearchRequest.source(other.source());
        }
        return shardSearchRequest;
    }

    @Override
    public ScrollContext scrollContext() {
        return scrollContext;
    }

    @Override
    public AggregatedDfs getAggregatedDfs(AggregatedDfs other) {
        return aggregatedDfs;
    }

    @Override
    public void setAggregatedDfs(AggregatedDfs aggregatedDfs) {
        this.aggregatedDfs = aggregatedDfs;
    }

    @Override
    public RescoreDocIds getRescoreDocIds(RescoreDocIds other) {
        return rescoreDocIds;
    }

    @Override
    public void setRescoreDocIds(RescoreDocIds rescoreDocIds) {
        this.rescoreDocIds = rescoreDocIds;
    }

    @Override
    public boolean singleSession() {
        return scrollContext == null || scrollContext.scroll == null;
    }
}
