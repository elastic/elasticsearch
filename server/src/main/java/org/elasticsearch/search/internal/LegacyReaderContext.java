/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.util.Objects;

public class LegacyReaderContext extends ReaderContext {
    private final ShardSearchRequest shardSearchRequest;
    private final ScrollContext scrollContext;
    private final Engine.Searcher searcher;

    private AggregatedDfs aggregatedDfs;
    private RescoreDocIds rescoreDocIds;

    public LegacyReaderContext(ShardSearchContextId id, IndexService indexService, IndexShard indexShard, Engine.SearcherSupplier reader,
                               ShardSearchRequest shardSearchRequest, long keepAliveInMillis) {
        super(id, indexService, indexShard, reader, keepAliveInMillis, false);
        assert shardSearchRequest.readerId() == null;
        assert shardSearchRequest.keepAlive() == null;
        this.shardSearchRequest = Objects.requireNonNull(shardSearchRequest);
        if (shardSearchRequest.scroll() != null) {
            // Search scroll requests are special, they don't hold indices names so we have
            // to reuse the searcher created on the request that initialized the scroll.
            // This ensures that we wrap the searcher's reader with the user's permissions
            // when they are available.
            final Engine.Searcher delegate = searcherSupplier.acquireSearcher("search");
            addOnClose(delegate);
            // wrap the searcher so that closing is a noop, the actual closing happens when this context is closed
            this.searcher = new Engine.Searcher(delegate.source(), delegate.getDirectoryReader(),
                delegate.getSimilarity(), delegate.getQueryCache(), delegate.getQueryCachingPolicy(), () -> {});
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
