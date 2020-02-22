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

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.util.Objects;

public class LegacyReaderContext extends ReaderContext {
    private final ShardSearchRequest shardSearchRequest;
    private final ScrollContext scrollContext;
    private AggregatedDfs aggregatedDfs;
    private RescoreDocIds rescoreDocIds;

    public LegacyReaderContext(long id, IndexShard indexShard, Engine.Searcher engineSearcher, ShardSearchRequest shardSearchRequest) {
        super(id, indexShard, engineSearcher);
        this.shardSearchRequest = Objects.requireNonNull(shardSearchRequest);
        if (shardSearchRequest.scroll() != null) {
            this.scrollContext = new ScrollContext();
        } else {
            this.scrollContext = null;
        }
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
}
