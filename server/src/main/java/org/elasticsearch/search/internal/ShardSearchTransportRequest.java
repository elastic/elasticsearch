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

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

/**
 * Shard level search request that represents an actual search sent from the coordinating node to the nodes holding
 * the shards where the query needs to be executed. Holds the same info as {@link org.elasticsearch.search.internal.ShardSearchLocalRequest}
 * but gets sent over the transport and holds also the indices coming from the original request that generated it, plus its headers and
 * context.
 */
public class ShardSearchTransportRequest extends TransportRequest implements ShardSearchRequest, IndicesRequest {

    private final OriginalIndices originalIndices;
    private final ShardSearchLocalRequest shardSearchLocalRequest;

    public ShardSearchTransportRequest(OriginalIndices originalIndices, SearchRequest searchRequest, ShardId shardId, int numberOfShards,
                                       AliasFilter aliasFilter, float indexBoost, long nowInMillis,
                                       @Nullable String clusterAlias, String[] indexRoutings) {
        this.shardSearchLocalRequest = new ShardSearchLocalRequest(searchRequest, shardId, numberOfShards, aliasFilter, indexBoost,
            nowInMillis, clusterAlias, indexRoutings);
        this.originalIndices = originalIndices;
    }

    public ShardSearchTransportRequest(StreamInput in) throws IOException {
        super(in);
        shardSearchLocalRequest = new ShardSearchLocalRequest(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardSearchLocalRequest.innerWriteTo(out, false);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

    @Override
    public String[] indices() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indicesOptions();
    }

    @Override
    public ShardId shardId() {
        return shardSearchLocalRequest.shardId();
    }

    @Override
    public SearchSourceBuilder source() {
        return shardSearchLocalRequest.source();
    }

    @Override
    public AliasFilter getAliasFilter() {
        return shardSearchLocalRequest.getAliasFilter();
    }

    @Override
    public void setAliasFilter(AliasFilter filter) {
        shardSearchLocalRequest.setAliasFilter(filter);
    }

    @Override
    public void source(SearchSourceBuilder source) {
        shardSearchLocalRequest.source(source);
    }

    @Override
    public int numberOfShards() {
        return shardSearchLocalRequest.numberOfShards();
    }

    @Override
    public SearchType searchType() {
        return shardSearchLocalRequest.searchType();
    }

    @Override
    public float indexBoost() {
        return shardSearchLocalRequest.indexBoost();
    }

    @Override
    public long nowInMillis() {
        return shardSearchLocalRequest.nowInMillis();
    }

    @Override
    public Boolean requestCache() {
        return shardSearchLocalRequest.requestCache();
    }

    @Override
    public boolean allowPartialSearchResults() {
        return shardSearchLocalRequest.allowPartialSearchResults();
    }

    @Override
    public Scroll scroll() {
        return shardSearchLocalRequest.scroll();
    }

    @Override
    public String[] indexRoutings() {
        return shardSearchLocalRequest.indexRoutings();
    }

    @Override
    public String preference() {
        return shardSearchLocalRequest.preference();
    }

    @Override
    public BytesReference cacheKey() throws IOException {
        return shardSearchLocalRequest.cacheKey();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        // Shard id is enough here, the request itself can be found by looking at the parent task description
        return "shardId[" + shardSearchLocalRequest.shardId() + "]";
    }

    @Override
    public String getClusterAlias() {
        return shardSearchLocalRequest.getClusterAlias();
    }

    @Override
    public Rewriteable<Rewriteable> getRewriteable() {
        return shardSearchLocalRequest.getRewriteable();
    }
}
