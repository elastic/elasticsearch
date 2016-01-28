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
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.Template;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Shard level search request that represents an actual search sent from the coordinating node to the nodes holding
 * the shards where the query needs to be executed. Holds the same info as {@link org.elasticsearch.search.internal.ShardSearchLocalRequest}
 * but gets sent over the transport and holds also the indices coming from the original request that generated it, plus its headers and context.
 */
public class ShardSearchTransportRequest extends TransportRequest implements ShardSearchRequest, IndicesRequest {

    private OriginalIndices originalIndices;

    private ShardSearchLocalRequest shardSearchLocalRequest;

    public ShardSearchTransportRequest(){
    }

    public ShardSearchTransportRequest(SearchRequest searchRequest, ShardRouting shardRouting, int numberOfShards,
                                       String[] filteringAliases, long nowInMillis) {
        this.shardSearchLocalRequest = new ShardSearchLocalRequest(searchRequest, shardRouting, numberOfShards, filteringAliases, nowInMillis);
        this.originalIndices = new OriginalIndices(searchRequest);
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
    public String index() {
        return shardSearchLocalRequest.index();
    }

    @Override
    public int shardId() {
        return shardSearchLocalRequest.shardId();
    }

    @Override
    public String[] types() {
        return shardSearchLocalRequest.types();
    }

    @Override
    public SearchSourceBuilder source() {
        return shardSearchLocalRequest.source();
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
    public String[] filteringAliases() {
        return shardSearchLocalRequest.filteringAliases();
    }

    @Override
    public long nowInMillis() {
        return shardSearchLocalRequest.nowInMillis();
    }
    @Override
    public Template template() {
        return shardSearchLocalRequest.template();
    }

    @Override
    public Boolean requestCache() {
        return shardSearchLocalRequest.requestCache();
    }

    @Override
    public Scroll scroll() {
        return shardSearchLocalRequest.scroll();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardSearchLocalRequest = new ShardSearchLocalRequest();
        shardSearchLocalRequest.innerReadFrom(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardSearchLocalRequest.innerWriteTo(out, false);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

    @Override
    public BytesReference cacheKey() throws IOException {
        return shardSearchLocalRequest.cacheKey();
    }

    @Override
    public void setProfile(boolean profile) {
        shardSearchLocalRequest.setProfile(profile);
    }

    @Override
    public boolean isProfile() {
        return shardSearchLocalRequest.isProfile();
    }
}
