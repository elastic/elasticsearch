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

import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

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
                                       boolean useSlowScroll, String[] filteringAliases, long nowInMillis) {
        super(searchRequest);
        this.shardSearchLocalRequest = new ShardSearchLocalRequest(searchRequest, shardRouting, numberOfShards,
                useSlowScroll, filteringAliases, nowInMillis);
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
    public BytesReference source() {
        return shardSearchLocalRequest.source();
    }

    @Override
    public void source(BytesReference source) {
        shardSearchLocalRequest.source(source);
    }

    @Override
    public BytesReference extraSource() {
        return shardSearchLocalRequest.extraSource();
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
    public String templateName() {
        return shardSearchLocalRequest.templateName();
    }

    @Override
    public ScriptService.ScriptType templateType() {
        return shardSearchLocalRequest.templateType();
    }

    @Override
    public Map<String, String> templateParams() {
        return shardSearchLocalRequest.templateParams();
    }

    @Override
    public BytesReference templateSource() {
        return shardSearchLocalRequest.templateSource();
    }

    @Override
    public Boolean queryCache() {
        return shardSearchLocalRequest.queryCache();
    }

    @Override
    public Scroll scroll() {
        return shardSearchLocalRequest.scroll();
    }

    @Override
    public boolean useSlowScroll() {
        return shardSearchLocalRequest.useSlowScroll();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardSearchLocalRequest = new ShardSearchLocalRequest();
        shardSearchLocalRequest.innerReadFrom(in);
        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1) && in.getVersion().before(Version.V_1_4_0)) {
            //original indices used to be optional in 1.4.0.Beta1 but ended up being empty only when the
            // shard search request was used locally and never serialized
            in.readBoolean();
        }
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardSearchLocalRequest.innerWriteTo(out, false);
        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1) && out.getVersion().before(Version.V_1_4_0)) {
            //original indices used to be optional in 1.4.0.Beta1 although ended up being empty only when the
            //shard search request was used locally and never serialized
            out.writeBoolean(true);
        }
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

    @Override
    public BytesReference cacheKey() throws IOException {
        return shardSearchLocalRequest.cacheKey();
    }
}
