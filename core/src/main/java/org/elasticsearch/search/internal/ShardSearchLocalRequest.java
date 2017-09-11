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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Optional;

/**
 * Shard level search request that gets created and consumed on the local node.
 * Used by warmers and by api that need to create a search context within their execution.
 *
 * Source structure:
 * <pre>
 * {
 *  from : 0, size : 20, (optional, can be set on the request)
 *  sort : { "name.first" : {}, "name.last" : { reverse : true } }
 *  fields : [ "name.first", "name.last" ]
 *  query : { ... }
 *  aggs : {
 *      "agg1" : {
 *          terms : { ... }
 *      }
 *  }
 * }
 * </pre>
 */

public class ShardSearchLocalRequest implements ShardSearchRequest {

    private String clusterAlias;
    private ShardId shardId;
    private int numberOfShards;
    private SearchType searchType;
    private Scroll scroll;
    private String[] types = Strings.EMPTY_ARRAY;
    private AliasFilter aliasFilter;
    private float indexBoost;
    private SearchSourceBuilder source;
    private Boolean requestCache;
    private long nowInMillis;

    private boolean profile;

    ShardSearchLocalRequest() {
    }

    ShardSearchLocalRequest(SearchRequest searchRequest, ShardId shardId, int numberOfShards,
                            AliasFilter aliasFilter, float indexBoost, long nowInMillis, String clusterAlias) {
        this(shardId, numberOfShards, searchRequest.searchType(),
                searchRequest.source(), searchRequest.types(), searchRequest.requestCache(), aliasFilter, indexBoost);
        this.scroll = searchRequest.scroll();
        this.nowInMillis = nowInMillis;
        this.clusterAlias = clusterAlias;
    }

    public ShardSearchLocalRequest(ShardId shardId, String[] types, long nowInMillis, AliasFilter aliasFilter) {
        this.types = types;
        this.nowInMillis = nowInMillis;
        this.aliasFilter = aliasFilter;
        this.shardId = shardId;
        indexBoost = 1.0f;
    }

    public ShardSearchLocalRequest(ShardId shardId, int numberOfShards, SearchType searchType, SearchSourceBuilder source, String[] types,
            Boolean requestCache, AliasFilter aliasFilter, float indexBoost) {
        this.shardId = shardId;
        this.numberOfShards = numberOfShards;
        this.searchType = searchType;
        this.source = source;
        this.types = types;
        this.requestCache = requestCache;
        this.aliasFilter = aliasFilter;
        this.indexBoost = indexBoost;
    }


    @Override
    public ShardId shardId() {
        return shardId;
    }

    @Override
    public String[] types() {
        return types;
    }

    @Override
    public SearchSourceBuilder source() {
        return source;
    }

    @Override
    public AliasFilter getAliasFilter() {
        return aliasFilter;
    }

    @Override
    public void setAliasFilter(AliasFilter aliasFilter) {
        this.aliasFilter = aliasFilter;
    }

    @Override
    public void source(SearchSourceBuilder source) {
        this.source = source;
    }

    @Override
    public int numberOfShards() {
        return numberOfShards;
    }

    @Override
    public SearchType searchType() {
        return searchType;
    }

    @Override
    public float indexBoost() {
        return indexBoost;
    }

    @Override
    public long nowInMillis() {
        return nowInMillis;
    }

    @Override
    public Boolean requestCache() {
        return requestCache;
    }

    @Override
    public Scroll scroll() {
        return scroll;
    }

    @Override
    public void setProfile(boolean profile) {
        this.profile = profile;
    }

    @Override
    public boolean isProfile() {
        return profile;
    }

    void setSearchType(SearchType type) {
        this.searchType = type;
    }

    protected void innerReadFrom(StreamInput in) throws IOException {
        shardId = ShardId.readShardId(in);
        searchType = SearchType.fromId(in.readByte());
        numberOfShards = in.readVInt();
        scroll = in.readOptionalWriteable(Scroll::new);
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        types = in.readStringArray();
        aliasFilter = new AliasFilter(in);
        if (in.getVersion().onOrAfter(Version.V_5_2_0)) {
            indexBoost = in.readFloat();
        } else {
            // Nodes < 5.2.0 doesn't send index boost. Read it from source.
            if (source != null) {
                Optional<SearchSourceBuilder.IndexBoost> boost = source.indexBoosts()
                    .stream()
                    .filter(ib -> ib.getIndex().equals(shardId.getIndexName()))
                    .findFirst();
                indexBoost = boost.isPresent() ? boost.get().getBoost() : 1.0f;
            } else {
                indexBoost = 1.0f;
            }
        }
        nowInMillis = in.readVLong();
        requestCache = in.readOptionalBoolean();
        if (in.getVersion().onOrAfter(Version.V_5_6_0)) {
            clusterAlias = in.readOptionalString();
        }
    }

    protected void innerWriteTo(StreamOutput out, boolean asKey) throws IOException {
        shardId.writeTo(out);
        out.writeByte(searchType.id());
        if (!asKey) {
            out.writeVInt(numberOfShards);
        }
        out.writeOptionalWriteable(scroll);
        out.writeOptionalWriteable(source);
        out.writeStringArray(types);
        aliasFilter.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_5_2_0)) {
            out.writeFloat(indexBoost);
        }
        if (!asKey) {
            out.writeVLong(nowInMillis);
        }
        out.writeOptionalBoolean(requestCache);
        if (out.getVersion().onOrAfter(Version.V_5_6_0)) {
            out.writeOptionalString(clusterAlias);
        }
    }

    @Override
    public BytesReference cacheKey() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        this.innerWriteTo(out, true);
        // copy it over, most requests are small, we might as well copy to make sure we are not sliced...
        // we could potentially keep it without copying, but then pay the price of extra unused bytes up to a page
        return new BytesArray(out.bytes().toBytesRef(), true);// do a deep copy
    }

    @Override
    public String getClusterAlias() {
        return clusterAlias;
    }

    @Override
    public Rewriteable<Rewriteable> getRewriteable() {
        return new RequestRewritable(this);
    }

    static class RequestRewritable implements Rewriteable<Rewriteable> {

        final ShardSearchRequest request;

        RequestRewritable(ShardSearchRequest request) {
            this.request = request;
        }

        @Override
        public Rewriteable rewrite(QueryRewriteContext ctx) throws IOException {
            SearchSourceBuilder newSource = request.source() == null ? null : Rewriteable.rewrite(request.source(), ctx);
            AliasFilter newAliasFilter = Rewriteable.rewrite(request.getAliasFilter(), ctx);
            if (newSource == request.source() && newAliasFilter == request.getAliasFilter()) {
                return this;
            } else {
                request.source(newSource);
                request.setAliasFilter(newAliasFilter);
                return new RequestRewritable(request);
            }
        }
    }

}
