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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * Shard level search request that gets created and consumed on the local node.
 * Used directly by api that need to create a search context within their execution.
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
    private final String clusterAlias;
    private final ShardId shardId;
    private final int numberOfShards;
    private final SearchType searchType;
    private final Scroll scroll;
    private final float indexBoost;
    private final Boolean requestCache;
    private final long nowInMillis;
    private final boolean allowPartialSearchResults;
    private final String[] indexRoutings;
    private final String preference;
    //these are the only two mutable fields, as they are subject to rewriting
    private AliasFilter aliasFilter;
    private SearchSourceBuilder source;

    public ShardSearchLocalRequest(SearchRequest searchRequest, ShardId shardId, int numberOfShards, AliasFilter aliasFilter,
                                   float indexBoost, long nowInMillis, @Nullable String clusterAlias, String[] indexRoutings) {
        this(shardId, numberOfShards, searchRequest.searchType(), searchRequest.source(),
            searchRequest.requestCache(), aliasFilter, indexBoost, searchRequest.allowPartialSearchResults(), indexRoutings,
            searchRequest.preference(), searchRequest.scroll(), nowInMillis, clusterAlias);
        // If allowPartialSearchResults is unset (ie null), the cluster-level default should have been substituted
        // at this stage. Any NPEs in the above are therefore an error in request preparation logic.
        assert searchRequest.allowPartialSearchResults() != null;
    }

    public ShardSearchLocalRequest(ShardId shardId, long nowInMillis, AliasFilter aliasFilter) {
        this(shardId, -1, null, null, null, aliasFilter, 1.0f, false, Strings.EMPTY_ARRAY, null, null, nowInMillis, null);
    }

    private ShardSearchLocalRequest(ShardId shardId, int numberOfShards, SearchType searchType, SearchSourceBuilder source,
                                    Boolean requestCache, AliasFilter aliasFilter, float indexBoost, boolean allowPartialSearchResults,
                                    String[] indexRoutings, String preference, Scroll scroll, long nowInMillis,
                                    @Nullable String clusterAlias) {
        this.shardId = shardId;
        this.numberOfShards = numberOfShards;
        this.searchType = searchType;
        this.source = source;
        this.requestCache = requestCache;
        this.aliasFilter = aliasFilter;
        this.indexBoost = indexBoost;
        this.allowPartialSearchResults = allowPartialSearchResults;
        this.indexRoutings = indexRoutings;
        this.preference = preference;
        this.scroll = scroll;
        this.nowInMillis = nowInMillis;
        this.clusterAlias = clusterAlias;
    }

    ShardSearchLocalRequest(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        searchType = SearchType.fromId(in.readByte());
        numberOfShards = in.readVInt();
        scroll = in.readOptionalWriteable(Scroll::new);
        source = in.readOptionalWriteable(SearchSourceBuilder::new);
        if (in.getVersion().before(Version.V_8_0_0)) {
            // types no longer relevant so ignore
            String[] types = in.readStringArray();
            if (types.length > 0) {
                throw new IllegalStateException(
                        "types are no longer supported in search requests but found [" + Arrays.toString(types) + "]");
            }
        }
        aliasFilter = new AliasFilter(in);
        indexBoost = in.readFloat();
        nowInMillis = in.readVLong();
        requestCache = in.readOptionalBoolean();
        clusterAlias = in.readOptionalString();
        allowPartialSearchResults = in.readBoolean();
        indexRoutings = in.readStringArray();
        preference = in.readOptionalString();
    }

    protected final void innerWriteTo(StreamOutput out, boolean asKey) throws IOException {
        shardId.writeTo(out);
        out.writeByte(searchType.id());
        if (!asKey) {
            out.writeVInt(numberOfShards);
        }
        out.writeOptionalWriteable(scroll);
        out.writeOptionalWriteable(source);
        if (out.getVersion().before(Version.V_8_0_0)) {
            // types not supported so send an empty array to previous versions
            out.writeStringArray(Strings.EMPTY_ARRAY);
        }
        aliasFilter.writeTo(out);
        out.writeFloat(indexBoost);
        if (asKey == false) {
            out.writeVLong(nowInMillis);
        }
        out.writeOptionalBoolean(requestCache);
        out.writeOptionalString(clusterAlias);
        out.writeBoolean(allowPartialSearchResults);
        if (asKey == false) {
            out.writeStringArray(indexRoutings);
            out.writeOptionalString(preference);
        }
    }

    @Override
    public ShardId shardId() {
        return shardId;
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
    public boolean allowPartialSearchResults() {
        return allowPartialSearchResults;
    }

    @Override
    public Scroll scroll() {
        return scroll;
    }

    @Override
    public String[] indexRoutings() {
        return indexRoutings;
    }

    @Override
    public String preference() {
        return preference;
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
