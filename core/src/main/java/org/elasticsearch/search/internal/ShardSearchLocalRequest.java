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

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.Template;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import static org.elasticsearch.search.Scroll.readScroll;

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

    private ShardId shardId;
    private int numberOfShards;
    private SearchType searchType;
    private Scroll scroll;
    private String[] types = Strings.EMPTY_ARRAY;
    private String[] filteringAliases;
    private SearchSourceBuilder source;
    private Template template;
    private Boolean requestCache;
    private long nowInMillis;

    private boolean profile;

    ShardSearchLocalRequest() {
    }

    ShardSearchLocalRequest(SearchRequest searchRequest, ShardRouting shardRouting, int numberOfShards,
                            String[] filteringAliases, long nowInMillis) {
        this(shardRouting.shardId(), numberOfShards, searchRequest.searchType(),
                searchRequest.source(), searchRequest.types(), searchRequest.requestCache());
        this.template = searchRequest.template();
        this.scroll = searchRequest.scroll();
        this.filteringAliases = filteringAliases;
        this.nowInMillis = nowInMillis;
    }

    public ShardSearchLocalRequest(String[] types, long nowInMillis) {
        this.types = types;
        this.nowInMillis = nowInMillis;
    }

    public ShardSearchLocalRequest(String[] types, long nowInMillis, String[] filteringAliases) {
        this(types, nowInMillis);
        this.filteringAliases = filteringAliases;
    }

    public ShardSearchLocalRequest(ShardId shardId, int numberOfShards, SearchType searchType, SearchSourceBuilder source, String[] types,
            Boolean requestCache) {
        this.shardId = shardId;
        this.numberOfShards = numberOfShards;
        this.searchType = searchType;
        this.source = source;
        this.types = types;
        this.requestCache = requestCache;
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
    public String[] filteringAliases() {
        return filteringAliases;
    }

    @Override
    public long nowInMillis() {
        return nowInMillis;
    }
    @Override
    public Template template() {
        return template;
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

    protected void innerReadFrom(StreamInput in) throws IOException {
        shardId = ShardId.readShardId(in);
        searchType = SearchType.fromId(in.readByte());
        numberOfShards = in.readVInt();
        if (in.readBoolean()) {
            scroll = readScroll(in);
        }
        if (in.readBoolean()) {
            source = new SearchSourceBuilder(in);
        }
        types = in.readStringArray();
        filteringAliases = in.readStringArray();
        nowInMillis = in.readVLong();
        template = in.readOptionalWriteable(Template::new);
        requestCache = in.readOptionalBoolean();
    }

    protected void innerWriteTo(StreamOutput out, boolean asKey) throws IOException {
        shardId.writeTo(out);
        out.writeByte(searchType.id());
        if (!asKey) {
            out.writeVInt(numberOfShards);
        }
        if (scroll == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            scroll.writeTo(out);
        }
        if (source == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            source.writeTo(out);

        }
        out.writeStringArray(types);
        out.writeStringArrayNullable(filteringAliases);
        if (!asKey) {
            out.writeVLong(nowInMillis);
        }

        out.writeOptionalWriteable(template);
        out.writeOptionalBoolean(requestCache);
    }

    @Override
    public BytesReference cacheKey() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        this.innerWriteTo(out, true);
        // copy it over, most requests are small, we might as well copy to make sure we are not sliced...
        // we could potentially keep it without copying, but then pay the price of extra unused bytes up to a page
        return out.bytes().copyBytesArray();
    }

    @Override
    public void rewrite(QueryShardContext context) throws IOException {
        SearchSourceBuilder source = this.source;
        SearchSourceBuilder rewritten = null;
        while (rewritten != source) {
            rewritten = source.rewrite(context);
            source = rewritten;
        }
        this.source = source;
    }
}
