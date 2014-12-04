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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.Scroll.readScroll;

/**
 * Shard level search request that gets created and consumed on the local node.
 * Used by warmers and by api that need to create a search context within their execution.
 *
 * Source structure:
 * <p/>
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

    private String index;

    private int shardId;

    private int numberOfShards;

    private SearchType searchType;

    private Scroll scroll;

    private String[] types = Strings.EMPTY_ARRAY;

    private String[] filteringAliases;

    private BytesReference source;
    private BytesReference extraSource;
    private BytesReference templateSource;
    private String templateName;
    private ScriptService.ScriptType templateType;
    private Map<String, Object> templateParams;
    private Boolean queryCache;

    private long nowInMillis;

    ShardSearchLocalRequest() {
    }

    ShardSearchLocalRequest(SearchRequest searchRequest, ShardRouting shardRouting, int numberOfShards,
                            String[] filteringAliases, long nowInMillis) {
        this(shardRouting.shardId(), numberOfShards, searchRequest.searchType(),
                searchRequest.source(), searchRequest.types(), searchRequest.queryCache());

        this.extraSource = searchRequest.extraSource();
        this.templateSource = searchRequest.templateSource();
        this.templateName = searchRequest.templateName();
        this.templateType = searchRequest.templateType();
        this.templateParams = searchRequest.templateParams();
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

    public ShardSearchLocalRequest(ShardId shardId, int numberOfShards, SearchType searchType,
                                   BytesReference source, String[] types, Boolean queryCache) {
        this.index = shardId.getIndex();
        this.shardId = shardId.id();
        this.numberOfShards = numberOfShards;
        this.searchType = searchType;
        this.source = source;
        this.types = types;
        this.queryCache = queryCache;
    }

    @Override
    public String index() {
        return index;
    }

    @Override
    public int shardId() {
        return shardId;
    }

    @Override
    public String[] types() {
        return types;
    }

    @Override
    public BytesReference source() {
        return source;
    }

    @Override
    public void source(BytesReference source) {
        this.source = source;
    }

    @Override
    public BytesReference extraSource() {
        return extraSource;
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
    public String templateName() {
        return templateName;
    }

    @Override
    public ScriptService.ScriptType templateType() {
        return templateType;
    }

    @Override
    public Map<String, Object> templateParams() {
        return templateParams;
    }

    @Override
    public BytesReference templateSource() {
        return templateSource;
    }

    @Override
    public Boolean queryCache() {
        return queryCache;
    }

    @Override
    public Scroll scroll() {
        return scroll;
    }

    @SuppressWarnings("unchecked")
    protected void innerReadFrom(StreamInput in) throws IOException {
        index = in.readString();
        shardId = in.readVInt();
        searchType = SearchType.fromId(in.readByte());
        numberOfShards = in.readVInt();
        if (in.readBoolean()) {
            scroll = readScroll(in);
        }

        source = in.readBytesReference();
        extraSource = in.readBytesReference();

        types = in.readStringArray();
        filteringAliases = in.readStringArray();
        nowInMillis = in.readVLong();

        templateSource = in.readBytesReference();
        templateName = in.readOptionalString();
        templateType = ScriptService.ScriptType.readFrom(in);
        if (in.readBoolean()) {
            templateParams = (Map<String, Object>) in.readGenericValue();
        }
        queryCache = in.readOptionalBoolean();
    }

    protected void innerWriteTo(StreamOutput out, boolean asKey) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
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
        out.writeBytesReference(source);
        out.writeBytesReference(extraSource);
        out.writeStringArray(types);
        out.writeStringArrayNullable(filteringAliases);
        if (!asKey) {
            out.writeVLong(nowInMillis);
        }

        out.writeBytesReference(templateSource);
        out.writeOptionalString(templateName);
        ScriptService.ScriptType.writeTo(templateType, out);
        boolean existTemplateParams = templateParams != null;
        out.writeBoolean(existTemplateParams);
        if (existTemplateParams) {
            out.writeGenericValue(templateParams);
        }
        out.writeOptionalBoolean(queryCache);
    }

    @Override
    public BytesReference cacheKey() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        this.innerWriteTo(out, true);
        // copy it over, most requests are small, we might as well copy to make sure we are not sliced...
        // we could potentially keep it without copying, but then pay the price of extra unused bytes up to a page
        return out.bytes().copyBytesArray();
    }
}
