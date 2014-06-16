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
import org.elasticsearch.action.search.type.ParsedScrollId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.Scroll.readScroll;

/**
 * Source structure:
 * <p/>
 * <pre>
 * {
 *  from : 0, size : 20, (optional, can be set on the request)
 *  sort : { "name.first" : {}, "name.last" : { reverse : true } }
 *  fields : [ "name.first", "name.last" ]
 *  query : { ... }
 *  facets : {
 *      "facet1" : {
 *          query : { ... }
 *      }
 *  }
 * }
 * </pre>
 */
public class ShardSearchRequest extends TransportRequest {

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
    private Map<String, String> templateParams;

    private long nowInMillis;

    private boolean useSlowScroll;

    public ShardSearchRequest() {
    }

    public ShardSearchRequest(SearchRequest searchRequest, ShardRouting shardRouting, int numberOfShards, boolean useSlowScroll) {
        super(searchRequest);
        this.index = shardRouting.index();
        this.shardId = shardRouting.id();
        this.numberOfShards = numberOfShards;
        this.searchType = searchRequest.searchType();
        this.source = searchRequest.source();
        this.extraSource = searchRequest.extraSource();
        this.templateSource = searchRequest.templateSource();
        this.templateName = searchRequest.templateName();
        this.templateType = searchRequest.templateType();
        this.templateParams = searchRequest.templateParams();
        this.scroll = searchRequest.scroll();
        this.types = searchRequest.types();
        this.useSlowScroll = useSlowScroll;
    }

    public ShardSearchRequest(ShardRouting shardRouting, int numberOfShards, SearchType searchType) {
        this(shardRouting.index(), shardRouting.id(), numberOfShards, searchType);
    }

    public ShardSearchRequest(String index, int shardId, int numberOfShards, SearchType searchType) {
        this.index = index;
        this.shardId = shardId;
        this.numberOfShards = numberOfShards;
        this.searchType = searchType;
    }

    public String index() {
        return index;
    }

    public int shardId() {
        return shardId;
    }

    public SearchType searchType() {
        return this.searchType;
    }

    public int numberOfShards() {
        return numberOfShards;
    }

    public BytesReference source() {
        return this.source;
    }

    public BytesReference extraSource() {
        return this.extraSource;
    }

    public ShardSearchRequest source(BytesReference source) {
        this.source = source;
        return this;
    }

    public ShardSearchRequest extraSource(BytesReference extraSource) {
        this.extraSource = extraSource;
        return this;
    }

    public BytesReference templateSource() {
        return this.templateSource;
    }

    public String templateName() {
        return templateName;
    }

    public ScriptService.ScriptType templateType() {
        return templateType;
    }

    public Map<String, String> templateParams() {
        return templateParams;
    }

    public ShardSearchRequest nowInMillis(long nowInMillis) {
        this.nowInMillis = nowInMillis;
        return this;
    }

    public long nowInMillis() {
        return this.nowInMillis;
    }

    public Scroll scroll() {
        return scroll;
    }

    public ShardSearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    public ShardSearchRequest filteringAliases(String[] filteringAliases) {
        this.filteringAliases = filteringAliases;
        return this;
    }

    public String[] types() {
        return types;
    }

    public ShardSearchRequest types(String[] types) {
        this.types = types;
        return this;
    }

    /**
     * This setting is internal and will be enabled when at least one node is on versions 1.0.x and 1.1.x to enable
     * scrolling that those versions support.
     *
     * @return Whether the scrolling should use regular search and incrementing the from on each round, which can
     * bring down nodes due to the big priority queues being generated to accommodate from + size hits for sorting.
     */
    public boolean useSlowScroll() {
        return useSlowScroll;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
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

        if (in.getVersion().onOrAfter(Version.V_1_1_0)) {
            templateSource = in.readBytesReference();
            templateName = in.readOptionalString();
            if (in.getVersion().onOrAfter(Version.V_1_3_0)) {
                templateType = ScriptService.ScriptType.readFrom(in);
            }
            if (in.readBoolean()) {
                templateParams = (Map<String, String>) in.readGenericValue();
            }
        }
        if (in.getVersion().onOrAfter(ParsedScrollId.SCROLL_SEARCH_AFTER_MINIMUM_VERSION)) {
            useSlowScroll = in.readBoolean();
        } else {
            // This means that this request was send from a 1.0.x or 1.1.x node and we need to fallback to slow scroll.
            useSlowScroll = in.getVersion().before(ParsedScrollId.SCROLL_SEARCH_AFTER_MINIMUM_VERSION);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeByte(searchType.id());
        out.writeVInt(numberOfShards);
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
        out.writeVLong(nowInMillis);

        if (out.getVersion().onOrAfter(Version.V_1_1_0)) {
            out.writeBytesReference(templateSource);
            out.writeOptionalString(templateName);
            if (out.getVersion().onOrAfter(Version.V_1_3_0)) {
                ScriptService.ScriptType.writeTo(templateType, out);
            }
            boolean existTemplateParams = templateParams != null;
            out.writeBoolean(existTemplateParams);
            if (existTemplateParams) {
                out.writeGenericValue(templateParams);
            }
        }
        if (out.getVersion().onOrAfter(ParsedScrollId.SCROLL_SEARCH_AFTER_MINIMUM_VERSION)) {
            out.writeBoolean(useSlowScroll);
        }
    }
}
