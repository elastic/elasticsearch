/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.search.Scroll;

import java.io.IOException;

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
public class InternalSearchRequest implements Streamable {

    private String index;

    private int shardId;

    private int numberOfShards;

    private SearchType searchType;

    private Scroll scroll;

    private String[] types = Strings.EMPTY_ARRAY;

    private String[] filteringAliases;

    private BytesReference source;
    private BytesReference extraSource;

    private long nowInMillis;

    public InternalSearchRequest() {
    }

    public InternalSearchRequest(ShardRouting shardRouting, int numberOfShards, SearchType searchType) {
        this(shardRouting.index(), shardRouting.id(), numberOfShards, searchType);
    }

    public InternalSearchRequest(String index, int shardId, int numberOfShards, SearchType searchType) {
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

    public InternalSearchRequest source(BytesReference source) {
        this.source = source;
        return this;
    }

    public InternalSearchRequest extraSource(BytesReference extraSource) {
        this.extraSource = extraSource;
        return this;
    }

    public InternalSearchRequest nowInMillis(long nowInMillis) {
        this.nowInMillis = nowInMillis;
        return this;
    }

    public long nowInMillis() {
        return this.nowInMillis;
    }

    public Scroll scroll() {
        return scroll;
    }

    public InternalSearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    public void filteringAliases(String[] filteringAliases) {
        this.filteringAliases = filteringAliases;
    }

    public String[] types() {
        return types;
    }

    public InternalSearchRequest types(String[] types) {
        this.types = types;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readUTF();
        shardId = in.readVInt();
        searchType = SearchType.fromId(in.readByte());
        numberOfShards = in.readVInt();
        if (in.readBoolean()) {
            scroll = readScroll(in);
        }

        source = in.readBytesReference();
        extraSource = in.readBytesReference();

        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
        int indicesSize = in.readVInt();
        if (indicesSize > 0) {
            filteringAliases = new String[indicesSize];
            for (int i = 0; i < indicesSize; i++) {
                filteringAliases[i] = in.readUTF();
            }
        } else {
            filteringAliases = null;
        }
        nowInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(index);
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
        out.writeVInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
        if (filteringAliases != null) {
            out.writeVInt(filteringAliases.length);
            for (String index : filteringAliases) {
                out.writeUTF(index);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeVLong(nowInMillis);
    }
}
