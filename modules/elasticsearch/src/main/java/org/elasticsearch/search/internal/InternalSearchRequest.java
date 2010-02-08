/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.search.Scroll.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * Source structure:
 * <p/>
 * <pre>
 * {
 *  from : 0, size : 20, (optional, can be set on the request)
 *  sort : { "name.first" : {}, "name.last" : { reverse : true } }
 *  fields : [ "name.first", "name.last" ]
 *  queryParserName : "",
 *  query : { ... }
 *  facets : {
 *      "facet1" : {
 *          query : { ... }
 *      }
 *  }
 * }
 * </pre>
 *
 * @author kimchy (Shay Banon)
 */
public class InternalSearchRequest implements Streamable {

    private String index;

    private int shardId;

    private Scroll scroll;

    private int from = -1;

    private int size = -1;

    private float queryBoost = 1.0f;

    private TimeValue timeout;

    private String[] types = Strings.EMPTY_ARRAY;

    private String source;

    public InternalSearchRequest() {
    }

    public InternalSearchRequest(ShardRouting shardRouting, String source) {
        this(shardRouting.index(), shardRouting.id(), source);
    }

    public InternalSearchRequest(String index, int shardId, String source) {
        this.index = index;
        this.shardId = shardId;
        this.source = source;
    }

    public String index() {
        return index;
    }

    public int shardId() {
        return shardId;
    }

    public String source() {
        return this.source;
    }

    public Scroll scroll() {
        return scroll;
    }

    public InternalSearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    public int from() {
        return from;
    }

    public InternalSearchRequest from(int from) {
        this.from = from;
        return this;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public void timeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    /**
     * Allows to set a dynamic query boost on an index level query. Very handy when, for example, each user has
     * his own index, and friends matter more than friends of friends.
     */
    public float queryBoost() {
        return queryBoost;
    }

    public InternalSearchRequest queryBoost(float queryBoost) {
        this.queryBoost = queryBoost;
        return this;
    }

    public int size() {
        return size;
    }

    public InternalSearchRequest size(int size) {
        this.size = size;
        return this;
    }

    public String[] types() {
        return types;
    }

    public void types(String[] types) {
        this.types = types;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        index = in.readUTF();
        shardId = in.readInt();
        if (in.readBoolean()) {
            scroll = readScroll(in);
        }
        from = in.readInt();
        size = in.readInt();
        if (in.readBoolean()) {
            timeout = readTimeValue(in);
        }
        source = in.readUTF();
        queryBoost = in.readFloat();
        int typesSize = in.readInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(index);
        out.writeInt(shardId);
        if (scroll == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            scroll.writeTo(out);
        }
        out.writeInt(from);
        out.writeInt(size);
        if (timeout == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            timeout.writeTo(out);
        }
        out.writeUTF(source);
        out.writeFloat(queryBoost);
        out.writeInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }
}
