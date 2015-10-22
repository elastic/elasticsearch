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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Internal validate request executed directly against a specific index shard.
 */
public class ShardValidateQueryRequest extends BroadcastShardRequest {

    private BytesReference source;
    private String[] types = Strings.EMPTY_ARRAY;
    private boolean explain;
    private boolean rewrite;
    private long nowInMillis;

    @Nullable
    private String[] filteringAliases;

    public ShardValidateQueryRequest() {

    }

    ShardValidateQueryRequest(ShardId shardId, @Nullable String[] filteringAliases, ValidateQueryRequest request) {
        super(shardId, request);
        this.source = request.source();
        this.types = request.types();
        this.explain = request.explain();
        this.rewrite = request.rewrite();
        this.filteringAliases = filteringAliases;
        this.nowInMillis = request.nowInMillis;
    }

    public BytesReference source() {
        return source;
    }

    public String[] types() {
        return this.types;
    }

    public boolean explain() {
        return this.explain;
    }

    public boolean rewrite() { 
        return this.rewrite; 
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    public long nowInMillis() {
        return this.nowInMillis;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        source = in.readBytesReference();

        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readString();
            }
        }
        int aliasesSize = in.readVInt();
        if (aliasesSize > 0) {
            filteringAliases = new String[aliasesSize];
            for (int i = 0; i < aliasesSize; i++) {
                filteringAliases[i] = in.readString();
            }
        }

        explain = in.readBoolean();
        rewrite = in.readBoolean();
        nowInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(source);

        out.writeVInt(types.length);
        for (String type : types) {
            out.writeString(type);
        }
        if (filteringAliases != null) {
            out.writeVInt(filteringAliases.length);
            for (String alias : filteringAliases) {
                out.writeString(alias);
            }
        } else {
            out.writeVInt(0);
        }

        out.writeBoolean(explain);
        out.writeBoolean(rewrite);
        out.writeVLong(nowInMillis);
    }
}
