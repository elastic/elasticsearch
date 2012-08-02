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

package org.elasticsearch.action.count;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Internal count request executed directly against a specific index shard.
 */
class ShardCountRequest extends BroadcastShardOperationRequest {

    private float minScore;

    private BytesReference querySource;
    private int querySourceOffset;
    private int querySourceLength;

    private String[] types = Strings.EMPTY_ARRAY;

    @Nullable
    private String[] filteringAliases;

    ShardCountRequest() {

    }

    public ShardCountRequest(String index, int shardId, @Nullable String[] filteringAliases, CountRequest request) {
        super(index, shardId);
        this.minScore = request.minScore();
        this.querySource = request.querySource();
        this.types = request.types();
        this.filteringAliases = filteringAliases;
    }

    public float minScore() {
        return minScore;
    }

    public BytesReference querySource() {
        return querySource;
    }

    public String[] types() {
        return this.types;
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        minScore = in.readFloat();

        querySource = in.readBytesReference();

        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
        int aliasesSize = in.readVInt();
        if (aliasesSize > 0) {
            filteringAliases = new String[aliasesSize];
            for (int i = 0; i < aliasesSize; i++) {
                filteringAliases[i] = in.readUTF();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeFloat(minScore);

        out.writeBytesReference(querySource);

        out.writeVInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
        if (filteringAliases != null) {
            out.writeVInt(filteringAliases.length);
            for (String alias : filteringAliases) {
                out.writeUTF(alias);
            }
        } else {
            out.writeVInt(0);
        }
    }
}
