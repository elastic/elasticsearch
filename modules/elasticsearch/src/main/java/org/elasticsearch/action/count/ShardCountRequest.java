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

package org.elasticsearch.action.count;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Internal count request executed directly against a specific index shard.
 *
 * @author kimchy (shay.banon)
 */
class ShardCountRequest extends BroadcastShardOperationRequest {

    private float minScore;

    private byte[] querySource;
    private int querySourceOffset;
    private int querySourceLength;

    private String[] types = Strings.EMPTY_ARRAY;
    @Nullable private String queryParserName;

    ShardCountRequest() {

    }

    public ShardCountRequest(String index, int shardId, CountRequest request) {
        super(index, shardId);
        this.minScore = request.minScore();
        this.querySource = request.querySource();
        this.querySourceOffset = request.querySourceOffset();
        this.querySourceLength = request.querySourceLength();
        this.queryParserName = request.queryParserName();
        this.types = request.types();
    }

    public float minScore() {
        return minScore;
    }

    public byte[] querySource() {
        return querySource;
    }

    public int querySourceOffset() {
        return querySourceOffset;
    }

    public int querySourceLength() {
        return querySourceLength;
    }

    public String queryParserName() {
        return queryParserName;
    }

    public String[] types() {
        return this.types;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        minScore = in.readFloat();
        querySourceLength = in.readVInt();
        querySourceOffset = 0;
        querySource = new byte[querySourceLength];
        in.readFully(querySource);
        if (in.readBoolean()) {
            queryParserName = in.readUTF();
        }
        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeFloat(minScore);
        out.writeVInt(querySourceLength);
        out.writeBytes(querySource, querySourceOffset, querySourceLength);
        if (queryParserName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryParserName);
        }
        out.writeVInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }
}
