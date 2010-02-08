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
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class ShardCountRequest extends BroadcastShardOperationRequest {

    private float minScore;
    private String querySource;
    private String[] types = Strings.EMPTY_ARRAY;
    @Nullable private String queryParserName;

    ShardCountRequest() {

    }

    public ShardCountRequest(String index, int shardId, String querySource, float minScore,
                             @Nullable String queryParserName, String... types) {
        super(index, shardId);
        this.minScore = minScore;
        this.querySource = querySource;
        this.queryParserName = queryParserName;
        this.types = types;
    }

    public float minScore() {
        return minScore;
    }

    public String querySource() {
        return querySource;
    }

    public String queryParserName() {
        return queryParserName;
    }

    public String[] types() {
        return this.types;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        minScore = in.readFloat();
        querySource = in.readUTF();
        if (in.readBoolean()) {
            queryParserName = in.readUTF();
        }
        int typesSize = in.readInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeFloat(minScore);
        out.writeUTF(querySource);
        if (queryParserName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryParserName);
        }
        out.writeInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }
}
