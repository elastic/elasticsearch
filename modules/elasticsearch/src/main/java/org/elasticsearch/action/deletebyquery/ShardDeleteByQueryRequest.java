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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.action.Actions.*;

/**
 * Delete by query request to execute on a specific shard.
 *
 * @author kimchy (shay.banon)
 */
public class ShardDeleteByQueryRequest extends ShardReplicationOperationRequest {

    private int shardId;
    private byte[] querySource;
    private String queryParserName;
    private String[] types = Strings.EMPTY_ARRAY;

    public ShardDeleteByQueryRequest(String index, byte[] querySource, @Nullable String queryParserName, String[] types, int shardId) {
        this.index = index;
        this.querySource = querySource;
        this.queryParserName = queryParserName;
        this.types = types;
        this.shardId = shardId;
    }

    ShardDeleteByQueryRequest(IndexDeleteByQueryRequest request, int shardId) {
        this(request.index(), request.querySource(), request.queryParserName(), request.types(), shardId);
        replicationType(request.replicationType());
        timeout = request.timeout();
    }

    ShardDeleteByQueryRequest() {
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (querySource == null) {
            addValidationError("querySource is missing", validationException);
        }
        return validationException;
    }

    public int shardId() {
        return this.shardId;
    }

    public byte[] querySource() {
        return querySource;
    }

    public String queryParserName() {
        return queryParserName;
    }

    public String[] types() {
        return this.types;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        querySource = new byte[in.readVInt()];
        in.readFully(querySource);
        if (in.readBoolean()) {
            queryParserName = in.readUTF();
        }
        shardId = in.readVInt();
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
        out.writeVInt(querySource.length);
        out.writeBytes(querySource);
        if (queryParserName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryParserName);
        }
        out.writeVInt(shardId);
        out.writeVInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }

    @Override public String toString() {
        return "[" + index + "]" + Arrays.toString(types) + ", query [" + Unicode.fromBytes(querySource) + "]";
    }
}