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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

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
    @Nullable private String routing;

    ShardDeleteByQueryRequest(IndexDeleteByQueryRequest request, int shardId) {
        this.index = request.index();
        this.querySource = request.querySource();
        this.queryParserName = request.queryParserName();
        this.types = request.types();
        this.shardId = shardId;
        replicationType(request.replicationType());
        consistencyLevel(request.consistencyLevel());
        timeout = request.timeout();
        this.routing = request.routing();
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

    public String routing() {
        return this.routing;
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
        if (in.readBoolean()) {
            routing = in.readUTF();
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
        if (routing == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(routing);
        }
    }

    @Override public String toString() {
        String sSource = "_na_";
        try {
            sSource = Unicode.fromBytes(querySource);
        } catch (Exception e) {
            // ignore
        }
        return "delete_by_query {[" + index + "]" + Arrays.toString(types) + ", query [" + sSource + "]}";
    }
}