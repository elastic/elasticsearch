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

package org.elasticsearch.action.updatebyquery;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents a shard update by query request, that will be performed on the targeted shard.
 */
public class ShardUpdateByQueryRequest extends ShardReplicationOperationRequest {

    private String[] types;
    private BulkResponseOption bulkResponseOption;
    private String[] filteringAliases = Strings.EMPTY_ARRAY;

    private BytesReference source;
    private boolean sourceUnsafe;

    private int shardId = -1;
    private String targetNodeId;

    ShardUpdateByQueryRequest() {
    }

    ShardUpdateByQueryRequest(IndexUpdateByQueryRequest request, int shardId, String targetNodeId) {
        index(request.index());
        replicationType(request.replicationType());
        consistencyLevel(request.consistencyLevel());
        timeout = request.timeout();
        listenerThreaded(request.listenerThreaded());
        types = request.types();
        source = request.source();
        sourceUnsafe = request.sourceUnsafe();
        bulkResponseOption = request.bulkResponseOptions();
        filteringAliases = request.filteringAliases();
        this.shardId = shardId;
        this.targetNodeId = targetNodeId;
    }

    public String[] types() {
        return types;
    }

    public BytesReference source() {
        return source;
    }

    public boolean sourceUnsafe() {
        return sourceUnsafe;
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    public int shardId() {
        return shardId;
    }

    public String targetNodeId() {
        return targetNodeId;
    }

    public BulkResponseOption bulkResponseOptions() {
        return bulkResponseOption;
    }

    @Override
    public void beforeLocalFork() {
        if (sourceUnsafe) {
            source = source.copyBytesArray();
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (source == null) {
            validationException = addValidationError("Source is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        types = in.readStringArray();
        bulkResponseOption = BulkResponseOption.fromId(in.readByte());
        filteringAliases = in.readStringArray();
        shardId = in.readVInt();
        targetNodeId = in.readString();
        source = in.readBytesReference();
        sourceUnsafe = false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(types);
        out.writeByte(bulkResponseOption.id());
        out.writeStringArray(filteringAliases);
        out.writeVInt(shardId);
        out.writeString(targetNodeId);
        out.writeBytesReference(source);
    }
}
