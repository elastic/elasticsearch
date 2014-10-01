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

package org.elasticsearch.action.delete;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Delete by query request to execute on a specific shard.
 */
public class ShardDeleteRequest extends ShardReplicationOperationRequest<ShardDeleteRequest> {

    private int shardId;
    private String type;
    private String id;
    private boolean refresh = false;
    private long version;
    private String originalIndex;

    ShardDeleteRequest(IndexDeleteRequest request, int shardId) {
        super(request);
        this.index = request.index();
        this.shardId = shardId;
        this.type = request.type();
        this.id = request.id();
        replicationType(request.replicationType());
        consistencyLevel(request.consistencyLevel());
        timeout = request.timeout();
        this.refresh = request.refresh();
        this.version = request.version();
        this.originalIndex = request.originalIndex();
    }

    ShardDeleteRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            addValidationError("type is missing", validationException);
        }
        if (id == null) {
            addValidationError("id is missing", validationException);
        }
        return validationException;
    }

    public int shardId() {
        return this.shardId;
    }

    public String type() {
        return this.type;
    }

    public String id() {
        return this.id;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public void version(long version) {
        this.version = version;
    }

    public long version() {
        return this.version;
    }

    @Override
    public String[] indices() {
        return new String[]{originalIndex};
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = in.readVInt();
        type = in.readString();
        id = in.readString();
        refresh = in.readBoolean();
        version = Versions.readVersion(in);
        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            originalIndex = in.readOptionalString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardId);
        out.writeString(type);
        out.writeString(id);
        out.writeBoolean(refresh);
        Versions.writeVersion(version, out);
        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            out.writeOptionalString(originalIndex);
        }
    }
}
