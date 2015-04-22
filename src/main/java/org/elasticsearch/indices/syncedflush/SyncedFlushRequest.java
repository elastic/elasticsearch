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

package org.elasticsearch.indices.syncedflush;

import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class SyncedFlushRequest extends ShardReplicationOperationRequest<SyncedFlushRequest> {

    private String syncId;
    private Map<String, byte[]> commitIds;
    private ShardId shardId;

    public SyncedFlushRequest() {
    }

    public SyncedFlushRequest(ShardId shardId, String syncId, Map<String, byte[]> commitIds) {
        this.commitIds = commitIds;
        this.shardId = shardId;
        this.syncId = syncId;
        this.index(shardId.index().getName());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
        commitIds = new HashMap<>();
        int numCommitIds = in.readVInt();
        for (int i = 0; i < numCommitIds; i++) {
            commitIds.put(in.readString(), in.readByteArray());
        }
        syncId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeVInt(commitIds.size());
        for (Map.Entry<String, byte[]> entry : commitIds.entrySet()) {
            out.writeString(entry.getKey());
            out.writeByteArray(entry.getValue());
        }
        out.writeString(syncId);
    }

    @Override
    public String toString() {
        return "write sync commit {" + shardId + "}";
    }

    public ShardId shardId() {
        return shardId;
    }

    public String syncId() {
        return syncId;
    }

    public Map<String, byte[]> commitIds() {
        return commitIds;
    }
}
