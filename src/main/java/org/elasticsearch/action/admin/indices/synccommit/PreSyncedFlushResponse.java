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

package org.elasticsearch.action.admin.indices.synccommit;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A response to sync commit action.
 */
public class PreSyncedFlushResponse extends BroadcastOperationResponse {

    Map<ShardRouting, byte[]> commitIds = new HashMap<>();

    PreSyncedFlushResponse() {

    }


    public PreSyncedFlushResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, AtomicReferenceArray shardsResponses) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        for (int i = 0; i < shardsResponses.length(); i++) {
            PreSyncedShardFlushResponse preSyncedShardFlushResponse = (PreSyncedShardFlushResponse) shardsResponses.get(i);
            commitIds.put(preSyncedShardFlushResponse.shardRouting(), preSyncedShardFlushResponse.id());
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int numCommitIds = in.readInt();
        for (int i = 0; i < numCommitIds; i++) {
            ImmutableShardRouting shardRouting = ImmutableShardRouting.readShardRoutingEntry(in);
            byte[] id = in.readByteArray();
            commitIds.put(shardRouting, id);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(commitIds.size());
        for (Map.Entry<ShardRouting, byte[]> entry : commitIds.entrySet()) {
            entry.getKey().writeTo(out);
            out.writeByteArray(entry.getValue());
        }
    }

    public Map<ShardRouting, byte[]> commitIds() {
        return commitIds;
    }
}
