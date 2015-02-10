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

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Information regarding the recovery state of indices and their associated shards.
 */
public class RecoveryResponse extends BroadcastOperationResponse implements ToXContent {

    private boolean detailed = false;
    private Map<String, List<ShardRecoveryResponse>> shardResponses = new HashMap<>();

    public RecoveryResponse() { }

    /**
     * Constructs recovery information for a collection of indices and associated shards. Keeps track of how many total shards
     * were seen, and out of those how many were successfully processed and how many failed.
     *
     * @param totalShards       Total count of shards seen
     * @param successfulShards  Count of shards successfully processed
     * @param failedShards      Count of shards which failed to process
     * @param detailed          Display detailed metrics
     * @param shardResponses    Map of indices to shard recovery information
     * @param shardFailures     List of failures processing shards
     */
    public RecoveryResponse(int totalShards, int successfulShards, int failedShards, boolean detailed,
                            Map<String, List<ShardRecoveryResponse>> shardResponses, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardResponses = shardResponses;
        this.detailed = detailed;
    }

    public boolean hasRecoveries() {
        return shardResponses.size() > 0;
    }

    public boolean detailed() {
        return detailed;
    }

    public void detailed(boolean detailed) {
        this.detailed = detailed;
    }

    public Map<String, List<ShardRecoveryResponse>> shardResponses() {
        return shardResponses;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (hasRecoveries()) {
            for (String index : shardResponses.keySet()) {
                List<ShardRecoveryResponse> responses = shardResponses.get(index);
                if (responses == null || responses.size() == 0) {
                    continue;
                }
                builder.startObject(index);
                builder.startArray("shards");
                for (ShardRecoveryResponse recoveryResponse : responses) {
                    builder.startObject();
                    recoveryResponse.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
            }
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardResponses.size());
        for (Map.Entry<String, List<ShardRecoveryResponse>> entry : shardResponses.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (ShardRecoveryResponse recoveryResponse : entry.getValue()) {
                recoveryResponse.writeTo(out);
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            String s = in.readString();
            int listSize = in.readVInt();
            List<ShardRecoveryResponse> list = new ArrayList<>(listSize);
            for (int j = 0; j < listSize; j++) {
                list.add(ShardRecoveryResponse.readShardRecoveryResponse(in));
            }
            shardResponses.put(s, list);
        }
    }
}