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

package org.elasticsearch.action.admin.indices.syncedflush;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.SyncedFlushService;

import java.io.IOException;
import java.util.*;

/**
 * A response to a synced flush action on several indices.
 */
public class SyncedFlushIndicesResponse extends ActionResponse implements ToXContent {

    private Set<SyncedFlushService.SyncedFlushResult> results;

    SyncedFlushIndicesResponse() {
    }

    SyncedFlushIndicesResponse(Set<SyncedFlushService.SyncedFlushResult> results) {
        this.results = results;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        results = new HashSet<>();
        for (int i = 0; i < size; i++) {
            SyncedFlushService.SyncedFlushResult syncedFlushResult = new SyncedFlushService.SyncedFlushResult();
            syncedFlushResult.readFrom(in);
            results.add(syncedFlushResult);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(results.size());
        for (SyncedFlushService.SyncedFlushResult syncedFlushResult : results) {
            syncedFlushResult.writeTo(out);
        }
    }

    public Set<SyncedFlushService.SyncedFlushResult> results() {
        return results;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Map<String, Map<Integer, List<Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse>>>> successfulShards = new HashMap<>();
        Map<String, Map<Integer, String>> unsuccessfulShards = new HashMap<>();
        // first, sort everything by index and shard id
        for (SyncedFlushService.SyncedFlushResult result : results) {
            if (result.shardResponses().size() > 0) {
                if (successfulShards.get(result.getShardId().index().name()) == null) {
                    successfulShards.put(result.getShardId().index().name(), new HashMap<Integer, List<Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse>>>());
                }
                if (successfulShards.get(result.getShardId().index().name()).get(result.getShardId().getId()) == null) {
                    successfulShards.get(result.getShardId().index().name()).put(result.getShardId().getId(), new ArrayList<Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse>>());
                }
                for (Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardResponse : result.shardResponses().entrySet()) {
                    successfulShards.get(result.getShardId().index().name()).get(result.getShardId().getId()).add(shardResponse);
                }
            } else {
                if (unsuccessfulShards.get(result.getShardId().index().name()) == null) {
                    unsuccessfulShards.put(result.getShardId().index().name(), new HashMap<Integer, String>());
                }
                unsuccessfulShards.get(result.getShardId().index().name()).put(result.getShardId().getId(), result.failureReason());
            }
        }
        for (Map.Entry<String, Map<Integer, List<Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse>>>> result : successfulShards.entrySet()) {
            builder.startArray(result.getKey());
            for (Map.Entry<Integer, List<Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse>>> shardResponse : result.getValue().entrySet()) {
                builder.startObject();
                builder.field("shard_id", shardResponse.getKey());
                builder.startObject("responses");
                boolean success = true;
                for (Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardCopy : shardResponse.getValue()) {
                    builder.field(shardCopy.getKey().currentNodeId(), shardCopy.getValue().success() ? "success" : shardCopy.getValue().failureReason());
                    if (shardCopy.getValue().success() == false) {
                        success = false;
                    }
                }
                builder.endObject();
                builder.field("message", success ? "success" : "failed on some copies");
                builder.endObject();
            }
            if (unsuccessfulShards.get(result.getKey()) != null) {
                for (Map.Entry<Integer, String> shardCopy : unsuccessfulShards.get(result.getKey()).entrySet()) {
                    builder.startObject();
                    builder.field("shard_id", shardCopy.getKey());
                    builder.field("message", shardCopy.getValue());
                    builder.endObject();
                }
            }
            builder.endArray();
        }
        return builder;
    }
}
