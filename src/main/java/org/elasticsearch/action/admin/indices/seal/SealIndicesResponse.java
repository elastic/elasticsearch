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

package org.elasticsearch.action.admin.indices.seal;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.SyncedFlushService;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.*;

/**
 * A response to a seal action on several indices.
 */
public class SealIndicesResponse extends ActionResponse implements ToXContent {

    final private Set<SyncedFlushService.SyncedFlushResult> results;

    private RestStatus restStatus;

    SealIndicesResponse() {
        results = new HashSet<>();
    }

    SealIndicesResponse(Set<SyncedFlushService.SyncedFlushResult> results) {
        this.results = results;
        if (allShardsFailed()) {
            restStatus = RestStatus.CONFLICT;
        } else if (someShardsFailed()) {
            restStatus = RestStatus.PARTIAL_CONTENT;
        } else {
            restStatus = RestStatus.OK;
        }
    }

    public RestStatus status() {
        return restStatus;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        results.clear();
        for (int i = 0; i < size; i++) {
            SyncedFlushService.SyncedFlushResult syncedFlushResult = new SyncedFlushService.SyncedFlushResult();
            syncedFlushResult.readFrom(in);
            results.add(syncedFlushResult);
        }
        restStatus = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(results.size());
        for (SyncedFlushService.SyncedFlushResult syncedFlushResult : results) {
            syncedFlushResult.writeTo(out);
        }
        RestStatus.writeTo(out, restStatus);
    }

    public Set<SyncedFlushService.SyncedFlushResult> results() {
        return results;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Map<String, Map<Integer, Object>> allResults = new HashMap<>();

        // first, sort everything by index and shard id
        for (SyncedFlushService.SyncedFlushResult result : results) {
            String indexName = result.getShardId().index().name();
            int shardId = result.getShardId().getId();

            if (allResults.get(indexName) == null) {
                // no results yet for this index
                allResults.put(indexName, new TreeMap<Integer, Object>());
            }
            if (result.shardResponses().size() > 0) {
                Map<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardResponses = new HashMap<>();
                for (Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardResponse : result.shardResponses().entrySet()) {
                    shardResponses.put(shardResponse.getKey(), shardResponse.getValue());
                }
                allResults.get(indexName).put(shardId, shardResponses);
            } else {
                allResults.get(indexName).put(shardId, result.failureReason());
            }
        }
        for (Map.Entry<String, Map<Integer, Object>> result : allResults.entrySet()) {
            builder.startArray(result.getKey());
            for (Map.Entry<Integer, Object> shardResponse : result.getValue().entrySet()) {
                builder.startObject();
                builder.field("shard_id", shardResponse.getKey());
                if (shardResponse.getValue() instanceof Map) {
                    builder.startObject("responses");
                    Map<ShardRouting, SyncedFlushService.SyncedFlushResponse> results = (Map<ShardRouting, SyncedFlushService.SyncedFlushResponse>) shardResponse.getValue();
                    boolean success = true;
                    for (Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardCopy : results.entrySet()) {
                        builder.field(shardCopy.getKey().currentNodeId(), shardCopy.getValue().success() ? "success" : shardCopy.getValue().failureReason());
                        if (shardCopy.getValue().success() == false) {
                            success = false;
                        }
                    }
                    builder.endObject();
                    builder.field("message", success ? "success" : "failed on some copies");

                } else {
                    builder.field("message", shardResponse.getValue()); // must be a string
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    public boolean allShardsFailed() {
        for (SyncedFlushService.SyncedFlushResult result : results) {
            if (result.success()) {
                return false;
            }
            if (result.shardResponses().size() > 0) {
                for (Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardResponse : result.shardResponses().entrySet()) {
                    if (shardResponse.getValue().success()) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public boolean someShardsFailed() {
        for (SyncedFlushService.SyncedFlushResult result : results) {
            if (result.success() == false) {
                return true;
            }
            if (result.shardResponses().size() > 0) {
                for (Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardResponse : result.shardResponses().entrySet()) {
                    if (shardResponse.getValue().success() == false) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
