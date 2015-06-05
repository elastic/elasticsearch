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
package org.elasticsearch.indices.flush;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;

/**
 * Result for all copies of a shard
 */
public class ShardsSyncedFlushResult {
    private String failureReason;
    private Map<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardResponses;
    private String syncId;
    private ShardId shardId;
    // some shards may be unassigned, so we need this as state
    private int totalShards;

    public ShardsSyncedFlushResult() {
    }

    public ShardId getShardId() {
        return shardId;
    }

    /**
     * failure constructor
     */
    public ShardsSyncedFlushResult(ShardId shardId, int totalShards, String failureReason) {
        this.syncId = null;
        this.failureReason = failureReason;
        this.shardResponses = ImmutableMap.of();
        this.shardId = shardId;
        this.totalShards = totalShards;
    }

    /**
     * success constructor
     */
    public ShardsSyncedFlushResult(ShardId shardId, String syncId, int totalShards, Map<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardResponses) {
        this.failureReason = null;
        ImmutableMap.Builder<ShardRouting, SyncedFlushService.SyncedFlushResponse> builder = ImmutableMap.builder();
        this.shardResponses = builder.putAll(shardResponses).build();
        this.syncId = syncId;
        this.totalShards = totalShards;
        this.shardId = shardId;
    }

    /**
     * @return true if the operation failed before reaching step three of synced flush. {@link #failureReason()} can be used for
     * more details
     */
    public boolean failed() {
        return failureReason != null;
    }

    /**
     * @return the reason for the failure if synced flush failed before step three of synced flush
     */
    public String failureReason() {
        return failureReason;
    }

    public String syncId() {
        return syncId;
    }

    /**
     * @return total number of shards for which a sync attempt was made
     */
    public int totalShards() {
        return totalShards;
    }

    /**
     * @return total number of successful shards
     */
    public int successfulShards() {
        int i = 0;
        for (SyncedFlushService.SyncedFlushResponse result : shardResponses.values()) {
            if (result.success()) {
                i++;
            }
        }
        return i;
    }

    /**
     * @return an array of shard failures
     */
    public Map<ShardRouting, SyncedFlushService.SyncedFlushResponse> failedShards() {
        Map<ShardRouting, SyncedFlushService.SyncedFlushResponse> failures = new HashMap<>();
        for (Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse> result : shardResponses.entrySet()) {
            if (result.getValue().success() == false) {
                failures.put(result.getKey(), result.getValue());
            }
        }
        return failures;
    }

    /**
     * @return Individual responses for each shard copy with a detailed failure message if the copy failed to perform the synced flush.
     * Empty if synced flush failed before step three.
     */
    public Map<ShardRouting, SyncedFlushService.SyncedFlushResponse> shardResponses() {
        return shardResponses;
    }

    public ShardId shardId() {
        return shardId;
    }
}
