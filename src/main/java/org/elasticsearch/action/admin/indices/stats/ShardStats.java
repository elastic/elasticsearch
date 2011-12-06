/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.cluster.routing.ImmutableShardRouting.readShardRoutingEntry;

/**
 */
public class ShardStats extends BroadcastShardOperationResponse {

    private ShardRouting shardRouting;

    CommonStats stats;

    ShardStats() {
    }

    ShardStats(ShardRouting shardRouting) {
        super(shardRouting.index(), shardRouting.id());
        this.shardRouting = shardRouting;
        this.stats = new CommonStats();
    }

    /**
     * The shard routing information (cluster wide shard state).
     */
    public ShardRouting shardRouting() {
        return this.shardRouting;
    }

    /**
     * The shard routing information (cluster wide shard state).
     */
    public ShardRouting getShardRouting() {
        return shardRouting();
    }

    public CommonStats stats() {
        return this.stats;
    }

    public CommonStats getStats() {
        return stats();
    }

    public static ShardStats readShardStats(StreamInput in) throws IOException {
        ShardStats stats = new ShardStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardRouting = readShardRoutingEntry(in);
        stats = CommonStats.readCommonStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardRouting.writeTo(out);
        stats.writeTo(out);
    }
}
