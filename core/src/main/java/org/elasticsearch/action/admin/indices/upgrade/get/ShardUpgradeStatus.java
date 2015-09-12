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

package org.elasticsearch.action.admin.indices.upgrade.get;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.cluster.routing.ShardRouting.readShardRoutingEntry;

public class ShardUpgradeStatus extends BroadcastShardResponse {

    private ShardRouting shardRouting;

    private long totalBytes;

    private long toUpgradeBytes;

    private long toUpgradeBytesAncient;

    ShardUpgradeStatus() {
    }

    ShardUpgradeStatus(ShardRouting shardRouting, long totalBytes, long toUpgradeBytes, long upgradeBytesAncient) {
        super(shardRouting.shardId());
        this.shardRouting = shardRouting;
        this.totalBytes = totalBytes;
        this.toUpgradeBytes = toUpgradeBytes;
        this.toUpgradeBytesAncient = upgradeBytesAncient;

    }

    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public long getToUpgradeBytes() {
        return toUpgradeBytes;
    }

    public long getToUpgradeBytesAncient() {
        return toUpgradeBytesAncient;
    }

    public static ShardUpgradeStatus readShardUpgradeStatus(StreamInput in) throws IOException {
        ShardUpgradeStatus shard = new ShardUpgradeStatus();
        shard.readFrom(in);
        return shard;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardRouting = readShardRoutingEntry(in);
        totalBytes = in.readLong();
        toUpgradeBytes = in.readLong();
        toUpgradeBytesAncient = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardRouting.writeTo(out);
        out.writeLong(totalBytes);
        out.writeLong(toUpgradeBytes);
        out.writeLong(toUpgradeBytesAncient);
    }
}