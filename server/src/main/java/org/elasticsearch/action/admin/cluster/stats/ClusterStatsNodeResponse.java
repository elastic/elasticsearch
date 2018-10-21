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

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ClusterStatsNodeResponse extends BaseNodeResponse {

    private NodeInfo nodeInfo;
    private NodeStats nodeStats;
    private ShardStats[] shardsStats;
    private ClusterHealthStatus clusterStatus;

    ClusterStatsNodeResponse() {
    }

    public ClusterStatsNodeResponse(DiscoveryNode node, @Nullable ClusterHealthStatus clusterStatus, NodeInfo nodeInfo, NodeStats nodeStats, ShardStats[] shardsStats) {
        super(node);
        this.nodeInfo = nodeInfo;
        this.nodeStats = nodeStats;
        this.shardsStats = shardsStats;
        this.clusterStatus = clusterStatus;
    }

    public NodeInfo nodeInfo() {
        return this.nodeInfo;
    }

    public NodeStats nodeStats() {
        return this.nodeStats;
    }

    /**
     * Cluster Health Status, only populated on master nodes.
     */
    @Nullable
    public ClusterHealthStatus clusterStatus() {
        return clusterStatus;
    }

    public ShardStats[] shardsStats() {
        return this.shardsStats;
    }

    public static ClusterStatsNodeResponse readNodeResponse(StreamInput in) throws IOException {
        ClusterStatsNodeResponse nodeResponse = new ClusterStatsNodeResponse();
        nodeResponse.readFrom(in);
        return nodeResponse;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        clusterStatus = null;
        if (in.readBoolean()) {
            clusterStatus = ClusterHealthStatus.fromValue(in.readByte());
        }
        this.nodeInfo = NodeInfo.readNodeInfo(in);
        this.nodeStats = NodeStats.readNodeStats(in);
        int size = in.readVInt();
        shardsStats = new ShardStats[size];
        for (int i = 0; i < size; i++) {
            shardsStats[i] = ShardStats.readShardStats(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (clusterStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(clusterStatus.value());
        }
        nodeInfo.writeTo(out);
        nodeStats.writeTo(out);
        out.writeVInt(shardsStats.length);
        for (ShardStats ss : shardsStats) {
            ss.writeTo(out);
        }
    }
}
