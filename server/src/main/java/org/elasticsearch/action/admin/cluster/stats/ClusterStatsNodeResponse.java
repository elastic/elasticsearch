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

import org.elasticsearch.Version;
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
import java.util.Collections;
import java.util.List;

public class ClusterStatsNodeResponse extends BaseNodeResponse {

    private NodeInfo nodeInfo;
    private NodeStats nodeStats;
    private ShardStatsAndIndexPatterns[] shardsStats;
    private ClusterHealthStatus clusterStatus;

    public ClusterStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        clusterStatus = null;
        if (in.readBoolean()) {
            clusterStatus = ClusterHealthStatus.readFrom(in);
        }
        this.nodeInfo = new NodeInfo(in);
        this.nodeStats = new NodeStats(in);
        int size = in.readVInt();
        shardsStats = new ShardStatsAndIndexPatterns[size];
        for (int i = 0; i < size; i++) {
            if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
                shardsStats[i] = new ShardStatsAndIndexPatterns(new ShardStats(in), in.readList(StreamInput::readString));
            } else {
                shardsStats[i] = new ShardStatsAndIndexPatterns(new ShardStats(in), Collections.emptyList());
            }
        }
    }

    public ClusterStatsNodeResponse(DiscoveryNode node, @Nullable ClusterHealthStatus clusterStatus,
                                    NodeInfo nodeInfo, NodeStats nodeStats, ShardStatsAndIndexPatterns[] shardsStats) {
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

    public ShardStatsAndIndexPatterns[] shardsStats() {
        return this.shardsStats;
    }

    public static ClusterStatsNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ClusterStatsNodeResponse(in);
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
        for (ShardStatsAndIndexPatterns ss : shardsStats) {
            ss.shardStats.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
                out.writeCollection(ss.indexPatterns, StreamOutput::writeString);
            }
        }
    }

    public static class ShardStatsAndIndexPatterns {
        public final ShardStats shardStats;
        public final List<String> indexPatterns;

        ShardStatsAndIndexPatterns(ShardStats shardStats, List<String> indexPatterns) {
            this.shardStats = shardStats;
            this.indexPatterns = indexPatterns;
        }
    }
}
