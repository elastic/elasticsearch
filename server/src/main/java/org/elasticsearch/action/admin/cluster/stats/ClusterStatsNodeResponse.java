/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

public class ClusterStatsNodeResponse extends BaseNodeResponse {

    private final NodeInfo nodeInfo;
    private final NodeStats nodeStats;
    private final ShardStats[] shardsStats;
    private ClusterHealthStatus clusterStatus;
    private final SearchUsageStats searchUsageStats;
    private final CCSTelemetrySnapshot ccsMetrics;

    public ClusterStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        clusterStatus = null;
        if (in.readBoolean()) {
            clusterStatus = ClusterHealthStatus.readFrom(in);
        }
        this.nodeInfo = new NodeInfo(in);
        this.nodeStats = new NodeStats(in);
        shardsStats = in.readArray(ShardStats::new, ShardStats[]::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_6_0)) {
            searchUsageStats = new SearchUsageStats(in);
        } else {
            searchUsageStats = new SearchUsageStats();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.CCS_TELEMETRY_STATS)) {
            ccsMetrics = new CCSTelemetrySnapshot(in);
        } else {
            ccsMetrics = new CCSTelemetrySnapshot();
        }
    }

    public ClusterStatsNodeResponse(
        DiscoveryNode node,
        @Nullable ClusterHealthStatus clusterStatus,
        NodeInfo nodeInfo,
        NodeStats nodeStats,
        ShardStats[] shardsStats,
        SearchUsageStats searchUsageStats,
        CCSTelemetrySnapshot ccsTelemetrySnapshot
    ) {
        super(node);
        this.nodeInfo = nodeInfo;
        this.nodeStats = nodeStats;
        this.shardsStats = shardsStats;
        this.clusterStatus = clusterStatus;
        this.searchUsageStats = searchUsageStats;
        this.ccsMetrics = ccsTelemetrySnapshot;
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

    public SearchUsageStats searchUsageStats() {
        return searchUsageStats;
    }

    public CCSTelemetrySnapshot getCcsMetrics() {
        return ccsMetrics;
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
        out.writeArray(shardsStats);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_6_0)) {
            searchUsageStats.writeTo(out);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.CCS_TELEMETRY_STATS)) {
            ccsMetrics.writeTo(out);
        }
    }

}
