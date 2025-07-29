/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.Objects;

public class ClusterStatsNodeResponse extends BaseNodeResponse {

    private final NodeInfo nodeInfo;
    private final NodeStats nodeStats;
    private final ShardStats[] shardsStats;
    private final ClusterHealthStatus clusterStatus;
    private final SearchUsageStats searchUsageStats;
    private final RepositoryUsageStats repositoryUsageStats;
    private final CCSTelemetrySnapshot searchCcsMetrics;
    private final CCSTelemetrySnapshot esqlCcsMetrics;

    public ClusterStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.clusterStatus = in.readOptionalWriteable(ClusterHealthStatus::readFrom);
        this.nodeInfo = new NodeInfo(in);
        this.nodeStats = new NodeStats(in);
        this.shardsStats = in.readArray(ShardStats::new, ShardStats[]::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_6_0)) {
            searchUsageStats = new SearchUsageStats(in);
        } else {
            searchUsageStats = new SearchUsageStats();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            repositoryUsageStats = RepositoryUsageStats.readFrom(in);
            searchCcsMetrics = new CCSTelemetrySnapshot(in);
        } else {
            repositoryUsageStats = RepositoryUsageStats.EMPTY;
            searchCcsMetrics = new CCSTelemetrySnapshot();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_CCS_TELEMETRY_STATS)) {
            esqlCcsMetrics = new CCSTelemetrySnapshot(in);
        } else {
            esqlCcsMetrics = new CCSTelemetrySnapshot();
        }
    }

    public ClusterStatsNodeResponse(
        DiscoveryNode node,
        @Nullable ClusterHealthStatus clusterStatus,
        NodeInfo nodeInfo,
        NodeStats nodeStats,
        ShardStats[] shardsStats,
        SearchUsageStats searchUsageStats,
        RepositoryUsageStats repositoryUsageStats,
        CCSTelemetrySnapshot ccsTelemetrySnapshot,
        CCSTelemetrySnapshot esqlTelemetrySnapshot
    ) {
        super(node);
        this.nodeInfo = nodeInfo;
        this.nodeStats = nodeStats;
        this.shardsStats = shardsStats;
        this.clusterStatus = clusterStatus;
        this.searchUsageStats = Objects.requireNonNull(searchUsageStats);
        this.repositoryUsageStats = Objects.requireNonNull(repositoryUsageStats);
        this.searchCcsMetrics = ccsTelemetrySnapshot;
        this.esqlCcsMetrics = esqlTelemetrySnapshot;
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

    public RepositoryUsageStats repositoryUsageStats() {
        return repositoryUsageStats;
    }

    public CCSTelemetrySnapshot getSearchCcsMetrics() {
        return searchCcsMetrics;
    }

    public CCSTelemetrySnapshot getEsqlCcsMetrics() {
        return esqlCcsMetrics;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(clusterStatus);
        nodeInfo.writeTo(out);
        nodeStats.writeTo(out);
        out.writeArray(shardsStats);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_6_0)) {
            searchUsageStats.writeTo(out);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            repositoryUsageStats.writeTo(out);
            searchCcsMetrics.writeTo(out);
        } // else just drop these stats, ok for bwc
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_CCS_TELEMETRY_STATS)) {
            esqlCcsMetrics.writeTo(out);
        }
    }

}
