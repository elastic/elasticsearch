/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class ClusterStatsResponse extends BaseNodesResponse<ClusterStatsNodeResponse> implements ToXContentFragment {

    final ClusterStatsNodes nodesStats;
    final ClusterStatsIndices indicesStats;
    final ClusterHealthStatus status;
    final long timestamp;
    final String clusterUUID;

    public ClusterStatsResponse(StreamInput in) throws IOException {
        super(in);
        timestamp = in.readVLong();
        // it may be that the master switched on us while doing the operation. In this case the status may be null.
        status = in.readOptionalWriteable(ClusterHealthStatus::readFrom);

        String clusterUUID = in.readOptionalString();
        MappingStats mappingStats = in.readOptionalWriteable(MappingStats::new);
        AnalysisStats analysisStats = in.readOptionalWriteable(AnalysisStats::new);
        VersionStats versionStats = null;
        if (in.getVersion().onOrAfter(Version.V_7_11_0)) {
            versionStats = in.readOptionalWriteable(VersionStats::new);
        }
        this.clusterUUID = clusterUUID;

        // built from nodes rather than from the stream directly
        nodesStats = new ClusterStatsNodes(getNodes());
        indicesStats = new ClusterStatsIndices(getNodes(), mappingStats, analysisStats, versionStats);
    }

    public ClusterStatsResponse(
        long timestamp,
        String clusterUUID,
        ClusterName clusterName,
        List<ClusterStatsNodeResponse> nodes,
        List<FailedNodeException> failures,
        MappingStats mappingStats,
        AnalysisStats analysisStats,
        VersionStats versionStats
    ) {
        super(clusterName, nodes, failures);
        this.clusterUUID = clusterUUID;
        this.timestamp = timestamp;
        nodesStats = new ClusterStatsNodes(nodes);
        indicesStats = new ClusterStatsIndices(nodes, mappingStats, analysisStats, versionStats);
        ClusterHealthStatus status = null;
        for (ClusterStatsNodeResponse response : nodes) {
            // only the master node populates the status
            if (response.clusterStatus() != null) {
                status = response.clusterStatus();
                break;
            }
        }
        this.status = status;
    }

    public String getClusterUUID() {
        return this.clusterUUID;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public ClusterHealthStatus getStatus() {
        return this.status;
    }

    public ClusterStatsNodes getNodesStats() {
        return nodesStats;
    }

    public ClusterStatsIndices getIndicesStats() {
        return indicesStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timestamp);
        out.writeOptionalWriteable(status);
        out.writeOptionalString(clusterUUID);
        out.writeOptionalWriteable(indicesStats.getMappings());
        out.writeOptionalWriteable(indicesStats.getAnalysis());
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            out.writeOptionalWriteable(indicesStats.getVersions());
        }
    }

    @Override
    protected List<ClusterStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ClusterStatsNodeResponse::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ClusterStatsNodeResponse> nodes) throws IOException {
        // nodeStats and indicesStats are rebuilt from nodes
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("cluster_uuid", getClusterUUID());
        builder.field("timestamp", getTimestamp());
        if (status != null) {
            builder.field("status", status.name().toLowerCase(Locale.ROOT));
        }
        builder.startObject("indices");
        indicesStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject("nodes");
        nodesStats.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

}
