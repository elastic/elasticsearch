/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterSnapshotStats;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ClusterStatsResponse extends BaseNodesResponse<ClusterStatsNodeResponse> implements ToXContentFragment {

    final ClusterStatsNodes nodesStats;
    final ClusterStatsIndices indicesStats;
    final ClusterHealthStatus status;
    final ClusterSnapshotStats clusterSnapshotStats;
    final RepositoryUsageStats repositoryUsageStats;
    final CCSTelemetrySnapshot ccsMetrics;
    final CCSTelemetrySnapshot esqlMetrics;
    final long timestamp;
    final String clusterUUID;
    final IndexLimitTier indexLimitTier;
    private final Map<String, RemoteClusterStats> remoteClustersStats;

    public static final String CCS_TELEMETRY_FIELD_NAME = "_search";
    public static final String ESQL_TELEMETRY_FIELD_NAME = "_esql";

    public ClusterStatsResponse(
        long timestamp,
        String clusterUUID,
        ClusterName clusterName,
        List<ClusterStatsNodeResponse> nodes,
        List<FailedNodeException> failures,
        MappingStats mappingStats,
        AnalysisStats analysisStats,
        VersionStats versionStats,
        ClusterSnapshotStats clusterSnapshotStats,
        Map<String, RemoteClusterStats> remoteClustersStats,
        boolean skipMRT,
        IndexLimitTier indexLimitTier
    ) {
        super(clusterName, nodes, failures);
        this.clusterUUID = clusterUUID;
        this.timestamp = timestamp;
        nodesStats = new ClusterStatsNodes(nodes);
        indicesStats = new ClusterStatsIndices(nodes, mappingStats, analysisStats, versionStats);
        ccsMetrics = new CCSTelemetrySnapshot(skipMRT == false);
        esqlMetrics = new CCSTelemetrySnapshot(false);
        ClusterHealthStatus status = null;
        for (ClusterStatsNodeResponse response : nodes) {
            // only the master node populates the status
            if (response.clusterStatus() != null) {
                status = response.clusterStatus();
                break;
            }
        }
        nodes.forEach(node -> {
            ccsMetrics.add(node.getSearchCcsMetrics());
            esqlMetrics.add(node.getEsqlCcsMetrics());
        });
        this.status = status;
        this.clusterSnapshotStats = clusterSnapshotStats;

        this.repositoryUsageStats = nodes.stream()
            .map(ClusterStatsNodeResponse::repositoryUsageStats)
            // only populated on snapshot nodes (i.e. master and data nodes)
            .filter(r -> r.isEmpty() == false)
            // stats should be the same on every node so just pick one of them
            .findAny()
            .orElse(RepositoryUsageStats.EMPTY);
        this.remoteClustersStats = remoteClustersStats;
        this.indexLimitTier = indexLimitTier;
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

    public CCSTelemetrySnapshot getCcsMetrics() {
        return ccsMetrics;
    }

    public Map<String, RemoteClusterStats> getRemoteClustersStats() {
        return remoteClustersStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    protected List<ClusterStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return TransportAction.localOnly();
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ClusterStatsNodeResponse> nodes) throws IOException {
        TransportAction.localOnly();
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

        builder.field("snapshots");
        clusterSnapshotStats.toXContent(builder, params);

        builder.field("repositories");
        repositoryUsageStats.toXContent(builder, params);

        builder.startObject("ccs");
        if (remoteClustersStats != null) {
            builder.field("clusters", remoteClustersStats);
        }
        builder.startObject(CCS_TELEMETRY_FIELD_NAME);
        ccsMetrics.toXContent(builder, params);
        builder.endObject();

        if (esqlMetrics.getTotalCount() > 0) {
            builder.startObject(ESQL_TELEMETRY_FIELD_NAME);
            esqlMetrics.toXContent(builder, params);
            builder.endObject();
        }

        indexLimitTier.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    /**
     * Represents the information about a remote cluster.
     */
    public record RemoteClusterStats(
        String clusterUUID,
        String mode,
        Optional<Boolean> skipUnavailable,
        String transportCompress,
        Set<String> versions,
        String status,
        long nodesCount,
        long shardsCount,
        long indicesCount,
        long indicesBytes,
        long heapBytes,
        long memBytes
    ) implements ToXContentFragment {
        public RemoteClusterStats(String mode, Optional<Boolean> skipUnavailable, String transportCompress) {
            this(
                "unavailable",
                mode,
                skipUnavailable,
                transportCompress.toLowerCase(Locale.ROOT),
                Set.of(),
                "unavailable",
                0,
                0,
                0,
                0,
                0,
                0
            );
        }

        public RemoteClusterStats acceptResponse(RemoteClusterStatsResponse remoteResponse) {
            return new RemoteClusterStats(
                remoteResponse.getClusterUUID(),
                mode,
                skipUnavailable,
                transportCompress,
                remoteResponse.getVersions(),
                remoteResponse.getStatus().name().toLowerCase(Locale.ROOT),
                remoteResponse.getNodesCount(),
                remoteResponse.getShardsCount(),
                remoteResponse.getIndicesCount(),
                remoteResponse.getIndicesBytes(),
                remoteResponse.getHeapBytes(),
                remoteResponse.getMemBytes()
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("cluster_uuid", clusterUUID);
            builder.field("mode", mode);
            if (skipUnavailable.isPresent()) {
                builder.field("skip_unavailable", skipUnavailable.get());
            }
            builder.field("transport.compress", transportCompress);
            builder.field("status", status);
            builder.field("version", versions);
            builder.field("nodes_count", nodesCount);
            builder.field("shards_count", shardsCount);
            builder.field("indices_count", indicesCount);
            builder.humanReadableField("indices_total_size_in_bytes", "indices_total_size", ByteSizeValue.ofBytes(indicesBytes));
            builder.humanReadableField("max_heap_in_bytes", "max_heap", ByteSizeValue.ofBytes(heapBytes));
            builder.humanReadableField("mem_total_in_bytes", "mem_total", ByteSizeValue.ofBytes(memBytes));
            builder.endObject();
            return builder;
        }
    }

    static final class Fields {
        static final String INDEX_COUNT_GUARDRAIL = "index_count_guardrail";
        static final String INDEX_COUNT_GUARDRAIL_MESSAGE = "message";
    }

    public enum IndexLimitTier implements ToXContentFragment {
        PASS(""),
        NUDGE(
            "To ensure the best performance, we recommend grouping related data into fewer, "
                + "larger indices. For time-based data, consider using a data stream."
        ),
        WARN(
            "Your project is approaching an operational limit due to a high number of indices. "
                + "To avoid service interruptions, please review your indexing strategy."
        ),
        CRITICAL(
            "CRITICAL: Your project is about to reach its index limit. "
                + "Further index creation may be blocked. You must reduce your number of indices immediately."
        ),
        BLOCK(
            "Too Many Indices. Your project has reached its operational limit for the number of indices it can contain. "
                + "Please consolidate smaller indices or delete unused ones."
        );

        private final String message;

        IndexLimitTier(String message) {
            this.message = message;
        }

        public String getMessage() {
            return this.message;
        }

        public static IndexLimitTier parse(int totalUserIndices, int nudge, int warn, int critical, int block) {
            if (totalUserIndices < nudge) {
                return PASS;
            } else if (totalUserIndices < warn) {
                return NUDGE;
            } else if (totalUserIndices < critical) {
                return WARN;
            } else if (totalUserIndices < block) {
                return CRITICAL;
            } else {
                return BLOCK;
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.INDEX_COUNT_GUARDRAIL);
            builder.field(Fields.INDEX_COUNT_GUARDRAIL_MESSAGE, message);
            builder.endObject();
            return builder;
        }
    }
}
