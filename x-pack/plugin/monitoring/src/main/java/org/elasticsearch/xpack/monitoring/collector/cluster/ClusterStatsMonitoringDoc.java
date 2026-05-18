/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.License;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Monitoring document collected by {@link ClusterStatsCollector}.
 * <p>
 * It contains all information about the current cluster, mostly for enabling/disabling features on Kibana side according to the license
 * and also for the "phone home" feature.
 * <p>
 * In the future, the usage stats (and possibly the license) may be collected <em>less</em> frequently and therefore
 * split into a separate monitoring document, but keeping them here simplifies the code.
 */
public class ClusterStatsMonitoringDoc extends MonitoringDoc {

    private static final ToXContent.MapParams CLUSTER_STATS_PARAMS = new ToXContent.MapParams(
        Collections.singletonMap(
            "metric",
            ClusterState.Metric.VERSION + "," + ClusterState.Metric.MASTER_NODE + "," + ClusterState.Metric.NODES
        )
    );

    public static final String TYPE = "cluster_stats";
    protected static final String SETTING_DISPLAY_NAME = "cluster.metadata.display_name";

    private final String clusterName;
    private final String version;
    private final License license;
    private final boolean apmIndicesExist;
    private final List<XPackFeatureUsage> usages;
    private final ClusterStatsResponse clusterStats;
    private final ClusterState clusterState;
    private final ClusterHealthStatus status;
    private final boolean clusterNeedsTLSEnabled;

    ClusterStatsMonitoringDoc(
        final String cluster,
        final long timestamp,
        final long intervalMillis,
        final MonitoringDoc.Node node,
        final String clusterName,
        final String version,
        final ClusterHealthStatus status,
        @Nullable final License license,
        final boolean apmIndicesExist,
        @Nullable final List<XPackFeatureUsage> usages,
        @Nullable final ClusterStatsResponse clusterStats,
        @Nullable final ClusterState clusterState,
        final boolean clusterNeedsTLSEnabled
    ) {

        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null);
        this.clusterName = Objects.requireNonNull(clusterName);
        this.version = Objects.requireNonNull(version);
        this.status = Objects.requireNonNull(status);
        this.license = license;
        this.apmIndicesExist = apmIndicesExist;
        this.usages = usages;
        this.clusterStats = clusterStats;
        this.clusterState = clusterState;
        this.clusterNeedsTLSEnabled = clusterNeedsTLSEnabled;
    }

    String getClusterName() {
        return clusterName;
    }

    String getVersion() {
        return version;
    }

    License getLicense() {
        return license;
    }

    boolean getAPMIndicesExist() {
        return apmIndicesExist;
    }

    List<XPackFeatureUsage> getUsages() {
        return usages;
    }

    ClusterStatsResponse getClusterStats() {
        return clusterStats;
    }

    ClusterState getClusterState() {
        return clusterState;
    }

    ClusterHealthStatus getStatus() {
        return status;
    }

    boolean getClusterNeedsTLSEnabled() {
        return clusterNeedsTLSEnabled;
    }

    String getClusterDisplayName() {
        Metadata metadata = this.clusterState.getMetadata();
        if (metadata == null) {
            return null;
        }
        return metadata.settings().get(SETTING_DISPLAY_NAME);
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("cluster_name", clusterName);
        builder.field("version", version);

        if (license != null) {
            builder.startObject("license");
            {
                Map<String, String> extraParams = Map.of(License.REST_VIEW_MODE, "true");
                params = new ToXContent.DelegatingMapParams(extraParams, params);
                license.toInnerXContent(builder, params);
                if (clusterNeedsTLSEnabled) {
                    builder.field("cluster_needs_tls", true);
                }
            }
            builder.endObject();
        }

        if (clusterStats != null) {
            builder.startObject("cluster_stats");
            {
                clusterStats.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (clusterState != null) {
            builder.startObject("cluster_state");
            {
                builder.field("nodes_hash", nodesHash(clusterState.nodes()));
                builder.field("status", status.name().toLowerCase(Locale.ROOT));
                // we need the whole doc in memory anyway so no need to preserve chunking here; moreover CLUSTER_STATS_PARAMS doesn't
                // include anything heavy so this should be fine.
                ChunkedToXContent.wrapAsToXContent(clusterState).toXContent(builder, CLUSTER_STATS_PARAMS);
            }
            builder.endObject();
        }

        String displayName = getClusterDisplayName();
        if (displayName != null) {
            builder.startObject("cluster_settings");
            {
                builder.startObject("cluster");
                {
                    builder.startObject("metadata");
                    {
                        builder.field("display_name", displayName);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }

        builder.startObject("stack_stats");
        {
            // in the future, it may be useful to pass in an object that represents APM (and others), but for now this
            // is good enough
            builder.startObject("apm");
            {
                builder.field("found", apmIndicesExist);
            }
            builder.endObject();

            if (usages != null) {
                builder.startObject("xpack");
                for (final XPackFeatureUsage usage : usages) {
                    builder.field(usage.name(), usage);
                }
                builder.endObject();
            }
        }
        builder.endObject();
    }

    /**
     * Create a simple hash value that can be used to determine if the nodes listing has changed since the last report.
     *
     * @param nodes All nodes in the cluster state.
     * @return A hash code value whose value can be used to determine if the node listing has changed (including node restarts).
     */
    public static int nodesHash(final DiscoveryNodes nodes) {
        final StringBuilder temp = new StringBuilder();

        // adds the Ephemeral ID (as opposed to the Persistent UUID) to catch node restarts, which is critical for 1 node clusters
        for (final DiscoveryNode node : nodes) {
            temp.append(node.getEphemeralId());
        }

        return temp.toString().hashCode();
    }

}
