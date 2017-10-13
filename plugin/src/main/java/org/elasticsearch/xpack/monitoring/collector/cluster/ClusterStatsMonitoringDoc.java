/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    private static final ToXContent.MapParams CLUSTER_STATS_PARAMS =
            new ToXContent.MapParams(
                    Collections.singletonMap("metric",
                                ClusterState.Metric.VERSION + "," +
                                ClusterState.Metric.MASTER_NODE + "," +
                                ClusterState.Metric.NODES));

    public static final String TYPE = "cluster_stats";

    private final String clusterName;
    private final String version;
    private final License license;
    private final List<XPackFeatureSet.Usage> usages;
    private final ClusterStatsResponse clusterStats;
    private final ClusterState clusterState;
    private final ClusterHealthStatus status;

    ClusterStatsMonitoringDoc(final String cluster,
                              final long timestamp,
                              final long intervalMillis,
                              final MonitoringDoc.Node node,
                              final String clusterName,
                              final String version,
                              final ClusterHealthStatus status,
                              @Nullable final License license,
                              @Nullable final List<XPackFeatureSet.Usage> usages,
                              @Nullable final ClusterStatsResponse clusterStats,
                              @Nullable final ClusterState clusterState) {

        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null);
        this.clusterName = Objects.requireNonNull(clusterName);
        this.version = Objects.requireNonNull(version);
        this.status = Objects.requireNonNull(status);
        this.license = license;
        this.usages = usages;
        this.clusterStats = clusterStats;
        this.clusterState = clusterState;
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

    List<XPackFeatureSet.Usage> getUsages() {
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

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("cluster_name", clusterName);
        builder.field("version", version);

        if (license != null) {
            builder.startObject("license");
            Map<String, String> extraParams = new MapBuilder<String, String>()
                    .put(License.REST_VIEW_MODE, "true")
                    .map();
            params = new ToXContent.DelegatingMapParams(extraParams, params);
            license.toInnerXContent(builder, params);
            builder.field("hkey", hash(license, getCluster()));
            builder.endObject();
        }

        if (clusterStats != null) {
            builder.startObject("cluster_stats");
            clusterStats.toXContent(builder, params);
            builder.endObject();
        }

        if (clusterState != null) {
            builder.startObject("cluster_state");
            builder.field("status", status.name().toLowerCase(Locale.ROOT));
            clusterState.toXContent(builder, CLUSTER_STATS_PARAMS);
            builder.endObject();
        }

        if (usages != null) {
            // in the future we may choose to add other usages under the stack_stats section, but it is only xpack for now
            // it may also be combined on the UI side of phone-home to add things like "kibana" and "logstash" under "stack_stats"
            builder.startObject("stack_stats");
            {
                builder.startObject("xpack");
                for (final XPackFeatureSet.Usage usage : usages) {
                    builder.field(usage.name(), usage);
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }

    public static String hash(License license, String clusterName) {
        return hash(license.status().label(), license.uid(), license.type(), String.valueOf(license.expiryDate()), clusterName);
    }

    public static String hash(String licenseStatus, String licenseUid, String licenseType, String licenseExpiryDate, String clusterUUID) {
        String toHash = licenseStatus + licenseUid + licenseType + licenseExpiryDate + clusterUUID;
        return MessageDigests.toHexString(MessageDigests.sha256().digest(toHash.getBytes(StandardCharsets.UTF_8)));
    }
}
