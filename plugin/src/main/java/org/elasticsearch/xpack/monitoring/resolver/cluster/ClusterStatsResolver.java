/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ClusterStatsResolver extends MonitoringIndexNameResolver.Timestamped<ClusterStatsMonitoringDoc> {

    private static final ToXContent.MapParams CLUSTER_STATS_PARAMS =
            new ToXContent.MapParams(
                    Collections.singletonMap("metric",
                                             ClusterState.Metric.VERSION + "," +
                                             ClusterState.Metric.MASTER_NODE + "," +
                                             ClusterState.Metric.NODES));

    public ClusterStatsResolver(MonitoredSystem system, Settings settings) {
        super(system, settings);
    }

    @Override
    protected void buildXContent(ClusterStatsMonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("cluster_name", document.getClusterName());
        builder.field("version", document.getVersion());

        final License license = document.getLicense();
        if (license != null) {
            builder.startObject("license");
            Map<String, String> extraParams = new MapBuilder<String, String>()
                    .put(License.REST_VIEW_MODE, "true")
                    .map();
            params = new ToXContent.DelegatingMapParams(extraParams, params);
            license.toInnerXContent(builder, params);
            builder.field("hkey", hash(license, document.getClusterUUID()));
            builder.endObject();
        }

        final ClusterStatsResponse clusterStats = document.getClusterStats();
        if (clusterStats != null) {
            builder.startObject("cluster_stats");
            clusterStats.toXContent(builder, params);
            builder.endObject();
        }

        final ClusterState clusterState = document.getClusterState();
        if (clusterState != null) {
            builder.startObject("cluster_state");
            builder.field("status", document.getStatus().name().toLowerCase(Locale.ROOT));
            clusterState.toXContent(builder, CLUSTER_STATS_PARAMS);
            builder.endObject();
        }

        final List<XPackFeatureSet.Usage> usages = document.getUsage();
        if (usages != null) {
            // in the future we may choose to add other usages under the stack_stats section, but it is only xpack for now
            // it may also be combined on the UI side of phone-home to add things like "kibana" and "logstash" under "stack_stats"
            builder.startObject("stack_stats").startObject("xpack");
            for (final XPackFeatureSet.Usage usage : usages) {
                builder.field(usage.name(), usage);
            }
            builder.endObject().endObject();
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
