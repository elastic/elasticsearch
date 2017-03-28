/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.List;

/**
 * Monitoring document collected by {@link ClusterStatsCollector} and indexed in the
 * monitoring data index. It contains all information about the current cluster, mostly
 * for enabling/disabling features on Kibana side according to the license and also for
 * the "phone home" feature.
 */
public class ClusterInfoMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "cluster_info";

    private final String clusterName;
    private final String version;
    private final License license;
    private final List<XPackFeatureSet.Usage> usage;
    private final ClusterStatsResponse clusterStats;

    public ClusterInfoMonitoringDoc(String monitoringId, String monitoringVersion,
                                    String clusterUUID, long timestamp, DiscoveryNode node,
                                    String clusterName, String version, License license,
                                    List<XPackFeatureSet.Usage> usage,
                                    ClusterStatsResponse clusterStats) {
        super(monitoringId, monitoringVersion, TYPE, clusterUUID, clusterUUID, timestamp, node);
        this.clusterName = clusterName;
        this.version = version;
        this.license = license;
        this.usage = usage;
        this.clusterStats = clusterStats;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getVersion() {
        return version;
    }

    public License getLicense() {
        return license;
    }

    public List<XPackFeatureSet.Usage> getUsage() {
        return usage;
    }

    public ClusterStatsResponse getClusterStats() {
        return clusterStats;
    }
}
