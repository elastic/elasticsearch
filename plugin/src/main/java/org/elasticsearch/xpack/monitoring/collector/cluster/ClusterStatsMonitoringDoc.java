/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

/**
 * Monitoring document collected by {@link ClusterStatsCollector} that contains the current
 * cluster stats.
 */
public class ClusterStatsMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "cluster_stats";

    private final ClusterStatsResponse clusterStats;

    public ClusterStatsMonitoringDoc(String monitoringId, String monitoringVersion,
                                     String clusterUUID, long timestamp, DiscoveryNode node,
                                     ClusterStatsResponse clusterStats) {
        super(monitoringId, monitoringVersion, TYPE, null, clusterUUID, timestamp, node);
        this.clusterStats = clusterStats;
    }

    public ClusterStatsResponse getClusterStats() {
        return clusterStats;
    }
}
