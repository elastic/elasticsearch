/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

/**
 * Monitoring document collected by {@link IndicesStatsCollector}
 */
public class IndicesStatsMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "indices_stats";

    private final IndicesStatsResponse indicesStats;

    public IndicesStatsMonitoringDoc(String monitoringId, String monitoringVersion,
                                     String clusterUUID, long timestamp, DiscoveryNode node,
                                     IndicesStatsResponse indicesStats) {
        super(monitoringId, monitoringVersion, TYPE, null, clusterUUID, timestamp, node);
        this.indicesStats = indicesStats;
    }

    public IndicesStatsResponse getIndicesStats() {
        return indicesStats;
    }
}
