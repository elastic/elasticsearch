/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

/**
 * Monitoring document collected by {@link IndexStatsCollector}
 */
public class IndexStatsMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "index_stats";

    private final IndexStats indexStats;

    public IndexStatsMonitoringDoc(String monitoringId, String monitoringVersion,
                                   String clusterUUID, long timestamp, DiscoveryNode node,
                                   IndexStats indexStats) {
        super(monitoringId, monitoringVersion, TYPE, null, clusterUUID, timestamp, node);
        this.indexStats = indexStats;
    }

    public IndexStats getIndexStats() {
        return indexStats;
    }
}
