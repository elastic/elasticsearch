/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

/**
 * Monitoring document collected by {@link NodeStatsCollector}
 */
public class NodeStatsMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "node_stats";

    private final String nodeId;
    private final boolean nodeMaster;
    private final NodeStats nodeStats;
    private final boolean mlockall;

    public NodeStatsMonitoringDoc(String monitoringId, String monitoringVersion,
                                  String clusterUUID, DiscoveryNode node,
                                  boolean isMaster, NodeStats nodeStats, boolean mlockall) {
        super(monitoringId, monitoringVersion, TYPE, null, clusterUUID,
                nodeStats.getTimestamp(), node);
        this.nodeId = node.getId();
        this.nodeMaster = isMaster;
        this.nodeStats = nodeStats;
        this.mlockall = mlockall;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isNodeMaster() {
        return nodeMaster;
    }

    public NodeStats getNodeStats() {
        return nodeStats;
    }

    public boolean isMlockall() {
        return mlockall;
    }
}