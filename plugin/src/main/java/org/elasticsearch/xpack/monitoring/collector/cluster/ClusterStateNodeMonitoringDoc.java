/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

/**
 * Monitoring document collected by {@link ClusterStateCollector} that contains the id of
 * every node of the cluster.
 */
public class ClusterStateNodeMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "node";

    private final String stateUUID;
    private final String nodeId;

    public ClusterStateNodeMonitoringDoc(String monitoringId, String monitoringVersion,
                                         String clusterUUID, long timestamp, DiscoveryNode node,
                                         String stateUUID, String nodeId) {
        super(monitoringId, monitoringVersion, TYPE, null, clusterUUID, timestamp, node);
        this.stateUUID = stateUUID;
        this.nodeId = nodeId;
    }

    public String getStateUUID() {
        return stateUUID;
    }

    public String getNodeId() {
        return nodeId;
    }
}