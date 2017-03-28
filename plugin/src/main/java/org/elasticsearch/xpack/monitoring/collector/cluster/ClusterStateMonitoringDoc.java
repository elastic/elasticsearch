/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

/**
 * Monitoring document collected by {@link ClusterStateCollector} that contains the
 * current cluster state.
 */
public class ClusterStateMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "cluster_state";

    private final ClusterState clusterState;
    private final ClusterHealthStatus status;

    public ClusterStateMonitoringDoc(String monitoringId, String monitoringVersion,
                                     String clusterUUID, long timestamp, DiscoveryNode node,
                                     ClusterState clusterState, ClusterHealthStatus status) {
        super(monitoringId, monitoringVersion, TYPE, null, clusterUUID, timestamp, node);
        this.clusterState = clusterState;
        this.status = status;
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }
}
