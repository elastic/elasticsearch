/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

public class ClusterStateMonitoringDoc extends MonitoringDoc {

    private ClusterState clusterState;
    private ClusterHealthStatus status;

    public ClusterStateMonitoringDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
    }

    public ClusterState getClusterState() {
        return clusterState;
    }

    public void setClusterState(ClusterState clusterState) {
        this.clusterState = clusterState;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public void setStatus(ClusterHealthStatus status) {
        this.status = status;
    }
}
