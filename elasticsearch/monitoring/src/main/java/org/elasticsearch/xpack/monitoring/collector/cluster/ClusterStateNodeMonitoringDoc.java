/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

public class ClusterStateNodeMonitoringDoc extends MonitoringDoc {

    private String stateUUID;
    private String nodeId;

    public ClusterStateNodeMonitoringDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
    }

    public String getStateUUID() {
        return stateUUID;
    }

    public void setStateUUID(String stateUUID) {
        this.stateUUID = stateUUID;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}

