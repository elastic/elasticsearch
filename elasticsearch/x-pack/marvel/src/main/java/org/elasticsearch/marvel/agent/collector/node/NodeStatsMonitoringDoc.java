/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;

public class NodeStatsMonitoringDoc extends MonitoringDoc {

    private String nodeId;
    private boolean nodeMaster;
    private NodeStats nodeStats;

    private boolean mlockall;
    private Double diskThresholdWaterMarkHigh;
    private boolean diskThresholdDeciderEnabled;

    public NodeStatsMonitoringDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public void setNodeMaster(boolean nodeMaster) {
        this.nodeMaster = nodeMaster;
    }

    public void setNodeStats(NodeStats nodeStats) {
        this.nodeStats = nodeStats;
    }

    public void setMlockall(boolean mlockall) {
        this.mlockall = mlockall;
    }

    public void setDiskThresholdWaterMarkHigh(Double diskThresholdWaterMarkHigh) {
        this.diskThresholdWaterMarkHigh = diskThresholdWaterMarkHigh;
    }

    public void setDiskThresholdDeciderEnabled(boolean diskThresholdDeciderEnabled) {
        this.diskThresholdDeciderEnabled = diskThresholdDeciderEnabled;
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

    public Double getDiskThresholdWaterMarkHigh() {
        return diskThresholdWaterMarkHigh;
    }

    public boolean isDiskThresholdDeciderEnabled() {
        return diskThresholdDeciderEnabled;
    }
}

