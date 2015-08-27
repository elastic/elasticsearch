/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class NodeStatsMarvelDoc extends MarvelDoc {

    private final String nodeId;
    private final boolean nodeMaster;
    private final NodeStats nodeStats;

    private final boolean mlockall;
    private final Double diskThresholdWaterMarkHigh;
    private final boolean diskThresholdDeciderEnabled;

    public NodeStatsMarvelDoc(String clusterUUID, String type, long timestamp,
                              String nodeId, boolean nodeMaster, NodeStats nodeStats, boolean mlockall, Double diskThresholdWaterMarkHigh, boolean diskThresholdDeciderEnabled) {
        super(clusterUUID, type, timestamp);
        this.nodeId = nodeId;
        this.nodeMaster = nodeMaster;
        this.nodeStats = nodeStats;
        this.mlockall = mlockall;
        this.diskThresholdWaterMarkHigh = diskThresholdWaterMarkHigh;
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

