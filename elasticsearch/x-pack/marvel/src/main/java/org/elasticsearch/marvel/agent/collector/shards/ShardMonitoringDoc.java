/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.shards;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;

public class ShardMonitoringDoc extends MonitoringDoc {

    private ShardRouting shardRouting;
    private String clusterStateUUID;

    public ShardMonitoringDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
    }

    public void setShardRouting(ShardRouting shardRouting) {
        this.shardRouting = shardRouting;
    }

    public void setClusterStateUUID(java.lang.String clusterStateUUID) {
        this.clusterStateUUID = clusterStateUUID;
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public String getClusterStateUUID() {
        return clusterStateUUID;
    }
}
