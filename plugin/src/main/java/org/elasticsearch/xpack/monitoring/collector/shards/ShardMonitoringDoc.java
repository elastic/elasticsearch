/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.shards;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

/**
 * Monitoring document collected by {@link ShardsCollector}
 */
public class ShardMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "shards";

    private ShardRouting shardRouting;
    private String clusterStateUUID;

    public ShardMonitoringDoc(String monitoringId, String monitoringVersion,
                              String clusterUUID, long timestamp, DiscoveryNode node,
                              ShardRouting shardRouting, String clusterStateUUID) {
        super(monitoringId, monitoringVersion, TYPE, id(clusterStateUUID, shardRouting),
                clusterUUID, timestamp, node);
        this.shardRouting = shardRouting;
        this.clusterStateUUID = clusterStateUUID;
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public String getClusterStateUUID() {
        return clusterStateUUID;
    }

    /**
     * Compute an id that has the format:
     *
     * {state_uuid}:{node_id || '_na'}:{index}:{shard}:{'p' || 'r'}
     */
    public static String id(String stateUUID, ShardRouting shardRouting) {
        StringBuilder builder = new StringBuilder();
        builder.append(stateUUID);
        builder.append(':');
        if (shardRouting.assignedToNode()) {
            builder.append(shardRouting.currentNodeId());
        } else {
            builder.append("_na");
        }
        builder.append(':');
        builder.append(shardRouting.getIndexName());
        builder.append(':');
        builder.append(Integer.valueOf(shardRouting.id()));
        builder.append(':');
        if (shardRouting.primary()) {
            builder.append("p");
        } else {
            builder.append("r");
        }
        return builder.toString();
    }
}