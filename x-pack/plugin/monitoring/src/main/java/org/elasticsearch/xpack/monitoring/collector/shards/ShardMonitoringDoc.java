/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.shards;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.FilteredMonitoringDoc;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * Monitoring document collected by {@link ShardsCollector}
 */
public class ShardMonitoringDoc extends FilteredMonitoringDoc {

    public static final String TYPE = "shards";

    private final ShardRouting shardRouting;
    private final String clusterStateUUID;

    ShardMonitoringDoc(
        final String cluster,
        final long timestamp,
        final long interval,
        final MonitoringDoc.Node node,
        final ShardRouting shardRouting,
        final String clusterStateUUID
    ) {

        super(cluster, timestamp, interval, node, MonitoredSystem.ES, TYPE, id(clusterStateUUID, shardRouting), XCONTENT_FILTERS);
        this.shardRouting = Objects.requireNonNull(shardRouting);
        this.clusterStateUUID = Objects.requireNonNull(clusterStateUUID);
    }

    ShardRouting getShardRouting() {
        return shardRouting;
    }

    String getClusterStateUUID() {
        return clusterStateUUID;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("state_uuid", clusterStateUUID);
        if (shardRouting != null) {
            // ShardRouting is rendered inside a startObject() / endObject() but without a name,
            // so we must use XContentBuilder.field(String, ToXContent, ToXContent.Params) here
            builder.field("shard", shardRouting, params);
        }
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

    public static final Set<String> XCONTENT_FILTERS = Set.of(
        "state_uuid",
        "shard.state",
        "shard.primary",
        "shard.node",
        "shard.relocating_node",
        "shard.shard",
        "shard.index"
    );
}
