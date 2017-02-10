/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.shards;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.collector.shards.ShardMonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class ShardsResolver extends MonitoringIndexNameResolver.Timestamped<ShardMonitoringDoc> {

    public static final String TYPE = "shards";

    static final Set<String> FILTERS;
    static {
        Set<String> filters = Sets.newHashSet(
            "cluster_uuid",
            "timestamp",
            "source_node",
            "state_uuid",
            "shard.state",
            "shard.primary",
            "shard.node",
            "shard.relocating_node",
            "shard.shard",
            "shard.index");
        FILTERS = Collections.unmodifiableSet(filters);
    }

    public ShardsResolver(MonitoredSystem id, Settings settings) {
        super(id, settings);
    }

    @Override
    public String type(ShardMonitoringDoc document) {
        return TYPE;
    }

    @Override
    public String id(ShardMonitoringDoc document) {
        return id(document.getClusterStateUUID(), document.getShardRouting());
    }

    @Override
    public Set<String> filters() {
        return FILTERS;
    }

    @Override
    protected void buildXContent(ShardMonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(Fields.STATE_UUID, document.getClusterStateUUID());

        ShardRouting shardRouting = document.getShardRouting();
        if (shardRouting != null) {
            // ShardRouting is rendered inside a startObject() / endObject() but without a name,
            // so we must use XContentBuilder.field(String, ToXContent, ToXContent.Params) here
            builder.field(Fields.SHARD, shardRouting, params);
        }
    }

    static final class Fields {
        static final String SHARD = "shard";
        static final String STATE_UUID = "state_uuid";
    }

    /**
     * Compute an id that has the format:
     *
     * {state_uuid}:{node_id || '_na'}:{index}:{shard}:{'p' || 'r'}
     */
    static String id(String stateUUID, ShardRouting shardRouting) {
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
