/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class ClusterStatsResolver extends MonitoringIndexNameResolver.Timestamped<ClusterStatsMonitoringDoc> {

    public static final String TYPE = "cluster_stats";

    static final Set<String> FILTERS;
    static {
        Set<String> filters = Sets.newHashSet(
            "cluster_uuid",
            "timestamp",
            "source_node",
            "cluster_stats.nodes.count.total",
            "cluster_stats.indices.count",
            "cluster_stats.indices.shards.total",
            "cluster_stats.indices.shards.primaries",
            "cluster_stats.indices.docs.count",
            "cluster_stats.indices.store.size_in_bytes",
            "cluster_stats.nodes.fs.total_in_bytes",
            "cluster_stats.nodes.fs.free_in_bytes",
            "cluster_stats.nodes.fs.available_in_bytes",
            "cluster_stats.nodes.jvm.max_uptime_in_millis",
            "cluster_stats.nodes.jvm.mem.heap_max_in_bytes",
            "cluster_stats.nodes.jvm.mem.heap_used_in_bytes",
            "cluster_stats.nodes.versions");
        FILTERS = Collections.unmodifiableSet(filters);
    }

    public ClusterStatsResolver(MonitoredSystem id, Settings settings) {
        super(id, settings);
    }

    @Override
    public Set<String> filters() {
        return FILTERS;
    }

    @Override
    protected void buildXContent(ClusterStatsMonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.CLUSTER_STATS);
        ClusterStatsResponse clusterStats = document.getClusterStats();
        if (clusterStats != null) {
            clusterStats.toXContent(builder, params);
        }
        builder.endObject();
    }

    static final class Fields {
        static final String CLUSTER_STATS = TYPE;
    }
}
