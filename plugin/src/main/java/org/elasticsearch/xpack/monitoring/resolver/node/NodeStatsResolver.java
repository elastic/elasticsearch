/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.collector.node.NodeStatsMonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class NodeStatsResolver extends MonitoringIndexNameResolver.Timestamped<NodeStatsMonitoringDoc> {

    public static final String TYPE = "node_stats";

    static final Set<String> FILTERS;
    static {
        Set<String> filters = Sets.newHashSet(
            // Common information
            "cluster_uuid",
            "timestamp",
            "source_node",
            // Extra information
            "node_stats.node_id",
            "node_stats.node_master",
            "node_stats.mlockall",
            // Node Stats
            "node_stats.indices.docs.count",
            "node_stats.indices.fielddata.memory_size_in_bytes",
            "node_stats.indices.fielddata.evictions",
            "node_stats.indices.store.size_in_bytes",
            "node_stats.indices.indexing.throttle_time_in_millis",
            "node_stats.indices.indexing.index_total",
            "node_stats.indices.indexing.index_time_in_millis",
            "node_stats.indices.query_cache.memory_size_in_bytes",
            "node_stats.indices.query_cache.evictions",
            "node_stats.indices.query_cache.hit_count",
            "node_stats.indices.query_cache.miss_count",
            "node_stats.indices.request_cache.memory_size_in_bytes",
            "node_stats.indices.request_cache.evictions",
            "node_stats.indices.request_cache.hit_count",
            "node_stats.indices.request_cache.miss_count",
            "node_stats.indices.search.query_total",
            "node_stats.indices.search.query_time_in_millis",
            "node_stats.indices.segments.count",
            "node_stats.indices.segments.memory_in_bytes",
            "node_stats.indices.segments.terms_memory_in_bytes",
            "node_stats.indices.segments.stored_fields_memory_in_bytes",
            "node_stats.indices.segments.term_vectors_memory_in_bytes",
            "node_stats.indices.segments.norms_memory_in_bytes",
            "node_stats.indices.segments.points_memory_in_bytes",
            "node_stats.indices.segments.doc_values_memory_in_bytes",
            "node_stats.indices.segments.index_writer_memory_in_bytes",
            "node_stats.indices.segments.version_map_memory_in_bytes",
            "node_stats.indices.segments.fixed_bit_set_memory_in_bytes",
            "node_stats.fs.total.total_in_bytes",
            "node_stats.fs.total.free_in_bytes",
            "node_stats.fs.total.available_in_bytes",
            "node_stats.os.cpu.load_average.1m",
            "node_stats.os.cpu.load_average.5m",
            "node_stats.os.cpu.load_average.15m",
            "node_stats.process.cpu.percent",
            "node_stats.process.max_file_descriptors",
            "node_stats.process.open_file_descriptors",
            "node_stats.jvm.mem.heap_max_in_bytes",
            "node_stats.jvm.mem.heap_used_in_bytes",
            "node_stats.jvm.mem.heap_used_percent",
            "node_stats.jvm.gc.collectors.young",
            "node_stats.jvm.gc.collectors.young.collection_count",
            "node_stats.jvm.gc.collectors.young.collection_time_in_millis",
            "node_stats.jvm.gc.collectors.old",
            "node_stats.jvm.gc.collectors.old.collection_count",
            "node_stats.jvm.gc.collectors.old.collection_time_in_millis",
            "node_stats.thread_pool.bulk.threads",
            "node_stats.thread_pool.bulk.queue",
            "node_stats.thread_pool.bulk.rejected",
            "node_stats.thread_pool.generic.threads",
            "node_stats.thread_pool.generic.queue",
            "node_stats.thread_pool.generic.rejected",
            "node_stats.thread_pool.get.threads",
            "node_stats.thread_pool.get.queue",
            "node_stats.thread_pool.get.rejected",
            "node_stats.thread_pool.index.threads",
            "node_stats.thread_pool.index.queue",
            "node_stats.thread_pool.index.rejected",
            "node_stats.thread_pool.management.threads",
            "node_stats.thread_pool.management.queue",
            "node_stats.thread_pool.management.rejected",
            "node_stats.thread_pool.search.threads",
            "node_stats.thread_pool.search.queue",
            "node_stats.thread_pool.search.rejected",
            "node_stats.thread_pool.watcher.threads",
            "node_stats.thread_pool.watcher.queue",
            "node_stats.thread_pool.watcher.rejected",
            // Cgroup data (generally Linux only and only sometimes on those systems)
            "node_stats.os.cgroup.cpuacct.control_group",
            "node_stats.os.cgroup.cpuacct.usage_nanos",
            "node_stats.os.cgroup.cpu.control_group",
            "node_stats.os.cgroup.cpu.cfs_period_micros",
            "node_stats.os.cgroup.cpu.cfs_quota_micros",
            "node_stats.os.cgroup.cpu.stat.number_of_elapsed_periods",
            "node_stats.os.cgroup.cpu.stat.number_of_times_throttled",
            "node_stats.os.cgroup.cpu.stat.time_throttled_nanos",
            // Linux Only (at least for now)
            // Disk Info
            "node_stats.fs.data.spins",
            // Node IO Stats
            "node_stats.fs.io_stats.total.operations",
            "node_stats.fs.io_stats.total.read_operations",
            "node_stats.fs.io_stats.total.write_operations",
            "node_stats.fs.io_stats.total.read_kilobytes",
            "node_stats.fs.io_stats.total.write_kilobytes");
        FILTERS = Collections.unmodifiableSet(filters);
    }

    public NodeStatsResolver(MonitoredSystem id, Settings settings) {
        super(id, settings);
    }

    @Override
    public Set<String> filters() {
        return FILTERS;
    }

    @Override
    protected void buildXContent(NodeStatsMonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.NODE_STATS);

        builder.field(Fields.NODE_ID, document.getNodeId());
        builder.field(Fields.NODE_MASTER, document.isNodeMaster());
        builder.field(Fields.MLOCKALL, document.isMlockall());

        NodeStats nodeStats = document.getNodeStats();
        if (nodeStats != null) {
            nodeStats.toXContent(builder, params);
        }

        builder.endObject();
    }

    static final class Fields {
        static final String NODE_STATS = TYPE;
        static final String NODE_ID = "node_id";
        static final String NODE_MASTER = "node_master";
        static final String MLOCKALL = "mlockall";
    }
}
