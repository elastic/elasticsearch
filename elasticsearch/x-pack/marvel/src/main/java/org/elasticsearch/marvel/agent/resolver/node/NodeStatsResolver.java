/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.marvel.MonitoringIds;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;

import java.io.IOException;

public class NodeStatsResolver extends MonitoringIndexNameResolver.Timestamped<NodeStatsMonitoringDoc> {

    public static final String TYPE = "node_stats";

    private static final String[] FILTERS = {
            // Common information
            "cluster_uuid",
            "timestamp",
            "source_node",
            // Extra information
            "node_stats.node_id",
            "node_stats.node_master",
            "node_stats.mlockall",
            "node_stats.disk_threshold_enabled",
            "node_stats.disk_threshold_watermark_high",
            // Node Stats
            "node_stats.indices.docs.count",
            "node_stats.indices.store.size_in_bytes",
            "node_stats.indices.store.throttle_time_in_millis",
            "node_stats.indices.indexing.throttle_time_in_millis",
            "node_stats.indices.indexing.index_total",
            "node_stats.indices.indexing.index_time_in_millis",
            "node_stats.indices.search.query_total",
            "node_stats.indices.search.query_time_in_millis",
            "node_stats.indices.segments.count",
            "node_stats.fs.total.total_in_bytes",
            "node_stats.fs.total.free_in_bytes",
            "node_stats.fs.total.available_in_bytes",
            "node_stats.os.cpu.load_average.1m",
            "node_stats.process.cpu.percent",
            "node_stats.process.max_file_descriptors",
            "node_stats.process.open_file_descriptors",
            "node_stats.jvm.mem.heap_used_in_bytes",
            "node_stats.jvm.mem.heap_used_percent",
            "node_stats.jvm.gc.collectors.young",
            "node_stats.jvm.gc.collectors.young.collection_count",
            "node_stats.jvm.gc.collectors.young.collection_time_in_millis",
            "node_stats.jvm.gc.collectors.old",
            "node_stats.jvm.gc.collectors.old.collection_count",
            "node_stats.jvm.gc.collectors.old.collection_time_in_millis",
            "node_stats.thread_pool.index.rejected",
            "node_stats.thread_pool.search.rejected",
            "node_stats.thread_pool.bulk.rejected",
    };

    public NodeStatsResolver(Settings settings) {
        super(MonitoringIds.ES.getId(), MarvelTemplateUtils.TEMPLATE_VERSION, settings);
    }

    @Override
    public String type(NodeStatsMonitoringDoc document) {
        return TYPE;
    }

    @Override
    public String[] filters() {
        return FILTERS;
    }

    @Override
    protected void buildXContent(NodeStatsMonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.NODE_STATS);

        builder.field(Fields.NODE_ID, document.getNodeId());
        builder.field(Fields.NODE_MASTER, document.isNodeMaster());
        builder.field(Fields.MLOCKALL, document.isMlockall());
        builder.field(Fields.DISK_THRESHOLD_ENABLED, document.isDiskThresholdDeciderEnabled());
        builder.field(Fields.DISK_THRESHOLD_WATERMARK_HIGH, document.getDiskThresholdWaterMarkHigh());

        NodeStats nodeStats = document.getNodeStats();
        if (nodeStats != null) {
            nodeStats.toXContent(builder, params);
        }

        builder.endObject();
    }

    static final class Fields {
        static final XContentBuilderString NODE_STATS = new XContentBuilderString(TYPE);
        static final XContentBuilderString NODE_ID = new XContentBuilderString("node_id");
        static final XContentBuilderString NODE_MASTER = new XContentBuilderString("node_master");
        static final XContentBuilderString MLOCKALL = new XContentBuilderString("mlockall");
        static final XContentBuilderString DISK_THRESHOLD_ENABLED = new XContentBuilderString("disk_threshold_enabled");
        static final XContentBuilderString DISK_THRESHOLD_WATERMARK_HIGH = new XContentBuilderString("disk_threshold_watermark_high");
    }
}
