/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.AbstractRenderer;

import java.io.IOException;

public class NodeStatsRenderer extends AbstractRenderer<NodeStatsMarvelDoc> {

    public static final String[] FILTERS = {
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
            "node_stats.os.cpu.load_average",
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

    public NodeStatsRenderer() {
        super(FILTERS, true);
    }

    @Override
    protected void doRender(NodeStatsMarvelDoc marvelDoc, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.NODE_STATS);

        builder.field(Fields.NODE_ID, marvelDoc.getNodeId());
        builder.field(Fields.NODE_MASTER, marvelDoc.isNodeMaster());
        builder.field(Fields.MLOCKALL, marvelDoc.isMlockall());
        builder.field(Fields.DISK_THRESHOLD_ENABLED, marvelDoc.isDiskThresholdDeciderEnabled());
        builder.field(Fields.DISK_THRESHOLD_WATERMARK_HIGH, marvelDoc.getDiskThresholdWaterMarkHigh());

        NodeStats nodeStats = marvelDoc.getNodeStats();
        if (nodeStats != null) {
            nodeStats.toXContent(builder, params);
        }

        builder.endObject();
    }

    static final class Fields {
        static final XContentBuilderString NODE_STATS = new XContentBuilderString("node_stats");
        static final XContentBuilderString NODE_ID = new XContentBuilderString("node_id");
        static final XContentBuilderString NODE_MASTER = new XContentBuilderString("node_master");
        static final XContentBuilderString MLOCKALL = new XContentBuilderString("mlockall");
        static final XContentBuilderString DISK_THRESHOLD_ENABLED = new XContentBuilderString("disk_threshold_enabled");
        static final XContentBuilderString DISK_THRESHOLD_WATERMARK_HIGH = new XContentBuilderString("disk_threshold_watermark_high");
    }
}



