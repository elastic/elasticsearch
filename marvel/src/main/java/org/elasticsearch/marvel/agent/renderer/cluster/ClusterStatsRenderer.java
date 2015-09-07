/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.AbstractRenderer;

import java.io.IOException;

public class ClusterStatsRenderer extends AbstractRenderer<ClusterStatsMarvelDoc> {

    public static final String[] FILTERS = {
            "cluster_stats.nodes.count.total",
            "cluster_stats.indices.count",
            "cluster_stats.indices.shards.total",
            "cluster_stats.indices.shards.index.replication.min",
            "cluster_stats.indices.docs.count",
            "cluster_stats.indices.store.size_in_bytes",
            "cluster_stats.nodes.fs.total_in_bytes",
            "cluster_stats.nodes.fs.free_in_bytes",
            "cluster_stats.nodes.fs.available_in_bytes",
            "cluster_stats.nodes.jvm.max_uptime_in_millis",
            "cluster_stats.nodes.jvm.mem.heap_max_in_bytes",
            "cluster_stats.nodes.jvm.mem.heap_used_in_bytes",
            "cluster_stats.nodes.versions",
    };

    public ClusterStatsRenderer() {
        super(FILTERS, true);
    }

    @Override
    protected void doRender(ClusterStatsMarvelDoc marvelDoc, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.CLUSTER_STATS);

        ClusterStatsResponse clusterStats = marvelDoc.getClusterStats();
        if (clusterStats != null) {
            clusterStats.toXContent(builder, params);
        }

        builder.endObject();
    }

    static final class Fields {
        static final XContentBuilderString CLUSTER_STATS = new XContentBuilderString("cluster_stats");
    }
}
