/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.FilteredMonitoringDoc;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Monitoring document collected by {@link IndexStatsCollector}
 */
public class IndicesStatsMonitoringDoc extends FilteredMonitoringDoc {

    public static final String TYPE = "indices_stats";

    private final List<IndexStats> indicesStats;

    IndicesStatsMonitoringDoc(
        final String cluster,
        final long timestamp,
        final long intervalMillis,
        final MonitoringDoc.Node node,
        final List<IndexStats> indicesStats
    ) {
        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null, XCONTENT_FILTERS);
        this.indicesStats = Objects.requireNonNull(indicesStats);
    }

    List<IndexStats> getIndicesStats() {
        return indicesStats;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        final CommonStats total = new CommonStats();
        final CommonStats primaries = new CommonStats();

        for (IndexStats indexStats : getIndicesStats()) {
            final ShardStats[] shardsStats = indexStats.getShards();
            if (shardsStats != null) {
                for (ShardStats shard : indexStats.getShards()) {
                    total.add(shard.getStats());
                    if (shard.getShardRouting().primary()) {
                        primaries.add(shard.getStats());
                    }
                }
            }
        }

        builder.startObject(TYPE);
        {
            builder.startObject("_all");
            {
                builder.startObject("primaries");
                primaries.toXContent(builder, params);
                builder.endObject();

                builder.startObject("total");
                total.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();

            builder.startObject("indices");
            for (IndexStats indexStats : getIndicesStats()) {
                builder.startObject(indexStats.getIndex());

                builder.startObject("primaries");
                indexStats.getPrimaries().toXContent(builder, params);
                builder.endObject();

                builder.startObject("total");
                indexStats.getTotal().toXContent(builder, params);
                builder.endObject();

                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
    }

    public static final Set<String> XCONTENT_FILTERS = Set.of(
        "indices_stats._all.primaries.docs.count",
        "indices_stats._all.primaries.indexing.index_time_in_millis",
        "indices_stats._all.primaries.indexing.index_total",
        "indices_stats._all.primaries.indexing.is_throttled",
        "indices_stats._all.primaries.indexing.throttle_time_in_millis",
        "indices_stats._all.primaries.search.query_time_in_millis",
        "indices_stats._all.primaries.search.query_total",
        "indices_stats._all.primaries.store.size_in_bytes",
        "indices_stats._all.primaries.bulk.total_operations",
        "indices_stats._all.primaries.bulk.total_time_in_millis",
        "indices_stats._all.primaries.bulk.total_size_in_bytes",
        "indices_stats._all.primaries.bulk.avg_time_in_millis",
        "indices_stats._all.primaries.bulk.avg_size_in_bytes",
        "indices_stats._all.total.docs.count",
        "indices_stats._all.total.indexing.index_time_in_millis",
        "indices_stats._all.total.indexing.index_total",
        "indices_stats._all.total.indexing.is_throttled",
        "indices_stats._all.total.indexing.throttle_time_in_millis",
        "indices_stats._all.total.search.query_time_in_millis",
        "indices_stats._all.total.search.query_total",
        "indices_stats._all.total.store.size_in_bytes",
        "indices_stats._all.total.bulk.total_operations",
        "indices_stats._all.total.bulk.total_time_in_millis",
        "indices_stats._all.total.bulk.total_size_in_bytes",
        "indices_stats._all.total.bulk.avg_time_in_millis",
        "indices_stats._all.total.bulk.avg_size_in_bytes"
    );
}
