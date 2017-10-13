/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.exporter.FilteredMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * Monitoring document collected by {@link IndexStatsCollector}
 */
public class IndexStatsMonitoringDoc extends FilteredMonitoringDoc {

    public static final String TYPE = "index_stats";

    private final IndexStats indexStats;

    IndexStatsMonitoringDoc(final String cluster,
                            final long timestamp,
                            final long intervalMillis,
                            final MonitoringDoc.Node node,
                            final IndexStats indexStats) {
        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null, XCONTENT_FILTERS);
        this.indexStats = Objects.requireNonNull(indexStats);
    }

    IndexStats getIndexStats() {
        return indexStats;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        {
            builder.field("index", indexStats.getIndex());

            final CommonStats totalStats = indexStats.getTotal();
            if (totalStats != null) {
                builder.startObject("total");
                {
                    totalStats.toXContent(builder, params);
                }
                builder.endObject();
            }

            final CommonStats primariesStats = indexStats.getPrimaries();
            if (primariesStats != null) {
                builder.startObject("primaries");
                {
                    primariesStats.toXContent(builder, params);
                }
                builder.endObject();
            }
        }
        builder.endObject();
    }

    public static final Set<String> XCONTENT_FILTERS =
        Sets.newHashSet("index_stats.index",
                        "index_stats.primaries.docs.count",
                        "index_stats.primaries.fielddata.memory_size_in_bytes",
                        "index_stats.primaries.fielddata.evictions",
                        "index_stats.primaries.indexing.index_total",
                        "index_stats.primaries.indexing.index_time_in_millis",
                        "index_stats.primaries.indexing.throttle_time_in_millis",
                        "index_stats.primaries.merges.total_size_in_bytes",
                        "index_stats.primaries.query_cache.memory_size_in_bytes",
                        "index_stats.primaries.query_cache.evictions",
                        "index_stats.primaries.query_cache.hit_count",
                        "index_stats.primaries.query_cache.miss_count",
                        "index_stats.primaries.request_cache.memory_size_in_bytes",
                        "index_stats.primaries.request_cache.evictions",
                        "index_stats.primaries.request_cache.hit_count",
                        "index_stats.primaries.request_cache.miss_count",
                        "index_stats.primaries.search.query_total",
                        "index_stats.primaries.search.query_time_in_millis",
                        "index_stats.primaries.segments.count",
                        "index_stats.primaries.segments.memory_in_bytes",
                        "index_stats.primaries.segments.terms_memory_in_bytes",
                        "index_stats.primaries.segments.stored_fields_memory_in_bytes",
                        "index_stats.primaries.segments.term_vectors_memory_in_bytes",
                        "index_stats.primaries.segments.norms_memory_in_bytes",
                        "index_stats.primaries.segments.points_memory_in_bytes",
                        "index_stats.primaries.segments.doc_values_memory_in_bytes",
                        "index_stats.primaries.segments.index_writer_memory_in_bytes",
                        "index_stats.primaries.segments.version_map_memory_in_bytes",
                        "index_stats.primaries.segments.fixed_bit_set_memory_in_bytes",
                        "index_stats.primaries.store.size_in_bytes",
                        "index_stats.primaries.refresh.total_time_in_millis",
                        "index_stats.total.docs.count",
                        "index_stats.total.fielddata.memory_size_in_bytes",
                        "index_stats.total.fielddata.evictions",
                        "index_stats.total.indexing.index_total",
                        "index_stats.total.indexing.index_time_in_millis",
                        "index_stats.total.indexing.throttle_time_in_millis",
                        "index_stats.total.merges.total_size_in_bytes",
                        "index_stats.total.query_cache.memory_size_in_bytes",
                        "index_stats.total.query_cache.evictions",
                        "index_stats.total.query_cache.hit_count",
                        "index_stats.total.query_cache.miss_count",
                        "index_stats.total.request_cache.memory_size_in_bytes",
                        "index_stats.total.request_cache.evictions",
                        "index_stats.total.request_cache.hit_count",
                        "index_stats.total.request_cache.miss_count",
                        "index_stats.total.search.query_total",
                        "index_stats.total.search.query_time_in_millis",
                        "index_stats.total.segments.count",
                        "index_stats.total.segments.memory_in_bytes",
                        "index_stats.total.segments.terms_memory_in_bytes",
                        "index_stats.total.segments.stored_fields_memory_in_bytes",
                        "index_stats.total.segments.term_vectors_memory_in_bytes",
                        "index_stats.total.segments.norms_memory_in_bytes",
                        "index_stats.total.segments.points_memory_in_bytes",
                        "index_stats.total.segments.doc_values_memory_in_bytes",
                        "index_stats.total.segments.index_writer_memory_in_bytes",
                        "index_stats.total.segments.version_map_memory_in_bytes",
                        "index_stats.total.segments.fixed_bit_set_memory_in_bytes",
                        "index_stats.total.store.size_in_bytes",
                        "index_stats.total.refresh.total_time_in_millis");
}
