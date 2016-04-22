/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.indices;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsMonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;

import java.io.IOException;

public class IndexStatsResolver extends MonitoringIndexNameResolver.Timestamped<IndexStatsMonitoringDoc> {

    public static final String TYPE = "index_stats";

    static final String[] FILTERS = {
            "cluster_uuid",
            "timestamp",
            "source_node",
            "index_stats.index",
            "index_stats.primaries.docs.count",
            "index_stats.primaries.fielddata.memory_size_in_bytes",
            "index_stats.primaries.indexing.index_total",
            "index_stats.primaries.indexing.index_time_in_millis",
            "index_stats.primaries.indexing.throttle_time_in_millis",
            "index_stats.primaries.merges.total_size_in_bytes",
            "index_stats.primaries.search.query_total",
            "index_stats.primaries.search.query_time_in_millis",
            "index_stats.primaries.segments.memory_in_bytes",
            "index_stats.primaries.segments.terms_memory_in_bytes",
            "index_stats.primaries.segments.stored_fields_memory_in_bytes",
            "index_stats.primaries.segments.term_vectors_memory_in_bytes",
            "index_stats.primaries.segments.norms_memory_in_bytes",
            "index_stats.primaries.segments.doc_values_memory_in_bytes",
            "index_stats.primaries.segments.index_writer_memory_in_bytes",
            "index_stats.primaries.segments.version_map_memory_in_bytes",
            "index_stats.primaries.segments.fixed_bit_set_memory_in_bytes",
            "index_stats.primaries.store.size_in_bytes",
            "index_stats.primaries.store.throttle_time_in_millis",
            "index_stats.primaries.refresh.total_time_in_millis",
            "index_stats.total.docs.count",
            "index_stats.total.fielddata.memory_size_in_bytes",
            "index_stats.total.indexing.index_total",
            "index_stats.total.indexing.index_time_in_millis",
            "index_stats.total.indexing.throttle_time_in_millis",
            "index_stats.total.merges.total_size_in_bytes",
            "index_stats.total.search.query_total",
            "index_stats.total.search.query_time_in_millis",
            "index_stats.total.segments.memory_in_bytes",
            "index_stats.total.segments.terms_memory_in_bytes",
            "index_stats.total.segments.stored_fields_memory_in_bytes",
            "index_stats.total.segments.term_vectors_memory_in_bytes",
            "index_stats.total.segments.norms_memory_in_bytes",
            "index_stats.total.segments.doc_values_memory_in_bytes",
            "index_stats.total.segments.index_writer_memory_in_bytes",
            "index_stats.total.segments.version_map_memory_in_bytes",
            "index_stats.total.segments.fixed_bit_set_memory_in_bytes",
            "index_stats.total.store.size_in_bytes",
            "index_stats.total.store.throttle_time_in_millis",
            "index_stats.total.refresh.total_time_in_millis",
    };

    public IndexStatsResolver(MonitoredSystem id, Settings settings) {
        super(id, settings);
    }

    @Override
    public String type(IndexStatsMonitoringDoc document) {
        return TYPE;
    }

    @Override
    public String[] filters() {
        return FILTERS;
    }

    @Override
    protected void buildXContent(IndexStatsMonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.INDEX_STATS);

        IndexStats indexStats = document.getIndexStats();
        if (indexStats != null) {
            builder.field(Fields.INDEX, indexStats.getIndex());

            builder.startObject(Fields.TOTAL);
            if (indexStats.getTotal() != null) {
                indexStats.getTotal().toXContent(builder, params);
            }
            builder.endObject();

            builder.startObject(Fields.PRIMARIES);
            if (indexStats.getPrimaries() != null) {
                indexStats.getPrimaries().toXContent(builder, params);
            }
            builder.endObject();
        }

        builder.endObject();
    }

    static final class Fields {
        static final String INDEX_STATS = TYPE;
        static final String INDEX = "index";
        static final String TOTAL = "total";
        static final String PRIMARIES = "primaries";
    }
}
