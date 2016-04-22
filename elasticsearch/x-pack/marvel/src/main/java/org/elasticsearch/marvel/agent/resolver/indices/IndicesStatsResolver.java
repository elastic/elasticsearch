/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.indices.IndicesStatsMonitoringDoc;
import org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver;

import java.io.IOException;

public class IndicesStatsResolver extends MonitoringIndexNameResolver.Timestamped<IndicesStatsMonitoringDoc> {

    public static final String TYPE = "indices_stats";

    static final String[] FILTERS = {
            "cluster_uuid",
            "timestamp",
            "source_node",
            "indices_stats._all.primaries.docs.count",
            "indices_stats._all.primaries.indexing.index_time_in_millis",
            "indices_stats._all.primaries.indexing.index_total",
            "indices_stats._all.primaries.indexing.is_throttled",
            "indices_stats._all.primaries.indexing.throttle_time_in_millis",
            "indices_stats._all.primaries.search.query_time_in_millis",
            "indices_stats._all.primaries.search.query_total",
            "indices_stats._all.primaries.store.size_in_bytes",
            "indices_stats._all.total.docs.count",
            "indices_stats._all.total.indexing.index_time_in_millis",
            "indices_stats._all.total.indexing.index_total",
            "indices_stats._all.total.indexing.is_throttled",
            "indices_stats._all.total.indexing.throttle_time_in_millis",
            "indices_stats._all.total.search.query_time_in_millis",
            "indices_stats._all.total.search.query_total",
            "indices_stats._all.total.store.size_in_bytes",
    };

    public IndicesStatsResolver(MonitoredSystem id, Settings settings) {
        super(id, settings);
    }

    @Override
    public String type(IndicesStatsMonitoringDoc document) {
        return TYPE;
    }

    @Override
    public String[] filters() {
        return FILTERS;
    }

    @Override
    protected void buildXContent(IndicesStatsMonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.INDICES_STATS);
        IndicesStatsResponse indicesStats = document.getIndicesStats();
        if (indicesStats != null) {
            indicesStats.toXContent(builder, params);
        }
        builder.endObject();
    }

    static final class Fields {
        static final String INDICES_STATS = TYPE;
    }
}
