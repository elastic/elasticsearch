/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
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
public class IndicesStatsMonitoringDoc extends FilteredMonitoringDoc {

    public static final String TYPE = "indices_stats";

    private final IndicesStatsResponse indicesStats;

    IndicesStatsMonitoringDoc(final String cluster,
                              final long timestamp,
                              final long intervalMillis,
                              final MonitoringDoc.Node node,
                              final IndicesStatsResponse indicesStats) {
        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null, XCONTENT_FILTERS);
        this.indicesStats = Objects.requireNonNull(indicesStats);
    }

    IndicesStatsResponse getIndicesStats() {
        return indicesStats;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TYPE);
        indicesStats.toXContent(builder, params);
        builder.endObject();
    }

    public static final Set<String> XCONTENT_FILTERS =
        Sets.newHashSet("indices_stats._all.primaries.docs.count",
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
                        "indices_stats._all.total.store.size_in_bytes");
}
