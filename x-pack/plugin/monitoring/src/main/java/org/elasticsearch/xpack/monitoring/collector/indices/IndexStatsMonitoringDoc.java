/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.FilteredMonitoringDoc;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Monitoring document collected by {@link IndexStatsCollector}
 */
public class IndexStatsMonitoringDoc extends FilteredMonitoringDoc {

    public static final String TYPE = "index_stats";

    private final IndexStats indexStats;
    private final IndexMetaData metaData;
    private final IndexRoutingTable routingTable;

    IndexStatsMonitoringDoc(final String cluster,
                            final long timestamp,
                            final long intervalMillis,
                            final MonitoringDoc.Node node,
                            @Nullable final IndexStats indexStats,
                            final IndexMetaData metaData,
                            final IndexRoutingTable routingTable) {
        super(cluster, timestamp, intervalMillis, node, MonitoredSystem.ES, TYPE, null, XCONTENT_FILTERS);
        this.indexStats = indexStats;
        this.metaData = Objects.requireNonNull(metaData);
        this.routingTable = Objects.requireNonNull(routingTable);
    }

    IndexStats getIndexStats() {
        return indexStats;
    }

    IndexMetaData getIndexMetaData() {
        return metaData;
    }

    IndexRoutingTable getIndexRoutingTable() {
        return routingTable;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        final ClusterIndexHealth health = new ClusterIndexHealth(metaData, routingTable);

        builder.startObject(TYPE);
        {
            builder.field("index", metaData.getIndex().getName());
            builder.field("uuid", metaData.getIndexUUID());
            builder.field("created", metaData.getCreationDate());
            builder.field("status", health.getStatus().name().toLowerCase(Locale.ROOT));

            builder.startObject("shards");
            {
                final int total = metaData.getTotalNumberOfShards();
                final int primaries = metaData.getNumberOfShards();
                final int activeTotal = health.getActiveShards();
                final int activePrimaries = health.getActivePrimaryShards();
                final int unassignedTotal = health.getUnassignedShards() + health.getInitializingShards();
                final int unassignedPrimaries = primaries - health.getActivePrimaryShards();

                builder.field("total", total);
                builder.field("primaries", primaries);
                builder.field("replicas", metaData.getNumberOfReplicas());
                builder.field("active_total", activeTotal);
                builder.field("active_primaries", activePrimaries);
                builder.field("active_replicas", activeTotal - activePrimaries);
                builder.field("unassigned_total", unassignedTotal);
                builder.field("unassigned_primaries", unassignedPrimaries);
                builder.field("unassigned_replicas", unassignedTotal - unassignedPrimaries);
                builder.field("initializing", health.getInitializingShards());
                builder.field("relocating", health.getRelocatingShards());
            }
            builder.endObject();

            // when an index is completely red, then we don't get stats for it
            if (indexStats != null) {
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
        }
        builder.endObject();
    }

    public static final Set<String> XCONTENT_FILTERS =
        Sets.newHashSet("index_stats.index",
                        "index_stats.uuid",
                        "index_stats.created",
                        "index_stats.status",
                        "index_stats.shards.total",
                        "index_stats.shards.primaries",
                        "index_stats.shards.replicas",
                        "index_stats.shards.active_total",
                        "index_stats.shards.active_primaries",
                        "index_stats.shards.active_replicas",
                        "index_stats.shards.unassigned_total",
                        "index_stats.shards.unassigned_primaries",
                        "index_stats.shards.unassigned_replicas",
                        "index_stats.shards.initializing",
                        "index_stats.shards.relocating",
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
                        "index_stats.primaries.refresh.external_total_time_in_millis",
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
                        "index_stats.total.refresh.total_time_in_millis",
                        "index_stats.total.refresh.external_total_time_in_millis");
}
