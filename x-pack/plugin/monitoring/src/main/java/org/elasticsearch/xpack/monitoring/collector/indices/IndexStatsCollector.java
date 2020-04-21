/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for indices and singular index statistics.
 * <p>
 * This collector runs on the master node only and collect a single {@link IndicesStatsMonitoringDoc} for the cluster and a
 * {@link IndexStatsMonitoringDoc} document for each existing index in the cluster.
 */
public class IndexStatsCollector extends Collector {

    /**
     * Timeout value when collecting index statistics (default to 10s)
     */
    public static final Setting<TimeValue> INDEX_STATS_TIMEOUT = collectionTimeoutSetting("index.stats.timeout");

    private final Client client;

    public IndexStatsCollector(final ClusterService clusterService,
                               final XPackLicenseState licenseState,
                               final Client client) {
        super("index-stats", clusterService, INDEX_STATS_TIMEOUT, licenseState);
        this.client = client;
    }

    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        return isElectedMaster && super.shouldCollect(isElectedMaster);
    }

    @Override
    protected Collection<MonitoringDoc> doCollect(final MonitoringDoc.Node node,
                                                  final long interval,
                                                  final ClusterState clusterState) throws Exception {
        final List<MonitoringDoc> results = new ArrayList<>();
        final IndicesStatsResponse indicesStatsResponse = client.admin().indices().prepareStats()
                .setIndices(getCollectionIndices())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .clear()
                .setDocs(true)
                .setFieldData(true)
                .setIndexing(true)
                .setMerge(true)
                .setSearch(true)
                .setSegments(true)
                .setStore(true)
                .setRefresh(true)
                .setQueryCache(true)
                .setRequestCache(true)
                .setBulk(true)
                .get(getCollectionTimeout());

        final long timestamp = timestamp();
        final String clusterUuid = clusterUuid(clusterState);
        final Metadata metadata = clusterState.metadata();
        final RoutingTable routingTable = clusterState.routingTable();

        // Filters the indices stats to only return the statistics for the indices known by the collector's
        // local cluster state. This way indices/index/shards stats all share a common view of indices state.
        final List<IndexStats> indicesStats = new ArrayList<>();
        for (final String indexName : metadata.getConcreteAllIndices()) {
            final IndexStats indexStats = indicesStatsResponse.getIndex(indexName);
            if (indexStats != null) {
                // The index appears both in the local cluster state and indices stats response
                indicesStats.add(indexStats);

                results.add(new IndexStatsMonitoringDoc(clusterUuid, timestamp, interval, node, indexStats,
                        metadata.index(indexName), routingTable.index(indexName)));
            }
        }
        results.add(new IndicesStatsMonitoringDoc(clusterUuid, timestamp, interval, node, indicesStats));

        return Collections.unmodifiableCollection(results);
    }
}
