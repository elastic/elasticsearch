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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

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

    public IndexStatsCollector(final Settings settings,
                               final ClusterService clusterService,
                               final XPackLicenseState licenseState,
                               final Client client) {
        super(settings, "index-stats", clusterService, INDEX_STATS_TIMEOUT, licenseState);
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
        final IndicesStatsResponse indicesStats = client.admin().indices().prepareStats()
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
                .get(getCollectionTimeout());

        final long timestamp = timestamp();
        final String clusterUuid = clusterUuid(clusterState);

        // add the indices stats that we use to collect the index stats
        results.add(new IndicesStatsMonitoringDoc(clusterUuid, timestamp, interval, node, indicesStats));

        // collect each index stats document
        for (final IndexStats indexStats : indicesStats.getIndices().values()) {
            final String index = indexStats.getIndex();
            final IndexMetaData metaData = clusterState.metaData().index(index);
            final IndexRoutingTable routingTable = clusterState.routingTable().index(index);

            results.add(new IndexStatsMonitoringDoc(clusterUuid, timestamp, interval, node, indexStats, metaData, routingTable));
        }

        return Collections.unmodifiableCollection(results);
    }
}
