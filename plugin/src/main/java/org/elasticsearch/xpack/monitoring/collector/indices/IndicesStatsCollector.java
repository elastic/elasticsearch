/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.security.InternalClient;

import java.util.Collection;
import java.util.Collections;

/**
 * Collector for indices statistics.
 * <p>
 * This collector runs on the master node only and collect one {@link IndicesStatsMonitoringDoc} document.
 */
public class IndicesStatsCollector extends Collector {

    public static final String NAME = "indices-stats-collector";

    private final Client client;

    public IndicesStatsCollector(Settings settings, ClusterService clusterService,
                                 MonitoringSettings monitoringSettings, XPackLicenseState licenseState, InternalClient client) {
        super(settings, NAME, clusterService, monitoringSettings, licenseState);
        this.client = client;
    }

    @Override
    protected boolean shouldCollect() {
        return super.shouldCollect() && isLocalNodeMaster();
    }

    @Override
    protected Collection<MonitoringDoc> doCollect() throws Exception {
        IndicesStatsResponse indicesStats = client.admin().indices().prepareStats()
                .setIndices(monitoringSettings.indices())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .clear()
                .setDocs(true)
                .setIndexing(true)
                .setSearch(true)
                .setStore(true)
                .get(monitoringSettings.indicesStatsTimeout());

        IndicesStatsMonitoringDoc indicesStatsDoc = new IndicesStatsMonitoringDoc(monitoringId(), monitoringVersion());
        indicesStatsDoc.setClusterUUID(clusterUUID());
        indicesStatsDoc.setTimestamp(System.currentTimeMillis());
        indicesStatsDoc.setSourceNode(localNode());
        indicesStatsDoc.setIndicesStats(indicesStats);
        return Collections.singletonList(indicesStatsDoc);
    }
}
