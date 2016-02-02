/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicenseUtils;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.shield.InternalClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for cluster stats.
 * <p>
 * This collector runs on the master node. It collect data about the current
 * license and also retrieves the cluster stats.
 * <p>
 * the license and cluster stats are indexed in the data index in a "cluster_info"
 * document; the cluster stats are also indexed in the timestamped index in a
 * "cluster_stats" document.
 */
public class ClusterStatsCollector extends AbstractCollector<ClusterStatsCollector> {

    public static final String NAME = "cluster-stats-collector";

    public static final String CLUSTER_INFO_TYPE = "cluster_info";
    public static final String CLUSTER_STATS_TYPE = "cluster_stats";

    private final ClusterName clusterName;
    private final LicensesManagerService licensesManagerService;
    private final Client client;

    @Inject
    public ClusterStatsCollector(Settings settings, ClusterService clusterService, MarvelSettings marvelSettings, MarvelLicensee marvelLicensee,
                                 InternalClient client, LicensesManagerService licensesManagerService, ClusterName clusterName) {
        super(settings, NAME, clusterService, marvelSettings, marvelLicensee);
        this.client = client;
        this.clusterName = clusterName;
        this.licensesManagerService = licensesManagerService;
    }

    @Override
    protected boolean shouldCollect() {
        // This collector can always collect data on the master node
        return isLocalNodeMaster();
    }

    @Override
    protected Collection<MarvelDoc> doCollect() throws Exception {
        List<MarvelDoc> results = new ArrayList<>(1);

        long timestamp = System.currentTimeMillis();
        String clusterUUID = clusterUUID();

        // Retrieves cluster stats
        ClusterStatsResponse clusterStats = null;
        try {
            clusterStats = client.admin().cluster().prepareClusterStats().get(marvelSettings.clusterStatsTimeout());
        } catch (ElasticsearchSecurityException e) {
            if (LicenseUtils.isLicenseExpiredException(e)) {
                logger.trace("collector [{}] - unable to collect data because of expired license", e, name());
            } else {
                throw e;
            }
        }

        // Adds a cluster info document
        results.add(new ClusterInfoMarvelDoc(dataIndexNameResolver.resolve(timestamp), CLUSTER_INFO_TYPE, clusterUUID, clusterUUID, timestamp,
                clusterName.value(), Version.CURRENT.toString(), licensesManagerService.getLicense(), clusterStats));

        // Adds a cluster stats document
        if (super.shouldCollect()) {
            results.add(new ClusterStatsMarvelDoc(clusterUUID, CLUSTER_STATS_TYPE, timestamp, clusterStats));
        }
        return Collections.unmodifiableCollection(results);
    }
}
