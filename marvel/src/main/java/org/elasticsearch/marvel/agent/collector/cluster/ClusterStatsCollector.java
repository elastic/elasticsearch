/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.marvel.shield.SecuredClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for cluster stats.
 * <p>
 * This collector runs on the master node only and collects the {@link ClusterStatsMarvelDoc} document
 * at a given frequency.
 */
public class ClusterStatsCollector extends AbstractCollector<ClusterStatsCollector> {

    public static final String NAME = "cluster-stats-collector";
    public static final String TYPE = "cluster_stats";

    private final Client client;

    @Inject
    public ClusterStatsCollector(Settings settings, ClusterService clusterService, MarvelSettings marvelSettings,  MarvelLicensee marvelLicensee,
                                 SecuredClient client) {
        super(settings, NAME, clusterService, marvelSettings, marvelLicensee);
        this.client = client;
    }

    @Override
    protected boolean shouldCollect() {
        return super.shouldCollect() && isLocalNodeMaster();
    }

    @Override
    protected Collection<MarvelDoc> doCollect() throws Exception {
        List<MarvelDoc> results = new ArrayList<>(1);

        ClusterStatsResponse clusterStatsResponse = client.admin().cluster().prepareClusterStats().get(marvelSettings.clusterStatsTimeout());
        results.add(new ClusterStatsMarvelDoc(clusterUUID(), TYPE, System.currentTimeMillis(), clusterStatsResponse));
        return Collections.unmodifiableCollection(results);
    }
}
