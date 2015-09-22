/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.LicenseService;
import org.elasticsearch.marvel.shield.SecuredClient;

import java.util.Collection;
import java.util.Collections;

/**
 * Collector for indices statistics.
 * <p>
 * This collector runs on the master node only and collect one {@link IndicesStatsMarvelDoc} document.
 */
public class IndicesStatsCollector extends AbstractCollector<IndicesStatsCollector> {

    public static final String NAME = "indices-stats-collector";
    public static final String TYPE = "indices_stats";

    private final Client client;

    @Inject
    public IndicesStatsCollector(Settings settings, ClusterService clusterService, MarvelSettings marvelSettings, LicenseService licenseService,
                                 SecuredClient client) {
        super(settings, NAME, clusterService, marvelSettings, licenseService);
        this.client = client;
    }

    @Override
    protected boolean canCollect() {
        return super.canCollect() && isLocalNodeMaster();
    }

    @Override
    protected Collection<MarvelDoc> doCollect() throws Exception {
        IndicesStatsResponse indicesStats = client.admin().indices().prepareStats()
                .setRefresh(true)
                .get(marvelSettings.indicesStatsTimeout());

        MarvelDoc result = new IndicesStatsMarvelDoc(clusterUUID(), TYPE, System.currentTimeMillis(), indicesStats);
        return Collections.singletonList(result);
    }
}
