/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.MarvelSettings;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.shield.Shield;

import java.util.Arrays;
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
    public IndicesStatsCollector(Settings settings, ClusterService clusterService, DiscoveryService discoveryService,
                                 MarvelSettings  marvelSettings, MarvelLicensee marvelLicensee, InternalClient client) {
        super(settings, NAME, clusterService, discoveryService, marvelSettings, marvelLicensee);
        this.client = client;
    }

    @Override
    protected boolean shouldCollect() {
        return super.shouldCollect() && isLocalNodeMaster();
    }

    @Override
    protected Collection<MarvelDoc> doCollect() throws Exception {
        try {
            IndicesStatsResponse indicesStats = client.admin().indices().prepareStats()
                    .setIndices(marvelSettings.indices())
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .clear()
                    .setDocs(true)
                    .setIndexing(true)
                    .setSearch(true)
                    .setStore(true)
                    .get(marvelSettings.indicesStatsTimeout());

            IndicesStatsMarvelDoc indicesStatsDoc = new IndicesStatsMarvelDoc();
            indicesStatsDoc.setClusterUUID(clusterUUID());
            indicesStatsDoc.setType(TYPE);
            indicesStatsDoc.setTimestamp(System.currentTimeMillis());
            indicesStatsDoc.setSourceNode(localNode());
            indicesStatsDoc.setIndicesStats(indicesStats);

            return Collections.singletonList(indicesStatsDoc);
        } catch (IndexNotFoundException e) {
            if (Shield.enabled(settings) && IndexNameExpressionResolver.isAllIndices(Arrays.asList(marvelSettings.indices()))) {
                logger.debug("collector [{}] - unable to collect data for missing index [{}]", name(), e.getIndex());
                return Collections.emptyList();
            }
            throw e;
        }
    }
}
