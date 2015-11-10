/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.marvel.shield.MarvelShieldIntegration;
import org.elasticsearch.marvel.shield.SecuredClient;

import java.util.*;

/**
 * Collector for indices statistics.
 * <p>
 * This collector runs on the master node only and collect a {@link IndexStatsMarvelDoc} document
 * for each existing index in the cluster.
 */
public class IndexStatsCollector extends AbstractCollector<IndexStatsCollector> {

    public static final String NAME = "index-stats-collector";
    public static final String TYPE = "index_stats";

    private final Client client;

    @Inject
    public IndexStatsCollector(Settings settings, ClusterService clusterService, MarvelSettings marvelSettings, MarvelLicensee marvelLicensee,
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
        try {
            IndicesStatsResponse indicesStats = client.admin().indices().prepareStats()
                    .setRefresh(true)
                    .setIndices(marvelSettings.indices())
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .get(marvelSettings.indexStatsTimeout());

            long timestamp = System.currentTimeMillis();
            String clusterUUID = clusterUUID();
            for (IndexStats indexStats : indicesStats.getIndices().values()) {
                results.add(new IndexStatsMarvelDoc(clusterUUID, TYPE, timestamp, indexStats));
            }
        } catch (IndexNotFoundException e) {
            if (MarvelShieldIntegration.enabled(settings) && IndexNameExpressionResolver.isAllIndices(Arrays.asList(marvelSettings.indices()))) {
                logger.debug("collector [{}] - unable to collect data for missing index [{}]", name(), e.getIndex());
            } else {
                throw e;
            }
        }
        return Collections.unmodifiableCollection(results);
    }
}
