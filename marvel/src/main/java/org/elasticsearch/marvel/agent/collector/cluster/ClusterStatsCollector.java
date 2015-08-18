/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;

import java.util.Collection;

/**
 * Collector for cluster stats.
 * <p/>
 * This collector runs on the master node only and collects the {@link ClusterStatsMarvelDoc} document
 * at a given frequency.
 */
public class ClusterStatsCollector extends AbstractCollector<ClusterStatsCollector> {

    public static final String NAME = "cluster-stats-collector";
    public static final String TYPE = "marvel_cluster_stats";

    private final Client client;

    @Inject
    public ClusterStatsCollector(Settings settings, ClusterService clusterService,
                                 ClusterName clusterName, MarvelSettings marvelSettings, Client client) {
        super(settings, NAME, clusterService, clusterName, marvelSettings);
        this.client = client;
    }

    @Override
    protected boolean masterOnly() {
        return true;
    }

    @Override
    protected Collection<MarvelDoc> doCollect() throws Exception {
        ImmutableList.Builder<MarvelDoc> results = ImmutableList.builder();

        ClusterStatsResponse clusterStatsResponse = client.admin().cluster().prepareClusterStats().get(marvelSettings.clusterStatsTimeout());
        results.add(buildMarvelDoc(clusterName.value(), TYPE, System.currentTimeMillis(), clusterStatsResponse));
        return results.build();
    }

    protected MarvelDoc buildMarvelDoc(String clusterName, String type, long timestamp, ClusterStatsResponse clusterStatsResponse) {
        return ClusterStatsMarvelDoc.createMarvelDoc(clusterName, type, timestamp, clusterStatsResponse);
    }
}
