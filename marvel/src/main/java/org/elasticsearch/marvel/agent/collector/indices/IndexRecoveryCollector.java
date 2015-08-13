/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettingsService;

import java.util.Collection;

/**
 * Collector for the Recovery API.
 * <p/>
 * This collector runs on the master node only and collects a {@link IndexRecoveryMarvelDoc} document
 * for every index that has on-going shard recoveries.
 */
public class IndexRecoveryCollector extends AbstractCollector<IndexRecoveryCollector> {

    public static final String NAME = "index-recovery-collector";
    public static final String TYPE = "marvel_index_recovery";

    private final Client client;

    @Inject
    public IndexRecoveryCollector(Settings settings, ClusterService clusterService,
                                  ClusterName clusterName, MarvelSettingsService marvelSettings, Client client) {
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

        RecoveryResponse recoveryResponse = client.admin().indices().prepareRecoveries()
                .setIndices(marvelSettings.indices())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setActiveOnly(marvelSettings.recoveryActiveOnly())
                .get(marvelSettings.recoveryTimeout());

        if (recoveryResponse.hasRecoveries()) {
            results.add(buildMarvelDoc(clusterName.value(), TYPE, System.currentTimeMillis(), recoveryResponse));
        }
        return results.build();
    }

    protected MarvelDoc buildMarvelDoc(String clusterName, String type, long timestamp, RecoveryResponse recoveryResponse) {
        return IndexRecoveryMarvelDoc.createMarvelDoc(clusterName, type, timestamp, recoveryResponse);
    }
}
