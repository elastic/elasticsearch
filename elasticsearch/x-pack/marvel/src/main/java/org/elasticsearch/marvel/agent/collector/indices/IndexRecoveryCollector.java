/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
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
import org.elasticsearch.shield.InternalClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for the Recovery API.
 * <p>
 * This collector runs on the master node only and collects a {@link IndexRecoveryMarvelDoc} document
 * for every index that has on-going shard recoveries.
 */
public class IndexRecoveryCollector extends AbstractCollector<IndexRecoveryCollector> {

    public static final String NAME = "index-recovery-collector";
    public static final String TYPE = "index_recovery";

    private final Client client;

    @Inject
    public IndexRecoveryCollector(Settings settings, ClusterService clusterService, MarvelSettings marvelSettings,
                                  MarvelLicensee marvelLicensee, InternalClient client) {
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
            RecoveryResponse recoveryResponse = client.admin().indices().prepareRecoveries()
                    .setIndices(marvelSettings.indices())
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .setActiveOnly(marvelSettings.recoveryActiveOnly())
                    .get(marvelSettings.recoveryTimeout());

            if (recoveryResponse.hasRecoveries()) {
                results.add(new IndexRecoveryMarvelDoc(clusterUUID(), TYPE, System.currentTimeMillis(), recoveryResponse));
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
