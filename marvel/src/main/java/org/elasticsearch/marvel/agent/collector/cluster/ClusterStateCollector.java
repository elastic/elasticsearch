/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.LicenseService;
import org.elasticsearch.marvel.shield.SecuredClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for cluster state.
 * <p>
 * This collector runs on the master node only and collects the {@link ClusterStateMarvelDoc} document
 * at a given frequency.
 */
public class ClusterStateCollector extends AbstractCollector<ClusterStateCollector> {

    public static final String NAME = "cluster-state-collector";
    public static final String TYPE = "cluster_state";

    private final Client client;

    @Inject
    public ClusterStateCollector(Settings settings, ClusterService clusterService, MarvelSettings marvelSettings,  LicenseService licenseService,
                                 SecuredClient client) {
        super(settings, NAME, clusterService, marvelSettings, licenseService);
        this.client = client;
    }

    @Override
    protected boolean shouldCollect() {
        return super.shouldCollect() && isLocalNodeMaster();
    }

    @Override
    protected Collection<MarvelDoc> doCollect() throws Exception {
        List<MarvelDoc> results = new ArrayList<>(1);

        ClusterState clusterState = clusterService.state();
        ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().get(marvelSettings.clusterStateTimeout());

        results.add(new ClusterStateMarvelDoc(clusterUUID(), TYPE, System.currentTimeMillis(), clusterState, clusterHealth.getStatus()));
        return Collections.unmodifiableCollection(results);
    }
}
