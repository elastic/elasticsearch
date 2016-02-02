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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
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
 * Collector for cluster state.
 * <p>
 * This collector runs on the master node only and collects {@link ClusterStateMarvelDoc} document
 * at a given frequency.
 */
public class ClusterStateCollector extends AbstractCollector<ClusterStateCollector> {

    public static final String NAME = "cluster-state-collector";
    public static final String TYPE = "cluster_state";
    public static final String NODES_TYPE = "nodes";
    public static final String NODE_TYPE = "node";

    private final Client client;

    @Inject
    public ClusterStateCollector(Settings settings, ClusterService clusterService, MarvelSettings marvelSettings,
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
        List<MarvelDoc> results = new ArrayList<>(3);

        ClusterState clusterState = clusterService.state();
        String clusterUUID = clusterState.metaData().clusterUUID();
        String stateUUID = clusterState.stateUUID();
        long timestamp = System.currentTimeMillis();

        // Adds a cluster_state document with associated status
        ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().get(marvelSettings.clusterStateTimeout());
        results.add(new ClusterStateMarvelDoc(clusterUUID, TYPE, timestamp, clusterState, clusterHealth.getStatus()));

        DiscoveryNodes nodes = clusterState.nodes();
        if (nodes != null) {
            for (DiscoveryNode node : nodes) {
                // Adds a document for every node in the marvel timestamped index (type "nodes")
                results.add(new ClusterStateNodeMarvelDoc(clusterUUID, NODES_TYPE, timestamp, stateUUID, node.getId()));

                // Adds a document for every node in the marvel data index (type "node")
                results.add(new DiscoveryNodeMarvelDoc(dataIndexNameResolver.resolve(timestamp), NODE_TYPE, node.getId(), clusterUUID, timestamp, node));
            }
        }

        return Collections.unmodifiableCollection(results);
    }
}
