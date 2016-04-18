/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.agent.collector.AbstractCollector;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.MonitoringLicensee;
import org.elasticsearch.shield.InternalClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for cluster state.
 * <p>
 * This collector runs on the master node only and collects {@link ClusterStateMonitoringDoc} document
 * at a given frequency.
 */
public class ClusterStateCollector extends AbstractCollector<ClusterStateCollector> {

    public static final String NAME = "cluster-state-collector";

    private final Client client;

    @Inject
    public ClusterStateCollector(Settings settings, ClusterService clusterService,
                                 MonitoringSettings monitoringSettings, MonitoringLicensee licensee, InternalClient client) {
        super(settings, NAME, clusterService, monitoringSettings, licensee);
        this.client = client;
    }

    @Override
    protected boolean shouldCollect() {
        return super.shouldCollect() && isLocalNodeMaster();
    }

    @Override
    protected Collection<MonitoringDoc> doCollect() throws Exception {
        List<MonitoringDoc> results = new ArrayList<>(3);

        ClusterState clusterState = clusterService.state();
        String clusterUUID = clusterState.metaData().clusterUUID();
        String stateUUID = clusterState.stateUUID();
        long timestamp = System.currentTimeMillis();
        DiscoveryNode sourceNode = localNode();

        ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().get(monitoringSettings.clusterStateTimeout());

        // Adds a cluster_state document with associated status
        ClusterStateMonitoringDoc clusterStateDoc = new ClusterStateMonitoringDoc(monitoringId(), monitoringVersion());
        clusterStateDoc.setClusterUUID(clusterUUID);
        clusterStateDoc.setTimestamp(timestamp);
        clusterStateDoc.setSourceNode(sourceNode);
        clusterStateDoc.setClusterState(clusterState);
        clusterStateDoc.setStatus(clusterHealth.getStatus());
        results.add(clusterStateDoc);

        DiscoveryNodes nodes = clusterState.nodes();
        if (nodes != null) {
            for (DiscoveryNode node : nodes) {
                // Adds a document for every node in the marvel timestamped index (type "nodes")
                ClusterStateNodeMonitoringDoc clusterStateNodeDoc = new ClusterStateNodeMonitoringDoc(monitoringId(), monitoringVersion());
                clusterStateNodeDoc.setClusterUUID(clusterUUID);;
                clusterStateNodeDoc.setTimestamp(timestamp);
                clusterStateNodeDoc.setSourceNode(sourceNode);
                clusterStateNodeDoc.setStateUUID(stateUUID);
                clusterStateNodeDoc.setNodeId(node.getId());
                results.add(clusterStateNodeDoc);

                // Adds a document for every node in the marvel data index (type "node")
                DiscoveryNodeMonitoringDoc discoveryNodeDoc = new DiscoveryNodeMonitoringDoc(monitoringId(), monitoringVersion());
                discoveryNodeDoc.setClusterUUID(clusterUUID);
                discoveryNodeDoc.setTimestamp(timestamp);
                discoveryNodeDoc.setSourceNode(node);
                discoveryNodeDoc.setNode(node);
                results.add(discoveryNodeDoc);
            }
        }

        return Collections.unmodifiableCollection(results);
    }
}
