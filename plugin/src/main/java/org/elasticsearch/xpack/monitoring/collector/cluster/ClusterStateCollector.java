/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.security.InternalClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Collector for cluster state.
 * <p>
 * This collector runs on the master node only and collects {@link ClusterStateMonitoringDoc}
 * document at a given frequency.
 */
public class ClusterStateCollector extends Collector {

    private final Client client;

    public ClusterStateCollector(Settings settings, ClusterService clusterService,
                                 MonitoringSettings monitoringSettings,
                                 XPackLicenseState licenseState, InternalClient client) {
        super(settings, "cluster-state", clusterService, monitoringSettings, licenseState);
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

        ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth()
                .get(monitoringSettings.clusterStateTimeout());

        // Adds a cluster_state document with associated status
        results.add(new ClusterStateMonitoringDoc(monitoringId(), monitoringVersion(), clusterUUID,
                timestamp, sourceNode, clusterState, clusterHealth.getStatus()));

        DiscoveryNodes nodes = clusterState.nodes();
        if (nodes != null) {
            for (DiscoveryNode node : nodes) {
                // Adds a document for every node in the monitoring timestamped index
                results.add(new ClusterStateNodeMonitoringDoc(monitoringId(), monitoringVersion(),
                        clusterUUID, timestamp, sourceNode, stateUUID, node.getId()));

                // Adds a document for every node in the monitoring data index
                results.add(new DiscoveryNodeMonitoringDoc(monitoringId(), monitoringVersion(),
                        clusterUUID, timestamp, node));
            }
        }

        return Collections.unmodifiableCollection(results);
    }
}
