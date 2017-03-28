/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.security.InternalClient;

import java.util.Collection;
import java.util.Collections;

/**
 * Collector for nodes statistics.
 * <p>
 * This collector runs on every non-client node and collect
 * a {@link NodeStatsMonitoringDoc} document for each node of the cluster.
 */
public class NodeStatsCollector extends Collector {

    private final Client client;

    public NodeStatsCollector(Settings settings, ClusterService clusterService,
                              MonitoringSettings monitoringSettings,
                              XPackLicenseState licenseState, InternalClient client) {
        super(settings, "node-stats", clusterService, monitoringSettings, licenseState);
        this.client = client;
    }

    @Override
    protected Collection<MonitoringDoc> doCollect() throws Exception {
        NodesStatsRequest request = new NodesStatsRequest("_local");
        request.indices(CommonStatsFlags.ALL);
        request.os(true);
        request.jvm(true);
        request.process(true);
        request.threadPool(true);
        request.fs(true);

        NodesStatsResponse response = client.admin().cluster().nodesStats(request).actionGet();

        // if there's a failure, then we failed to work with the
        // _local node (guaranteed a single exception)
        if (response.hasFailures()) {
            throw response.failures().get(0);
        }

        NodeStats nodeStats = response.getNodes().get(0);
        DiscoveryNode sourceNode = localNode();

        NodeStatsMonitoringDoc nodeStatsDoc = new NodeStatsMonitoringDoc(monitoringId(),
                monitoringVersion(), clusterUUID(), sourceNode, isLocalNodeMaster(),
                nodeStats, BootstrapInfo.isMemoryLocked());

        return Collections.singletonList(nodeStatsDoc);
    }
}
