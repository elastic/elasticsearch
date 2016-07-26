/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.agent.collector.node;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.license.plugin.core.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.agent.collector.AbstractCollector;
import org.elasticsearch.xpack.monitoring.agent.exporter.MonitoringDoc;
import org.elasticsearch.xpack.security.InternalClient;

/**
 * Collector for nodes statistics.
 * <p>
 * This collector runs on every non-client node and collect
 * a {@link NodeStatsMonitoringDoc} document for each node of the cluster.
 */
public class NodeStatsCollector extends AbstractCollector {

    public static final String NAME = "node-stats-collector";

    private final Client client;

    public NodeStatsCollector(Settings settings, ClusterService clusterService, MonitoringSettings monitoringSettings,
                              XPackLicenseState licenseState, InternalClient client) {
        super(settings, NAME, clusterService, monitoringSettings, licenseState);
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

        // if there's a failure, then we failed to work with the _local node (guaranteed a single exception)
        if (response.hasFailures()) {
            throw response.failures().get(0);
        }

        NodeStats nodeStats = response.getNodes().get(0);
        DiscoveryNode sourceNode = localNode();

        NodeStatsMonitoringDoc nodeStatsDoc = new NodeStatsMonitoringDoc(monitoringId(), monitoringVersion());
        nodeStatsDoc.setClusterUUID(clusterUUID());
        nodeStatsDoc.setTimestamp(System.currentTimeMillis());
        nodeStatsDoc.setSourceNode(sourceNode);
        nodeStatsDoc.setNodeId(sourceNode.getId());
        nodeStatsDoc.setNodeMaster(isLocalNodeMaster());
        nodeStatsDoc.setNodeStats(nodeStats);
        nodeStatsDoc.setMlockall(BootstrapInfo.isMemoryLocked());

        return Collections.singletonList(nodeStatsDoc);
    }
}
