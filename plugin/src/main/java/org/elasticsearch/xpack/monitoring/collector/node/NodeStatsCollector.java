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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Collector for nodes statistics.
 * <p>
 * This collector runs on every non-client node and collect
 * a {@link NodeStatsMonitoringDoc} document for each node of the cluster.
 */
public class NodeStatsCollector extends Collector {

    private static final CommonStatsFlags FLAGS =
            new CommonStatsFlags(CommonStatsFlags.Flag.Docs,
                                 CommonStatsFlags.Flag.FieldData,
                                 CommonStatsFlags.Flag.Store,
                                 CommonStatsFlags.Flag.Indexing,
                                 CommonStatsFlags.Flag.QueryCache,
                                 CommonStatsFlags.Flag.RequestCache,
                                 CommonStatsFlags.Flag.Search,
                                 CommonStatsFlags.Flag.Segments);

    private final Client client;

    public NodeStatsCollector(final Settings settings,
                              final ClusterService clusterService,
                              final MonitoringSettings monitoringSettings,
                              final XPackLicenseState licenseState,
                              final Client client) {
        super(settings, NodeStatsMonitoringDoc.TYPE, clusterService, monitoringSettings, licenseState);
        this.client = Objects.requireNonNull(client);
    }

    // For testing purpose
    @Override
    protected boolean shouldCollect() {
        return super.shouldCollect();
    }

    @Override
    protected Collection<MonitoringDoc> doCollect(final MonitoringDoc.Node node) throws Exception {
        NodesStatsRequest request = new NodesStatsRequest("_local");
        request.indices(FLAGS);
        request.os(true);
        request.jvm(true);
        request.process(true);
        request.threadPool(true);
        request.fs(true);

        final NodesStatsResponse response = client.admin().cluster().nodesStats(request).actionGet(monitoringSettings.nodeStatsTimeout());

        // if there's a failure, then we failed to work with the
        // _local node (guaranteed a single exception)
        if (response.hasFailures()) {
            throw response.failures().get(0);
        }

        final NodeStats nodeStats = response.getNodes().get(0);

        return Collections.singletonList(new NodeStatsMonitoringDoc(clusterUUID(), nodeStats.getTimestamp(), node,
                node.getUUID(), isLocalNodeMaster(), nodeStats, BootstrapInfo.isMemoryLocked()));
    }

}
