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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

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

    /**
     * Timeout value when collecting the nodes statistics (default to 10s)
     */
    public static final Setting<TimeValue> NODE_STATS_TIMEOUT = collectionTimeoutSetting("node.stats.timeout");

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

    public NodeStatsCollector(final ClusterService clusterService,
                              final XPackLicenseState licenseState,
                              final Client client) {
        super(NodeStatsMonitoringDoc.TYPE, clusterService, NODE_STATS_TIMEOUT, licenseState);
        this.client = Objects.requireNonNull(client);
    }

    // For testing purpose
    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        return super.shouldCollect(isElectedMaster);
    }

    @Override
    protected Collection<MonitoringDoc> doCollect(final MonitoringDoc.Node node,
                                                  final long interval,
                                                  final ClusterState clusterState) throws Exception {
        NodesStatsRequest request = new NodesStatsRequest("_local");
        request.indices(FLAGS);
        request.os(true);
        request.jvm(true);
        request.process(true);
        request.threadPool(true);
        request.fs(true);

        final NodesStatsResponse response = client.admin().cluster().nodesStats(request).actionGet(getCollectionTimeout());

        // if there's a failure, then we failed to work with the
        // _local node (guaranteed a single exception)
        if (response.hasFailures()) {
            throw response.failures().get(0);
        }

        final String clusterUuid = clusterUuid(clusterState);
        final NodeStats nodeStats = response.getNodes().get(0);

        return Collections.singletonList(new NodeStatsMonitoringDoc(clusterUuid, nodeStats.getTimestamp(), interval, node,
                node.getUUID(), clusterState.getNodes().isLocalNodeElectedMaster(), nodeStats, BootstrapInfo.isMemoryLocked()));
    }

}
