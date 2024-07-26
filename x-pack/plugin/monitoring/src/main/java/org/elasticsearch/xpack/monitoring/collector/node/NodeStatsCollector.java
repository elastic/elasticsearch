/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.node;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import static org.elasticsearch.xpack.monitoring.collector.TimeoutUtils.ensureNoTimeouts;

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

    private static final CommonStatsFlags FLAGS = new CommonStatsFlags(
        CommonStatsFlags.Flag.Docs,
        CommonStatsFlags.Flag.FieldData,
        CommonStatsFlags.Flag.Store,
        CommonStatsFlags.Flag.Indexing,
        CommonStatsFlags.Flag.QueryCache,
        CommonStatsFlags.Flag.RequestCache,
        CommonStatsFlags.Flag.Search,
        CommonStatsFlags.Flag.Segments,
        CommonStatsFlags.Flag.Bulk
    );

    private final Client client;

    public NodeStatsCollector(final ClusterService clusterService, final XPackLicenseState licenseState, final Client client) {
        super(NodeStatsMonitoringDoc.TYPE, clusterService, NODE_STATS_TIMEOUT, licenseState);
        this.client = Objects.requireNonNull(client);
    }

    // For testing purpose
    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        return super.shouldCollect(isElectedMaster);
    }

    @Override
    protected Collection<MonitoringDoc> doCollect(final MonitoringDoc.Node node, final long interval, final ClusterState clusterState) {
        NodesStatsRequest request = new NodesStatsRequest("_local");
        request.setIncludeShardsStats(false);
        request.indices(FLAGS);
        request.addMetrics(
            NodesStatsRequestParameters.Metric.OS,
            NodesStatsRequestParameters.Metric.JVM,
            NodesStatsRequestParameters.Metric.PROCESS,
            NodesStatsRequestParameters.Metric.THREAD_POOL,
            NodesStatsRequestParameters.Metric.FS
        );
        request.timeout(getCollectionTimeout());

        final NodesStatsResponse response = client.admin().cluster().nodesStats(request).actionGet();
        ensureNoTimeouts(getCollectionTimeout(), response);

        // if there's a failure, then we failed to work with the
        // _local node (guaranteed a single exception)
        if (response.hasFailures()) {
            throw response.failures().get(0);
        }

        final String clusterUuid = clusterUuid(clusterState);
        final NodeStats nodeStats = response.getNodes().get(0);

        return Collections.singletonList(
            new NodeStatsMonitoringDoc(
                clusterUuid,
                nodeStats.getTimestamp(),
                interval,
                node,
                node.getUUID(),
                clusterState.getNodes().isLocalNodeElectedMaster(),
                nodeStats,
                BootstrapInfo.isMemoryLocked()
            )
        );
    }

}
