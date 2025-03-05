/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.monitor.metrics.IndexModeStatsActionType;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.application.LogsDBFeatureSetUsage;

public class LogsDBUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public LogsDBUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client
    ) {
        super(XPackUsageFeatureAction.LOGSDB.name(), transportService, clusterService, threadPool, actionFilters);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        int numIndices = 0;
        int numIndicesWithSyntheticSources = 0;
        for (IndexMetadata indexMetadata : state.metadata().getProject()) {
            if (indexMetadata.getIndexMode() == IndexMode.LOGSDB) {
                numIndices++;
                if (IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.get(indexMetadata.getSettings()) == SourceFieldMapper.Mode.SYNTHETIC) {
                    numIndicesWithSyntheticSources++;
                }
            }
        }
        final boolean enabled = LogsDBPlugin.CLUSTER_LOGSDB_ENABLED.get(clusterService.getSettings());
        final boolean hasCustomCutoffDate = System.getProperty(LogsdbLicenseService.CUTOFF_DATE_SYS_PROP_NAME) != null;
        final DiscoveryNode[] nodes = state.nodes().getDataNodes().values().toArray(DiscoveryNode[]::new);
        final var statsRequest = new IndexModeStatsActionType.StatsRequest(nodes);
        final int finalNumIndices = numIndices;
        final int finalNumIndicesWithSyntheticSources = numIndicesWithSyntheticSources;
        client.execute(IndexModeStatsActionType.TYPE, statsRequest, listener.map(statsResponse -> {
            final var indexStats = statsResponse.stats().get(IndexMode.LOGSDB);
            return new XPackUsageFeatureResponse(
                new LogsDBFeatureSetUsage(
                    true,
                    enabled,
                    finalNumIndices,
                    finalNumIndicesWithSyntheticSources,
                    indexStats.numDocs(),
                    indexStats.numBytes(),
                    hasCustomCutoffDate
                )
            );
        }));
    }
}
