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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
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
import org.elasticsearch.xpack.core.application.LogsDBColumnarFeatureSetUsage;

public class LogsDBColumnarUsageTransportAction extends XPackUsageFeatureTransportAction {
    private final ClusterService clusterService;
    private final Client client;
    private final ProjectResolver projectResolver;

    @Inject
    public LogsDBColumnarUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        ProjectResolver projectResolver
    ) {
        super(XPackUsageFeatureAction.LOGSDB_COLUMNAR.name(), transportService, clusterService, threadPool, actionFilters);
        this.clusterService = clusterService;
        this.client = client;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        final ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(state);
        int numIndices = 0;
        int numIndicesWithSyntheticSources = 0;
        for (IndexMetadata indexMetadata : projectMetadata) {
            if (indexMetadata.getIndexMode() == IndexMode.LOGSDB_COLUMNAR) {
                numIndices++;
                if (IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.get(indexMetadata.getSettings()) == SourceFieldMapper.Mode.SYNTHETIC) {
                    numIndicesWithSyntheticSources++;
                }
            }
        }
        int dataStreamsCount = 0;
        int dataStreamsManagedByIlm = 0;
        int dataStreamsManagedByDlm = 0;
        for (DataStream dataStream : projectMetadata.dataStreams().values()) {
            Index writeIndex = dataStream.getWriteIndex();
            IndexMetadata writeIndexMetadata = projectMetadata.index(writeIndex);
            if (writeIndexMetadata.getIndexMode() != IndexMode.LOGSDB_COLUMNAR) {
                continue;
            }
            dataStreamsCount++;
            if (projectMetadata.isIndexManagedByILM(writeIndexMetadata)) {
                dataStreamsManagedByIlm++;
            } else if (dataStream.isIndexManagedByDataStreamLifecycle(writeIndex, projectMetadata::index)) {
                dataStreamsManagedByDlm++;
            }
        }
        final boolean enabled = clusterService.getClusterSettings().get(LogsDBPlugin.CLUSTER_LOGSDB_COLUMNAR_ENABLED);
        final DiscoveryNode[] nodes = state.nodes().getDataNodes().values().toArray(DiscoveryNode[]::new);
        final var statsRequest = new IndexModeStatsActionType.StatsRequest(nodes);
        final int finalNumIndices = numIndices;
        final int finalNumIndicesWithSyntheticSources = numIndicesWithSyntheticSources;
        final int finalDataStreamsCount = dataStreamsCount;
        final int finalDataStreamsManagedByIlm = dataStreamsManagedByIlm;
        final int finalDataStreamsManagedByDlm = dataStreamsManagedByDlm;
        client.execute(IndexModeStatsActionType.TYPE, statsRequest, listener.map(statsResponse -> {
            final var indexStats = statsResponse.stats().get(IndexMode.LOGSDB_COLUMNAR);
            return new XPackUsageFeatureResponse(
                new LogsDBColumnarFeatureSetUsage(
                    true,
                    enabled,
                    finalNumIndices,
                    finalNumIndicesWithSyntheticSources,
                    indexStats.numDocs(),
                    indexStats.numBytes(),
                    finalDataStreamsCount,
                    finalDataStreamsManagedByIlm,
                    finalDataStreamsManagedByDlm
                )
            );
        }));
    }
}
