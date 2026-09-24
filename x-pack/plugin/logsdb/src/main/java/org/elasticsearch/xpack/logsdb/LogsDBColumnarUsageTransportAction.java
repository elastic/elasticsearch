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
import org.elasticsearch.common.settings.ClusterSettings;
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
        final IndexModeStats counts = computeIndexModeStats(
            projectMetadata,
            clusterService.getClusterSettings(),
            IndexMode.LOGSDB_COLUMNAR
        );

        final DiscoveryNode[] nodes = state.nodes().getDataNodes().values().toArray(DiscoveryNode[]::new);
        final var statsRequest = new IndexModeStatsActionType.StatsRequest(nodes);
        client.execute(IndexModeStatsActionType.TYPE, statsRequest, listener.map(statsResponse -> {
            final var indexStats = statsResponse.stats().get(counts.indexMode);
            return new XPackUsageFeatureResponse(
                new LogsDBColumnarFeatureSetUsage(
                    true,
                    counts.enabled,
                    counts.numIndices,
                    counts.numIndicesWithSyntheticSources,
                    indexStats.numDocs(),
                    indexStats.numBytes(),
                    counts.dataStreamsCount,
                    counts.dataStreamsManagedByIlm,
                    counts.dataStreamsManagedByDlm
                )
            );
        }));
    }

    static IndexModeStats computeIndexModeStats(ProjectMetadata projectMetadata, ClusterSettings clusterSettings, IndexMode indexMode) {
        // cluster.columnar.enabled is a cluster setting that controls whether all columnar index modes are enabled. If this is disabled,
        // then creating any new indices with columnar index modes will fail.
        // The cluster.logsdb_columnar.enabled is a setting that controls whether data steams with logs-*-* use logsdb_columnar index mode,
        // but only for snapshot builds.
        final boolean enabled = clusterSettings.get(LogsDBPlugin.CLUSTER_COLUMNAR_ENABLED);
        int numIndices = 0;
        int numIndicesWithSyntheticSources = 0;
        for (IndexMetadata indexMetadata : projectMetadata) {
            if (indexMetadata.getIndexMode() == indexMode) {
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
            if (writeIndexMetadata.getIndexMode() != indexMode) {
                continue;
            }
            dataStreamsCount++;
            if (projectMetadata.isIndexManagedByILM(writeIndexMetadata)) {
                dataStreamsManagedByIlm++;
            } else if (dataStream.isIndexManagedByDataStreamLifecycle(writeIndex, projectMetadata::index)) {
                dataStreamsManagedByDlm++;
            }
        }
        return new IndexModeStats(
            indexMode,
            enabled,
            numIndices,
            numIndicesWithSyntheticSources,
            dataStreamsCount,
            dataStreamsManagedByIlm,
            dataStreamsManagedByDlm
        );
    }

    record IndexModeStats(
        IndexMode indexMode,
        boolean enabled,
        int numIndices,
        int numIndicesWithSyntheticSources,
        int dataStreamsCount,
        int dataStreamsManagedByIlm,
        int dataStreamsManagedByDlm
    ) {}
}
