/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.datastreams.lifecycle.ExplainDataStreamLifecycleAction;
import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexDataStreamLifecycle;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadProjectAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * Transport action handling the explain the data stream lifecycle requests for one or more data stream lifecycle managed indices.
 */
public class TransportExplainDataStreamLifecycleAction extends TransportMasterNodeReadProjectAction<
    ExplainDataStreamLifecycleAction.Request,
    ExplainDataStreamLifecycleAction.Response> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final DataStreamLifecycleErrorStore errorStore;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings;

    @Inject
    public TransportExplainDataStreamLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DataStreamLifecycleErrorStore dataLifecycleServiceErrorStore,
        DataStreamGlobalRetentionSettings globalRetentionSettings
    ) {
        super(
            ExplainDataStreamLifecycleAction.INSTANCE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ExplainDataStreamLifecycleAction.Request::new,
            projectResolver,
            ExplainDataStreamLifecycleAction.Response::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.errorStore = dataLifecycleServiceErrorStore;
        this.globalRetentionSettings = globalRetentionSettings;
    }

    @Override
    protected void masterOperation(
        Task task,
        ExplainDataStreamLifecycleAction.Request request,
        ProjectState state,
        ActionListener<ExplainDataStreamLifecycleAction.Response> listener
    ) throws Exception {

        ProjectMetadata metadata = state.metadata();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(metadata, request);
        List<ExplainIndexDataStreamLifecycle> explainIndices = new ArrayList<>(concreteIndices.length);
        for (String index : concreteIndices) {
            IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(index);
            if (indexAbstraction == null) {
                continue;
            }
            IndexMetadata idxMetadata = metadata.index(index);
            if (idxMetadata == null) {
                continue;
            }
            DataStream parentDataStream = indexAbstraction.getParentDataStream();
            if (parentDataStream == null
                || parentDataStream.isIndexManagedByDataStreamLifecycle(idxMetadata.getIndex(), metadata::index) == false) {
                explainIndices.add(new ExplainIndexDataStreamLifecycle(index, false, false, null, null, null, null, null));
                continue;
            }

            RolloverInfo rolloverInfo = idxMetadata.getRolloverInfos().get(parentDataStream.getName());
            TimeValue generationDate = parentDataStream.getGenerationLifecycleDate(idxMetadata);
            ExplainIndexDataStreamLifecycle explainIndexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                index,
                true,
                parentDataStream.isInternal(),
                idxMetadata.getCreationDate(),
                rolloverInfo == null ? null : rolloverInfo.getTime(),
                generationDate,
                parentDataStream.getDataLifecycleForIndex(idxMetadata.getIndex()),
                errorStore.getError(state.projectId(), index)
            );
            explainIndices.add(explainIndexDataStreamLifecycle);
        }

        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        listener.onResponse(
            new ExplainDataStreamLifecycleAction.Response(
                explainIndices,
                request.includeDefaults() ? clusterSettings.get(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING) : null,
                globalRetentionSettings.get(false),
                globalRetentionSettings.get(true)
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(ExplainDataStreamLifecycleAction.Request request, ProjectState state) {
        return state.blocks()
            .indicesBlockedException(
                state.projectId(),
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state.metadata(), request)
            );
    }
}
