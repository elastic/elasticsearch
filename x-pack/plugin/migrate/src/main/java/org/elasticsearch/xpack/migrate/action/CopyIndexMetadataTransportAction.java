/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDataStreamsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersAction;

import java.util.HashMap;

public class CopyIndexMetadataTransportAction extends TransportMasterNodeAction<CopyIndexMetadataAction.Request, AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(CopyIndexMetadataTransportAction.class);
    private final ClusterStateTaskExecutor<UpdateIndexMetadataTask> executor;
    private final MasterServiceTaskQueue<UpdateIndexMetadataTask> taskQueue;
    private final ClusterService clusterService;

    @Inject
    public CopyIndexMetadataTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            MigrateToDataTiersAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CopyIndexMetadataAction.Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.clusterService = clusterService;
        this.executor = new SimpleBatchedAckListenerTaskExecutor<>() {
            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(UpdateIndexMetadataTask task, ClusterState clusterState) {
                return new Tuple<>(applyUpdate(clusterState, task), task);
            }
        };
        this.taskQueue = clusterService.createTaskQueue("migrate-copy-index-metadata", Priority.NORMAL, this.executor);
    }

    @Override
    protected void masterOperation(
        Task task,
        CopyIndexMetadataAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        taskQueue.submitTask(
            "migrate-copy-index-metadata",
            new UpdateIndexMetadataTask(request.sourceIndex(), request.destIndex(), request.ackTimeout(), listener),
            request.masterNodeTimeout()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(CopyIndexMetadataAction.Request request, ClusterState state) {
        return null;
    }

    static ClusterState applyUpdate(ClusterState state, UpdateIndexMetadataTask updateTask) {

        IndexMetadata sourceMetadata = state.metadata().index(updateTask.sourceIndex);
        if (sourceMetadata == null) {
            throw new IndexNotFoundException(updateTask.sourceIndex);
        }

        IndexMetadata destMetadata = state.metadata().index(updateTask.destIndex);
        if (destMetadata == null) {
            throw new IndexNotFoundException(updateTask.destIndex);
        }

        IndexMetadata newDestMetadata = IndexMetadata.builder(destMetadata)
            .putRolloverInfos(sourceMetadata.getRolloverInfos())
            .putLifecycleState(sourceMetadata.getLifecycleExecutionState())
            .build();

        var indices = new HashMap<>(state.metadata().indices());
        indices.put(updateTask.destIndex, newDestMetadata);

        Metadata newMetadata = Metadata.builder(state.metadata()).indices(indices).build();
        return ClusterState.builder(state).metadata(newMetadata).build();
    }

    static class UpdateIndexMetadataTask extends AckedBatchedClusterStateUpdateTask {
        private final String sourceIndex;
        private final String destIndex;

        UpdateIndexMetadataTask(String sourceIndex, String destIndex, TimeValue ackTimeout, ActionListener<AcknowledgedResponse> listener) {
            super(ackTimeout, listener);
            this.sourceIndex = sourceIndex;
            this.destIndex = destIndex;
        }
    }
}
