/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Objects;

public class CopyLifecycleIndexMetadataTransportAction extends TransportMasterNodeAction<
    CopyLifecycleIndexMetadataAction.Request,
    AcknowledgedResponse> {
    private final ClusterStateTaskExecutor<UpdateIndexMetadataTask> executor;
    private final MasterServiceTaskQueue<UpdateIndexMetadataTask> taskQueue;
    private final ProjectResolver projectResolver;

    @Inject
    public CopyLifecycleIndexMetadataTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            CopyLifecycleIndexMetadataAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CopyLifecycleIndexMetadataAction.Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.executor = new SimpleBatchedAckListenerTaskExecutor<>() {
            @Override
            public Tuple<ClusterState, ClusterStateAckListener> executeTask(UpdateIndexMetadataTask task, ClusterState state) {
                var projectMetadata = state.metadata().getProject(task.projectId);
                var updatedMetadata = applyUpdate(projectMetadata, task);
                return new Tuple<>(ClusterState.builder(state).putProjectMetadata(updatedMetadata).build(), task);
            }
        };
        this.taskQueue = clusterService.createTaskQueue("migrate-copy-index-metadata", Priority.NORMAL, this.executor);
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        CopyLifecycleIndexMetadataAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask(
            "migrate-copy-index-metadata",
            new UpdateIndexMetadataTask(
                projectResolver.getProjectId(),
                request.sourceIndex(),
                request.destIndex(),
                request.ackTimeout(),
                listener
            ),
            request.masterNodeTimeout()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(CopyLifecycleIndexMetadataAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private static ProjectMetadata applyUpdate(ProjectMetadata projectMetadata, UpdateIndexMetadataTask updateTask) {
        assert projectMetadata != null && updateTask != null;
        assert Objects.equals(updateTask.projectId, projectMetadata.id());

        IndexMetadata sourceMetadata = projectMetadata.index(updateTask.sourceIndex);
        if (sourceMetadata == null) {
            throw new IndexNotFoundException(updateTask.sourceIndex);
        }
        IndexMetadata destMetadata = projectMetadata.index(updateTask.destIndex);
        if (destMetadata == null) {
            throw new IndexNotFoundException(updateTask.destIndex);
        }

        IndexMetadata.Builder newDestMetadata = IndexMetadata.builder(destMetadata);

        var sourceILM = sourceMetadata.getCustomData(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY);
        if (sourceILM != null) {
            newDestMetadata.putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, sourceILM);
        }

        newDestMetadata.putRolloverInfos(sourceMetadata.getRolloverInfos())
            // creation date is required for ILM to function
            .creationDate(sourceMetadata.getCreationDate())
            // creation date updates settings so must increment settings version
            .settingsVersion(destMetadata.getSettingsVersion() + 1);

        var indices = new HashMap<>(projectMetadata.indices());
        indices.put(updateTask.destIndex, newDestMetadata.build());

        return ProjectMetadata.builder(projectMetadata).indices(indices).build();
    }

    static class UpdateIndexMetadataTask extends AckedBatchedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final String sourceIndex;
        private final String destIndex;

        UpdateIndexMetadataTask(
            ProjectId projectId,
            String sourceIndex,
            String destIndex,
            TimeValue ackTimeout,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(ackTimeout, listener);
            this.projectId = projectId;
            this.sourceIndex = sourceIndex;
            this.destIndex = destIndex;
        }
    }
}
