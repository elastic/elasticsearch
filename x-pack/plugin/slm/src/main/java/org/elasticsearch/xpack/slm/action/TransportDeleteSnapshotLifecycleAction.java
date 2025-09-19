/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.action.DeleteSnapshotLifecycleAction;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportDeleteSnapshotLifecycleAction extends AcknowledgedTransportMasterNodeProjectAction<
    DeleteSnapshotLifecycleAction.Request> {

    @Inject
    public TransportDeleteSnapshotLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            DeleteSnapshotLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteSnapshotLifecycleAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteSnapshotLifecycleAction.Request request,
        ProjectState projectState,
        ActionListener<AcknowledgedResponse> listener
    ) {
        submitUnbatchedTask(
            "delete-snapshot-lifecycle-" + request.getLifecycleId(),
            new DeleteSnapshotPolicyTask(projectState.projectId(), request, listener)
        );

    }

    /**
     * Extracted extension of {@link AckedClusterStateUpdateTask} with only the execute method
     * implementation, so that the execute() transformation can be reused for {@link ReservedSnapshotAction}
     */
    public static class DeleteSnapshotPolicyTask extends AckedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final DeleteSnapshotLifecycleAction.Request request;

        DeleteSnapshotPolicyTask(
            ProjectId projectId,
            DeleteSnapshotLifecycleAction.Request request,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(request, listener);
            this.projectId = projectId;
            this.request = request;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            final var project = currentState.metadata().getProject(projectId);
            SnapshotLifecycleMetadata snapMeta = project.custom(SnapshotLifecycleMetadata.TYPE);
            if (snapMeta == null) {
                throw new ResourceNotFoundException("snapshot lifecycle policy not found: {}", request.getLifecycleId());
            }
            var currentMode = LifecycleOperationMetadata.currentSLMMode(project);
            // Check that the policy exists in the first place
            snapMeta.getSnapshotConfigurations()
                .entrySet()
                .stream()
                .filter(e -> e.getValue().getPolicy().getId().equals(request.getLifecycleId()))
                .findAny()
                .orElseThrow(() -> new ResourceNotFoundException("snapshot lifecycle policy not found: {}", request.getLifecycleId()));

            Map<String, SnapshotLifecyclePolicyMetadata> newConfigs = snapMeta.getSnapshotConfigurations()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().equals(request.getLifecycleId()) == false)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return currentState.copyAndUpdateProject(
                project.id(),
                builder -> builder.putCustom(
                    SnapshotLifecycleMetadata.TYPE,
                    new SnapshotLifecycleMetadata(newConfigs, currentMode, snapMeta.getStats().removePolicy(request.getLifecycleId()))
                )
            );
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteSnapshotLifecycleAction.Request request, ProjectState projectState) {
        return projectState.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedSnapshotAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(DeleteSnapshotLifecycleAction.Request request) {
        return Set.of(request.getLifecycleId());
    }
}
