/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction.Request;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;

public class TransportDeleteLifecycleAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private final ProjectResolver projectResolver;

    @Inject
    public TransportDeleteLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            DeleteLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        final var projectId = projectResolver.getProjectId();
        submitUnbatchedTask("delete-lifecycle-" + request.getPolicyName(), new DeleteLifecyclePolicyTask(projectId, request, listener));
    }

    public static class DeleteLifecyclePolicyTask extends AckedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final Request request;

        public DeleteLifecyclePolicyTask(ProjectId projectId, Request request, ActionListener<AcknowledgedResponse> listener) {
            super(request, listener);
            this.projectId = projectId;
            this.request = request;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            String policyToDelete = request.getPolicyName();
            ProjectMetadata projectMetadata = currentState.metadata().getProject(projectId);
            List<String> indicesUsingPolicy = projectMetadata.indices()
                .values()
                .stream()
                .filter(idxMeta -> policyToDelete.equals(idxMeta.getLifecyclePolicyName()))
                .map(idxMeta -> idxMeta.getIndex().getName())
                .toList();
            if (indicesUsingPolicy.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Cannot delete policy [" + request.getPolicyName() + "]. It is in use by one or more indices: " + indicesUsingPolicy
                );
            }
            IndexLifecycleMetadata currentMetadata = projectMetadata.custom(IndexLifecycleMetadata.TYPE);
            if (currentMetadata == null || currentMetadata.getPolicyMetadatas().containsKey(request.getPolicyName()) == false) {
                throw new ResourceNotFoundException("Lifecycle policy not found: {}", request.getPolicyName());
            }
            SortedMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.getPolicyMetadatas());
            newPolicies.remove(request.getPolicyName());
            IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentILMMode(projectMetadata));
            ProjectMetadata.Builder newProjectMetadata = ProjectMetadata.builder(projectMetadata)
                .putCustom(IndexLifecycleMetadata.TYPE, newMetadata);
            return ClusterState.builder(currentState).putProjectMetadata(newProjectMetadata).build();
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedLifecycleAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(Request request) {
        return Set.of(request.getPolicyName());
    }
}
