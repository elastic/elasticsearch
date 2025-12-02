/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.SnapshotLifecycleService;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TransportPutSnapshotLifecycleAction extends AcknowledgedTransportMasterNodeProjectAction<PutSnapshotLifecycleAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportPutSnapshotLifecycleAction.class);

    @Inject
    public TransportPutSnapshotLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            PutSnapshotLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutSnapshotLifecycleAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(
        final Task task,
        final PutSnapshotLifecycleAction.Request request,
        final ProjectState projectState,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        SnapshotLifecycleService.validateRepositoryExists(projectState.metadata(), request.getLifecycle().getRepository());
        SnapshotLifecycleService.validateMinimumInterval(request.getLifecycle(), projectState.cluster());

        // headers from the thread context stored by the AuthenticationService to be shared between the
        // REST layer and the Transport layer here must be accessed within this thread and not in the
        // cluster state thread in the ClusterStateUpdateTask below since that thread does not share the
        // same context, and therefore does not have access to the appropriate security headers.
        final Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(
            threadPool.getThreadContext(),
            projectState.cluster()
        );
        LifecyclePolicy.validatePolicyName(request.getLifecycleId());

        submitUnbatchedTask(
            "put-snapshot-lifecycle-" + request.getLifecycleId(),
            new UpdateSnapshotPolicyTask(projectState.projectId(), request, listener, filteredHeaders)
        );
    }

    /**
     * Extracted extension of {@link AckedClusterStateUpdateTask} with only the execute method
     * implementation, so that the execute() transformation can be reused for {@link ReservedSnapshotAction}
     */
    public static class UpdateSnapshotPolicyTask extends AckedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final PutSnapshotLifecycleAction.Request request;
        private final Map<String, String> filteredHeaders;

        UpdateSnapshotPolicyTask(
            ProjectId projectId,
            PutSnapshotLifecycleAction.Request request,
            ActionListener<AcknowledgedResponse> listener,
            Map<String, String> filteredHeaders
        ) {
            super(request, listener);
            this.projectId = projectId;
            this.request = request;
            this.filteredHeaders = filteredHeaders;
        }

        /**
         * Used by the {@link ReservedClusterStateHandler} for SLM
         * {@link ReservedSnapshotAction}
         */
        UpdateSnapshotPolicyTask(ProjectId projectId, PutSnapshotLifecycleAction.Request request) {
            super(request, null);
            this.projectId = projectId;
            this.request = request;
            this.filteredHeaders = Map.of();
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            final var project = currentState.metadata().getProject(projectId);
            SnapshotLifecycleMetadata snapMeta = project.custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);
            var currentMode = LifecycleOperationMetadata.currentSLMMode(project);
            final SnapshotLifecyclePolicyMetadata existingPolicyMetadata = snapMeta.getSnapshotConfigurations()
                .get(request.getLifecycleId());

            // check for no-op in the state update task, in case it was changed/reset in the meantime
            if (isNoopUpdate(existingPolicyMetadata, request.getLifecycle(), filteredHeaders)) {
                return currentState;
            }

            long nextVersion = (existingPolicyMetadata == null) ? 1L : existingPolicyMetadata.getVersion() + 1L;
            Map<String, SnapshotLifecyclePolicyMetadata> snapLifecycles = new HashMap<>(snapMeta.getSnapshotConfigurations());
            SnapshotLifecyclePolicyMetadata newLifecycle = SnapshotLifecyclePolicyMetadata.builder(existingPolicyMetadata)
                .setPolicy(request.getLifecycle())
                .setHeaders(filteredHeaders)
                .setVersion(nextVersion)
                .setModifiedDate(Instant.now().toEpochMilli())
                .build();

            SnapshotLifecyclePolicyMetadata oldPolicy = snapLifecycles.put(newLifecycle.getId(), newLifecycle);
            if (oldPolicy == null) {
                logger.info("adding new snapshot lifecycle [{}]", newLifecycle.getId());
            } else {
                logger.info("updating existing snapshot lifecycle [{}]", newLifecycle.getId());
            }

            final var updatedMetadata = new SnapshotLifecycleMetadata(snapLifecycles, currentMode, snapMeta.getStats());
            return currentState.copyAndUpdateProject(project.id(), b -> b.putCustom(SnapshotLifecycleMetadata.TYPE, updatedMetadata));
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected ClusterBlockException checkBlock(PutSnapshotLifecycleAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedSnapshotAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(PutSnapshotLifecycleAction.Request request) {
        return Set.of(request.getLifecycleId());
    }

    /**
     * Returns 'true' if the SLM is effectually the same (same policy and headers), and thus can be a no-op update.
     */
    static boolean isNoopUpdate(
        @Nullable SnapshotLifecyclePolicyMetadata existingPolicyMeta,
        SnapshotLifecyclePolicy newPolicy,
        Map<String, String> filteredHeaders
    ) {
        if (existingPolicyMeta == null) {
            return false;
        } else {
            return newPolicy.equals(existingPolicyMeta.getPolicy()) && filteredHeaders.equals(existingPolicyMeta.getHeaders());
        }
    }
}
