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
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
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

public class TransportPutSnapshotLifecycleAction extends TransportMasterNodeAction<
    PutSnapshotLifecycleAction.Request,
    AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutSnapshotLifecycleAction.class);

    @Inject
    public TransportPutSnapshotLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PutSnapshotLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutSnapshotLifecycleAction.Request::new,
            indexNameExpressionResolver,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(
        final Task task,
        final PutSnapshotLifecycleAction.Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        SnapshotLifecycleService.validateRepositoryExists(request.getLifecycle().getRepository(), state);

        SnapshotLifecycleService.validateMinimumInterval(request.getLifecycle(), state);

        // headers from the thread context stored by the AuthenticationService to be shared between the
        // REST layer and the Transport layer here must be accessed within this thread and not in the
        // cluster state thread in the ClusterStateUpdateTask below since that thread does not share the
        // same context, and therefore does not have access to the appropriate security headers.
        final Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(threadPool.getThreadContext(), state);
        LifecyclePolicy.validatePolicyName(request.getLifecycleId());

        submitUnbatchedTask(
            "put-snapshot-lifecycle-" + request.getLifecycleId(),
            new UpdateSnapshotPolicyTask(request, listener, filteredHeaders)
        );
    }

    /**
     * Extracted extension of {@link AckedClusterStateUpdateTask} with only the execute method
     * implementation, so that the execute() transformation can be reused for {@link ReservedSnapshotAction}
     */
    public static class UpdateSnapshotPolicyTask extends AckedClusterStateUpdateTask {
        private final PutSnapshotLifecycleAction.Request request;
        private final Map<String, String> filteredHeaders;

        UpdateSnapshotPolicyTask(
            PutSnapshotLifecycleAction.Request request,
            ActionListener<AcknowledgedResponse> listener,
            Map<String, String> filteredHeaders
        ) {
            super(request, listener);
            this.request = request;
            this.filteredHeaders = filteredHeaders;
        }

        /**
         * Used by the {@link ReservedClusterStateHandler} for SLM
         * {@link ReservedSnapshotAction}
         */
        UpdateSnapshotPolicyTask(PutSnapshotLifecycleAction.Request request) {
            super(request, null);
            this.request = request;
            this.filteredHeaders = Map.of();
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            SnapshotLifecycleMetadata snapMeta = currentState.metadata()
                .custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);
            var currentMode = LifecycleOperationMetadata.currentSLMMode(currentState);
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

            return ClusterState.builder(currentState)
                .metadata(
                    Metadata.builder(currentState.metadata())
                        .putCustom(
                            SnapshotLifecycleMetadata.TYPE,
                            new SnapshotLifecycleMetadata(snapLifecycles, currentMode, snapMeta.getStats())
                        )
                )
                .build();
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected ClusterBlockException checkBlock(PutSnapshotLifecycleAction.Request request, ClusterState state) {
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
