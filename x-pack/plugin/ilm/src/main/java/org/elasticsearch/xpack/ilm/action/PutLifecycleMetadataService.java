/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.BulkMetadataService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCacheManagement;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;
import static org.elasticsearch.xpack.ilm.action.PutLifecycleMetadataService.BulkPutLifecycleOperation;

public class PutLifecycleMetadataService implements BulkMetadataService<BulkPutLifecycleOperation, Void, Void> {

    private static final Logger logger = LogManager.getLogger(PutLifecycleMetadataService.class);

    private static final String BULK_METADATA_SERVICE_NAME = "elasticsearch.xpack.ilm";

    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final XPackLicenseState licenseState;
    private final ThreadPool threadPool;

    @Inject
    public PutLifecycleMetadataService(
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Client client,
        XPackLicenseState licenseState,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.licenseState = licenseState;
        this.threadPool = threadPool;
    }

    @Override
    public String getBulkMetadataServiceName() {
        return BULK_METADATA_SERVICE_NAME;
    }

    public static class BulkPutLifecycleOperation extends BulkMetadataOperation {
        final List<PutLifecycleRequest> requests;
        final Map<String, String> filteredHeaders;
        final boolean verboseLogging;

        public BulkPutLifecycleOperation(List<PutLifecycleRequest> requests, boolean verboseLogging) {
            this(requests, null, verboseLogging);
        }

        public BulkPutLifecycleOperation(List<PutLifecycleRequest> requests, Map<String, String> filteredHeaders, boolean verboseLogging) {
            super(BULK_METADATA_SERVICE_NAME);
            this.requests = requests;
            this.filteredHeaders = filteredHeaders;
            this.verboseLogging = verboseLogging;
        }

        @Override
        public boolean isEmpty() {
            return requests == null || requests.isEmpty();
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    public void validateBatch(BulkPutLifecycleOperation batch, ClusterState currentState) {
        for (PutLifecycleRequest request : batch.requests) {
            LifecyclePolicy.validatePolicyName(request.getPolicy().getName());
            request.getPolicy().maybeAddDeprecationWarningForFreezeAction(request.getPolicy().getName());
        }
    }

    @Override
    public BulkPutLifecycleOperation filterBatch(BulkPutLifecycleOperation batch, ClusterState currentState) {
        // headers from the thread context stored by the AuthenticationService to be shared between the
        // REST layer and the Transport layer here must be accessed within this thread and not in the
        // cluster state thread in the ClusterStateUpdateTask below since that thread does not share the
        // same context, and therefore does not have access to the appropriate security headers.
        Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(threadPool.getThreadContext(), currentState);

        List<PutLifecycleRequest> filteredRequests = new ArrayList<>(batch.requests);
        IndexLifecycleMetadata lifecycleMetadata = currentState.metadata()
            .custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        for (PutLifecycleRequest request : batch.requests) {
            LifecyclePolicyMetadata existingPolicy = lifecycleMetadata.getPolicyMetadatas().get(request.getPolicy().getName());
            // Skip a request if it is a no-op (if the policy and filtered headers match exactly)
            if (isNoopUpdate(existingPolicy, request.getPolicy(), filteredHeaders) == false) {
                filteredRequests.add(request);
            }
        }
        // Capture the filtered headers on the operation
        return new BulkPutLifecycleOperation(filteredRequests, filteredHeaders, batch.verboseLogging);
    }

    @Override
    public BulkMetadataOperationContext<Void> applyBatch(
        BulkPutLifecycleOperation batch,
        ClusterState previousState,
        Metadata.Builder accumulator
    ) {
        return doApplyBatch(batch, previousState, accumulator, licenseState, xContentRegistry, client);
    }

    private static BulkMetadataOperationContext<Void> doApplyBatch(
        BulkPutLifecycleOperation batch,
        ClusterState previousState,
        Metadata.Builder accumulator,
        XPackLicenseState licenseState,
        NamedXContentRegistry xContentRegistry,
        Client client
    ) {
        final IndexLifecycleMetadata currentMetadata = previousState.metadata()
            .custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);

        SortedMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.getPolicyMetadatas());

        boolean updatesToPolicies = false;
        for (PutLifecycleRequest request : batch.requests) {
            final LifecyclePolicyMetadata existingPolicyMetadata = currentMetadata.getPolicyMetadatas().get(request.getPolicy().getName());

            // Double-check for no-op in the state update task, in case it was changed/reset in the meantime
            if (isNoopUpdate(existingPolicyMetadata, request.getPolicy(), batch.filteredHeaders)) {
                continue;
            }

            // PRTODO: This checks repositories and SLM in the metadata
            validatePrerequisites(request.getPolicy(), previousState, licenseState);
            long nextVersion = (existingPolicyMetadata == null) ? 1L : existingPolicyMetadata.getVersion() + 1L;
            LifecyclePolicyMetadata lifecyclePolicyMetadata = new LifecyclePolicyMetadata(
                request.getPolicy(),
                batch.filteredHeaders,
                nextVersion,
                Instant.now().toEpochMilli()
            );
            updatesToPolicies = true;
            LifecyclePolicyMetadata oldPolicy = newPolicies.put(lifecyclePolicyMetadata.getName(), lifecyclePolicyMetadata);
            if (batch.verboseLogging) {
                if (oldPolicy == null) {
                    logger.info("adding index lifecycle policy [{}]", request.getPolicy().getName());
                } else {
                    logger.info("updating index lifecycle policy [{}]", request.getPolicy().getName());
                }
            }

            if (oldPolicy != null) {
                try {
                    PhaseCacheManagement.updateIndicesForPolicy(
                        accumulator,
                        previousState,
                        xContentRegistry,
                        client,
                        oldPolicy.getPolicy(),
                        lifecyclePolicyMetadata,
                        licenseState
                    );
                } catch (Exception e) {
                    // PRTODO: This is a mess - we try and apply all index metadata refreshes for this policy change at the very end of
                    // this function, but I don't know for sure if it can't throw an exception and end up here. The old logic just returns
                    // the new state with the policy and without any refreshes. We don't really have that luxury unless we build the
                    // cluster state after every application. Maybe that's fine and I'm over complicating things.
                    logger.warn(() -> "unable to refresh indices phase JSON for updated policy [" + oldPolicy.getName() + "]", e);
                }
            }
        }
        if (updatesToPolicies) {
            IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentILMMode(previousState));
            accumulator.putCustom(IndexLifecycleMetadata.TYPE, newMetadata);
            return BulkMetadataOperationContext.stateModified();
        } else {
            return BulkMetadataOperationContext.stateUnmodified();
        }
    }

    public void addLifecycle(PutLifecycleRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        var bulkOp = new BulkPutLifecycleOperation(List.of(request), true);
        validateBatch(bulkOp, state);
        var finalBulkOp = filterBatch(bulkOp, state);
        if (finalBulkOp.isEmpty()) {
            return;
        }
        submitUnbatchedTask("put-lifecycle-" + request.getPolicy().getName(), new BulkUpdateLifecyclePolicyTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                var mb = Metadata.builder(currentState.metadata());
                BulkMetadataOperationContext<Void> ctx = applyBatch(finalBulkOp, currentState, mb);
                if (ctx != null && ctx.isMetadataModified()) {
                    return ClusterState.builder(currentState).metadata(mb).build();
                } else {
                    return currentState;
                }
            }
        });
    }

    /**
     * Returns 'true' if the ILM policy is effectually the same (same policy and headers), and thus can be a no-op update.
     */
    static boolean isNoopUpdate(
        @Nullable LifecyclePolicyMetadata existingPolicy,
        LifecyclePolicy newPolicy,
        Map<String, String> filteredHeaders
    ) {
        if (existingPolicy == null) {
            return false;
        } else {
            return newPolicy.equals(existingPolicy.getPolicy()) && filteredHeaders.equals(existingPolicy.getHeaders());
        }
    }

    /**
     * Validate that the license level is compliant for searchable-snapshots, that any referenced snapshot
     * repositories exist, and that any referenced SLM policies exist.
     *
     * @param policy The lifecycle policy
     * @param state  The cluster state
     */
    private static void validatePrerequisites(LifecyclePolicy policy, ClusterState state, XPackLicenseState licenseState) {
        List<Phase> phasesWithSearchableSnapshotActions = policy.getPhases()
            .values()
            .stream()
            .filter(phase -> phase.getActions().containsKey(SearchableSnapshotAction.NAME))
            .toList();
        // check license level for searchable snapshots
        if (phasesWithSearchableSnapshotActions.isEmpty() == false
            && SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(licenseState) == false) {
            throw new IllegalArgumentException(
                "policy ["
                    + policy.getName()
                    + "] defines the ["
                    + SearchableSnapshotAction.NAME
                    + "] action but the current license is non-compliant for [searchable-snapshots]"
            );
        }
        // make sure any referenced snapshot repositories exist
        for (Phase phase : phasesWithSearchableSnapshotActions) {
            SearchableSnapshotAction action = (SearchableSnapshotAction) phase.getActions().get(SearchableSnapshotAction.NAME);
            String repository = action.getSnapshotRepository();
            if (RepositoriesMetadata.get(state).repository(repository) == null) {
                throw new IllegalArgumentException(
                    "no such repository ["
                        + repository
                        + "], the snapshot repository "
                        + "referenced by the ["
                        + SearchableSnapshotAction.NAME
                        + "] action in the ["
                        + phase.getName()
                        + "] phase "
                        + "must exist before it can be referenced by an ILM policy"
                );
            }
        }

        List<Phase> phasesWithWaitForSnapshotActions = policy.getPhases()
            .values()
            .stream()
            .filter(phase -> phase.getActions().containsKey(WaitForSnapshotAction.NAME))
            .toList();
        // make sure any referenced snapshot lifecycle policies exist
        for (Phase phase : phasesWithWaitForSnapshotActions) {
            WaitForSnapshotAction action = (WaitForSnapshotAction) phase.getActions().get(WaitForSnapshotAction.NAME);
            String slmPolicy = action.getPolicy();
            if (state.metadata()
                .custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY)
                .getSnapshotConfigurations()
                .get(slmPolicy) == null) {
                throw new IllegalArgumentException(
                    "no such snapshot lifecycle policy ["
                        + slmPolicy
                        + "], the snapshot lifecycle policy "
                        + "referenced by the ["
                        + WaitForSnapshotAction.NAME
                        + "] action in the ["
                        + phase.getName()
                        + "] phase "
                        + "must exist before it can be referenced by an ILM policy"
                );
            }
        }
    }

    private abstract static class BulkUpdateLifecyclePolicyTask extends AckedClusterStateUpdateTask {
        protected BulkUpdateLifecyclePolicyTask(PutLifecycleRequest request, ActionListener<AcknowledgedResponse> listener) {
            super(request, listener);
        }

        @Override
        public abstract ClusterState execute(ClusterState currentState) throws Exception;
    }

    public static class UpdateLifecyclePolicyTask extends AckedClusterStateUpdateTask {
        private final PutLifecycleRequest request;
        private final XPackLicenseState licenseState;
        private final Map<String, String> filteredHeaders;
        private final NamedXContentRegistry xContentRegistry;
        private final Client client;
        private final boolean verboseLogging;

        /**
         * Used by the {@link ReservedClusterStateHandler} for ILM
         * {@link ReservedLifecycleAction}
         * <p>
         * It disables verbose logging and has no filtered headers.
         */
        UpdateLifecyclePolicyTask(
            PutLifecycleRequest request,
            XPackLicenseState licenseState,
            NamedXContentRegistry xContentRegistry,
            Client client
        ) {
            super(request, null);
            this.request = request;
            this.licenseState = licenseState;
            this.filteredHeaders = Collections.emptyMap();
            this.xContentRegistry = xContentRegistry;
            this.client = client;
            this.verboseLogging = false;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            var mb = Metadata.builder(currentState.metadata());
            BulkMetadataOperationContext<Void> result = doApplyBatch(
                new BulkPutLifecycleOperation(List.of(request), filteredHeaders, verboseLogging),
                currentState,
                mb,
                licenseState,
                xContentRegistry,
                client
            );
            if (result != null && result.isMetadataModified()) {
                return ClusterState.builder(currentState).metadata(mb).build();
            } else {
                return currentState;
            }
        }
    }
}
