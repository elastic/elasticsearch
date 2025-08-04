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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;
import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagement.updateIndicesForPolicy;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

/**
 * This class is responsible for bootstrapping {@link IndexLifecycleMetadata} into the cluster-state, as well
 * as adding the desired new policy to be inserted.
 */
public class TransportPutLifecycleAction extends TransportMasterNodeAction<PutLifecycleRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutLifecycleAction.class);
    private final NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final XPackLicenseState licenseState;
    private final ProjectResolver projectResolver;
    private final MasterServiceTaskQueue<UpdateLifecyclePolicyTask> taskQueue;

    @Inject
    public TransportPutLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        NamedXContentRegistry namedXContentRegistry,
        XPackLicenseState licenseState,
        Client client,
        ProjectResolver projectResolver
    ) {
        super(
            ILMActions.PUT.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutLifecycleRequest::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.xContentRegistry = namedXContentRegistry;
        this.licenseState = licenseState;
        this.client = client;
        this.projectResolver = projectResolver;
        this.taskQueue = clusterService.createTaskQueue("ilm-put-lifecycle-queue", Priority.NORMAL, new IlmLifecycleExecutor());
    }

    @Override
    protected void masterOperation(
        Task task,
        PutLifecycleRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // headers from the thread context stored by the AuthenticationService to be shared between the
        // REST layer and the Transport layer here must be accessed within this thread and not in the
        // cluster state thread in the ClusterStateUpdateTask below since that thread does not share the
        // same context, and therefore does not have access to the appropriate security headers.
        Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(threadPool.getThreadContext(), state);

        LifecyclePolicy.validatePolicyName(request.getPolicy().getName());
        request.getPolicy().maybeAddDeprecationWarningForFreezeAction(request.getPolicy().getName());

        ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(state);
        IndexLifecycleMetadata lifecycleMetadata = projectMetadata.custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        LifecyclePolicyMetadata existingPolicy = lifecycleMetadata.getPolicyMetadatas().get(request.getPolicy().getName());
        // Make the request a no-op if the policy and filtered headers match exactly
        if (isNoopUpdate(existingPolicy, request.getPolicy(), filteredHeaders)) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        UpdateLifecyclePolicyTask putTask = new UpdateLifecyclePolicyTask(
            projectMetadata.id(),
            request,
            listener,
            licenseState,
            filteredHeaders,
            xContentRegistry,
            client
        );
        taskQueue.submitTask("put-lifecycle-" + request.getPolicy().getName(), putTask, putTask.timeout());
    }

    public static class UpdateLifecyclePolicyTask extends AckedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final PutLifecycleRequest request;
        private final XPackLicenseState licenseState;
        private final Map<String, String> filteredHeaders;
        private final NamedXContentRegistry xContentRegistry;
        private final Client client;
        private final boolean verboseLogging;

        public UpdateLifecyclePolicyTask(
            ProjectId projectId,
            PutLifecycleRequest request,
            ActionListener<AcknowledgedResponse> listener,
            XPackLicenseState licenseState,
            Map<String, String> filteredHeaders,
            NamedXContentRegistry xContentRegistry,
            Client client
        ) {
            super(request, listener);
            this.projectId = projectId;
            this.request = request;
            this.licenseState = licenseState;
            this.filteredHeaders = filteredHeaders;
            this.xContentRegistry = xContentRegistry;
            this.client = client;
            this.verboseLogging = true;
        }

        /**
         * Used by the {@link ReservedClusterStateHandler} for ILM
         * {@link ReservedLifecycleAction}
         * <p>
         * It disables verbose logging and has no filtered headers.
         */
        UpdateLifecyclePolicyTask(
            ProjectId projectId,
            PutLifecycleRequest request,
            XPackLicenseState licenseState,
            NamedXContentRegistry xContentRegistry,
            Client client
        ) {
            super(request, null);
            this.projectId = projectId;
            this.request = request;
            this.licenseState = licenseState;
            this.filteredHeaders = Collections.emptyMap();
            this.xContentRegistry = xContentRegistry;
            this.client = client;
            this.verboseLogging = false;
        }

        @Override
        public ClusterState execute(ClusterState clusterState) throws Exception {
            var projectState = clusterState.projectState(projectId);
            final IndexLifecycleMetadata currentMetadata = projectState.metadata()
                .custom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
            final LifecyclePolicyMetadata existingPolicyMetadata = currentMetadata.getPolicyMetadatas().get(request.getPolicy().getName());

            // Double-check for no-op in the state update task, in case it was changed/reset in the meantime
            if (isNoopUpdate(existingPolicyMetadata, request.getPolicy(), filteredHeaders)) {
                return clusterState;
            }

            validatePrerequisites(request.getPolicy(), projectState, licenseState);

            long nextVersion = (existingPolicyMetadata == null) ? 1L : existingPolicyMetadata.getVersion() + 1L;
            SortedMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.getPolicyMetadatas());
            LifecyclePolicyMetadata lifecyclePolicyMetadata = new LifecyclePolicyMetadata(
                request.getPolicy(),
                filteredHeaders,
                nextVersion,
                Instant.now().toEpochMilli()
            );
            LifecyclePolicyMetadata oldPolicy = newPolicies.put(lifecyclePolicyMetadata.getName(), lifecyclePolicyMetadata);
            if (verboseLogging) {
                if (oldPolicy == null) {
                    logger.info("adding index lifecycle policy [{}]", request.getPolicy().getName());
                } else {
                    logger.info("updating index lifecycle policy [{}]", request.getPolicy().getName());
                }
            }
            IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentILMMode(projectState.metadata()));
            ProjectMetadata newProjectMetadata = ProjectMetadata.builder(projectState.metadata())
                .putCustom(IndexLifecycleMetadata.TYPE, newMetadata)
                .build();
            ClusterState nonRefreshedState = ClusterState.builder(clusterState).putProjectMetadata(newProjectMetadata).build();
            if (oldPolicy == null) {
                return nonRefreshedState;
            } else {
                try {
                    ProjectMetadata refreshedProjectMetadata = updateIndicesForPolicy(
                        newProjectMetadata,
                        xContentRegistry,
                        client,
                        oldPolicy.getPolicy(),
                        lifecyclePolicyMetadata,
                        licenseState
                    );
                    return ClusterState.builder(clusterState).putProjectMetadata(refreshedProjectMetadata).build();
                } catch (Exception e) {
                    logger.warn(() -> "unable to refresh indices phase JSON for updated policy [" + oldPolicy.getName() + "]", e);
                    // Revert to the non-refreshed state
                    return nonRefreshedState;
                }
            }
        }
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
     * @param state The project state
     */
    private static void validatePrerequisites(LifecyclePolicy policy, ProjectState state, XPackLicenseState licenseState) {
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
            if (RepositoriesMetadata.get(state.cluster()).repository(repository) == null) {
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

    @Override
    protected ClusterBlockException checkBlock(PutLifecycleRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedLifecycleAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(PutLifecycleRequest request) {
        return Set.of(request.getPolicy().getName());
    }

    private static class IlmLifecycleExecutor extends SimpleBatchedAckListenerTaskExecutor<UpdateLifecyclePolicyTask> {

        @Override
        public Tuple<ClusterState, ClusterStateAckListener> executeTask(UpdateLifecyclePolicyTask task, ClusterState clusterState)
            throws Exception {
            return Tuple.tuple(task.execute(clusterState), task);
        }

    }

}
