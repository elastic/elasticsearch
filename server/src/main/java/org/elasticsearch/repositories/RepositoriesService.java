/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.stats.RepositoryUsageStats;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.VerifyNodeRepositoryAction.Request;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.repositories.ProjectRepo.projectRepoString;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY;

/**
 * Service responsible for maintaining and providing access to multiple repositories.
 *
 * The elected master creates new repositories on request and persists the {@link RepositoryMetadata} in the cluster state. The cluster
 * state update then goes out to the rest of the cluster nodes so that all nodes know how to access the new repository. This class contains
 * factory information to create new repositories, and provides access to and maintains the lifecycle of repositories. New nodes can easily
 * find all the repositories via the cluster state after joining a cluster.
 *
 * {@link #repository(ProjectId, String)} can be used to fetch a repository.
 * {@link #createRepository(ProjectId, RepositoryMetadata)} does the heavy lifting of creation.
 * {@link #applyClusterState(ClusterChangedEvent)} handles adding and removing repositories per cluster state updates.
 */
public class RepositoriesService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(RepositoriesService.class);

    public static final Setting<TimeValue> REPOSITORIES_STATS_ARCHIVE_RETENTION_PERIOD = Setting.positiveTimeSetting(
        "repositories.stats.archive.retention_period",
        TimeValue.timeValueHours(2),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> REPOSITORIES_STATS_ARCHIVE_MAX_ARCHIVED_STATS = Setting.intSetting(
        "repositories.stats.archive.max_archived_stats",
        100,
        0,
        Setting.Property.NodeScope
    );

    private final Map<String, Repository.Factory> typesRegistry;
    private final Map<String, Repository.Factory> internalTypesRegistry;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;
    private final NodeClient client;

    private final Map<ProjectId, Map<String, Repository>> internalRepositories = ConcurrentCollections.newConcurrentMap();
    private final Map<ProjectId, Map<String, Repository>> repositories = ConcurrentCollections.newConcurrentMap();
    private final RepositoriesStatsArchive repositoriesStatsArchive;

    private final List<BiConsumer<Snapshot, IndexVersion>> preRestoreChecks;

    @SuppressWarnings("this-escape")
    public RepositoriesService(
        Settings settings,
        ClusterService clusterService,
        Map<String, Repository.Factory> typesRegistry,
        Map<String, Repository.Factory> internalTypesRegistry,
        ThreadPool threadPool,
        NodeClient client,
        List<BiConsumer<Snapshot, IndexVersion>> preRestoreChecks
    ) {
        this.typesRegistry = typesRegistry;
        this.internalTypesRegistry = internalTypesRegistry;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
        // Doesn't make sense to maintain repositories on non-master and non-data nodes
        // Nothing happens there anyway
        if (DiscoveryNode.canContainData(settings) || DiscoveryNode.isMasterNode(settings)) {
            if (isDedicatedVotingOnlyNode(DiscoveryNode.getRolesFromSettings(settings)) == false) {
                clusterService.addHighPriorityApplier(this);
            }
        }
        this.repositoriesStatsArchive = new RepositoriesStatsArchive(
            REPOSITORIES_STATS_ARCHIVE_RETENTION_PERIOD.get(settings),
            REPOSITORIES_STATS_ARCHIVE_MAX_ARCHIVED_STATS.get(settings),
            threadPool.relativeTimeInMillisSupplier()
        );
        this.preRestoreChecks = preRestoreChecks;
    }

    /**
     * Registers new repository in the cluster
     * <p>
     * This method can be only called on the master node.
     * It tries to create a new repository on the master, and if it was successful, it adds a new repository to cluster metadata.
     *
     * @param projectId the project ID to which the repository belongs.
     * @param request  register repository request
     * @param responseListener register repository listener
     */
    public void registerRepository(
        final ProjectId projectId,
        final PutRepositoryRequest request,
        final ActionListener<AcknowledgedResponse> responseListener
    ) {
        assert lifecycle.started() : "Trying to register new repository but service is in state [" + lifecycle.state() + "]";
        validateRepositoryName(request.name());

        // Aggregated result of two asynchronous operations when the cluster acknowledged and state changed
        record RegisterRepositoryTaskResult(AcknowledgedResponse ackResponse, boolean changed) {}

        SubscribableListener

            // Trying to create the new repository on master to make sure it works
            .<Void>newForked(validationStep -> validatePutRepositoryRequest(projectId, request, validationStep))

            // When publication has completed (and all acks received or timed out) then verify the repository.
            // (if acks timed out then acknowledgementStep completes before the master processes this cluster state, hence why we have
            // to wait for the publication to be complete too)
            .<RegisterRepositoryTaskResult>andThen(clusterUpdateStep -> {
                final ListenableFuture<AcknowledgedResponse> acknowledgementStep = new ListenableFuture<>();
                final ListenableFuture<Boolean> publicationStep = new ListenableFuture<>(); // Boolean==changed.
                submitUnbatchedTask(
                    "put_repository " + projectRepoString(projectId, request.name()),
                    new RegisterRepositoryTask(this, projectId, request, acknowledgementStep) {
                        @Override
                        public void onFailure(Exception e) {
                            logger.warn(() -> "failed to create repository " + projectRepoString(projectId, request.name()), e);
                            publicationStep.onFailure(e);
                            super.onFailure(e);
                        }

                        @Override
                        public boolean mustAck(DiscoveryNode discoveryNode) {
                            // repository is created on both master and data nodes
                            return discoveryNode.isMasterNode() || discoveryNode.canContainData();
                        }

                        @Override
                        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                            if (changed) {
                                if (found) {
                                    logger.info("updated repository {}", projectRepoString(projectId, request.name()));
                                } else {
                                    logger.info("put repository {}", projectRepoString(projectId, request.name()));
                                }
                            }
                            publicationStep.onResponse(oldState != newState);
                        }
                    }
                );
                publicationStep.addListener(
                    clusterUpdateStep.delegateFailureAndWrap(
                        (stateChangeListener, changed) -> acknowledgementStep.addListener(
                            stateChangeListener.map(acknowledgedResponse -> new RegisterRepositoryTaskResult(acknowledgedResponse, changed))
                        )
                    )
                );
            })
            .<AcknowledgedResponse>andThen((verificationStep, taskResult) -> {
                if (request.verify() == false) {
                    verificationStep.onResponse(taskResult.ackResponse);
                } else {
                    SubscribableListener

                        .<List<DiscoveryNode>>newForked(verifyRepositoryStep -> {
                            if (taskResult.ackResponse.isAcknowledged() && taskResult.changed) {
                                final ThreadContext threadContext = threadPool.getThreadContext();
                                verifyRepository(projectId, request.name(), verifyRepositoryStep);
                            } else {
                                verifyRepositoryStep.onResponse(null);
                            }
                        })
                        // When verification has completed, get the repository data for the first time
                        .<RepositoryData>andThen(
                            getRepositoryDataStep -> threadPool.generic()
                                .execute(
                                    ActionRunnable.wrap(
                                        getRepositoryDataStep,
                                        ll -> repository(projectId, request.name()).getRepositoryData(
                                            // TODO contemplate threading, do we need to fork, see #101445?
                                            EsExecutors.DIRECT_EXECUTOR_SERVICE,
                                            ll
                                        )
                                    )
                                )
                        )
                        // When the repository metadata is ready, update the repository UUID stored in the cluster state, if available
                        .<Void>andThen(
                            (updateRepoUuidStep, repositoryData) -> updateRepositoryUuidInMetadata(
                                clusterService,
                                projectId,
                                request.name(),
                                repositoryData,
                                updateRepoUuidStep
                            )
                        )
                        .andThenApply(uuidUpdated -> taskResult.ackResponse)
                        .addListener(verificationStep);
                }
            })
            .addListener(responseListener);
    }

    /**
     * Task class that extracts the 'execute' part of the functionality for registering
     * repositories.
     */
    public static class RegisterRepositoryTask extends AckedClusterStateUpdateTask {
        protected boolean found = false;
        protected boolean changed = false;

        private final ProjectId projectId;
        private final PutRepositoryRequest request;
        private final RepositoriesService repositoriesService;

        RegisterRepositoryTask(
            final RepositoriesService repositoriesService,
            final ProjectId projectId,
            final PutRepositoryRequest request,
            final ListenableFuture<AcknowledgedResponse> acknowledgementStep
        ) {
            super(request, acknowledgementStep);
            this.repositoriesService = repositoriesService;
            this.projectId = projectId;
            this.request = request;
        }

        /**
         * Constructor used by {@link org.elasticsearch.action.admin.cluster.repositories.reservedstate.ReservedRepositoryAction}
         * @param repositoriesService
         * @param request
         */
        public RegisterRepositoryTask(
            final RepositoriesService repositoriesService,
            final ProjectId projectId,
            final PutRepositoryRequest request
        ) {
            this(repositoriesService, projectId, request, null);
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            final var projectState = currentState.projectState(projectId);
            RepositoriesMetadata repositories = RepositoriesMetadata.get(projectState.metadata());
            List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size() + 1);
            for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                if (repositoryMetadata.name().equals(request.name())) {
                    rejectInvalidReadonlyFlagChange(repositoryMetadata, request.settings());
                    final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(
                        request.name(),
                        // Copy the UUID from the existing instance rather than resetting it back to MISSING_UUID which would force us to
                        // re-read the RepositoryData to get it again. In principle the new RepositoryMetadata might point to a different
                        // underlying repository at this point, but if so that'll cause things to fail in clear ways and eventually (before
                        // writing anything) we'll read the RepositoryData again and update the UUID in the RepositoryMetadata to match. See
                        // also #109936.
                        repositoryMetadata.uuid(),
                        request.type(),
                        request.settings()
                    );
                    Repository existing = repositoriesService.repositoryOrNull(projectId, request.name());
                    assert existing != null : "repository [" + newRepositoryMetadata.name() + "] must exist";
                    assert existing.getMetadata() == repositoryMetadata;
                    final RepositoryMetadata updatedMetadata;
                    if (canUpdateInPlace(newRepositoryMetadata, existing)) {
                        if (repositoryMetadata.settings().equals(newRepositoryMetadata.settings())) {
                            if (repositoryMetadata.generation() == RepositoryData.CORRUPTED_REPO_GEN) {
                                // If recreating a corrupted repository with the same settings, reset the corrupt flag.
                                // Setting the safe generation to unknown, so that a consistent generation is found.
                                ensureRepositoryNotInUse(projectState, request.name());
                                logger.info(
                                    "repository [{}/{}] is marked as corrupted, resetting the corruption marker",
                                    repositoryMetadata.name(),
                                    repositoryMetadata.uuid()
                                );
                                repositoryMetadata = repositoryMetadata.withGeneration(
                                    RepositoryData.UNKNOWN_REPO_GEN,
                                    repositoryMetadata.pendingGeneration()
                                );
                            } else {
                                // Previous version is the same as this one no update is needed.
                                return currentState;
                            }
                        }
                        // we're updating in place so the updated metadata must point at the same uuid and generations
                        updatedMetadata = repositoryMetadata.withSettings(newRepositoryMetadata.settings());
                    } else {
                        ensureRepositoryNotInUse(projectState, request.name());
                        updatedMetadata = newRepositoryMetadata;
                    }
                    found = true;
                    repositoriesMetadata.add(updatedMetadata);
                } else {
                    repositoriesMetadata.add(repositoryMetadata);
                }
            }
            if (found == false) {
                repositoriesMetadata.add(new RepositoryMetadata(request.name(), request.type(), request.settings()));
            }
            repositories = new RepositoriesMetadata(repositoriesMetadata);
            changed = true;
            return ClusterState.builder(currentState)
                .putProjectMetadata(ProjectMetadata.builder(projectState.metadata()).putCustom(RepositoriesMetadata.TYPE, repositories))
                .build();
        }
    }

    /**
     * Ensures that we can create the repository and that it's creation actually works
     * <p>
     * This verification method will create and then close the repository we want to create.
     *
     * @param request
     */
    public void validateRepositoryCanBeCreated(final ProjectId projectId, final PutRepositoryRequest request) {
        final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(request.name(), request.type(), request.settings());

        // Trying to create the new repository on master to make sure it works
        closeRepository(createRepository(projectId, newRepositoryMetadata));
    }

    private void validatePutRepositoryRequest(
        final ProjectId projectId,
        final PutRepositoryRequest request,
        ActionListener<Void> resultListener
    ) {
        final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(request.name(), request.type(), request.settings());
        try {
            final var repository = createRepository(projectId, newRepositoryMetadata);
            if (request.verify()) {
                // verify repository on local node only, different from verifyRepository method that runs on other cluster nodes
                threadPool.executor(ThreadPool.Names.SNAPSHOT)
                    .execute(ActionRunnable.run(ActionListener.runBefore(resultListener, () -> closeRepository(repository)), () -> {
                        final var token = repository.startVerification();
                        if (token != null) {
                            repository.verify(token, clusterService.localNode());
                            repository.endVerification(token);
                        }
                    }));
            } else {
                closeRepository(repository);
                resultListener.onResponse(null);
            }
        } catch (Exception e) {
            resultListener.onFailure(e);
        }
    }

    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        submitUnbatchedTask(clusterService, source, task);
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static void submitUnbatchedTask(
        ClusterService clusterService,
        @SuppressWarnings("SameParameterValue") String source,
        ClusterStateUpdateTask task
    ) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    /**
     * Set the repository UUID in the named repository's {@link RepositoryMetadata} to match the UUID in its {@link RepositoryData},
     * which may involve a cluster state update.
     *
     * @param listener notified when the {@link RepositoryMetadata} is updated, possibly on this thread or possibly on the master service
     *                 thread
     */
    public static void updateRepositoryUuidInMetadata(
        ClusterService clusterService,
        final ProjectId projectId,
        final String repositoryName,
        RepositoryData repositoryData,
        ActionListener<Void> listener
    ) {

        final String repositoryUuid = repositoryData.getUuid();
        if (repositoryUuid.equals(RepositoryData.MISSING_UUID)) {
            listener.onResponse(null);
            return;
        }

        final RepositoryMetadata repositoryMetadata = RepositoriesMetadata.get(clusterService.state().metadata().getProject(projectId))
            .repository(repositoryName);
        if (repositoryMetadata == null || repositoryMetadata.uuid().equals(repositoryUuid)) {
            listener.onResponse(null);
            return;
        }

        logger.info(
            Strings.format(
                "Registering repository [%s] with repository UUID [%s] and generation [%d]",
                repositoryName,
                repositoryData.getUuid(),
                repositoryData.getGenId()
            )
        );

        submitUnbatchedTask(
            clusterService,
            "update repository UUID " + projectRepoString(projectId, repositoryName) + " to [" + repositoryUuid + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final var project = currentState.metadata().getProject(projectId);
                    final RepositoriesMetadata currentReposMetadata = RepositoriesMetadata.get(project);

                    final RepositoryMetadata repositoryMetadata = currentReposMetadata.repository(repositoryName);
                    if (repositoryMetadata == null || repositoryMetadata.uuid().equals(repositoryUuid)) {
                        return currentState;
                    } else {
                        final RepositoriesMetadata newReposMetadata = currentReposMetadata.withUuid(repositoryName, repositoryUuid);
                        return ClusterState.builder(currentState)
                            .putProjectMetadata(ProjectMetadata.builder(project).putCustom(RepositoriesMetadata.TYPE, newReposMetadata))
                            .build();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    listener.onResponse(null);
                }
            }
        );
    }

    /**
     * Unregisters repository in the cluster
     * <p>
     * This method can be only called on the master node. It removes repository information from cluster metadata.
     *
     * @param projectId project to look for the repository
     * @param request  unregister repository request
     * @param listener unregister repository listener
     */
    public void unregisterRepository(
        final ProjectId projectId,
        final DeleteRepositoryRequest request,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        submitUnbatchedTask(
            "delete_repository " + projectRepoString(projectId, request.name()),
            new UnregisterRepositoryTask(projectId, request, listener) {
                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    if (deletedRepositories.isEmpty() == false) {
                        logger.info("deleted repositories [{}] for project [{}]", deletedRepositories, projectId);
                    }
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // repository was created on both master and data nodes
                    return discoveryNode.isMasterNode() || discoveryNode.canContainData();
                }
            }
        );
    }

    /**
     * Task class that extracts the 'execute' part of the functionality for unregistering
     * repositories.
     */
    public static class UnregisterRepositoryTask extends AckedClusterStateUpdateTask {
        protected final List<String> deletedRepositories = new ArrayList<>();
        private final ProjectId projectId;
        private final DeleteRepositoryRequest request;

        UnregisterRepositoryTask(
            final ProjectId projectId,
            final DeleteRepositoryRequest request,
            final ActionListener<AcknowledgedResponse> listener
        ) {
            super(request, listener);
            this.projectId = projectId;
            this.request = request;
        }

        /**
         * Constructor used by {@link org.elasticsearch.action.admin.cluster.repositories.reservedstate.ReservedRepositoryAction}
         * @param name the repository name
         */
        public UnregisterRepositoryTask(TimeValue dummyTimeout, ProjectId projectId, String name) {
            this(projectId, new DeleteRepositoryRequest(dummyTimeout, dummyTimeout, name), null);
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            final var projectState = currentState.projectState(projectId);
            RepositoriesMetadata repositories = RepositoriesMetadata.get(projectState.metadata());
            if (repositories.repositories().size() > 0) {
                List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size());
                boolean changed = false;
                for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                    if (Regex.simpleMatch(request.name(), repositoryMetadata.name())) {
                        ensureRepositoryNotInUse(projectState, repositoryMetadata.name());
                        ensureNoSearchableSnapshotsIndicesInUse(currentState, repositoryMetadata);
                        deletedRepositories.add(repositoryMetadata.name());
                        changed = true;
                    } else {
                        repositoriesMetadata.add(repositoryMetadata);
                    }
                }
                if (changed) {
                    repositories = new RepositoriesMetadata(repositoriesMetadata);
                    return ClusterState.builder(currentState)
                        .putProjectMetadata(
                            ProjectMetadata.builder(projectState.metadata()).putCustom(RepositoriesMetadata.TYPE, repositories)
                        )
                        .build();
                }
            }
            if (Regex.isMatchAllPattern(request.name())) { // we use a wildcard so we don't barf if it's not present.
                return currentState;
            }
            throw new RepositoryMissingException(request.name());
        }
    }

    public void verifyRepository(
        final ProjectId projectId,
        final String repositoryName,
        final ActionListener<List<DiscoveryNode>> listener
    ) {
        final Repository repository = repository(projectId, repositoryName);
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                final String verificationToken = repository.startVerification();
                if (verificationToken != null) {
                    try {
                        var nodeRequest = new Request(repositoryName, verificationToken);
                        client.execute(
                            VerifyNodeRepositoryCoordinationAction.TYPE,
                            nodeRequest,
                            listener.delegateFailure(
                                (delegatedListener, response) -> threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                                    try {
                                        repository.endVerification(verificationToken);
                                    } catch (Exception e) {
                                        logger.warn(
                                            () -> projectRepoString(projectId, repositoryName)
                                                + " failed to finish repository verification",
                                            e
                                        );
                                        delegatedListener.onFailure(e);
                                        return;
                                    }
                                    delegatedListener.onResponse(response.nodes);
                                })
                            )
                        );
                    } catch (Exception e) {
                        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                            try {
                                repository.endVerification(verificationToken);
                            } catch (Exception inner) {
                                inner.addSuppressed(e);
                                logger.warn(
                                    () -> projectRepoString(projectId, repositoryName) + " failed to finish repository verification",
                                    inner
                                );
                            }
                            listener.onFailure(e);
                        });
                    }
                } else {
                    listener.onResponse(Collections.emptyList());
                }
            }
        });
    }

    public static boolean isDedicatedVotingOnlyNode(Set<DiscoveryNodeRole> roles) {
        return roles.contains(DiscoveryNodeRole.MASTER_ROLE)
            && roles.stream().noneMatch(DiscoveryNodeRole::canContainData)
            && roles.contains(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE);
    }

    /**
     * Checks if new repositories appeared in or disappeared from cluster metadata and updates current list of
     * repositories accordingly.
     *
     * @param event cluster changed event
     */
    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            final ClusterState state = event.state();
            final ClusterState previousState = event.previousState();

            for (var projectId : event.projectDelta().removed()) { // removed projects
                applyProjectStateForRemovedProject(state.version(), previousState.projectState(projectId));
            }

            for (var projectId : event.projectDelta().added()) { // added projects
                applyProjectStateForAddedOrExistingProject(state.version(), state.projectState(projectId), null);
            }

            // existing projects
            final var common = event.projectDelta().added().isEmpty()
                ? state.metadata().projects().keySet()
                : Sets.difference(state.metadata().projects().keySet(), event.projectDelta().added());
            for (var projectId : common) {
                applyProjectStateForAddedOrExistingProject(
                    state.version(),
                    state.projectState(projectId),
                    previousState.projectState(projectId)
                );
            }
        } catch (Exception ex) {
            assert false : new AssertionError(ex);
            logger.warn("failure updating cluster state ", ex);
        }
    }

    /**
     * Apply changes for one removed project.
     *
     * @param version The cluster state version of the change.
     * @param previousState The previous project state for the removed project.
     */
    private void applyProjectStateForRemovedProject(long version, ProjectState previousState) {
        final var projectId = previousState.projectId();
        assert ProjectId.DEFAULT.equals(projectId) == false : "default project cannot be removed";
        final var survivors = closeRemovedRepositories(version, projectId, getProjectRepositories(projectId), RepositoriesMetadata.EMPTY);
        assert survivors.isEmpty() : "expect no repositories for removed project [" + projectId + "], but got " + survivors.keySet();
        repositories.remove(projectId);
    }

    /**
     * Apply changes for one project. The project can be either newly added or an existing one.
     *
     * @param version The cluster state version of the change.
     * @param state The current project state
     * @param previousState The previous project state, or {@code null} if the project was newly added.
     */
    private void applyProjectStateForAddedOrExistingProject(long version, ProjectState state, @Nullable ProjectState previousState) {
        assert assertReadonlyRepositoriesNotInUseForWrites(state);
        final var projectId = state.projectId();
        assert ProjectId.DEFAULT.equals(projectId) == false || previousState != null : "default project cannot be added";
        assert previousState == null || projectId.equals(previousState.projectId())
            : "current and previous states must refer to the same project, but got " + projectId + " != " + previousState.projectId();

        final RepositoriesMetadata newMetadata = RepositoriesMetadata.get(state.metadata());
        final RepositoriesMetadata oldMetadata = previousState == null
            ? RepositoriesMetadata.EMPTY
            : RepositoriesMetadata.get(previousState.metadata());

        final Map<String, Repository> projectRepositories = getProjectRepositories(projectId);
        // Check if repositories got changed
        if (oldMetadata.equalsIgnoreGenerations(newMetadata)) {
            for (Repository repo : projectRepositories.values()) {
                repo.updateState(state.cluster());
            }
            return;
        }

        logger.trace("processing new index repositories for project [{}] and state version [{}]", projectId, version);

        // First, remove repositories that are no longer there
        final var survivors = closeRemovedRepositories(version, projectId, projectRepositories, newMetadata);

        Map<String, Repository> builder = new HashMap<>();

        // Now go through all repositories and update existing or create missing
        for (RepositoryMetadata repositoryMetadata : newMetadata.repositories()) {
            Repository repository = survivors.get(repositoryMetadata.name());
            if (repository != null) {
                // Found previous version of this repository
                if (canUpdateInPlace(repositoryMetadata, repository) == false) {
                    // Previous version is different from the version in settings
                    logger.debug("updating repository {}", projectRepoString(projectId, repositoryMetadata.name()));
                    closeRepository(repository);
                    archiveRepositoryStats(repository, version);
                    repository = null;
                    try {
                        repository = createRepository(
                            projectId,
                            repositoryMetadata,
                            typesRegistry,
                            RepositoriesService::createUnknownTypeRepository
                        );
                    } catch (RepositoryException ex) {
                        // TODO: this catch is bogus, it means the old repo is already closed,
                        // but we have nothing to replace it
                        logger.warn(() -> "failed to change repository " + projectRepoString(projectId, repositoryMetadata.name()), ex);
                        repository = new InvalidRepository(projectId, repositoryMetadata, ex);
                    }
                }
            } else {
                try {
                    repository = createRepository(
                        projectId,
                        repositoryMetadata,
                        typesRegistry,
                        RepositoriesService::createUnknownTypeRepository
                    );
                } catch (RepositoryException ex) {
                    logger.warn(() -> "failed to create repository " + projectRepoString(projectId, repositoryMetadata.name()), ex);
                    repository = new InvalidRepository(projectId, repositoryMetadata, ex);
                }
            }
            assert repository != null : "repository should not be null here";
            logger.debug("registering repository [{}]", projectRepoString(projectId, repositoryMetadata.name()));
            builder.put(repositoryMetadata.name(), repository);
        }
        for (Repository repo : builder.values()) {
            repo.updateState(state.cluster());
        }
        if (builder.isEmpty() == false) {
            repositories.put(projectId, unmodifiableMap(builder));
        } else {
            repositories.remove(projectId);
        }
    }

    private Map<String, Repository> closeRemovedRepositories(
        long version,
        ProjectId projectId,
        Map<String, Repository> projectRepositories,
        RepositoriesMetadata newMetadata
    ) {
        Map<String, Repository> survivors = new HashMap<>();
        for (Map.Entry<String, Repository> entry : projectRepositories.entrySet()) {
            if (newMetadata.repository(entry.getKey()) == null) {
                logger.debug("unregistering repository {}", projectRepoString(projectId, entry.getKey()));
                Repository repository = entry.getValue();
                closeRepository(repository);
                archiveRepositoryStats(repository, version);
            } else {
                survivors.put(entry.getKey(), entry.getValue());
            }
        }
        return survivors;
    }

    private static boolean canUpdateInPlace(RepositoryMetadata updatedMetadata, Repository repository) {
        assert updatedMetadata.name().equals(repository.getMetadata().name());
        return repository.getMetadata().type().equals(updatedMetadata.type())
            && repository.canUpdateInPlace(updatedMetadata.settings(), Collections.emptySet());
    }

    /**
     * Gets the {@link RepositoryData} for the given repository.
     *
     * @param projectId project to look for the repository
     * @param repositoryName repository name
     * @param listener       listener to pass {@link RepositoryData} to
     */
    public void getRepositoryData(final ProjectId projectId, final String repositoryName, final ActionListener<RepositoryData> listener) {
        try {
            Repository repository = repository(projectId, repositoryName);
            assert repository != null; // should only be called once we've validated the repository exists
            repository.getRepositoryData(
                EsExecutors.DIRECT_EXECUTOR_SERVICE, // TODO contemplate threading here, do we need to fork, see #101445?
                listener
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Returns registered repository, either internal or external
     *
     * @param repositoryName repository name
     * @return registered repository
     * @throws RepositoryMissingException if repository with such name isn't registered
     */
    @FixForMultiProject
    @Deprecated(forRemoval = true)
    public Repository repository(String repositoryName) {
        return repository(ProjectId.DEFAULT, repositoryName);
    }

    /**
     * Returns registered repository, either internal or external
     *
     * @param projectId the project to look for the repository
     * @param repositoryName repository name
     * @return registered repository
     * @throws RepositoryMissingException if repository with such name isn't registered
     */
    public Repository repository(ProjectId projectId, String repositoryName) {
        Repository repository = repositoryOrNull(projectId, repositoryName);
        if (repository != null) {
            return repository;
        }
        throw new RepositoryMissingException(repositoryName);
    }

    /**
     * Similar to {@link #repository(ProjectId, String)}, but returns {@code null} instead of throw if the repository is not found.
     */
    public Repository repositoryOrNull(ProjectId projectId, String repositoryName) {
        Repository repository = repositories.getOrDefault(projectId, Map.of()).get(repositoryName);
        if (repository != null) {
            return repository;
        }
        return internalRepositories.getOrDefault(projectId, Map.of()).get(repositoryName);
    }

    /**
     * @return the current collection of registered repositories from all projects.
     */
    public List<Repository> getRepositories() {
        return repositories.values().stream().map(Map::values).flatMap(Collection::stream).toList();
    }

    /**
     * @return the current collection of registered repositories for the given project, keyed by name.
     */
    public Map<String, Repository> getProjectRepositories(ProjectId projectId) {
        return repositories.getOrDefault(projectId, Map.of());
    }

    // Package private for testing
    boolean hasRepositoryTrackingForProject(ProjectId projectId) {
        return repositories.containsKey(projectId) || internalRepositories.containsKey(projectId);
    }

    public List<RepositoryStatsSnapshot> repositoriesStats() {
        List<RepositoryStatsSnapshot> archivedRepoStats = repositoriesStatsArchive.getArchivedStats();
        List<RepositoryStatsSnapshot> activeRepoStats = getRepositoryStatsForActiveRepositories();

        List<RepositoryStatsSnapshot> repositoriesStats = new ArrayList<>(archivedRepoStats);
        repositoriesStats.addAll(activeRepoStats);
        return repositoriesStats;
    }

    public RepositoriesStats getRepositoriesThrottlingStats() {
        return new RepositoriesStats(
            getRepositories().stream()
                .collect(
                    Collectors.toMap(
                        r -> r.getMetadata().name(),
                        r -> new RepositoriesStats.ThrottlingStats(r.getRestoreThrottleTimeInNanos(), r.getSnapshotThrottleTimeInNanos())
                    )
                )
        );
    }

    private List<RepositoryStatsSnapshot> getRepositoryStatsForActiveRepositories() {
        return Stream.concat(
            repositories.values().stream().map(Map::values).flatMap(Collection::stream),
            internalRepositories.values().stream().map(Map::values).flatMap(Collection::stream)
        )
            .filter(r -> r instanceof MeteredBlobStoreRepository)
            .map(r -> (MeteredBlobStoreRepository) r)
            .map(MeteredBlobStoreRepository::statsSnapshot)
            .toList();
    }

    public List<RepositoryStatsSnapshot> clearRepositoriesStatsArchive(long maxVersionToClear) {
        return repositoriesStatsArchive.clear(maxVersionToClear);
    }

    public void registerInternalRepository(ProjectId projectId, String name, String type) {
        RepositoryMetadata metadata = new RepositoryMetadata(name, type, Settings.EMPTY);
        Repository repository = internalRepositories.compute(projectId, (ignored, existingRepos) -> {
            if (existingRepos == null) {
                existingRepos = Map.of();
            }
            if (existingRepos.containsKey(name)) {
                return existingRepos;
            }
            logger.debug("put internal repository [{}][{}]", projectRepoString(projectId, name), type);
            final var repo = createRepository(
                projectId,
                metadata,
                internalTypesRegistry,
                RepositoriesService::throwRepositoryTypeDoesNotExists
            );
            final var newRepos = new HashMap<>(existingRepos);
            newRepos.put(name, repo);
            return unmodifiableMap(newRepos);
        }).get(name);
        if (type.equals(repository.getMetadata().type()) == false) {
            logger.warn(
                () -> format(
                    "internal repository [%s][%s] already registered. this prevented the registration of "
                        + "internal repository [%s][%s].",
                    name,
                    repository.getMetadata().type(),
                    name,
                    type
                )
            );
        } else if (getProjectRepositories(projectId).containsKey(name)) {
            logger.warn(
                () -> format(
                    "non-internal repository [%s] already registered. this repository will block the "
                        + "usage of internal repository [%s][%s].",
                    name,
                    metadata.type(),
                    name
                )
            );
        }
    }

    public void unregisterInternalRepository(ProjectId projectId, String name) {
        final var repositoryRef = new AtomicReference<Repository>();
        internalRepositories.computeIfPresent(projectId, (ignored, existingRepos) -> {
            if (existingRepos.containsKey(name) == false) {
                return existingRepos;
            }
            final var newRepos = new HashMap<>(existingRepos);
            repositoryRef.set(newRepos.remove(name));
            if (newRepos.isEmpty()) {
                return null;
            } else {
                return unmodifiableMap(newRepos);
            }
        });
        Repository repository = repositoryRef.get();
        if (repository != null) {
            RepositoryMetadata metadata = repository.getMetadata();
            logger.debug(() -> format("delete internal repository [%s][%s].", metadata.type(), projectRepoString(projectId, name)));
            closeRepository(repository);
        }
    }

    /**
     * Closes the given repository.
     */
    private static void closeRepository(Repository repository) {
        logger.debug(
            "closing repository [{}]{}",
            repository.getMetadata().type(),
            projectRepoString(repository.getProjectId(), repository.getMetadata().name())
        );
        repository.close();
    }

    private void archiveRepositoryStats(Repository repository, long clusterStateVersion) {
        if (repository instanceof MeteredBlobStoreRepository) {
            RepositoryStatsSnapshot stats = ((MeteredBlobStoreRepository) repository).statsSnapshotForArchival(clusterStateVersion);
            if (repositoriesStatsArchive.archive(stats) == false) {
                logger.warn("Unable to archive the repository stats [{}] as the archive is full.", stats);
            }
        }
    }

    /**
     * Creates repository holder. This method starts the repository
     */
    private static Repository createRepository(
        @Nullable ProjectId projectId,
        RepositoryMetadata repositoryMetadata,
        Map<String, Repository.Factory> factories,
        BiFunction<ProjectId, RepositoryMetadata, Repository> defaultFactory
    ) {
        logger.debug("creating repository [{}][{}]", repositoryMetadata.type(), repositoryMetadata.name());
        Repository.Factory factory = factories.get(repositoryMetadata.type());
        if (factory == null) {
            return defaultFactory.apply(projectId, repositoryMetadata);
        }
        Repository repository = null;
        try {
            repository = factory.create(projectId, repositoryMetadata, factories::get);
            repository.start();
            return repository;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(repository);
            logger.warn(() -> format("failed to create repository [%s][%s]", repositoryMetadata.type(), repositoryMetadata.name()), e);
            throw new RepositoryException(repositoryMetadata.name(), "failed to create repository", e);
        }
    }

    /**
     * Creates a repository holder.
     *
     * <p>WARNING: This method is intended for expert only usage mainly in plugins/modules. Please take note of the following:</p>
     *
     * <ul>
     *     <li>This method does not register the repository (e.g., in the cluster state).</li>
     *     <li>This method starts the repository. The repository should be closed after use.</li>
     *     <li>The repository metadata should be associated to an already registered non-internal repository type and factory pair.</li>
     * </ul>
     *
     * @param projectId the project that the repository is associated with
     * @param repositoryMetadata the repository metadata
     * @return the started repository
     * @throws RepositoryException if repository type is not registered
     */
    public Repository createRepository(ProjectId projectId, RepositoryMetadata repositoryMetadata) {
        return createRepository(
            Objects.requireNonNull(projectId),
            repositoryMetadata,
            typesRegistry,
            RepositoriesService::throwRepositoryTypeDoesNotExists
        );
    }

    /**
     * Similar to {@link #createRepository(ProjectId, RepositoryMetadata)}, but repository is not associated with a project, i.e. the
     * repository is at the cluster level.
     */
    public Repository createNonProjectRepository(RepositoryMetadata repositoryMetadata) {
        assert DiscoveryNode.isStateless(clusterService.getSettings())
            : "outside stateless only project level repositories are allowed: " + repositoryMetadata;
        return createRepository(null, repositoryMetadata, typesRegistry, RepositoriesService::throwRepositoryTypeDoesNotExists);
    }

    private static Repository throwRepositoryTypeDoesNotExists(ProjectId projectId, RepositoryMetadata repositoryMetadata) {
        throw new RepositoryException(
            repositoryMetadata.name(),
            "repository type [" + repositoryMetadata.type() + "] does not exist for project [" + projectId + "]"
        );
    }

    private static Repository createUnknownTypeRepository(ProjectId projectId, RepositoryMetadata repositoryMetadata) {
        logger.warn(
            "[{}] repository type [{}] is unknown; ensure that all required plugins are installed on this node",
            repositoryMetadata.name(),
            repositoryMetadata.type()
        );
        return new UnknownTypeRepository(projectId, repositoryMetadata);
    }

    public static void validateRepositoryName(final String repositoryName) {
        if (Strings.hasLength(repositoryName) == false) {
            throw new RepositoryException(repositoryName, "cannot be empty");
        }
        if (repositoryName.contains("#")) {
            throw new RepositoryException(repositoryName, "must not contain '#'");
        }
        if (Strings.validFileName(repositoryName) == false) {
            throw new RepositoryException(repositoryName, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
    }

    private static void ensureRepositoryNotInUseForWrites(ProjectState projectState, String repository) {
        final ProjectId projectId = projectState.projectId();
        if (SnapshotsInProgress.get(projectState.cluster()).forRepo(projectId, repository).isEmpty() == false) {
            throw newRepositoryConflictException(repository, "snapshot is in progress");
        }
        for (SnapshotDeletionsInProgress.Entry entry : SnapshotDeletionsInProgress.get(projectState.cluster()).getEntries()) {
            if (entry.projectId().equals(projectId) && entry.repository().equals(repository)) {
                throw newRepositoryConflictException(repository, "snapshot deletion is in progress");
            }
        }
        for (RepositoryCleanupInProgress.Entry entry : RepositoryCleanupInProgress.get(projectState.cluster()).entries()) {
            if (entry.projectId().equals(projectId) && entry.repository().equals(repository)) {
                throw newRepositoryConflictException(repository, "repository clean up is in progress");
            }
        }
    }

    private static void ensureRepositoryNotInUse(ProjectState projectState, String repository) {
        ensureRepositoryNotInUseForWrites(projectState, repository);
        for (RestoreInProgress.Entry entry : RestoreInProgress.get(projectState.cluster())) {
            if (repository.equals(entry.snapshot().getRepository())) {
                throw newRepositoryConflictException(repository, "snapshot restore is in progress");
            }
        }
    }

    public static boolean isReadOnly(Settings repositorySettings) {
        return Boolean.TRUE.equals(repositorySettings.getAsBoolean(BlobStoreRepository.READONLY_SETTING_KEY, null));
    }

    /**
     * Test-only check for the invariant that read-only repositories never have any write activities.
     */
    private static boolean assertReadonlyRepositoriesNotInUseForWrites(ProjectState projectState) {
        assert projectState != null;
        for (final var repositoryMetadata : RepositoriesMetadata.get(projectState.metadata()).repositories()) {
            if (isReadOnly(repositoryMetadata.settings())) {
                try {
                    ensureRepositoryNotInUseForWrites(projectState, repositoryMetadata.name());
                } catch (Exception e) {
                    throw new AssertionError("repository [" + repositoryMetadata + "] is readonly but still in use", e);
                }
            }
        }
        return true;
    }

    /**
     * Reject a change to the {@code readonly} setting if there is a pending generation change in progress, i.e. some node somewhere is
     * updating the root {@link RepositoryData} blob.
     */
    private static void rejectInvalidReadonlyFlagChange(RepositoryMetadata existingRepositoryMetadata, Settings newSettings) {
        if (isReadOnly(newSettings)
            && isReadOnly(existingRepositoryMetadata.settings()) == false
            && existingRepositoryMetadata.generation() >= RepositoryData.EMPTY_REPO_GEN
            && existingRepositoryMetadata.generation() != existingRepositoryMetadata.pendingGeneration()) {
            throw newRepositoryConflictException(
                existingRepositoryMetadata.name(),
                Strings.format(
                    "currently updating root blob generation from [%d] to [%d], cannot update readonly flag",
                    existingRepositoryMetadata.generation(),
                    existingRepositoryMetadata.pendingGeneration()
                )
            );
        }
    }

    private static void ensureNoSearchableSnapshotsIndicesInUse(ClusterState clusterState, RepositoryMetadata repositoryMetadata) {
        long count = 0L;
        List<Index> indices = null;
        for (ProjectMetadata project : clusterState.metadata().projects().values()) {
            for (IndexMetadata indexMetadata : project) {
                if (indexSettingsMatchRepositoryMetadata(indexMetadata, repositoryMetadata)) {
                    if (indices == null) {
                        indices = new ArrayList<>();
                    }
                    if (indices.size() < 5) {
                        indices.add(indexMetadata.getIndex());
                    }
                    count += 1L;
                }
            }
        }
        if (indices != null && indices.isEmpty() == false) {
            throw newRepositoryConflictException(
                repositoryMetadata.name(),
                "found "
                    + count
                    + " searchable snapshots indices that use the repository: "
                    + Strings.collectionToCommaDelimitedString(indices)
                    + (count > indices.size() ? ",..." : "")
            );
        }
    }

    private static boolean indexSettingsMatchRepositoryMetadata(IndexMetadata indexMetadata, RepositoryMetadata repositoryMetadata) {
        if (indexMetadata.isSearchableSnapshot()) {
            final Settings indexSettings = indexMetadata.getSettings();
            final String indexRepositoryUuid = indexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY);
            if (Strings.hasLength(indexRepositoryUuid)) {
                return Objects.equals(repositoryMetadata.uuid(), indexRepositoryUuid);
            } else {
                return Objects.equals(repositoryMetadata.name(), indexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY));
            }
        }
        return false;
    }

    private static RepositoryConflictException newRepositoryConflictException(String repository, String reason) {
        return new RepositoryConflictException(
            repository,
            "trying to modify or unregister repository that is currently used (" + reason + ')'
        );
    }

    public List<BiConsumer<Snapshot, IndexVersion>> getPreRestoreVersionChecks() {
        return preRestoreChecks;
    }

    public static final String COUNT_USAGE_STATS_NAME = "count";

    public RepositoryUsageStats getUsageStats() {
        if (repositories.isEmpty()) {
            return RepositoryUsageStats.EMPTY;
        }
        final var statsByType = new HashMap<String, Map<String, Long>>();
        for (final var repository : getRepositories()) {
            final var repositoryType = repository.getMetadata().type();
            final var typeStats = statsByType.computeIfAbsent(repositoryType, ignored -> new HashMap<>());
            typeStats.compute(COUNT_USAGE_STATS_NAME, (k, count) -> (count == null ? 0L : count) + 1);
            final var repositoryUsageTags = repository.getUsageFeatures();
            assert repositoryUsageTags.contains(COUNT_USAGE_STATS_NAME) == false : repositoryUsageTags;
            for (final var repositoryUsageTag : repositoryUsageTags) {
                typeStats.compute(repositoryUsageTag, (k, count) -> (count == null ? 0L : count) + 1);
            }
        }
        return new RepositoryUsageStats(
            statsByType.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> Map.copyOf(e.getValue())))
        );
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {
        clusterService.removeApplier(this);
        final Collection<Repository> repos = new ArrayList<>();
        repos.addAll(internalRepositories.values().stream().map(Map::values).flatMap(Collection::stream).toList());
        repos.addAll(getRepositories());
        IOUtils.close(repos);
        for (Repository repo : repos) {
            repo.awaitIdle();
        }
    }
}
