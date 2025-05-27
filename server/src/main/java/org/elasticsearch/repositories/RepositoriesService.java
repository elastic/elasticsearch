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
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.core.Strings.format;
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
 * {@link #repository(String)} can be used to fetch a repository. {@link #createRepository(RepositoryMetadata)} does the heavy lifting of
 * creation. {@link #applyClusterState(ClusterChangedEvent)} handles adding and removing repositories per cluster state updates.
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

    private final Map<String, Repository> internalRepositories = ConcurrentCollections.newConcurrentMap();
    private volatile Map<String, Repository> repositories = Collections.emptyMap();
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
     * @param request  register repository request
     * @param responseListener register repository listener
     */
    public void registerRepository(final PutRepositoryRequest request, final ActionListener<AcknowledgedResponse> responseListener) {
        assert lifecycle.started() : "Trying to register new repository but service is in state [" + lifecycle.state() + "]";
        validateRepositoryName(request.name());

        // Aggregated result of two asynchronous operations when the cluster acknowledged and state changed
        record RegisterRepositoryTaskResult(AcknowledgedResponse ackResponse, boolean changed) {}

        SubscribableListener

            // Trying to create the new repository on master to make sure it works
            .<Void>newForked(validationStep -> validatePutRepositoryRequest(request, validationStep))

            // When publication has completed (and all acks received or timed out) then verify the repository.
            // (if acks timed out then acknowledgementStep completes before the master processes this cluster state, hence why we have
            // to wait for the publication to be complete too)
            .<RegisterRepositoryTaskResult>andThen(clusterUpdateStep -> {
                final ListenableFuture<AcknowledgedResponse> acknowledgementStep = new ListenableFuture<>();
                final ListenableFuture<Boolean> publicationStep = new ListenableFuture<>(); // Boolean==changed.
                submitUnbatchedTask(
                    "put_repository [" + request.name() + "]",
                    new RegisterRepositoryTask(this, request, acknowledgementStep) {
                        @Override
                        public void onFailure(Exception e) {
                            logger.warn(() -> "failed to create repository [" + request.name() + "]", e);
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
                                    logger.info("updated repository [{}]", request.name());
                                } else {
                                    logger.info("put repository [{}]", request.name());
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
                                verifyRepository(request.name(), verifyRepositoryStep);
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
                                        ll -> repository(request.name()).getRepositoryData(
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

        private final PutRepositoryRequest request;
        private final RepositoriesService repositoriesService;

        RegisterRepositoryTask(
            final RepositoriesService repositoriesService,
            final PutRepositoryRequest request,
            final ListenableFuture<AcknowledgedResponse> acknowledgementStep
        ) {
            super(request, acknowledgementStep);
            this.repositoriesService = repositoriesService;
            this.request = request;
        }

        /**
         * Constructor used by {@link org.elasticsearch.action.admin.cluster.repositories.reservedstate.ReservedRepositoryAction}
         * @param repositoriesService
         * @param request
         */
        public RegisterRepositoryTask(final RepositoriesService repositoriesService, final PutRepositoryRequest request) {
            this(repositoriesService, request, null);
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
            RepositoriesMetadata repositories = RepositoriesMetadata.get(currentState);
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
                    Repository existing = repositoriesService.repositories.get(request.name());
                    if (existing == null) {
                        existing = repositoriesService.internalRepositories.get(request.name());
                    }
                    assert existing != null : "repository [" + newRepositoryMetadata.name() + "] must exist";
                    assert existing.getMetadata() == repositoryMetadata;
                    final RepositoryMetadata updatedMetadata;
                    if (canUpdateInPlace(newRepositoryMetadata, existing)) {
                        if (repositoryMetadata.settings().equals(newRepositoryMetadata.settings())) {
                            if (repositoryMetadata.generation() == RepositoryData.CORRUPTED_REPO_GEN) {
                                // If recreating a corrupted repository with the same settings, reset the corrupt flag.
                                // Setting the safe generation to unknown, so that a consistent generation is found.
                                ensureRepositoryNotInUse(currentState, request.name());
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
                        ensureRepositoryNotInUse(currentState, request.name());
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
            mdBuilder.putDefaultProjectCustom(RepositoriesMetadata.TYPE, repositories);
            changed = true;
            return ClusterState.builder(currentState).metadata(mdBuilder).build();
        }
    }

    /**
     * Ensures that we can create the repository and that it's creation actually works
     * <p>
     * This verification method will create and then close the repository we want to create.
     *
     * @param request
     */
    public void validateRepositoryCanBeCreated(final PutRepositoryRequest request) {
        final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(request.name(), request.type(), request.settings());

        // Trying to create the new repository on master to make sure it works
        closeRepository(createRepository(newRepositoryMetadata));
    }

    private void validatePutRepositoryRequest(final PutRepositoryRequest request, ActionListener<Void> resultListener) {
        final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(request.name(), request.type(), request.settings());
        try {
            final var repository = createRepository(newRepositoryMetadata);
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
        final String repositoryName,
        RepositoryData repositoryData,
        ActionListener<Void> listener
    ) {

        final String repositoryUuid = repositoryData.getUuid();
        if (repositoryUuid.equals(RepositoryData.MISSING_UUID)) {
            listener.onResponse(null);
            return;
        }

        final RepositoryMetadata repositoryMetadata = RepositoriesMetadata.get(clusterService.state()).repository(repositoryName);
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
            "update repository UUID [" + repositoryName + "] to [" + repositoryUuid + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final RepositoriesMetadata currentReposMetadata = RepositoriesMetadata.get(currentState);

                    final RepositoryMetadata repositoryMetadata = currentReposMetadata.repository(repositoryName);
                    if (repositoryMetadata == null || repositoryMetadata.uuid().equals(repositoryUuid)) {
                        return currentState;
                    } else {
                        final RepositoriesMetadata newReposMetadata = currentReposMetadata.withUuid(repositoryName, repositoryUuid);
                        final Metadata.Builder metadata = Metadata.builder(currentState.metadata())
                            .putDefaultProjectCustom(RepositoriesMetadata.TYPE, newReposMetadata);
                        return ClusterState.builder(currentState).metadata(metadata).build();
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
     * @param request  unregister repository request
     * @param listener unregister repository listener
     */
    public void unregisterRepository(final DeleteRepositoryRequest request, final ActionListener<AcknowledgedResponse> listener) {
        submitUnbatchedTask("delete_repository [" + request.name() + "]", new UnregisterRepositoryTask(request, listener) {
            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                if (deletedRepositories.isEmpty() == false) {
                    logger.info("deleted repositories [{}]", deletedRepositories);
                }
            }

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                // repository was created on both master and data nodes
                return discoveryNode.isMasterNode() || discoveryNode.canContainData();
            }
        });
    }

    /**
     * Task class that extracts the 'execute' part of the functionality for unregistering
     * repositories.
     */
    public static class UnregisterRepositoryTask extends AckedClusterStateUpdateTask {
        protected final List<String> deletedRepositories = new ArrayList<>();
        private final DeleteRepositoryRequest request;

        UnregisterRepositoryTask(final DeleteRepositoryRequest request, final ActionListener<AcknowledgedResponse> listener) {
            super(request, listener);
            this.request = request;
        }

        /**
         * Constructor used by {@link org.elasticsearch.action.admin.cluster.repositories.reservedstate.ReservedRepositoryAction}
         * @param name the repository name
         */
        public UnregisterRepositoryTask(TimeValue dummyTimeout, String name) {
            this(new DeleteRepositoryRequest(dummyTimeout, dummyTimeout, name), null);
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
            RepositoriesMetadata repositories = RepositoriesMetadata.get(currentState);
            if (repositories.repositories().size() > 0) {
                List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size());
                boolean changed = false;
                for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                    if (Regex.simpleMatch(request.name(), repositoryMetadata.name())) {
                        ensureRepositoryNotInUse(currentState, repositoryMetadata.name());
                        ensureNoSearchableSnapshotsIndicesInUse(currentState, repositoryMetadata);
                        deletedRepositories.add(repositoryMetadata.name());
                        changed = true;
                    } else {
                        repositoriesMetadata.add(repositoryMetadata);
                    }
                }
                if (changed) {
                    repositories = new RepositoriesMetadata(repositoriesMetadata);
                    mdBuilder.putDefaultProjectCustom(RepositoriesMetadata.TYPE, repositories);
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }
            }
            if (Regex.isMatchAllPattern(request.name())) { // we use a wildcard so we don't barf if it's not present.
                return currentState;
            }
            throw new RepositoryMissingException(request.name());
        }
    }

    public void verifyRepository(final String repositoryName, final ActionListener<List<DiscoveryNode>> listener) {
        final Repository repository = repository(repositoryName);
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
                                        logger.warn(() -> "[" + repositoryName + "] failed to finish repository verification", e);
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
                                logger.warn(() -> "[" + repositoryName + "] failed to finish repository verification", inner);
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
            assert assertReadonlyRepositoriesNotInUseForWrites(state);
            final RepositoriesMetadata oldMetadata = RepositoriesMetadata.get(event.previousState());
            final RepositoriesMetadata newMetadata = RepositoriesMetadata.get(state);

            // Check if repositories got changed
            if (oldMetadata.equalsIgnoreGenerations(newMetadata)) {
                for (Repository repo : repositories.values()) {
                    repo.updateState(state);
                }
                return;
            }

            logger.trace("processing new index repositories for state version [{}]", event.state().version());

            Map<String, Repository> survivors = new HashMap<>();
            // First, remove repositories that are no longer there
            for (Map.Entry<String, Repository> entry : repositories.entrySet()) {
                if (newMetadata.repository(entry.getKey()) == null) {
                    logger.debug("unregistering repository [{}]", entry.getKey());
                    Repository repository = entry.getValue();
                    closeRepository(repository);
                    archiveRepositoryStats(repository, state.version());
                } else {
                    survivors.put(entry.getKey(), entry.getValue());
                }
            }

            Map<String, Repository> builder = new HashMap<>();

            // Now go through all repositories and update existing or create missing
            for (RepositoryMetadata repositoryMetadata : newMetadata.repositories()) {
                Repository repository = survivors.get(repositoryMetadata.name());
                if (repository != null) {
                    // Found previous version of this repository
                    if (canUpdateInPlace(repositoryMetadata, repository) == false) {
                        // Previous version is different from the version in settings
                        logger.debug("updating repository [{}]", repositoryMetadata.name());
                        closeRepository(repository);
                        archiveRepositoryStats(repository, state.version());
                        repository = null;
                        try {
                            repository = createRepository(
                                repositoryMetadata,
                                typesRegistry,
                                RepositoriesService::createUnknownTypeRepository
                            );
                        } catch (RepositoryException ex) {
                            // TODO: this catch is bogus, it means the old repo is already closed,
                            // but we have nothing to replace it
                            logger.warn(() -> "failed to change repository [" + repositoryMetadata.name() + "]", ex);
                            repository = new InvalidRepository(state.metadata().getProject().id(), repositoryMetadata, ex);
                        }
                    }
                } else {
                    try {
                        repository = createRepository(repositoryMetadata, typesRegistry, RepositoriesService::createUnknownTypeRepository);
                    } catch (RepositoryException ex) {
                        logger.warn(() -> "failed to create repository [" + repositoryMetadata.name() + "]", ex);
                        repository = new InvalidRepository(state.metadata().getProject().id(), repositoryMetadata, ex);
                    }
                }
                assert repository != null : "repository should not be null here";
                logger.debug("registering repository [{}]", repositoryMetadata.name());
                builder.put(repositoryMetadata.name(), repository);
            }
            for (Repository repo : builder.values()) {
                repo.updateState(state);
            }
            repositories = unmodifiableMap(builder);
        } catch (Exception ex) {
            assert false : new AssertionError(ex);
            logger.warn("failure updating cluster state ", ex);
        }
    }

    private static boolean canUpdateInPlace(RepositoryMetadata updatedMetadata, Repository repository) {
        assert updatedMetadata.name().equals(repository.getMetadata().name());
        return repository.getMetadata().type().equals(updatedMetadata.type())
            && repository.canUpdateInPlace(updatedMetadata.settings(), Collections.emptySet());
    }

    /**
     * Gets the {@link RepositoryData} for the given repository.
     *
     * @param repositoryName repository name
     * @param listener       listener to pass {@link RepositoryData} to
     */
    public void getRepositoryData(final String repositoryName, final ActionListener<RepositoryData> listener) {
        try {
            Repository repository = repository(repositoryName);
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
     * Returns registered repository
     *
     * @param repositoryName repository name
     * @return registered repository
     * @throws RepositoryMissingException if repository with such name isn't registered
     */
    public Repository repository(String repositoryName) {
        Repository repository = repositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }
        repository = internalRepositories.get(repositoryName);
        if (repository != null) {
            return repository;
        }
        throw new RepositoryMissingException(repositoryName);
    }

    /**
     * @return the current collection of registered repositories, keyed by name.
     */
    public Map<String, Repository> getRepositories() {
        return unmodifiableMap(repositories);
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
            repositories.values()
                .stream()
                .collect(
                    Collectors.toMap(
                        r -> r.getMetadata().name(),
                        r -> new RepositoriesStats.ThrottlingStats(r.getRestoreThrottleTimeInNanos(), r.getSnapshotThrottleTimeInNanos())
                    )
                )
        );
    }

    private List<RepositoryStatsSnapshot> getRepositoryStatsForActiveRepositories() {
        return Stream.concat(repositories.values().stream(), internalRepositories.values().stream())
            .filter(r -> r instanceof MeteredBlobStoreRepository)
            .map(r -> (MeteredBlobStoreRepository) r)
            .map(MeteredBlobStoreRepository::statsSnapshot)
            .toList();
    }

    public List<RepositoryStatsSnapshot> clearRepositoriesStatsArchive(long maxVersionToClear) {
        return repositoriesStatsArchive.clear(maxVersionToClear);
    }

    public void registerInternalRepository(String name, String type) {
        RepositoryMetadata metadata = new RepositoryMetadata(name, type, Settings.EMPTY);
        Repository repository = internalRepositories.computeIfAbsent(name, (n) -> {
            logger.debug("put internal repository [{}][{}]", name, type);
            return createRepository(metadata, internalTypesRegistry, RepositoriesService::throwRepositoryTypeDoesNotExists);
        });
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
        } else if (repositories.containsKey(name)) {
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

    public void unregisterInternalRepository(String name) {
        Repository repository = internalRepositories.remove(name);
        if (repository != null) {
            RepositoryMetadata metadata = repository.getMetadata();
            logger.debug(() -> format("delete internal repository [%s][%s].", metadata.type(), name));
            closeRepository(repository);
        }
    }

    /**
     * Closes the given repository.
     */
    private static void closeRepository(Repository repository) {
        logger.debug("closing repository [{}][{}]", repository.getMetadata().type(), repository.getMetadata().name());
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
    @FixForMultiProject(description = "resolve the actual ProjectId")
    @Deprecated(forRemoval = true)
    private static Repository createRepository(
        RepositoryMetadata repositoryMetadata,
        Map<String, Repository.Factory> factories,
        BiFunction<ProjectId, RepositoryMetadata, Repository> defaultFactory
    ) {
        return createRepository(ProjectId.DEFAULT, repositoryMetadata, factories, defaultFactory);
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
     * @param repositoryMetadata the repository metadata
     * @return the started repository
     * @throws RepositoryException if repository type is not registered
     */
    @FixForMultiProject(description = "resolve the actual ProjectId")
    @Deprecated(forRemoval = true)
    public Repository createRepository(RepositoryMetadata repositoryMetadata) {
        return createRepository(ProjectId.DEFAULT, repositoryMetadata);
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

    private static void ensureRepositoryNotInUseForWrites(ClusterState clusterState, String repository) {
        if (SnapshotsInProgress.get(clusterState).forRepo(repository).isEmpty() == false) {
            throw newRepositoryConflictException(repository, "snapshot is in progress");
        }
        for (SnapshotDeletionsInProgress.Entry entry : SnapshotDeletionsInProgress.get(clusterState).getEntries()) {
            if (entry.repository().equals(repository)) {
                throw newRepositoryConflictException(repository, "snapshot deletion is in progress");
            }
        }
        for (RepositoryCleanupInProgress.Entry entry : RepositoryCleanupInProgress.get(clusterState).entries()) {
            if (entry.repository().equals(repository)) {
                throw newRepositoryConflictException(repository, "repository clean up is in progress");
            }
        }
    }

    private static void ensureRepositoryNotInUse(ClusterState clusterState, String repository) {
        ensureRepositoryNotInUseForWrites(clusterState, repository);
        for (RestoreInProgress.Entry entry : RestoreInProgress.get(clusterState)) {
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
    private static boolean assertReadonlyRepositoriesNotInUseForWrites(ClusterState clusterState) {
        for (final var repositoryMetadata : RepositoriesMetadata.get(clusterState).repositories()) {
            if (isReadOnly(repositoryMetadata.settings())) {
                try {
                    ensureRepositoryNotInUseForWrites(clusterState, repositoryMetadata.name());
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
        for (final var repository : repositories.values()) {
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
        repos.addAll(internalRepositories.values());
        repos.addAll(repositories.values());
        IOUtils.close(repos);
        for (Repository repo : repos) {
            repo.awaitIdle();
        }
    }
}
