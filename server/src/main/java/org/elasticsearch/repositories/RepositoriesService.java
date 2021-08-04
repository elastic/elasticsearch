/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.isSearchableSnapshotStore;

/**
 * Service responsible for maintaining and providing access to snapshot repositories on nodes.
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

    private final VerifyNodeRepositoryAction verifyAction;

    private final Map<String, Repository> internalRepositories = ConcurrentCollections.newConcurrentMap();
    private volatile Map<String, Repository> repositories = Collections.emptyMap();
    private final RepositoriesStatsArchive repositoriesStatsArchive;

    public RepositoriesService(
        Settings settings,
        ClusterService clusterService,
        TransportService transportService,
        Map<String, Repository.Factory> typesRegistry,
        Map<String, Repository.Factory> internalTypesRegistry,
        ThreadPool threadPool
    ) {
        this.typesRegistry = typesRegistry;
        this.internalTypesRegistry = internalTypesRegistry;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // Doesn't make sense to maintain repositories on non-master and non-data nodes
        // Nothing happens there anyway
        if (DiscoveryNode.canContainData(settings) || DiscoveryNode.isMasterNode(settings)) {
            if (isDedicatedVotingOnlyNode(DiscoveryNode.getRolesFromSettings(settings)) == false) {
                clusterService.addHighPriorityApplier(this);
            }
        }
        this.verifyAction = new VerifyNodeRepositoryAction(transportService, clusterService, this);
        this.repositoriesStatsArchive = new RepositoriesStatsArchive(
            REPOSITORIES_STATS_ARCHIVE_RETENTION_PERIOD.get(settings),
            REPOSITORIES_STATS_ARCHIVE_MAX_ARCHIVED_STATS.get(settings),
            threadPool::relativeTimeInMillis
        );
    }

    /**
     * Registers new repository in the cluster
     * <p>
     * This method can be only called on the master node. It tries to create a new repository on the master
     * and if it was successful it adds new repository to cluster metadata.
     *
     * @param request  register repository request
     * @param listener register repository listener
     */
    public void registerRepository(final PutRepositoryRequest request, final ActionListener<AcknowledgedResponse> listener) {
        assert lifecycle.started() : "Trying to register new repository but service is in state [" + lifecycle.state() + "]";

        final RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(request.name(), request.type(), request.settings());
        validate(request.name());

        // Trying to create the new repository on master to make sure it works
        try {
            closeRepository(createRepository(newRepositoryMetadata, typesRegistry));
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        final StepListener<AcknowledgedResponse> acknowledgementStep = new StepListener<>();
        final StepListener<Boolean> publicationStep = new StepListener<>(); // Boolean==changed.

        if (request.verify()) {

            // When publication has completed (and all acks received or timed out) then verify the repository.
            // (if acks timed out then acknowledgementStep completes before the master processes this cluster state, hence why we have
            // to wait for the publication to be complete too)
            final StepListener<List<DiscoveryNode>> verifyStep = new StepListener<>();
            publicationStep.whenComplete(changed -> acknowledgementStep.whenComplete(clusterStateUpdateResponse -> {
                if (clusterStateUpdateResponse.isAcknowledged() && changed) {
                    // The response was acknowledged - all nodes should know about the new repository, let's verify them
                    verifyRepository(request.name(), verifyStep);
                } else {
                    verifyStep.onResponse(null);
                }
            }, listener::onFailure), listener::onFailure);

            // When verification has completed, get the repository data for the first time
            final StepListener<RepositoryData> getRepositoryDataStep = new StepListener<>();
            verifyStep.whenComplete(
                ignored -> threadPool.generic()
                    .execute(ActionRunnable.wrap(getRepositoryDataStep, l -> repository(request.name()).getRepositoryData(l))),
                listener::onFailure
            );

            // When the repository metadata is ready, update the repository UUID stored in the cluster state, if available
            final StepListener<Void> updateRepoUuidStep = new StepListener<>();
            getRepositoryDataStep.whenComplete(
                repositoryData -> updateRepositoryUuidInMetadata(clusterService, request.name(), repositoryData, updateRepoUuidStep),
                listener::onFailure
            );

            // Finally respond to the outer listener with the response from the original cluster state update
            updateRepoUuidStep.whenComplete(ignored -> acknowledgementStep.addListener(listener), listener::onFailure);

        } else {
            acknowledgementStep.addListener(listener);
        }

        clusterService.submitStateUpdateTask(
            "put_repository [" + request.name() + "]",
            new AckedClusterStateUpdateTask(request, acknowledgementStep) {

                private boolean found = false;
                private boolean changed = false;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                    RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
                    List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>(repositories.repositories().size() + 1);
                    for (RepositoryMetadata repositoryMetadata : repositories.repositories()) {
                        if (repositoryMetadata.name().equals(newRepositoryMetadata.name())) {
                            Repository existing = RepositoriesService.this.repositories.get(request.name());
                            if (existing == null) {
                                existing = RepositoriesService.this.internalRepositories.get(request.name());
                            }
                            assert existing != null : "repository [" + newRepositoryMetadata.name() + "] must exist";
                            assert existing.getMetadata() == repositoryMetadata;
                            final RepositoryMetadata updatedMetadata;
                            if (canUpdateInPlace(newRepositoryMetadata, existing)) {
                                if (repositoryMetadata.settings().equals(newRepositoryMetadata.settings())) {
                                    // Previous version is the same as this one no update is needed.
                                    return currentState;
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
                    mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
                    changed = true;
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", request.name()), e);
                    publicationStep.onFailure(e);
                    super.onFailure(source, e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // repository is created on both master and data nodes
                    return discoveryNode.isMasterNode() || discoveryNode.canContainData();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
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

        final RepositoriesMetadata currentReposMetadata = clusterService.state()
            .metadata()
            .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        final RepositoryMetadata repositoryMetadata = currentReposMetadata.repository(repositoryName);
        if (repositoryMetadata == null || repositoryMetadata.uuid().equals(repositoryUuid)) {
            listener.onResponse(null);
            return;
        }

        clusterService.submitStateUpdateTask(
            "update repository UUID [" + repositoryName + "] to [" + repositoryUuid + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final RepositoriesMetadata currentReposMetadata = currentState.metadata()
                        .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);

                    final RepositoryMetadata repositoryMetadata = currentReposMetadata.repository(repositoryName);
                    if (repositoryMetadata == null || repositoryMetadata.uuid().equals(repositoryUuid)) {
                        return currentState;
                    } else {
                        final RepositoriesMetadata newReposMetadata = currentReposMetadata.withUuid(repositoryName, repositoryUuid);
                        final Metadata.Builder metadata = Metadata.builder(currentState.metadata())
                            .putCustom(RepositoriesMetadata.TYPE, newReposMetadata);
                        return ClusterState.builder(currentState).metadata(metadata).build();
                    }
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
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
        clusterService.submitStateUpdateTask(
            "delete_repository [" + request.name() + "]",
            new AckedClusterStateUpdateTask(request, listener) {

                private final List<String> deletedRepositories = new ArrayList<>();

                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                    RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
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
                            mdBuilder.putCustom(RepositoriesMetadata.TYPE, repositories);
                            return ClusterState.builder(currentState).metadata(mdBuilder).build();
                        }
                    }
                    if (Regex.isMatchAllPattern(request.name())) { // we use a wildcard so we don't barf if it's not present.
                        return currentState;
                    }
                    throw new RepositoryMissingException(request.name());
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    if (deletedRepositories.isEmpty() == false) {
                        logger.info("deleted repositories [{}]", deletedRepositories);
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

    public void verifyRepository(final String repositoryName, final ActionListener<List<DiscoveryNode>> listener) {
        final Repository repository = repository(repositoryName);
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                final String verificationToken = repository.startVerification();
                if (verificationToken != null) {
                    try {
                        verifyAction.verify(
                            repositoryName,
                            verificationToken,
                            listener.delegateFailure(
                                (delegatedListener, verifyResponse) -> threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                                    try {
                                        repository.endVerification(verificationToken);
                                    } catch (Exception e) {
                                        logger.warn(
                                            () -> new ParameterizedMessage("[{}] failed to finish repository verification", repositoryName),
                                            e
                                        );
                                        delegatedListener.onFailure(e);
                                        return;
                                    }
                                    delegatedListener.onResponse(verifyResponse);
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
                                    () -> new ParameterizedMessage("[{}] failed to finish repository verification", repositoryName),
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
            final RepositoriesMetadata oldMetadata = event.previousState()
                .getMetadata()
                .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
            final RepositoriesMetadata newMetadata = state.getMetadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);

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
                            repository = createRepository(repositoryMetadata, typesRegistry);
                        } catch (RepositoryException ex) {
                            // TODO: this catch is bogus, it means the old repo is already closed,
                            // but we have nothing to replace it
                            logger.warn(() -> new ParameterizedMessage("failed to change repository [{}]", repositoryMetadata.name()), ex);
                        }
                    }
                } else {
                    try {
                        repository = createRepository(repositoryMetadata, typesRegistry);
                    } catch (RepositoryException ex) {
                        logger.warn(() -> new ParameterizedMessage("failed to create repository [{}]", repositoryMetadata.name()), ex);
                    }
                }
                if (repository != null) {
                    logger.debug("registering repository [{}]", repositoryMetadata.name());
                    builder.put(repositoryMetadata.name(), repository);
                }
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

    private boolean canUpdateInPlace(RepositoryMetadata updatedMetadata, Repository repository) {
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
            repository.getRepositoryData(listener);
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

    private List<RepositoryStatsSnapshot> getRepositoryStatsForActiveRepositories() {
        return Stream.concat(repositories.values().stream(), internalRepositories.values().stream())
            .filter(r -> r instanceof MeteredBlobStoreRepository)
            .map(r -> (MeteredBlobStoreRepository) r)
            .map(MeteredBlobStoreRepository::statsSnapshot)
            .collect(Collectors.toList());
    }

    public List<RepositoryStatsSnapshot> clearRepositoriesStatsArchive(long maxVersionToClear) {
        return repositoriesStatsArchive.clear(maxVersionToClear);
    }

    public void registerInternalRepository(String name, String type) {
        RepositoryMetadata metadata = new RepositoryMetadata(name, type, Settings.EMPTY);
        Repository repository = internalRepositories.computeIfAbsent(name, (n) -> {
            logger.debug("put internal repository [{}][{}]", name, type);
            return createRepository(metadata, internalTypesRegistry);
        });
        if (type.equals(repository.getMetadata().type()) == false) {
            logger.warn(
                new ParameterizedMessage(
                    "internal repository [{}][{}] already registered. this prevented the registration of "
                        + "internal repository [{}][{}].",
                    name,
                    repository.getMetadata().type(),
                    name,
                    type
                )
            );
        } else if (repositories.containsKey(name)) {
            logger.warn(
                new ParameterizedMessage(
                    "non-internal repository [{}] already registered. this repository will block the "
                        + "usage of internal repository [{}][{}].",
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
            logger.debug(() -> new ParameterizedMessage("delete internal repository [{}][{}].", metadata.type(), name));
            closeRepository(repository);
        }
    }

    /**
     * Closes the given repository.
     */
    private void closeRepository(Repository repository) {
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
    private Repository createRepository(RepositoryMetadata repositoryMetadata, Map<String, Repository.Factory> factories) {
        logger.debug("creating repository [{}][{}]", repositoryMetadata.type(), repositoryMetadata.name());
        Repository.Factory factory = factories.get(repositoryMetadata.type());
        if (factory == null) {
            throw new RepositoryException(repositoryMetadata.name(), "repository type [" + repositoryMetadata.type() + "] does not exist");
        }
        Repository repository = null;
        try {
            repository = factory.create(repositoryMetadata, factories::get);
            repository.start();
            return repository;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(repository);
            logger.warn(
                new ParameterizedMessage("failed to create repository [{}][{}]", repositoryMetadata.type(), repositoryMetadata.name()),
                e
            );
            throw new RepositoryException(repositoryMetadata.name(), "failed to create repository", e);
        }
    }

    private static void validate(final String repositoryName) {
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

    private static void ensureRepositoryNotInUse(ClusterState clusterState, String repository) {
        final SnapshotsInProgress snapshots = clusterState.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
        for (SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
            if (repository.equals(snapshot.snapshot().getRepository())) {
                throw newRepositoryInUseException(repository, "snapshot is in progress");
            }
        }
        for (SnapshotDeletionsInProgress.Entry entry : clusterState.custom(
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress.EMPTY
        ).getEntries()) {
            if (entry.repository().equals(repository)) {
                throw newRepositoryInUseException(repository, "snapshot deletion is in progress");
            }
        }
        for (RepositoryCleanupInProgress.Entry entry : clusterState.custom(
            RepositoryCleanupInProgress.TYPE,
            RepositoryCleanupInProgress.EMPTY
        ).entries()) {
            if (entry.repository().equals(repository)) {
                throw newRepositoryInUseException(repository, "repository clean up is in progress");
            }
        }
        for (RestoreInProgress.Entry entry : clusterState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            if (repository.equals(entry.snapshot().getRepository())) {
                throw newRepositoryInUseException(repository, "snapshot restore is in progress");
            }
        }
    }

    private static void ensureNoSearchableSnapshotsIndicesInUse(ClusterState clusterState, RepositoryMetadata repositoryMetadata) {
        long count = 0L;
        List<Index> indices = null;
        for (IndexMetadata indexMetadata : clusterState.metadata()) {
            if (indexSettingsMatchRepositoryMetadata(indexMetadata.getSettings(), repositoryMetadata)) {
                if (indices == null) {
                    indices = new ArrayList<>();
                }
                if (indices.size() < 5) {
                    indices.add(indexMetadata.getIndex());
                }
                count += 1L;
            }
        }
        if (indices != null && indices.isEmpty() == false) {
            throw newRepositoryInUseException(
                repositoryMetadata.name(),
                "found "
                    + count
                    + " searchable snapshots indices that use the repository: "
                    + Strings.collectionToCommaDelimitedString(indices)
                    + (count > indices.size() ? ",..." : "")
            );
        }
    }

    private static boolean indexSettingsMatchRepositoryMetadata(Settings indexSettings, RepositoryMetadata repositoryMetadata) {
        if (isSearchableSnapshotStore(indexSettings)) {
            final String indexRepositoryUuid = indexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY);
            if (Strings.hasLength(indexRepositoryUuid)) {
                return Objects.equals(repositoryMetadata.uuid(), indexRepositoryUuid);
            } else {
                return Objects.equals(repositoryMetadata.name(), indexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY));
            }
        }
        return false;
    }

    private static IllegalStateException newRepositoryInUseException(String repository, String reason) {
        return new IllegalStateException(
            "trying to modify or unregister repository [" + repository + "] that is currently used (" + reason + ')'
        );
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {
        clusterService.removeApplier(this);
        final Collection<Repository> repos = new ArrayList<>();
        repos.addAll(internalRepositories.values());
        repos.addAll(repositories.values());
        IOUtils.close(repos);
    }
}
