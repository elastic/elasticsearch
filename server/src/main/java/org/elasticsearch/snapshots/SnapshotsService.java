/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

/**
 * Service responsible for creating snapshots
 * <p>
 * A typical snapshot creating process looks like this:
 * <ul>
 * <li>On the master node the {@link #createSnapshot(CreateSnapshotRequest, ActionListener)} is called and makes sure that
 * no snapshot is currently running and registers the new snapshot in cluster state</li>
 * <li>When cluster state is updated
 * the {@link #beginSnapshot(ClusterState, SnapshotsInProgress.Entry, boolean, ActionListener)} method kicks in and initializes
 * the snapshot in the repository and then populates list of shards that needs to be snapshotted in cluster state</li>
 * <li>Each data node is watching for these shards and when new shards scheduled for snapshotting appear in the cluster state, data nodes
 * start processing them through {@link SnapshotShardsService#startNewSnapshots} method</li>
 * <li>Once shard snapshot is created data node updates state of the shard in the cluster state using
 * the {@link SnapshotShardsService#sendSnapshotShardUpdate(Snapshot, ShardId, ShardSnapshotStatus)} method</li>
 * <li>When last shard is completed master node in {@link SnapshotShardsService#innerUpdateSnapshotState} method marks the snapshot
 * as completed</li>
 * <li>After cluster state is updated, the {@link #endSnapshot(SnapshotsInProgress.Entry, MetaData)} finalizes snapshot in the repository,
 * notifies all {@link #snapshotCompletionListeners} that snapshot is completed, and finally calls
 * {@link #removeSnapshotFromClusterState(Snapshot, SnapshotInfo, Exception)} to remove snapshot from cluster state</li>
 * </ul>
 */
public class SnapshotsService extends AbstractLifecycleComponent implements ClusterStateApplier {

    public static final Version SHARD_GEN_IN_REPO_DATA_VERSION = Version.V_7_6_0;

    private static final Logger logger = LogManager.getLogger(SnapshotsService.class);

    private final ClusterService clusterService;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final RepositoriesService repositoriesService;

    private final ThreadPool threadPool;

    private final Map<Snapshot, List<ActionListener<SnapshotInfo>>> snapshotCompletionListeners = new ConcurrentHashMap<>();

    // Set of snapshots that are currently being initialized by this node
    private final Set<Snapshot> initializingSnapshots = Collections.synchronizedSet(new HashSet<>());

    // Set of snapshots that are currently being ended by this node
    private final Set<Snapshot> endingSnapshots = Collections.synchronizedSet(new HashSet<>());

    public SnapshotsService(Settings settings, ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver,
                            RepositoriesService repositoriesService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;

        if (DiscoveryNode.isMasterNode(settings)) {
            // addLowPriorityApplier to make sure that Repository will be created before snapshot
            clusterService.addLowPriorityApplier(this);
        }
    }

    /**
     * Gets the {@link RepositoryData} for the given repository.
     *
     * @param repositoryName repository name
     * @param listener       listener to pass {@link RepositoryData} to
     */
    public void getRepositoryData(final String repositoryName, final ActionListener<RepositoryData> listener) {
        try {
            Repository repository = repositoriesService.repository(repositoryName);
            assert repository != null; // should only be called once we've validated the repository exists
            repository.getRepositoryData(listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Retrieves snapshot from repository
     *
     * @param repositoryName  repository name
     * @param snapshotId      snapshot id
     * @return snapshot
     * @throws SnapshotMissingException if snapshot is not found
     */
    public SnapshotInfo snapshot(final String repositoryName, final SnapshotId snapshotId) {
        List<SnapshotsInProgress.Entry> entries = currentSnapshots(repositoryName, Collections.singletonList(snapshotId.getName()));
        if (!entries.isEmpty()) {
            return inProgressSnapshot(entries.iterator().next());
        }
        return repositoriesService.repository(repositoryName).getSnapshotInfo(snapshotId);
    }

    /**
     * Returns a list of snapshots from repository sorted by snapshot creation date
     *
     * @param repositoryName repository name
     * @param snapshotIds       snapshots for which to fetch snapshot information
     * @param ignoreUnavailable if true, snapshots that could not be read will only be logged with a warning,
     *                          if false, they will throw an error
     * @return list of snapshots
     */
    public List<SnapshotInfo> snapshots(final String repositoryName, final List<SnapshotId> snapshotIds, final boolean ignoreUnavailable) {
        final Set<SnapshotInfo> snapshotSet = new HashSet<>();
        final Set<SnapshotId> snapshotIdsToIterate = new HashSet<>(snapshotIds);
        // first, look at the snapshots in progress
        final List<SnapshotsInProgress.Entry> entries =
            currentSnapshots(repositoryName, snapshotIdsToIterate.stream().map(SnapshotId::getName).collect(Collectors.toList()));
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotSet.add(inProgressSnapshot(entry));
            snapshotIdsToIterate.remove(entry.snapshot().getSnapshotId());
        }
        // then, look in the repository
        final Repository repository = repositoriesService.repository(repositoryName);
        for (SnapshotId snapshotId : snapshotIdsToIterate) {
            try {
                snapshotSet.add(repository.getSnapshotInfo(snapshotId));
            } catch (Exception ex) {
                if (ignoreUnavailable) {
                    logger.warn(() -> new ParameterizedMessage("failed to get snapshot [{}]", snapshotId), ex);
                } else {
                    if (ex instanceof SnapshotException) {
                        throw ex;
                    }
                    throw new SnapshotException(repositoryName, snapshotId, "Snapshot could not be read", ex);
                }
            }
        }
        final ArrayList<SnapshotInfo> snapshotList = new ArrayList<>(snapshotSet);
        CollectionUtil.timSort(snapshotList);
        return unmodifiableList(snapshotList);
    }

    /**
     * Returns a list of currently running snapshots from repository sorted by snapshot creation date
     *
     * @param repositoryName repository name
     * @return list of snapshots
     */
    public List<SnapshotInfo> currentSnapshots(final String repositoryName) {
        List<SnapshotInfo> snapshotList = new ArrayList<>();
        List<SnapshotsInProgress.Entry> entries = currentSnapshots(repositoryName, Collections.emptyList());
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotList.add(inProgressSnapshot(entry));
        }
        CollectionUtil.timSort(snapshotList);
        return unmodifiableList(snapshotList);
    }

    /**
     * Same as {@link #createSnapshot(CreateSnapshotRequest, ActionListener)} but invokes its callback on completion of
     * the snapshot.
     *
     * @param request snapshot request
     * @param listener snapshot completion listener
     */
    public void executeSnapshot(final CreateSnapshotRequest request, final ActionListener<SnapshotInfo> listener) {
        createSnapshot(request, ActionListener.wrap(snapshot -> addListener(snapshot, listener), listener::onFailure));
    }

    /**
     * Initializes the snapshotting process.
     * <p>
     * This method is used by clients to start snapshot. It makes sure that there is no snapshots are currently running and
     * creates a snapshot record in cluster state metadata.
     *
     * @param request  snapshot request
     * @param listener snapshot creation listener
     */
    public void createSnapshot(final CreateSnapshotRequest request, final ActionListener<Snapshot> listener) {
        final String repositoryName = request.repository();
        final String snapshotName = indexNameExpressionResolver.resolveDateMathExpression(request.snapshot());
        validate(repositoryName, snapshotName);
        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID()); // new UUID for the snapshot
        final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
        repositoriesService.repository(repositoryName).getRepositoryData(repositoryDataListener);
        repositoryDataListener.whenComplete(repositoryData -> {
            clusterService.submitStateUpdateTask("create_snapshot [" + snapshotName + ']', new ClusterStateUpdateTask() {

                private SnapshotsInProgress.Entry newSnapshot = null;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    validate(repositoryName, snapshotName, currentState);
                    SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                    if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                        throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName,
                            "cannot snapshot while a snapshot deletion is in-progress in [" + deletionsInProgress + "]");
                    }
                    final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
                    if (repositoryCleanupInProgress != null && repositoryCleanupInProgress.hasCleanupInProgress()) {
                        throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName,
                            "cannot snapshot while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]");
                    }
                    SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                    if (snapshots == null || snapshots.entries().isEmpty()) {
                        // Store newSnapshot here to be processed in clusterStateProcessed
                        List<String> indices = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(currentState,
                            request.indicesOptions(), request.indices()));
                        logger.trace("[{}][{}] creating snapshot for indices [{}]", repositoryName, snapshotName, indices);
                        List<IndexId> snapshotIndices = repositoryData.resolveNewIndices(indices);
                        newSnapshot = new SnapshotsInProgress.Entry(
                            new Snapshot(repositoryName, snapshotId),
                            request.includeGlobalState(), request.partial(),
                            State.INIT,
                            snapshotIndices,
                            threadPool.absoluteTimeInMillis(),
                            repositoryData.getGenId(),
                            null,
                            request.userMetadata(),
                            clusterService.state().nodes().getMinNodeVersion().onOrAfter(SHARD_GEN_IN_REPO_DATA_VERSION));
                        initializingSnapshots.add(newSnapshot.snapshot());
                        snapshots = new SnapshotsInProgress(newSnapshot);
                    } else {
                        throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName, " a snapshot is already running");
                    }
                    return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to create snapshot", repositoryName, snapshotName), e);
                    if (newSnapshot != null) {
                        initializingSnapshots.remove(newSnapshot.snapshot());
                    }
                    newSnapshot = null;
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                    if (newSnapshot != null) {
                        final Snapshot current = newSnapshot.snapshot();
                        assert initializingSnapshots.contains(current);
                        beginSnapshot(newState, newSnapshot, request.partial(), new ActionListener<>() {
                            @Override
                            public void onResponse(final Snapshot snapshot) {
                                initializingSnapshots.remove(snapshot);
                                listener.onResponse(snapshot);
                            }

                            @Override
                            public void onFailure(final Exception e) {
                                initializingSnapshots.remove(current);
                                listener.onFailure(e);
                            }
                        });
                    }
                }

                @Override
                public TimeValue timeout() {
                    return request.masterNodeTimeout();
                }
            });
        }, listener::onFailure);
    }

    /**
     * Validates snapshot request
     *
     * @param repositoryName repository name
     * @param snapshotName snapshot name
     * @param state   current cluster state
     */
    private static void validate(String repositoryName, String snapshotName, ClusterState state) {
        RepositoriesMetaData repositoriesMetaData = state.getMetaData().custom(RepositoriesMetaData.TYPE);
        if (repositoriesMetaData == null || repositoriesMetaData.repository(repositoryName) == null) {
            throw new RepositoryMissingException(repositoryName);
        }
        validate(repositoryName, snapshotName);
    }

    private static void validate(final String repositoryName, final String snapshotName) {
        if (Strings.hasLength(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "cannot be empty");
        }
        if (snapshotName.contains(" ")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain whitespace");
        }
        if (snapshotName.contains(",")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain ','");
        }
        if (snapshotName.contains("#")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain '#'");
        }
        if (snapshotName.charAt(0) == '_') {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not start with '_'");
        }
        if (snapshotName.toLowerCase(Locale.ROOT).equals(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must be lowercase");
        }
        if (Strings.validFileName(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName,
                                                   snapshotName,
                                                   "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
    }

    /**
     * Starts snapshot.
     * <p>
     * Creates snapshot in repository and updates snapshot metadata record with list of shards that needs to be processed.
     *
     * @param clusterState               cluster state
     * @param snapshot                   snapshot meta data
     * @param partial                    allow partial snapshots
     * @param userCreateSnapshotListener listener
     */
    private void beginSnapshot(final ClusterState clusterState,
                               final SnapshotsInProgress.Entry snapshot,
                               final boolean partial,
                               final ActionListener<Snapshot> userCreateSnapshotListener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {

            boolean snapshotCreated;

            boolean hadAbortedInitializations;

            @Override
            protected void doRun() {
                assert initializingSnapshots.contains(snapshot.snapshot());
                Repository repository = repositoriesService.repository(snapshot.snapshot().getRepository());

                if (repository.isReadOnly()) {
                    throw new RepositoryException(repository.getMetadata().name(), "cannot create snapshot in a readonly repository");
                }
                final String snapshotName = snapshot.snapshot().getSnapshotId().getName();
                final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
                repository.getRepositoryData(repositoryDataListener);
                repositoryDataListener.whenComplete(repositoryData -> {
                    // check if the snapshot name already exists in the repository
                    if (repositoryData.getSnapshotIds().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
                        throw new InvalidSnapshotNameException(
                            repository.getMetadata().name(), snapshotName, "snapshot with the same name already exists");
                    }
                    snapshotCreated = true;

                    logger.info("snapshot [{}] started", snapshot.snapshot());
                    if (snapshot.indices().isEmpty()) {
                        // No indices in this snapshot - we are done
                        userCreateSnapshotListener.onResponse(snapshot.snapshot());
                        endSnapshot(snapshot, clusterState.metaData());
                        return;
                    }
                    clusterService.submitStateUpdateTask("update_snapshot [" + snapshot.snapshot() + "]", new ClusterStateUpdateTask() {

                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                            List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                            for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                                if (entry.snapshot().equals(snapshot.snapshot()) == false) {
                                    entries.add(entry);
                                    continue;
                                }

                                if (entry.state() == State.ABORTED) {
                                    entries.add(entry);
                                    assert entry.shards().isEmpty();
                                    hadAbortedInitializations = true;
                                } else {
                                    // Replace the snapshot that was just initialized
                                    ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards = shards(currentState, entry, repositoryData);
                                    if (!partial) {
                                        Tuple<Set<String>, Set<String>> indicesWithMissingShards = indicesWithMissingShards(shards,
                                            currentState.metaData());
                                        Set<String> missing = indicesWithMissingShards.v1();
                                        Set<String> closed = indicesWithMissingShards.v2();
                                        if (missing.isEmpty() == false || closed.isEmpty() == false) {
                                            final StringBuilder failureMessage = new StringBuilder();
                                            if (missing.isEmpty() == false) {
                                                failureMessage.append("Indices don't have primary shards ");
                                                failureMessage.append(missing);
                                            }
                                            if (closed.isEmpty() == false) {
                                                if (failureMessage.length() > 0) {
                                                    failureMessage.append("; ");
                                                }
                                                failureMessage.append("Indices are closed ");
                                                failureMessage.append(closed);
                                            }
                                            entries.add(
                                                new SnapshotsInProgress.Entry(entry, State.FAILED, shards, failureMessage.toString()));
                                            continue;
                                        }
                                    }
                                    entries.add(new SnapshotsInProgress.Entry(entry, State.STARTED, shards));
                                }
                            }
                            return ClusterState.builder(currentState)
                                .putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(unmodifiableList(entries)))
                                .build();
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.warn(() -> new ParameterizedMessage("[{}] failed to create snapshot",
                                snapshot.snapshot().getSnapshotId()), e);
                            removeSnapshotFromClusterState(snapshot.snapshot(), null, e,
                                new CleanupAfterErrorListener(snapshot, true, userCreateSnapshotListener, e));
                        }

                        @Override
                        public void onNoLongerMaster(String source) {
                            // We are not longer a master - we shouldn't try to do any cleanup
                            // The new master will take care of it
                            logger.warn("[{}] failed to create snapshot - no longer a master", snapshot.snapshot().getSnapshotId());
                            userCreateSnapshotListener.onFailure(
                                new SnapshotException(snapshot.snapshot(), "master changed during snapshot initialization"));
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            // The userCreateSnapshotListener.onResponse() notifies caller that the snapshot was accepted
                            // for processing. If client wants to wait for the snapshot completion, it can register snapshot
                            // completion listener in this method. For the snapshot completion to work properly, the snapshot
                            // should still exist when listener is registered.
                            userCreateSnapshotListener.onResponse(snapshot.snapshot());

                            if (hadAbortedInitializations) {
                                final SnapshotsInProgress snapshotsInProgress = newState.custom(SnapshotsInProgress.TYPE);
                                assert snapshotsInProgress != null;
                                final SnapshotsInProgress.Entry entry = snapshotsInProgress.snapshot(snapshot.snapshot());
                                assert entry != null;
                                endSnapshot(entry, newState.metaData());
                            }
                        }
                    });
                }, this::onFailure);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to create snapshot [{}]",
                    snapshot.snapshot().getSnapshotId()), e);
                removeSnapshotFromClusterState(snapshot.snapshot(), null, e,
                    new CleanupAfterErrorListener(snapshot, snapshotCreated, userCreateSnapshotListener, e));
            }
        });
    }

    private class CleanupAfterErrorListener implements ActionListener<SnapshotInfo> {

        private final SnapshotsInProgress.Entry snapshot;
        private final boolean snapshotCreated;
        private final ActionListener<Snapshot> userCreateSnapshotListener;
        private final Exception e;

        CleanupAfterErrorListener(SnapshotsInProgress.Entry snapshot, boolean snapshotCreated,
                                  ActionListener<Snapshot> userCreateSnapshotListener, Exception e) {
            this.snapshot = snapshot;
            this.snapshotCreated = snapshotCreated;
            this.userCreateSnapshotListener = userCreateSnapshotListener;
            this.e = e;
        }

        @Override
        public void onResponse(SnapshotInfo snapshotInfo) {
            cleanupAfterError(this.e);
        }

        @Override
        public void onFailure(Exception e) {
            e.addSuppressed(this.e);
            cleanupAfterError(e);
        }

        public void onNoLongerMaster() {
            userCreateSnapshotListener.onFailure(e);
        }

        private void cleanupAfterError(Exception exception) {
            threadPool.generic().execute(() -> {
                if (snapshotCreated) {
                    final MetaData metaData = clusterService.state().metaData();
                    repositoriesService.repository(snapshot.snapshot().getRepository())
                        .finalizeSnapshot(snapshot.snapshot().getSnapshotId(),
                            buildGenerations(snapshot, metaData),
                            snapshot.startTime(),
                            ExceptionsHelper.stackTrace(exception),
                            0,
                            Collections.emptyList(),
                            snapshot.repositoryStateId(),
                            snapshot.includeGlobalState(),
                            metaDataForSnapshot(snapshot, metaData),
                            snapshot.userMetadata(),
                            snapshot.useShardGenerations(),
                            ActionListener.runAfter(ActionListener.wrap(ignored -> {
                            }, inner -> {
                                inner.addSuppressed(exception);
                                logger.warn(() -> new ParameterizedMessage("[{}] failed to finalize snapshot in repository",
                                    snapshot.snapshot()), inner);
                            }), () -> userCreateSnapshotListener.onFailure(e)));
                } else {
                    userCreateSnapshotListener.onFailure(e);
                }
            });
        }
    }

    private static ShardGenerations buildGenerations(SnapshotsInProgress.Entry snapshot, MetaData metaData) {
        ShardGenerations.Builder builder = ShardGenerations.builder();
        final Map<String, IndexId> indexLookup = new HashMap<>();
        snapshot.indices().forEach(idx -> indexLookup.put(idx.getName(), idx));
        snapshot.shards().forEach(c -> {
            if (metaData.index(c.key.getIndex()) == null) {
                assert snapshot.partial() :
                    "Index [" + c.key.getIndex() + "] was deleted during a snapshot but snapshot was not partial.";
                return;
            }
            final IndexId indexId = indexLookup.get(c.key.getIndexName());
            if (indexId != null) {
                builder.put(indexId, c.key.id(), c.value.generation());
            }
        });
        return builder.build();
    }

    private static MetaData metaDataForSnapshot(SnapshotsInProgress.Entry snapshot, MetaData metaData) {
        if (snapshot.includeGlobalState() == false) {
            // Remove global state from the cluster state
            MetaData.Builder builder = MetaData.builder();
            for (IndexId index : snapshot.indices()) {
                builder.put(metaData.index(index.getName()), false);
            }
            metaData = builder.build();
        }
        return metaData;
    }

    private static SnapshotInfo inProgressSnapshot(SnapshotsInProgress.Entry entry) {
        return new SnapshotInfo(entry.snapshot().getSnapshotId(),
                                   entry.indices().stream().map(IndexId::getName).collect(Collectors.toList()),
                                   entry.startTime(), entry.includeGlobalState(), entry.userMetadata());
    }

    /**
     * Returns status of the currently running snapshots
     * <p>
     * This method is executed on master node
     * </p>
     *
     * @param repository repository id
     * @param snapshots  list of snapshots that will be used as a filter, empty list means no snapshots are filtered
     * @return list of metadata for currently running snapshots
     */
    public List<SnapshotsInProgress.Entry> currentSnapshots(final String repository, final List<String> snapshots) {
        SnapshotsInProgress snapshotsInProgress = clusterService.state().custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
            return Collections.emptyList();
        }
        if ("_all".equals(repository)) {
            return snapshotsInProgress.entries();
        }
        if (snapshotsInProgress.entries().size() == 1) {
            // Most likely scenario - one snapshot is currently running
            // Check this snapshot against the query
            SnapshotsInProgress.Entry entry = snapshotsInProgress.entries().get(0);
            if (entry.snapshot().getRepository().equals(repository) == false) {
                return Collections.emptyList();
            }
            if (snapshots.isEmpty() == false) {
                for (String snapshot : snapshots) {
                    if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                        return snapshotsInProgress.entries();
                    }
                }
                return Collections.emptyList();
            } else {
                return snapshotsInProgress.entries();
            }
        }
        List<SnapshotsInProgress.Entry> builder = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.snapshot().getRepository().equals(repository) == false) {
                continue;
            }
            if (snapshots.isEmpty() == false) {
                for (String snapshot : snapshots) {
                    if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                        builder.add(entry);
                        break;
                    }
                }
            } else {
                builder.add(entry);
            }
        }
        return unmodifiableList(builder);
    }

    /**
     * Returns status of shards  currently finished snapshots
     * <p>
     * This method is executed on master node and it's complimentary to the
     * {@link SnapshotShardsService#currentSnapshotShards(Snapshot)} because it
     * returns similar information but for already finished snapshots.
     * </p>
     *
     * @param repositoryName  repository name
     * @param snapshotInfo    snapshot info
     * @return map of shard id to snapshot status
     */
    public Map<ShardId, IndexShardSnapshotStatus> snapshotShards(final String repositoryName,
                                                                 final RepositoryData repositoryData,
                                                                 final SnapshotInfo snapshotInfo) throws IOException {
        final Repository repository = repositoriesService.repository(repositoryName);
        final Map<ShardId, IndexShardSnapshotStatus> shardStatus = new HashMap<>();
        for (String index : snapshotInfo.indices()) {
            IndexId indexId = repositoryData.resolveIndexId(index);
            IndexMetaData indexMetaData = repository.getSnapshotIndexMetaData(snapshotInfo.snapshotId(), indexId);
            if (indexMetaData != null) {
                int numberOfShards = indexMetaData.getNumberOfShards();
                for (int i = 0; i < numberOfShards; i++) {
                    ShardId shardId = new ShardId(indexMetaData.getIndex(), i);
                    SnapshotShardFailure shardFailure = findShardFailure(snapshotInfo.shardFailures(), shardId);
                    if (shardFailure != null) {
                        shardStatus.put(shardId, IndexShardSnapshotStatus.newFailed(shardFailure.reason()));
                    } else {
                        final IndexShardSnapshotStatus shardSnapshotStatus;
                        if (snapshotInfo.state() == SnapshotState.FAILED) {
                            // If the snapshot failed, but the shard's snapshot does
                            // not have an exception, it means that partial snapshots
                            // were disabled and in this case, the shard snapshot will
                            // *not* have any metadata, so attempting to read the shard
                            // snapshot status will throw an exception.  Instead, we create
                            // a status for the shard to indicate that the shard snapshot
                            // could not be taken due to partial being set to false.
                            shardSnapshotStatus = IndexShardSnapshotStatus.newFailed("skipped");
                        } else {
                            shardSnapshotStatus = repository.getShardSnapshotStatus(
                                snapshotInfo.snapshotId(),
                                indexId,
                                shardId);
                        }
                        shardStatus.put(shardId, shardSnapshotStatus);
                    }
                }
            }
        }
        return unmodifiableMap(shardStatus);
    }

    private static SnapshotShardFailure findShardFailure(List<SnapshotShardFailure> shardFailures, ShardId shardId) {
        for (SnapshotShardFailure shardFailure : shardFailures) {
            if (shardId.getIndexName().equals(shardFailure.index()) && shardId.getId() == shardFailure.shardId()) {
                return shardFailure;
            }
        }
        return null;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                // We don't remove old master when master flips anymore. So, we need to check for change in master
                final SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
                final boolean newMaster = event.previousState().nodes().isLocalNodeElectedMaster() == false;
                if (snapshotsInProgress != null) {
                    if (newMaster || removedNodesCleanupNeeded(snapshotsInProgress, event.nodesDelta().removedNodes())) {
                        processSnapshotsOnRemovedNodes();
                    }
                    if (event.routingTableChanged() && waitingShardsStartedOrUnassigned(snapshotsInProgress, event)) {
                        processStartedShards();
                    }
                    // Cleanup all snapshots that have no more work left:
                    // 1. Completed snapshots
                    // 2. Snapshots in state INIT that the previous master failed to start
                    // 3. Snapshots in any other state that have all their shard tasks completed
                    snapshotsInProgress.entries().stream().filter(
                        entry -> entry.state().completed()
                            || initializingSnapshots.contains(entry.snapshot()) == false
                               && (entry.state() == State.INIT || completed(entry.shards().values()))
                    ).forEach(entry -> endSnapshot(entry, event.state().metaData()));
                }
                if (newMaster) {
                    finalizeSnapshotDeletionFromPreviousMaster(event.state());
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to update snapshot state ", e);
        }
    }

    /**
     * Finalizes a snapshot deletion in progress if the current node is the master but it
     * was not master in the previous cluster state and there is still a lingering snapshot
     * deletion in progress in the cluster state.  This means that the old master failed
     * before it could clean up an in-progress snapshot deletion.  We attempt to delete the
     * snapshot files and remove the deletion from the cluster state.  It is possible that the
     * old master was in a state of long GC and then it resumes and tries to delete the snapshot
     * that has already been deleted by the current master.  This is acceptable however, since
     * the old master's snapshot deletion will just respond with an error but in actuality, the
     * snapshot was deleted and a call to GET snapshots would reveal that the snapshot no longer exists.
     */
    private void finalizeSnapshotDeletionFromPreviousMaster(ClusterState state) {
        SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
        if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
            assert deletionsInProgress.getEntries().size() == 1 : "only one in-progress deletion allowed per cluster";
            SnapshotDeletionsInProgress.Entry entry = deletionsInProgress.getEntries().get(0);
            deleteSnapshotFromRepository(entry.getSnapshot(), null, entry.repositoryStateId(),
                state.nodes().getMinNodeVersion());
        }
    }

    /**
     * Cleans up shard snapshots that were running on removed nodes
     */
    private void processSnapshotsOnRemovedNodes() {
        clusterService.submitStateUpdateTask("update snapshot state after node removal", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                DiscoveryNodes nodes = currentState.nodes();
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots == null) {
                    return currentState;
                }
                boolean changed = false;
                ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                for (final SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                    SnapshotsInProgress.Entry updatedSnapshot = snapshot;
                    if (snapshot.state() == State.STARTED || snapshot.state() == State.ABORTED) {
                        ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                        boolean snapshotChanged = false;
                        for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : snapshot.shards()) {
                            final ShardSnapshotStatus shardStatus = shardEntry.value;
                            final ShardId shardId = shardEntry.key;
                            if (!shardStatus.state().completed() && shardStatus.nodeId() != null) {
                                if (nodes.nodeExists(shardStatus.nodeId())) {
                                    shards.put(shardId, shardStatus);
                                } else {
                                    // TODO: Restart snapshot on another node?
                                    snapshotChanged = true;
                                    logger.warn("failing snapshot of shard [{}] on closed node [{}]",
                                        shardId, shardStatus.nodeId());
                                    shards.put(shardId,
                                        new ShardSnapshotStatus(shardStatus.nodeId(), ShardState.FAILED, "node shutdown",
                                            shardStatus.generation()));
                                }
                            } else {
                                shards.put(shardId, shardStatus);
                            }
                        }
                        if (snapshotChanged) {
                            changed = true;
                            ImmutableOpenMap<ShardId, ShardSnapshotStatus> shardsMap = shards.build();
                            if (!snapshot.state().completed() && completed(shardsMap.values())) {
                                updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, State.SUCCESS, shardsMap);
                            } else {
                                updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, snapshot.state(), shardsMap);
                            }
                        }
                        entries.add(updatedSnapshot);
                    } else if (snapshot.state() == State.INIT && initializingSnapshots.contains(snapshot.snapshot()) == false) {
                        changed = true;
                        // Mark the snapshot as aborted as it failed to start from the previous master
                        updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, State.ABORTED, snapshot.shards());
                        entries.add(updatedSnapshot);

                        // Clean up the snapshot that failed to start from the old master
                        deleteSnapshot(snapshot.snapshot(), new ActionListener<>() {
                            @Override
                            public void onResponse(Void aVoid) {
                                logger.debug("cleaned up abandoned snapshot {} in INIT state", snapshot.snapshot());
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn("failed to clean up abandoned snapshot {} in INIT state", snapshot.snapshot());
                            }
                        }, updatedSnapshot.repositoryStateId(), false);
                    }
                    assert updatedSnapshot.shards().size() == snapshot.shards().size()
                        : "Shard count changed during snapshot status update from [" + snapshot + "] to [" + updatedSnapshot + "]";
                }
                if (changed) {
                    return ClusterState.builder(currentState)
                        .putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(unmodifiableList(entries))).build();
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to update snapshot state after node removal");
            }
        });
    }

    private void processStartedShards() {
        clusterService.submitStateUpdateTask("update snapshot state after shards started", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                RoutingTable routingTable = currentState.routingTable();
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots != null) {
                    boolean changed = false;
                    ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                    for (final SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                        SnapshotsInProgress.Entry updatedSnapshot = snapshot;
                        if (snapshot.state() == State.STARTED) {
                            ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards = processWaitingShards(snapshot.shards(),
                                routingTable);
                            if (shards != null) {
                                changed = true;
                                if (!snapshot.state().completed() && completed(shards.values())) {
                                    updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, State.SUCCESS, shards);
                                } else {
                                    updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, shards);
                                }
                            }
                            entries.add(updatedSnapshot);
                        }
                    }
                    if (changed) {
                        return ClusterState.builder(currentState)
                            .putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(unmodifiableList(entries))).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() ->
                    new ParameterizedMessage("failed to update snapshot state after shards started from [{}] ", source), e);
            }
        });
    }

    private static ImmutableOpenMap<ShardId, ShardSnapshotStatus> processWaitingShards(
            ImmutableOpenMap<ShardId, ShardSnapshotStatus> snapshotShards, RoutingTable routingTable) {
        boolean snapshotChanged = false;
        ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
        for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : snapshotShards) {
            ShardSnapshotStatus shardStatus = shardEntry.value;
            ShardId shardId = shardEntry.key;
            if (shardStatus.state() == ShardState.WAITING) {
                IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                if (indexShardRoutingTable != null) {
                    IndexShardRoutingTable shardRouting = indexShardRoutingTable.shard(shardId.id());
                    if (shardRouting != null && shardRouting.primaryShard() != null) {
                        if (shardRouting.primaryShard().started()) {
                            // Shard that we were waiting for has started on a node, let's process it
                            snapshotChanged = true;
                            logger.trace("starting shard that we were waiting for [{}] on node [{}]", shardId, shardStatus.nodeId());
                            shards.put(shardId,
                                new ShardSnapshotStatus(shardRouting.primaryShard().currentNodeId(), shardStatus.generation()));
                            continue;
                        } else if (shardRouting.primaryShard().initializing() || shardRouting.primaryShard().relocating()) {
                            // Shard that we were waiting for hasn't started yet or still relocating - will continue to wait
                            shards.put(shardId, shardStatus);
                            continue;
                        }
                    }
                }
                // Shard that we were waiting for went into unassigned state or disappeared - giving up
                snapshotChanged = true;
                logger.warn("failing snapshot of shard [{}] on unassigned shard [{}]", shardId, shardStatus.nodeId());
                shards.put(shardId, new ShardSnapshotStatus(
                    shardStatus.nodeId(), ShardState.FAILED, "shard is unassigned", shardStatus.generation()));
            } else {
                shards.put(shardId, shardStatus);
            }
        }
        if (snapshotChanged) {
            return shards.build();
        } else {
            return null;
        }
    }

    private static boolean waitingShardsStartedOrUnassigned(SnapshotsInProgress snapshotsInProgress, ClusterChangedEvent event) {
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.state() == State.STARTED) {
                for (ObjectCursor<String> index : entry.waitingIndices().keys()) {
                    if (event.indexRoutingTableChanged(index.value)) {
                        IndexRoutingTable indexShardRoutingTable = event.state().getRoutingTable().index(index.value);
                        for (ShardId shardId : entry.waitingIndices().get(index.value)) {
                            ShardRouting shardRouting = indexShardRoutingTable.shard(shardId.id()).primaryShard();
                            if (shardRouting != null && (shardRouting.started() || shardRouting.unassigned())) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private static boolean removedNodesCleanupNeeded(SnapshotsInProgress snapshotsInProgress, List<DiscoveryNode> removedNodes) {
        // If at least one shard was running on a removed node - we need to fail it
        return removedNodes.isEmpty() == false && snapshotsInProgress.entries().stream().flatMap(snapshot ->
                StreamSupport.stream(((Iterable<ShardSnapshotStatus>) () -> snapshot.shards().valuesIt()).spliterator(), false)
                    .filter(s -> s.state().completed() == false).map(ShardSnapshotStatus::nodeId))
                .anyMatch(removedNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet())::contains);
    }

    /**
     * Returns list of indices with missing shards, and list of indices that are closed
     *
     * @param shards list of shard statuses
     * @return list of failed and closed indices
     */
    private static Tuple<Set<String>, Set<String>> indicesWithMissingShards(
        ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards, MetaData metaData) {
        Set<String> missing = new HashSet<>();
        Set<String> closed = new HashSet<>();
        for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> entry : shards) {
            if (entry.value.state() == ShardState.MISSING) {
                if (metaData.hasIndex(entry.key.getIndex().getName()) &&
                    metaData.getIndexSafe(entry.key.getIndex()).getState() == IndexMetaData.State.CLOSE) {
                    closed.add(entry.key.getIndex().getName());
                } else {
                    missing.add(entry.key.getIndex().getName());
                }
            }
        }
        return new Tuple<>(missing, closed);
    }

    /**
     * Finalizes the shard in repository and then removes it from cluster state
     * <p>
     * This is non-blocking method that runs on a thread from SNAPSHOT thread pool
     *
     * @param entry snapshot
     */
    private void endSnapshot(SnapshotsInProgress.Entry entry, MetaData metaData) {
        if (endingSnapshots.add(entry.snapshot()) == false) {
            return;
        }
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                final Snapshot snapshot = entry.snapshot();
                final Repository repository = repositoriesService.repository(snapshot.getRepository());
                final String failure = entry.failure();
                logger.trace("[{}] finalizing snapshot in repository, state: [{}], failure[{}]", snapshot, entry.state(), failure);
                ArrayList<SnapshotShardFailure> shardFailures = new ArrayList<>();
                for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardStatus : entry.shards()) {
                    ShardId shardId = shardStatus.key;
                    ShardSnapshotStatus status = shardStatus.value;
                    if (status.state().failed()) {
                        shardFailures.add(new SnapshotShardFailure(status.nodeId(), shardId, status.reason()));
                    }
                }
                final ShardGenerations shardGenerations = buildGenerations(entry, metaData);
                repository.finalizeSnapshot(
                    snapshot.getSnapshotId(),
                    shardGenerations,
                    entry.startTime(),
                    failure,
                    entry.partial() ? shardGenerations.totalShards() : entry.shards().size(),
                    unmodifiableList(shardFailures),
                    entry.repositoryStateId(),
                    entry.includeGlobalState(),
                    metaDataForSnapshot(entry, metaData),
                    entry.userMetadata(),
                    entry.useShardGenerations(),
                    ActionListener.wrap(snapshotInfo -> {
                        removeSnapshotFromClusterState(snapshot, snapshotInfo, null);
                        logger.info("snapshot [{}] completed with state [{}]", snapshot, snapshotInfo.state());
                    }, this::onFailure));
            }

            @Override
            public void onFailure(final Exception e) {
                Snapshot snapshot = entry.snapshot();
                if (ExceptionsHelper.unwrap(e, NotMasterException.class, FailedToCommitClusterStateException.class) != null) {
                    // Failure due to not being master any more, don't try to remove snapshot from cluster state the next master
                    // will try ending this snapshot again
                    logger.debug(() -> new ParameterizedMessage(
                        "[{}] failed to update cluster state during snapshot finalization", snapshot), e);
                    endingSnapshots.remove(snapshot);
                } else {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed to finalize snapshot", snapshot), e);
                    removeSnapshotFromClusterState(snapshot, null, e);
                }
            }
        });
    }

    /**
     * Removes record of running snapshot from cluster state
     * @param snapshot       snapshot
     * @param snapshotInfo   snapshot info if snapshot was successful
     * @param e              exception if snapshot failed, {@code null} otherwise
     */
    private void removeSnapshotFromClusterState(final Snapshot snapshot, final SnapshotInfo snapshotInfo, @Nullable Exception e) {
        removeSnapshotFromClusterState(snapshot, snapshotInfo, e, null);
    }

    /**
     * Removes record of running snapshot from cluster state and notifies the listener when this action is complete
     * @param snapshot   snapshot
     * @param failure    exception if snapshot failed, {@code null} otherwise
     * @param listener   listener to notify when snapshot information is removed from the cluster state
     */
    private void removeSnapshotFromClusterState(final Snapshot snapshot, @Nullable SnapshotInfo snapshotInfo, @Nullable Exception failure,
                                                @Nullable CleanupAfterErrorListener listener) {
        assert snapshotInfo != null || failure != null : "Either snapshotInfo or failure must be supplied";
        clusterService.submitStateUpdateTask("remove snapshot metadata", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) {
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots != null) {
                    boolean changed = false;
                    ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                    for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                        if (entry.snapshot().equals(snapshot)) {
                            changed = true;
                        } else {
                            entries.add(entry);
                        }
                    }
                    if (changed) {
                        return ClusterState.builder(currentState)
                            .putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(unmodifiableList(entries))).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to remove snapshot metadata", snapshot), e);
                endingSnapshots.remove(snapshot);
                if (listener != null) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onNoLongerMaster(String source) {
                endingSnapshots.remove(snapshot);
                if (listener != null) {
                    listener.onNoLongerMaster();
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                final List<ActionListener<SnapshotInfo>> completionListeners = snapshotCompletionListeners.remove(snapshot);
                if (completionListeners != null) {
                    try {
                        if (snapshotInfo == null) {
                            ActionListener.onFailure(completionListeners, failure);
                        } else {
                            ActionListener.onResponse(completionListeners, snapshotInfo);
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to notify listeners", e);
                    }
                }
                endingSnapshots.remove(snapshot);
                if (listener != null) {
                    listener.onResponse(snapshotInfo);
                }
            }
        });
    }

    /**
     * Deletes a snapshot from the repository, looking up the {@link Snapshot} reference before deleting.
     * If the snapshot is still running cancels the snapshot first and then deletes it from the repository.
     *
     * @param repositoryName  repositoryName
     * @param snapshotName    snapshotName
     * @param listener        listener
     */
    public void deleteSnapshot(final String repositoryName, final String snapshotName, final ActionListener<Void> listener,
                               final boolean immediatePriority) {
        // First, look for the snapshot in the repository
        final Repository repository = repositoriesService.repository(repositoryName);
        repository.getRepositoryData(ActionListener.wrap(repositoryData -> {
            Optional<SnapshotId> matchedEntry = repositoryData.getSnapshotIds()
                .stream()
                .filter(s -> s.getName().equals(snapshotName))
                .findFirst();
            // if nothing found by the same name, then look in the cluster state for current in progress snapshots
            long repoGenId = repositoryData.getGenId();
            if (matchedEntry.isPresent() == false) {
                Optional<SnapshotsInProgress.Entry> matchedInProgress = currentSnapshots(repositoryName, Collections.emptyList()).stream()
                    .filter(s -> s.snapshot().getSnapshotId().getName().equals(snapshotName)).findFirst();
                if (matchedInProgress.isPresent()) {
                    matchedEntry = matchedInProgress.map(s -> s.snapshot().getSnapshotId());
                    // Derive repository generation if a snapshot is in progress because it will increment the generation when it finishes
                    repoGenId = matchedInProgress.get().repositoryStateId() + 1L;
                }
            }
            if (matchedEntry.isPresent() == false) {
                throw new SnapshotMissingException(repositoryName, snapshotName);
            }
            deleteSnapshot(new Snapshot(repositoryName, matchedEntry.get()), listener, repoGenId, immediatePriority);
        }, listener::onFailure));
    }

    /**
     * Deletes snapshot from repository.
     * <p>
     * If the snapshot is still running cancels the snapshot first and then deletes it from the repository.
     *
     * @param snapshot snapshot
     * @param listener listener
     * @param repositoryStateId the unique id for the state of the repository
     */
    private void deleteSnapshot(final Snapshot snapshot, final ActionListener<Void> listener, final long repositoryStateId,
                                final boolean immediatePriority) {
        logger.info("deleting snapshot [{}]", snapshot);
        Priority priority = immediatePriority ? Priority.IMMEDIATE : Priority.NORMAL;
        clusterService.submitStateUpdateTask("delete snapshot", new ClusterStateUpdateTask(priority) {

            boolean waitForSnapshot = false;

            @Override
            public ClusterState execute(ClusterState currentState) {
                SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(snapshot,
                        "cannot delete - another snapshot is currently being deleted in [" + deletionsInProgress + "]");
                }
                final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
                if (repositoryCleanupInProgress != null && repositoryCleanupInProgress.hasCleanupInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(snapshot.getRepository(), snapshot.getSnapshotId().getName(),
                        "cannot delete snapshot while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]");
                }
                RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE);
                if (restoreInProgress != null) {
                    // don't allow snapshot deletions while a restore is taking place,
                    // otherwise we could end up deleting a snapshot that is being restored
                    // and the files the restore depends on would all be gone
                    if (restoreInProgress.isEmpty() == false) {
                        throw new ConcurrentSnapshotExecutionException(snapshot,
                            "cannot delete snapshot during a restore in progress in [" + restoreInProgress + "]");
                    }
                }
                ClusterState.Builder clusterStateBuilder = ClusterState.builder(currentState);
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                SnapshotsInProgress.Entry snapshotEntry = snapshots != null ? snapshots.snapshot(snapshot) : null;
                if (snapshotEntry == null) {
                    // This snapshot is not running - delete
                    if (snapshots != null && !snapshots.entries().isEmpty()) {
                        // However other snapshots are running - cannot continue
                        throw new ConcurrentSnapshotExecutionException(snapshot, "another snapshot is currently running cannot delete");
                    }
                    // add the snapshot deletion to the cluster state
                    SnapshotDeletionsInProgress.Entry entry = new SnapshotDeletionsInProgress.Entry(
                        snapshot,
                        threadPool.absoluteTimeInMillis(),
                        repositoryStateId
                    );
                    if (deletionsInProgress != null) {
                        deletionsInProgress = deletionsInProgress.withAddedEntry(entry);
                    } else {
                        deletionsInProgress = SnapshotDeletionsInProgress.newInstance(entry);
                    }
                    clusterStateBuilder.putCustom(SnapshotDeletionsInProgress.TYPE, deletionsInProgress);
                } else {
                    // This snapshot is currently running - stopping shards first
                    waitForSnapshot = true;

                    final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards;

                    final State state = snapshotEntry.state();
                    final String failure;
                    if (state == State.INIT) {
                        // snapshot is still initializing, mark it as aborted
                        shards = snapshotEntry.shards();
                        assert shards.isEmpty();
                        failure = "Snapshot was aborted during initialization";
                    } else if (state == State.STARTED) {
                        // snapshot is started - mark every non completed shard as aborted
                        final ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shardsBuilder = ImmutableOpenMap.builder();
                        for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardEntry : snapshotEntry.shards()) {
                            ShardSnapshotStatus status = shardEntry.value;
                            if (status.state().completed() == false) {
                                status = new ShardSnapshotStatus(
                                    status.nodeId(), ShardState.ABORTED, "aborted by snapshot deletion", status.generation());
                            }
                            shardsBuilder.put(shardEntry.key, status);
                        }
                        shards = shardsBuilder.build();
                        failure = "Snapshot was aborted by deletion";
                    } else {
                        boolean hasUncompletedShards = false;
                        // Cleanup in case a node gone missing and snapshot wasn't updated for some reason
                        for (ObjectCursor<ShardSnapshotStatus> shardStatus : snapshotEntry.shards().values()) {
                            // Check if we still have shard running on existing nodes
                            if (shardStatus.value.state().completed() == false && shardStatus.value.nodeId() != null
                                    && currentState.nodes().get(shardStatus.value.nodeId()) != null) {
                                hasUncompletedShards = true;
                                break;
                            }
                        }
                        if (hasUncompletedShards) {
                            // snapshot is being finalized - wait for shards to complete finalization process
                            logger.debug("trying to delete completed snapshot - should wait for shards to finalize on all nodes");
                            return currentState;
                        } else {
                            // no shards to wait for but a node is gone - this is the only case
                            // where we force to finish the snapshot
                            logger.debug("trying to delete completed snapshot with no finalizing shards - can delete immediately");
                            shards = snapshotEntry.shards();
                        }
                        failure = snapshotEntry.failure();
                    }
                    SnapshotsInProgress.Entry newSnapshot = new SnapshotsInProgress.Entry(snapshotEntry, State.ABORTED, shards, failure);
                    clusterStateBuilder.putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(newSnapshot));
                }
                return clusterStateBuilder.build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (waitForSnapshot) {
                    logger.trace("adding snapshot completion listener to wait for deleted snapshot to finish");
                    addListener(snapshot, ActionListener.wrap(
                        snapshotInfo -> {
                            logger.debug("deleted snapshot completed - deleting files");
                            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                                    try {
                                        deleteSnapshot(snapshot.getRepository(), snapshot.getSnapshotId().getName(), listener, true);
                                    } catch (Exception ex) {
                                        logger.warn(() -> new ParameterizedMessage("[{}] failed to delete snapshot", snapshot), ex);
                                    }
                                }
                            );
                        },
                        e -> {
                            logger.warn("deleted snapshot failed - deleting files", e);
                            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
                                try {
                                    deleteSnapshot(snapshot.getRepository(), snapshot.getSnapshotId().getName(), listener, true);
                                } catch (SnapshotMissingException smex) {
                                    logger.info(() -> new ParameterizedMessage(
                                        "Tried deleting in-progress snapshot [{}], but it could not be found after failing to abort.",
                                        smex.getSnapshotName()), e);
                                    listener.onFailure(new SnapshotException(snapshot,
                                        "Tried deleting in-progress snapshot [" + smex.getSnapshotName() + "], but it " +
                                            "could not be found after failing to abort.", smex));
                                }
                            });
                        }
                    ));
                } else {
                    logger.debug("deleted snapshot is not running - deleting files");
                    deleteSnapshotFromRepository(snapshot, listener, repositoryStateId, newState.nodes().getMinNodeVersion());
                }
            }
        });
    }

    /**
     * Checks if a repository is currently in use by one of the snapshots
     *
     * @param clusterState cluster state
     * @param repository   repository id
     * @return true if repository is currently in use by one of the running snapshots
     */
    public static boolean isRepositoryInUse(ClusterState clusterState, String repository) {
        SnapshotsInProgress snapshots = clusterState.custom(SnapshotsInProgress.TYPE);
        if (snapshots != null) {
            for (SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                if (repository.equals(snapshot.snapshot().getRepository())) {
                    return true;
                }
            }
        }
        SnapshotDeletionsInProgress deletionsInProgress = clusterState.custom(SnapshotDeletionsInProgress.TYPE);
        if (deletionsInProgress != null) {
            for (SnapshotDeletionsInProgress.Entry entry : deletionsInProgress.getEntries()) {
                if (entry.getSnapshot().getRepository().equals(repository)) {
                    return true;
                }
            }
        }
        final RepositoryCleanupInProgress repositoryCleanupInProgress = clusterState.custom(RepositoryCleanupInProgress.TYPE);
        if (repositoryCleanupInProgress != null) {
            for (RepositoryCleanupInProgress.Entry entry : repositoryCleanupInProgress.entries()) {
                if (entry.repository().equals(repository)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Deletes snapshot from repository
     *
     * @param snapshot   snapshot
     * @param listener   listener
     * @param repositoryStateId the unique id representing the state of the repository at the time the deletion began
     * @param version minimum ES version the repository should be readable by
     */
    private void deleteSnapshotFromRepository(Snapshot snapshot, @Nullable ActionListener<Void> listener, long repositoryStateId,
                                              Version version) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(listener, l -> {
            Repository repository = repositoriesService.repository(snapshot.getRepository());
            repository.deleteSnapshot(snapshot.getSnapshotId(), repositoryStateId, version.onOrAfter(SHARD_GEN_IN_REPO_DATA_VERSION),
                ActionListener.wrap(v -> {
                        logger.info("snapshot [{}] deleted", snapshot);
                        removeSnapshotDeletionFromClusterState(snapshot, null, l);
                    }, ex -> removeSnapshotDeletionFromClusterState(snapshot, ex, l)
                ));
        }));
    }

    /**
     * Removes the snapshot deletion from {@link SnapshotDeletionsInProgress} in the cluster state.
     */
    private void removeSnapshotDeletionFromClusterState(final Snapshot snapshot, @Nullable final Exception failure,
                                                        @Nullable final ActionListener<Void> listener) {
        clusterService.submitStateUpdateTask("remove snapshot deletion metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                SnapshotDeletionsInProgress deletions = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                if (deletions != null) {
                    boolean changed = false;
                    if (deletions.hasDeletionsInProgress()) {
                        assert deletions.getEntries().size() == 1 : "should have exactly one deletion in progress";
                        SnapshotDeletionsInProgress.Entry entry = deletions.getEntries().get(0);
                        deletions = deletions.withRemovedEntry(entry);
                        changed = true;
                    }
                    if (changed) {
                        return ClusterState.builder(currentState).putCustom(SnapshotDeletionsInProgress.TYPE, deletions).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to remove snapshot deletion metadata", snapshot), e);
                if (listener != null) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (listener != null) {
                    if (failure != null) {
                        listener.onFailure(failure);
                    } else {
                        listener.onResponse(null);
                    }
                }
            }
        });
    }

    /**
     * Calculates the list of shards that should be included into the current snapshot
     *
     * @param clusterState cluster state
     * @param snapshot     SnapshotsInProgress Entry
     * @return list of shard to be included into current snapshot
     */
    private static ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards(ClusterState clusterState,
                                                                                             SnapshotsInProgress.Entry snapshot,
                                                                                             RepositoryData repositoryData) {
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
        MetaData metaData = clusterState.metaData();
        final ShardGenerations shardGenerations = repositoryData.shardGenerations();
        for (IndexId index : snapshot.indices()) {
            final String indexName = index.getName();
            final boolean isNewIndex = repositoryData.getIndices().containsKey(indexName) == false;
            IndexMetaData indexMetaData = metaData.index(indexName);
            if (indexMetaData == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                builder.put(new ShardId(indexName, IndexMetaData.INDEX_UUID_NA_VALUE, 0),
                    new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING, "missing index", null));
            } else {
                IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(indexName);
                for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
                    ShardId shardId = new ShardId(indexMetaData.getIndex(), i);
                    final String shardRepoGeneration;
                    if (snapshot.useShardGenerations()) {
                        if (isNewIndex) {
                            assert shardGenerations.getShardGen(index, shardId.getId()) == null
                                : "Found shard generation for new index [" + index + "]";
                            shardRepoGeneration = ShardGenerations.NEW_SHARD_GEN;
                        } else {
                            shardRepoGeneration = shardGenerations.getShardGen(index, shardId.getId());
                        }
                    } else {
                        shardRepoGeneration = null;
                    }
                    if (indexRoutingTable != null) {
                        ShardRouting primary = indexRoutingTable.shard(i).primaryShard();
                        if (primary == null || !primary.assignedToNode()) {
                            builder.put(shardId,
                                new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING, "primary shard is not allocated",
                                    shardRepoGeneration));
                        } else if (primary.relocating() || primary.initializing()) {
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(
                                primary.currentNodeId(), ShardState.WAITING, shardRepoGeneration));
                        } else if (!primary.started()) {
                            builder.put(shardId,
                                new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), ShardState.MISSING,
                                    "primary shard hasn't been started yet", shardRepoGeneration));
                        } else {
                            builder.put(shardId,
                                new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), shardRepoGeneration));
                        }
                    } else {
                        builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING,
                            "missing routing table", shardRepoGeneration));
                    }
                }
            }
        }

        return builder.build();
    }

    /**
     * Returns the indices that are currently being snapshotted (with partial == false) and that are contained in the indices-to-check set.
     */
    public static Set<Index> snapshottingIndices(final ClusterState currentState, final Set<Index> indicesToCheck) {
        final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
        if (snapshots == null) {
            return emptySet();
        }

        final Set<Index> indices = new HashSet<>();
        for (final SnapshotsInProgress.Entry entry : snapshots.entries()) {
            if (entry.partial() == false) {
                for (IndexId index : entry.indices()) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index.getName());
                    if (indexMetaData != null && indicesToCheck.contains(indexMetaData.getIndex())) {
                        indices.add(indexMetaData.getIndex());
                    }
                }
            }
        }
        return indices;
    }

    /**
     * Adds snapshot completion listener
     *
     * @param snapshot Snapshot to listen for
     * @param listener listener
     */
    private void addListener(Snapshot snapshot, ActionListener<SnapshotInfo> listener) {
        snapshotCompletionListeners.computeIfAbsent(snapshot, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        clusterService.removeApplier(this);
    }
}
