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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

/**
 * Service responsible for creating snapshots. This service runs all the steps executed on the master node during snapshot creation and
 * deletion.
 * See package level documentation of {@link org.elasticsearch.snapshots} for details.
 */
public class SnapshotsService extends AbstractLifecycleComponent implements ClusterStateApplier {

    public static final Version SHARD_GEN_IN_REPO_DATA_VERSION = Version.V_7_6_0;

    public static final Version OLD_SNAPSHOT_FORMAT = Version.V_7_5_0;

    public static final Version MULTI_DELETE_VERSION = Version.V_7_8_0;

    private static final Logger logger = LogManager.getLogger(SnapshotsService.class);

    public static final String UPDATE_SNAPSHOT_STATUS_ACTION_NAME = "internal:cluster/snapshot/update_snapshot_status";

    private final ClusterService clusterService;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final RepositoriesService repositoriesService;

    private final ThreadPool threadPool;

    private final Map<Snapshot, List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>>> snapshotCompletionListeners =
        new ConcurrentHashMap<>();

    // Set of snapshots that are currently being ended by this node
    private final Set<Snapshot> endingSnapshots = Collections.synchronizedSet(new HashSet<>());

    private final SnapshotStateExecutor snapshotStateExecutor = new SnapshotStateExecutor();
    private final UpdateSnapshotStatusAction updateSnapshotStatusHandler;

    private final TransportService transportService;

    public SnapshotsService(Settings settings, ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver,
                            RepositoriesService repositoriesService, TransportService transportService, ActionFilters actionFilters) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.repositoriesService = repositoriesService;
        this.threadPool = transportService.getThreadPool();
        this.transportService = transportService;

        // The constructor of UpdateSnapshotStatusAction will register itself to the TransportService.
        this.updateSnapshotStatusHandler = new UpdateSnapshotStatusAction(
                transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver);
        if (DiscoveryNode.isMasterNode(settings)) {
            // addLowPriorityApplier to make sure that Repository will be created before snapshot
            clusterService.addLowPriorityApplier(this);
        }
    }

    /**
     * Same as {@link #createSnapshot(CreateSnapshotRequest, ActionListener)} but invokes its callback on completion of
     * the snapshot.
     *
     * @param request snapshot request
     * @param listener snapshot completion listener
     */
    public void executeSnapshot(final CreateSnapshotRequest request, final ActionListener<SnapshotInfo> listener) {
        createSnapshot(request,
            ActionListener.wrap(snapshot -> addListener(snapshot, ActionListener.map(listener, Tuple::v2)), listener::onFailure));
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
        Repository repository = repositoriesService.repository(request.repository());
        if (repository.isReadOnly()) {
            listener.onFailure(
                    new RepositoryException(repository.getMetadata().name(), "cannot create snapshot in a readonly repository"));
            return;
        }
        final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);
        final Map<String, Object> userMeta = repository.adaptUserMetadata(request.userMetadata());
        repository.executeConsistentStateUpdate(repositoryData -> new ClusterStateUpdateTask() {

            private SnapshotsInProgress.Entry newEntry;

            @Override
            public ClusterState execute(ClusterState currentState) {
                // check if the snapshot name already exists in the repository
                if (repositoryData.getSnapshotIds().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
                    throw new InvalidSnapshotNameException(
                            repository.getMetadata().name(), snapshotName, "snapshot with the same name already exists");
                }
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
                // Fail if there are any concurrently running snapshots. The only exception to this being a snapshot in INIT state from a
                // previous master that we can simply ignore and remove from the cluster state because we would clean it up from the
                // cluster state anyway in #applyClusterState.
                if (snapshots != null && snapshots.entries().stream().anyMatch(entry -> entry.state() != State.INIT)) {
                    throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName, " a snapshot is already running");
                }
                // Store newSnapshot here to be processed in clusterStateProcessed
                List<String> indices = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(currentState,
                    request.indicesOptions(), request.indices()));
                logger.trace("[{}][{}] creating snapshot for indices [{}]", repositoryName, snapshotName, indices);

                final List<IndexId> indexIds = repositoryData.resolveNewIndices(indices);
                final Version version = minCompatibleVersion(
                        clusterService.state().nodes().getMinNodeVersion(), repositoryData, null);
                ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards =
                        shards(currentState, indexIds, useShardGenerations(version), repositoryData);
                if (request.partial() == false) {
                    Set<String> missing = new HashSet<>();
                    for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> entry : shards) {
                        if (entry.value.state() == ShardState.MISSING) {
                            missing.add(entry.key.getIndex().getName());
                        }
                    }
                    if (missing.isEmpty() == false) {
                        // TODO: We should just throw here instead of creating a FAILED and hence useless snapshot in the repository
                        newEntry = new SnapshotsInProgress.Entry(
                                new Snapshot(repositoryName, snapshotId), request.includeGlobalState(), false,
                                State.FAILED, indexIds, threadPool.absoluteTimeInMillis(), repositoryData.getGenId(), shards,
                                "Indices don't have primary shards " + missing, userMeta, version);
                    }
                }
                if (newEntry == null) {
                    newEntry = new SnapshotsInProgress.Entry(
                            new Snapshot(repositoryName, snapshotId), request.includeGlobalState(), request.partial(),
                            State.STARTED, indexIds, threadPool.absoluteTimeInMillis(), repositoryData.getGenId(), shards,
                            null, userMeta, version);
                }
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE,
                        new SnapshotsInProgress(List.of(newEntry))).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to create snapshot", repositoryName, snapshotName), e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                try {
                    logger.info("snapshot [{}] started", snapshot);
                    listener.onResponse(snapshot);
                } finally {
                    if (newEntry.state().completed() || newEntry.shards().isEmpty()) {
                        endSnapshot(newEntry, newState.metadata());
                    }
                }
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }
        }, "create_snapshot [" + snapshotName + ']', listener::onFailure);
    }

    /**
     * Validates snapshot request
     *
     * @param repositoryName repository name
     * @param snapshotName snapshot name
     * @param state   current cluster state
     */
    private static void validate(String repositoryName, String snapshotName, ClusterState state) {
        RepositoriesMetadata repositoriesMetadata = state.getMetadata().custom(RepositoriesMetadata.TYPE);
        if (repositoriesMetadata == null || repositoriesMetadata.repository(repositoryName) == null) {
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

    private static ShardGenerations buildGenerations(SnapshotsInProgress.Entry snapshot, Metadata metadata) {
        ShardGenerations.Builder builder = ShardGenerations.builder();
        final Map<String, IndexId> indexLookup = new HashMap<>();
        snapshot.indices().forEach(idx -> indexLookup.put(idx.getName(), idx));
        snapshot.shards().forEach(c -> {
            if (metadata.index(c.key.getIndex()) == null) {
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

    private static Metadata metadataForSnapshot(SnapshotsInProgress.Entry snapshot, Metadata metadata) {
        if (snapshot.includeGlobalState() == false) {
            // Remove global state from the cluster state
            Metadata.Builder builder = Metadata.builder();
            for (IndexId index : snapshot.indices()) {
                final IndexMetadata indexMetadata = metadata.index(index.getName());
                if (indexMetadata == null) {
                    assert snapshot.partial() : "Index [" + index + "] was deleted during a snapshot but snapshot was not partial.";
                } else {
                    builder.put(indexMetadata, false);
                }
            }
            metadata = builder.build();
        }
        return metadata;
    }

    /**
     * Returns status of the currently running snapshots
     * <p>
     * This method is executed on master node
     * </p>
     *
     * @param snapshotsInProgress snapshots in progress in the cluster state
     * @param repository          repository id
     * @param snapshots           list of snapshots that will be used as a filter, empty list means no snapshots are filtered
     * @return list of metadata for currently running snapshots
     */
    public static List<SnapshotsInProgress.Entry> currentSnapshots(@Nullable SnapshotsInProgress snapshotsInProgress, String repository,
                                                                   List<String> snapshots) {
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
                    if (newMaster) {
                        // Cleanup all snapshots that have no more work left:
                        // 1. Completed snapshots
                        // 2. Snapshots in state INIT that the previous master failed to start
                        // 3. Snapshots in any other state that have all their shard tasks completed
                        snapshotsInProgress.entries().stream().filter(
                                entry -> entry.state().completed() || entry.state() == State.INIT || completed(entry.shards().values())
                        ).forEach(entry -> endSnapshot(entry, event.state().metadata()));
                    }
                }
                if (newMaster) {
                    finalizeSnapshotDeletionFromPreviousMaster(event.state());
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to update snapshot state ", e);
        }
        assert assertConsistentWithClusterState(event.state());
    }

    private boolean assertConsistentWithClusterState(ClusterState state) {
        final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress != null && snapshotsInProgress.entries().isEmpty() == false) {
            synchronized (endingSnapshots) {
                final Set<Snapshot> runningSnapshots = Stream.concat(
                        snapshotsInProgress.entries().stream().map(SnapshotsInProgress.Entry::snapshot),
                        endingSnapshots.stream())
                        .collect(Collectors.toSet());
                final Set<Snapshot> snapshotListenerKeys = snapshotCompletionListeners.keySet();
                assert runningSnapshots.containsAll(snapshotListenerKeys) : "Saw completion listeners for unknown snapshots in "
                        + snapshotListenerKeys + " but running snapshots are " + runningSnapshots;
            }
        }
        return true;
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
            deleteSnapshotsFromRepository(entry.repository(), entry.getSnapshots(), null, entry.repositoryStateId(),
                    state.nodes().getMinNodeVersion());
        }
    }

    /**
     * Cleans up shard snapshots that were running on removed nodes
     */
    private void processSnapshotsOnRemovedNodes() {
        clusterService.submitStateUpdateTask("update snapshot state after node removal", new ClusterStateUpdateTask() {

            private final Collection<SnapshotsInProgress.Entry> finishedSnapshots = new ArrayList<>();

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
                                finishedSnapshots.add(updatedSnapshot);
                            } else {
                                updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, snapshot.state(), shardsMap);
                            }
                        }
                        entries.add(updatedSnapshot);
                    } else if (snapshot.state() == State.INIT) {
                        changed = true;
                        // A snapshot in INIT state hasn't yet written anything to the repository so we simply remove it
                        // from the cluster state  without any further cleanup
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

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                finishedSnapshots.forEach(entry -> endSnapshot(entry, newState.metadata()));
            }
        });
    }

    private void processStartedShards() {
        clusterService.submitStateUpdateTask("update snapshot state after shards started", new ClusterStateUpdateTask() {

            private final Collection<SnapshotsInProgress.Entry> finishedSnapshots = new ArrayList<>();

            @Override
            public ClusterState execute(ClusterState currentState) {
                RoutingTable routingTable = currentState.routingTable();
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                boolean changed = false;
                if (snapshots != null) {
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
                                    finishedSnapshots.add(updatedSnapshot);
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

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                finishedSnapshots.forEach(entry -> endSnapshot(entry, newState.metadata()));
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
     * Finalizes the shard in repository and then removes it from cluster state
     * <p>
     * This is non-blocking method that runs on a thread from SNAPSHOT thread pool
     *
     * @param entry snapshot
     */
    private void endSnapshot(SnapshotsInProgress.Entry entry, Metadata metadata) {
        if (endingSnapshots.add(entry.snapshot()) == false) {
            return;
        }
        final Snapshot snapshot = entry.snapshot();
        if (entry.repositoryStateId() == RepositoryData.UNKNOWN_REPO_GEN) {
            // BwC logic to handle master fail-over from an older version that still used unknown repo generation snapshot entries
            logger.debug("[{}] was aborted before starting", snapshot);
            removeSnapshotFromClusterState(snapshot, new SnapshotException(snapshot, "Aborted on initialization"));
            return;
        }
        try {
            final String failure = entry.failure();
            logger.trace("[{}] finalizing snapshot in repository, state: [{}], failure[{}]", snapshot, entry.state(), failure);
            ArrayList<SnapshotShardFailure> shardFailures = new ArrayList<>();
            for (ObjectObjectCursor<ShardId, ShardSnapshotStatus> shardStatus : entry.shards()) {
                ShardId shardId = shardStatus.key;
                ShardSnapshotStatus status = shardStatus.value;
                final ShardState state = status.state();
                if (state.failed()) {
                    shardFailures.add(new SnapshotShardFailure(status.nodeId(), shardId, status.reason()));
                } else if (state.completed() == false) {
                    shardFailures.add(new SnapshotShardFailure(status.nodeId(), shardId, "skipped"));
                } else {
                    assert state == ShardState.SUCCESS;
                }
            }
            final ShardGenerations shardGenerations = buildGenerations(entry, metadata);
            repositoriesService.repository(snapshot.getRepository()).finalizeSnapshot(
                    snapshot.getSnapshotId(),
                    shardGenerations,
                    entry.startTime(),
                    failure,
                    entry.partial() ? shardGenerations.totalShards() : entry.shards().size(),
                    unmodifiableList(shardFailures),
                    entry.repositoryStateId(),
                    entry.includeGlobalState(),
                    metadataForSnapshot(entry, metadata),
                    entry.userMetadata(),
                    entry.version(),
                    state -> stateWithoutSnapshot(state, snapshot),
                    ActionListener.wrap(result -> {
                        final List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>> completionListeners =
                                snapshotCompletionListeners.remove(snapshot);
                        if (completionListeners != null) {
                            try {
                                ActionListener.onResponse(completionListeners, result);
                            } catch (Exception e) {
                                logger.warn("Failed to notify listeners", e);
                            }
                        }
                        endingSnapshots.remove(snapshot);
                        logger.info("snapshot [{}] completed with state [{}]", snapshot, result.v2().state());
                    }, e -> handleFinalizationFailure(e, entry)));
        } catch (Exception e) {
            handleFinalizationFailure(e, entry);
        }
    }

    private void handleFinalizationFailure(Exception e, SnapshotsInProgress.Entry entry) {
        Snapshot snapshot = entry.snapshot();
        if (ExceptionsHelper.unwrap(e, NotMasterException.class, FailedToCommitClusterStateException.class) != null) {
            // Failure due to not being master any more, don't try to remove snapshot from cluster state the next master
            // will try ending this snapshot again
            logger.debug(() -> new ParameterizedMessage(
                "[{}] failed to update cluster state during snapshot finalization", snapshot), e);
            failSnapshotCompletionListeners(snapshot,
                new SnapshotException(snapshot, "Failed to update cluster state during snapshot finalization", e));
        } else {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to finalize snapshot", snapshot), e);
            removeSnapshotFromClusterState(snapshot, e);
        }
    }

    private static ClusterState stateWithoutSnapshot(ClusterState state, Snapshot snapshot) {
        SnapshotsInProgress snapshots = state.custom(SnapshotsInProgress.TYPE);
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
                return ClusterState.builder(state).putCustom(
                        SnapshotsInProgress.TYPE, new SnapshotsInProgress(unmodifiableList(entries))).build();
            }
        }
        return state;
    }

    /**
     * Removes record of running snapshot from cluster state and notifies the listener when this action is complete
     * @param snapshot   snapshot
     * @param failure    exception if snapshot failed
     */
    private void removeSnapshotFromClusterState(final Snapshot snapshot, Exception failure) {
        assert failure != null : "Failure must be supplied";
        clusterService.submitStateUpdateTask("remove snapshot metadata", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) {
                return stateWithoutSnapshot(currentState, snapshot);
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to remove snapshot metadata", snapshot), e);
                failSnapshotCompletionListeners(
                    snapshot, new SnapshotException(snapshot, "Failed to remove snapshot from cluster state", e));
            }

            @Override
            public void onNoLongerMaster(String source) {
                failSnapshotCompletionListeners(
                    snapshot, ExceptionsHelper.useOrSuppress(failure, new SnapshotException(snapshot, "no longer master")));
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                failSnapshotCompletionListeners(snapshot, failure);
            }
        });
    }

    private void failSnapshotCompletionListeners(Snapshot snapshot, Exception e) {
        final List<ActionListener<Tuple<RepositoryData, SnapshotInfo>>> completionListeners = snapshotCompletionListeners.remove(snapshot);
        if (completionListeners != null) {
            try {
                ActionListener.onFailure(completionListeners, e);
            } catch (Exception ex) {
                logger.warn("Failed to notify listeners", ex);
            }
        }
        endingSnapshots.remove(snapshot);
    }

    /**
     * Deletes snapshots from the repository. In-progress snapshots matched by the delete will be aborted before deleting them.
     *
     * @param request         delete snapshot request
     * @param listener        listener
     */
    public void deleteSnapshots(final DeleteSnapshotRequest request, final ActionListener<Void> listener) {

        final String[] snapshotNames = request.snapshots();
        final String repositoryName = request.repository();
        logger.info(() -> new ParameterizedMessage("deleting snapshots [{}] from repository [{}]",
                Strings.arrayToCommaDelimitedString(snapshotNames), repositoryName));

        final Repository repository = repositoriesService.repository(repositoryName);
        repository.executeConsistentStateUpdate(repositoryData -> new ClusterStateUpdateTask(Priority.NORMAL) {

            private Snapshot runningSnapshot;

            private ClusterStateUpdateTask deleteFromRepoTask;

            private boolean abortedDuringInit = false;

            private List<SnapshotId> outstandingDeletes;

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                if (snapshotNames.length > 1 && currentState.nodes().getMinNodeVersion().before(MULTI_DELETE_VERSION)) {
                    throw new IllegalArgumentException("Deleting multiple snapshots in a single request is only supported in version [ "
                            + MULTI_DELETE_VERSION + "] but cluster contained node of version [" + currentState.nodes().getMinNodeVersion()
                            + "]");
                }
                final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                final SnapshotsInProgress.Entry snapshotEntry = findInProgressSnapshot(snapshots, snapshotNames, repositoryName);
                final List<SnapshotId> snapshotIds = matchingSnapshotIds(
                        snapshotEntry == null ? null : snapshotEntry.snapshot().getSnapshotId(),
                        repositoryData, snapshotNames, repositoryName);
                if (snapshotEntry == null) {
                    deleteFromRepoTask =
                            createDeleteStateUpdate(snapshotIds, repositoryName, repositoryData.getGenId(), Priority.NORMAL, listener);
                    return deleteFromRepoTask.execute(currentState);
                }

                runningSnapshot = snapshotEntry.snapshot();
                final ImmutableOpenMap<ShardId, ShardSnapshotStatus> shards;

                final State state = snapshotEntry.state();
                final String failure;

                outstandingDeletes = new ArrayList<>(snapshotIds);
                if (state != State.INIT) {
                    // INIT state snapshots won't ever be physically written to the repository but all other states will end up in the repo
                    outstandingDeletes.add(runningSnapshot.getSnapshotId());
                }
                if (state == State.INIT) {
                    // snapshot was created by an older version of ES and never moved past INIT, remove it from the cluster state
                    shards = snapshotEntry.shards();
                    assert shards.isEmpty();
                    failure = null;
                    abortedDuringInit = true;
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
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE,
                    new SnapshotsInProgress(snapshots.entries().stream()
                        // remove init state snapshot we found from a previous master if there was one
                        .filter(existing -> abortedDuringInit == false || existing.equals(snapshotEntry) == false)
                        .map(existing -> {
                            if (existing.equals(snapshotEntry)) {
                                return new SnapshotsInProgress.Entry(snapshotEntry, State.ABORTED, shards, failure);
                            }
                            return existing;
                        }).collect(Collectors.toUnmodifiableList()))).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (deleteFromRepoTask != null) {
                    assert outstandingDeletes == null : "Shouldn't have outstanding deletes after already starting delete task";
                    deleteFromRepoTask.clusterStateProcessed(source, oldState, newState);
                    return;
                }
                if (abortedDuringInit) {
                    // BwC Path where we removed an outdated INIT state snapshot from the cluster state
                    logger.info("Successfully aborted snapshot [{}]", runningSnapshot);
                    if (outstandingDeletes.isEmpty()) {
                        listener.onResponse(null);
                    } else {
                        clusterService.submitStateUpdateTask("delete snapshot",
                                createDeleteStateUpdate(outstandingDeletes, repositoryName, repositoryData.getGenId(),
                                        Priority.IMMEDIATE, listener));
                    }
                    return;
                }
                logger.trace("adding snapshot completion listener to wait for deleted snapshot to finish");
                addListener(runningSnapshot, ActionListener.wrap(
                    result -> {
                        logger.debug("deleted snapshot completed - deleting files");
                        clusterService.submitStateUpdateTask("delete snapshot",
                                createDeleteStateUpdate(outstandingDeletes, repositoryName,
                                        result.v1().getGenId(), Priority.IMMEDIATE, listener));
                    },
                    e -> {
                       if (ExceptionsHelper.unwrap(e, NotMasterException.class, FailedToCommitClusterStateException.class) != null) {
                                logger.warn("master failover before deleted snapshot could complete", e);
                                // Just pass the exception to the transport handler as is so it is retried on the new master
                                listener.onFailure(e);
                            } else {
                                logger.warn("deleted snapshot failed", e);
                                listener.onFailure(
                                    new SnapshotMissingException(runningSnapshot.getRepository(), runningSnapshot.getSnapshotId(), e));
                            }
                        }
                ));
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }
        }, "delete snapshot", listener::onFailure);
    }

    private static List<SnapshotId> matchingSnapshotIds(@Nullable SnapshotId inProgress, RepositoryData repositoryData,
                                                        String[] snapshotsOrPatterns, String repositoryName) {
        final Map<String, SnapshotId> allSnapshotIds = repositoryData.getSnapshotIds().stream().collect(
                Collectors.toMap(SnapshotId::getName, Function.identity()));
        final Set<SnapshotId> foundSnapshots = new HashSet<>();
        for (String snapshotOrPattern : snapshotsOrPatterns) {
            if (Regex.isSimpleMatchPattern(snapshotOrPattern)) {
                for (Map.Entry<String, SnapshotId> entry : allSnapshotIds.entrySet()) {
                    if (Regex.simpleMatch(snapshotOrPattern, entry.getKey())) {
                        foundSnapshots.add(entry.getValue());
                    }
                }
            } else {
                final SnapshotId foundId = allSnapshotIds.get(snapshotOrPattern);
                if (foundId == null) {
                    if (inProgress == null || inProgress.getName().equals(snapshotOrPattern) == false) {
                        throw new SnapshotMissingException(repositoryName, snapshotOrPattern);
                    }
                } else {
                    foundSnapshots.add(allSnapshotIds.get(snapshotOrPattern));
                }
            }
        }
        return List.copyOf(foundSnapshots);
    }

    // Return in-progress snapshot entry by name and repository in the given cluster state or null if none is found
    @Nullable
    private static SnapshotsInProgress.Entry findInProgressSnapshot(@Nullable SnapshotsInProgress snapshots, String[] snapshotNames,
                                                                    String repositoryName) {
        if (snapshots == null) {
            return null;
        }
        SnapshotsInProgress.Entry snapshotEntry = null;
        for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
            if (entry.repository().equals(repositoryName)
                && Regex.simpleMatch(snapshotNames, entry.snapshot().getSnapshotId().getName())) {
                snapshotEntry = entry;
                break;
            }
        }
        return snapshotEntry;
    }

    private ClusterStateUpdateTask createDeleteStateUpdate(List<SnapshotId> snapshotIds, String repoName, long repositoryStateId,
                                                           Priority priority, ActionListener<Void> listener) {
        // Short circuit to noop state update if there isn't anything to delete
        if (snapshotIds.isEmpty()) {
            return new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(null);
                }
            };
        }
        return new ClusterStateUpdateTask(priority) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(new Snapshot(repoName, snapshotIds.get(0)),
                        "cannot delete - another snapshot is currently being deleted in [" + deletionsInProgress + "]");
                }
                final RepositoryCleanupInProgress repositoryCleanupInProgress = currentState.custom(RepositoryCleanupInProgress.TYPE);
                if (repositoryCleanupInProgress != null && repositoryCleanupInProgress.hasCleanupInProgress()) {
                    throw new ConcurrentSnapshotExecutionException(new Snapshot(repoName, snapshotIds.get(0)),
                        "cannot delete snapshots while a repository cleanup is in-progress in [" + repositoryCleanupInProgress + "]");
                }
                RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE);
                if (restoreInProgress != null) {
                    // don't allow snapshot deletions while a restore is taking place,
                    // otherwise we could end up deleting a snapshot that is being restored
                    // and the files the restore depends on would all be gone

                    for (RestoreInProgress.Entry entry : restoreInProgress) {
                        if (repoName.equals(entry.snapshot().getRepository()) && snapshotIds.contains(entry.snapshot().getSnapshotId())) {
                            throw new ConcurrentSnapshotExecutionException(new Snapshot(repoName, snapshotIds.get(0)),
                                "cannot delete snapshot during a restore in progress in [" + restoreInProgress + "]");
                        }
                    }
                }
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots != null && snapshots.entries().isEmpty() == false) {
                    // However other snapshots are running - cannot continue
                    throw new ConcurrentSnapshotExecutionException(
                            repoName, snapshotIds.toString(), "another snapshot is currently running cannot delete");
                }
                // add the snapshot deletion to the cluster state
                SnapshotDeletionsInProgress.Entry entry = new SnapshotDeletionsInProgress.Entry(
                        snapshotIds,
                        repoName,
                        threadPool.absoluteTimeInMillis(),
                        repositoryStateId
                );
                if (deletionsInProgress != null) {
                    deletionsInProgress = deletionsInProgress.withAddedEntry(entry);
                } else {
                    deletionsInProgress = SnapshotDeletionsInProgress.newInstance(entry);
                }
                return ClusterState.builder(currentState).putCustom(SnapshotDeletionsInProgress.TYPE, deletionsInProgress).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                deleteSnapshotsFromRepository(repoName, snapshotIds, listener, repositoryStateId, newState.nodes().getMinNodeVersion());
            }
        };
    }

    /**
     * Determines the minimum {@link Version} that the snapshot repository must be compatible with from the current nodes in the cluster
     * and the contents of the repository. The minimum version is determined as the lowest version found across all snapshots in the
     * repository and all nodes in the cluster.
     *
     * @param minNodeVersion minimum node version in the cluster
     * @param repositoryData current {@link RepositoryData} of that repository
     * @param excluded       snapshot id to ignore when computing the minimum version
     *                       (used to use newer metadata version after a snapshot delete)
     * @return minimum node version that must still be able to read the repository metadata
     */
    public Version minCompatibleVersion(Version minNodeVersion, RepositoryData repositoryData, @Nullable Collection<SnapshotId> excluded) {
        Version minCompatVersion = minNodeVersion;
        final Collection<SnapshotId> snapshotIds = repositoryData.getSnapshotIds();
        for (SnapshotId snapshotId : snapshotIds.stream().filter(excluded == null ? sn -> true : Predicate.not(excluded::contains))
                .collect(Collectors.toList())) {
            final Version known = repositoryData.getVersion(snapshotId);
            // If we don't have the version cached in the repository data yet we load it from the snapshot info blobs
            if (known == null) {
                assert repositoryData.shardGenerations().totalShards() == 0 :
                    "Saw shard generations [" + repositoryData.shardGenerations() +
                        "] but did not have versions tracked for snapshot [" + snapshotId + "]";
                return OLD_SNAPSHOT_FORMAT;
            } else {
                minCompatVersion = minCompatVersion.before(known) ? minCompatVersion : known;
            }
        }
        return minCompatVersion;
    }

    /**
     * Checks whether the metadata version supports writing {@link ShardGenerations} to the repository.
     *
     * @param repositoryMetaVersion version to check
     * @return true if version supports {@link ShardGenerations}
     */
    public static boolean useShardGenerations(Version repositoryMetaVersion) {
        return repositoryMetaVersion.onOrAfter(SHARD_GEN_IN_REPO_DATA_VERSION);
    }

    /**
     * Deletes snapshot from repository
     *
     * @param repoName          repository name
     * @param snapshotIds       snapshot ids
     * @param listener          listener
     * @param repositoryStateId the unique id representing the state of the repository at the time the deletion began
     * @param minNodeVersion    minimum node version in the cluster
     */
    private void deleteSnapshotsFromRepository(String repoName, Collection<SnapshotId> snapshotIds, @Nullable ActionListener<Void> listener,
                                               long repositoryStateId, Version minNodeVersion) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(listener, l -> {
            Repository repository = repositoriesService.repository(repoName);
            repository.getRepositoryData(ActionListener.wrap(repositoryData -> repository.deleteSnapshots(snapshotIds,
                repositoryStateId,
                minCompatibleVersion(minNodeVersion, repositoryData, snapshotIds),
                ActionListener.wrap(v -> {
                        logger.info("snapshots {} deleted", snapshotIds);
                        removeSnapshotDeletionFromClusterState(snapshotIds, null, l);
                    }, ex -> removeSnapshotDeletionFromClusterState(snapshotIds, ex, l)
                )), ex -> removeSnapshotDeletionFromClusterState(snapshotIds, ex, l)));
        }));
    }

    /**
     * Removes the snapshot deletion from {@link SnapshotDeletionsInProgress} in the cluster state.
     */
    private void removeSnapshotDeletionFromClusterState(final Collection<SnapshotId> snapshotIds, @Nullable final Exception failure,
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
                logger.warn(() -> new ParameterizedMessage("{} failed to remove snapshot deletion metadata", snapshotIds), e);
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
                        logger.info("Successfully deleted snapshots {}", snapshotIds);
                        listener.onResponse(null);
                    }
                }
            }
        });
    }

    /**
     * Calculates the list of shards that should be included into the current snapshot
     *
     * @param clusterState        cluster state
     * @param indices             Indices to snapshot
     * @param useShardGenerations whether to write {@link ShardGenerations} during the snapshot
     * @return list of shard to be included into current snapshot
     */
    private static ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards(ClusterState clusterState,
                                                                                             List<IndexId> indices,
                                                                                             boolean useShardGenerations,
                                                                                             RepositoryData repositoryData) {
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
        Metadata metadata = clusterState.metadata();
        final ShardGenerations shardGenerations = repositoryData.shardGenerations();
        for (IndexId index : indices) {
            final String indexName = index.getName();
            final boolean isNewIndex = repositoryData.getIndices().containsKey(indexName) == false;
            IndexMetadata indexMetadata = metadata.index(indexName);
            if (indexMetadata == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                builder.put(new ShardId(indexName, IndexMetadata.INDEX_UUID_NA_VALUE, 0),
                    new SnapshotsInProgress.ShardSnapshotStatus(null, ShardState.MISSING, "missing index", null));
            } else {
                IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(indexName);
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                    final String shardRepoGeneration;
                    if (useShardGenerations) {
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
                    IndexMetadata indexMetadata = currentState.metadata().index(index.getName());
                    if (indexMetadata != null && indicesToCheck.contains(indexMetadata.getIndex())) {
                        indices.add(indexMetadata.getIndex());
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
    private void addListener(Snapshot snapshot, ActionListener<Tuple<RepositoryData, SnapshotInfo>> listener) {
        snapshotCompletionListeners.computeIfAbsent(snapshot, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    @Override
    protected void doStart() {
        assert this.updateSnapshotStatusHandler != null;
        assert transportService.getRequestHandler(UPDATE_SNAPSHOT_STATUS_ACTION_NAME) != null;
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        clusterService.removeApplier(this);
    }

    private static class SnapshotStateExecutor implements ClusterStateTaskExecutor<UpdateIndexShardSnapshotStatusRequest> {

        @Override
        public ClusterTasksResult<UpdateIndexShardSnapshotStatusRequest>
                        execute(ClusterState currentState, List<UpdateIndexShardSnapshotStatusRequest> tasks) {
            final SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
            if (snapshots != null) {
                int changedCount = 0;
                final List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                    ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                    boolean updated = false;

                    for (UpdateIndexShardSnapshotStatusRequest updateSnapshotState : tasks) {
                        if (entry.snapshot().equals(updateSnapshotState.snapshot())) {
                            logger.trace("[{}] Updating shard [{}] with status [{}]", updateSnapshotState.snapshot(),
                                updateSnapshotState.shardId(), updateSnapshotState.status().state());
                            if (updated == false) {
                                shards.putAll(entry.shards());
                                updated = true;
                            }
                            shards.put(updateSnapshotState.shardId(), updateSnapshotState.status());
                            changedCount++;
                        }
                    }

                    if (updated) {
                        if (completed(shards.values()) == false) {
                            entries.add(new SnapshotsInProgress.Entry(entry, shards.build()));
                        } else {
                            // Snapshot is finished - mark it as done
                            // TODO: Add PARTIAL_SUCCESS status?
                            SnapshotsInProgress.Entry updatedEntry = new SnapshotsInProgress.Entry(entry, State.SUCCESS, shards.build());
                            entries.add(updatedEntry);
                        }
                    } else {
                        entries.add(entry);
                    }
                }
                if (changedCount > 0) {
                    logger.trace("changed cluster state triggered by {} snapshot state updates", changedCount);
                    return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks)
                        .build(ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE,
                            new SnapshotsInProgress(unmodifiableList(entries))).build());
                }
            }
            return ClusterTasksResult.<UpdateIndexShardSnapshotStatusRequest>builder().successes(tasks).build(currentState);
        }
    }

    /**
     * Updates the shard status on master node
     *
     * @param request update shard status request
     */
    private void innerUpdateSnapshotState(final UpdateIndexShardSnapshotStatusRequest request,
                                          ActionListener<UpdateIndexShardSnapshotStatusResponse> listener) {
        logger.trace("received updated snapshot restore state [{}]", request);
        clusterService.submitStateUpdateTask(
                "update snapshot state",
                request,
                ClusterStateTaskConfig.build(Priority.NORMAL),
                snapshotStateExecutor,
                new ClusterStateTaskListener() {
                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        try {
                            listener.onResponse(new UpdateIndexShardSnapshotStatusResponse());
                        } finally {
                            // Maybe this state update completed the snapshot. If we are not already ending it because of a concurrent
                            // state update we check if its state is completed and end it if it is.
                            if (endingSnapshots.contains(request.snapshot()) == false) {
                                final SnapshotsInProgress snapshotsInProgress = newState.custom(SnapshotsInProgress.TYPE);
                                final SnapshotsInProgress.Entry updatedEntry = snapshotsInProgress.snapshot(request.snapshot());
                                if (updatedEntry.state().completed()) {
                                    endSnapshot(updatedEntry, newState.metadata());
                                }
                            }
                        }
                    }
                });
    }

    private class UpdateSnapshotStatusAction
            extends TransportMasterNodeAction<UpdateIndexShardSnapshotStatusRequest, UpdateIndexShardSnapshotStatusResponse> {
        UpdateSnapshotStatusAction(TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters,
                                   IndexNameExpressionResolver indexNameExpressionResolver) {
            super(UPDATE_SNAPSHOT_STATUS_ACTION_NAME, false, transportService, clusterService, threadPool,
                    actionFilters, UpdateIndexShardSnapshotStatusRequest::new, indexNameExpressionResolver
            );
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected UpdateIndexShardSnapshotStatusResponse read(StreamInput in) throws IOException {
            return new UpdateIndexShardSnapshotStatusResponse(in);
        }

        @Override
        protected void masterOperation(Task task, UpdateIndexShardSnapshotStatusRequest request, ClusterState state,
                                       ActionListener<UpdateIndexShardSnapshotStatusResponse> listener) {
            innerUpdateSnapshotState(request, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(UpdateIndexShardSnapshotStatusRequest request, ClusterState state) {
            return null;
        }
    }
}
