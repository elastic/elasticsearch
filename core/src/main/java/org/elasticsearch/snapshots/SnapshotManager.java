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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.snapshots.SnapshotsService.CreateSnapshotListener;
import org.elasticsearch.snapshots.SnapshotsService.SnapshotCompletionListener;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.snapshots.SnapshotsShardService.completed;

/**
 * Internal component that is responsible for single snapshot operations and it's lifecycle.
 *
 * It creates, deletes and aborts snapshot.
 */
public class SnapshotManager extends AbstractComponent {

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final ThreadPool threadPool;

    private final CopyOnWriteArrayList<SnapshotsService.SnapshotCompletionListener> snapshotCompletionListeners = new CopyOnWriteArrayList<>();


    @Inject
    public SnapshotManager(Settings settings, ClusterService clusterService, RepositoriesService repositoriesService, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
    }

    public void createSnapshot(final SnapshotsService.SnapshotRequest request, final CreateSnapshotListener listener) {
        final SnapshotId snapshotId = new SnapshotId(request.repository(), request.name());
        clusterService.submitStateUpdateTask(request.cause(), new TimeoutClusterStateUpdateTask() {

            private SnapshotsInProgress.Entry newSnapshot = null;

            @Override
            public ClusterState execute(ClusterState currentState) {
                validate(request, currentState);

                MetaData metaData = currentState.metaData();
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots == null || snapshots.entries().isEmpty()) {
                    // Store newSnapshot here to be processed in clusterStateProcessed
                    ImmutableList<String> indices = ImmutableList.copyOf(metaData.concreteIndices(request.indicesOptions(), request.indices()));
                    logger.trace("[{}][{}] creating snapshot for indices [{}]", request.repository(), request.name(), indices);
                    newSnapshot = new SnapshotsInProgress.Entry(snapshotId, request.includeGlobalState(), SnapshotsInProgress.State.INIT, indices, System.currentTimeMillis(), null);
                    snapshots = new SnapshotsInProgress(newSnapshot);
                } else {
                    // TODO: What should we do if a snapshot is already running?
                    throw new ConcurrentSnapshotExecutionException(snapshotId, "a snapshot is already running");
                }
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("[{}][{}] failed to create snapshot", t, request.repository(), request.name());
                newSnapshot = null;
                listener.onFailure(t);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                if (newSnapshot != null) {
                    threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new Runnable() {
                        @Override
                        public void run() {
                            beginSnapshot(newState, newSnapshot, request.partial(), listener);
                        }
                    });
                }
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

        });
    }

    /**
     * Validates snapshot request
     *
     * @param request snapshot request
     * @param state   current cluster state
     * @throws org.elasticsearch.ElasticsearchException
     */
    private void validate(SnapshotsService.SnapshotRequest request, ClusterState state) {
        RepositoriesMetaData repositoriesMetaData = state.getMetaData().custom(RepositoriesMetaData.TYPE);
        if (repositoriesMetaData == null || repositoriesMetaData.repository(request.repository()) == null) {
            throw new RepositoryMissingException(request.repository());
        }
        if (!Strings.hasLength(request.name())) {
            throw new InvalidSnapshotNameException(new SnapshotId(request.repository(), request.name()), "cannot be empty");
        }
        if (request.name().contains(" ")) {
            throw new InvalidSnapshotNameException(new SnapshotId(request.repository(), request.name()), "must not contain whitespace");
        }
        if (request.name().contains(",")) {
            throw new InvalidSnapshotNameException(new SnapshotId(request.repository(), request.name()), "must not contain ','");
        }
        if (request.name().contains("#")) {
            throw new InvalidSnapshotNameException(new SnapshotId(request.repository(), request.name()), "must not contain '#'");
        }
        if (request.name().charAt(0) == '_') {
            throw new InvalidSnapshotNameException(new SnapshotId(request.repository(), request.name()), "must not start with '_'");
        }
        if (!request.name().toLowerCase(Locale.ROOT).equals(request.name())) {
            throw new InvalidSnapshotNameException(new SnapshotId(request.repository(), request.name()), "must be lowercase");
        }
        if (!Strings.validFileName(request.name())) {
            throw new InvalidSnapshotNameException(new SnapshotId(request.repository(), request.name()), "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
    }

    /**
     * Starts snapshot.
     * <p/>
     * Creates snapshot in repository and updates snapshot metadata record with list of shards that needs to be processed.
     *
     * @param clusterState               cluster state
     * @param snapshot                   snapshot meta data
     * @param partial                    allow partial snapshots
     * @param userCreateSnapshotListener listener
     */
    private void beginSnapshot(ClusterState clusterState, final SnapshotsInProgress.Entry snapshot, final boolean partial, final CreateSnapshotListener userCreateSnapshotListener) {
        boolean snapshotCreated = false;
        try {
            Repository repository = repositoriesService.repository(snapshot.snapshotId().getRepository());

            MetaData metaData = clusterState.metaData();
            if (!snapshot.includeGlobalState()) {
                // Remove global state from the cluster state
                MetaData.Builder builder = MetaData.builder();
                for (String index : snapshot.indices()) {
                    builder.put(metaData.index(index), false);
                }
                metaData = builder.build();
            }

            repository.initializeSnapshot(snapshot.snapshotId(), snapshot.indices(), metaData);
            snapshotCreated = true;
            if (snapshot.indices().isEmpty()) {
                // No indices in this snapshot - we are done
                userCreateSnapshotListener.onResponse();
                endSnapshot(snapshot);
                return;
            }
            clusterService.submitStateUpdateTask("update_snapshot [" + snapshot.snapshotId().getSnapshot() + "]", new ProcessedClusterStateUpdateTask() {
                boolean accepted = false;
                SnapshotsInProgress.Entry updatedSnapshot;
                String failure = null;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    MetaData metaData = currentState.metaData();
                    SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                    ImmutableList.Builder<SnapshotsInProgress.Entry> entries = ImmutableList.builder();
                    for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                        if (entry.snapshotId().equals(snapshot.snapshotId())) {
                            // Replace the snapshot that was just created
                            ImmutableMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = shards(currentState, entry.indices());
                            if (!partial) {
                                Tuple<Set<String>, Set<String>> indicesWithMissingShards = indicesWithMissingShards(shards, currentState.metaData());
                                Set<String> missing = indicesWithMissingShards.v1();
                                Set<String> closed = indicesWithMissingShards.v2();
                                if (missing.isEmpty() == false || closed.isEmpty() == false) {
                                    StringBuilder failureMessage = new StringBuilder();
                                    updatedSnapshot = new SnapshotsInProgress.Entry(entry, SnapshotsInProgress.State.FAILED, shards);
                                    entries.add(updatedSnapshot);
                                    if (missing.isEmpty() == false ) {
                                        failureMessage.append("Indices don't have primary shards ");
                                        failureMessage.append(missing);
                                    }
                                    if (closed.isEmpty() == false ) {
                                        if (failureMessage.length() > 0) {
                                            failureMessage.append("; ");
                                        }
                                        failureMessage.append("Indices are closed ");
                                        failureMessage.append(closed);
                                    }
                                    failure = failureMessage.toString();
                                    continue;
                                }
                            }
                            updatedSnapshot = new SnapshotsInProgress.Entry(entry, SnapshotsInProgress.State.STARTED, shards);
                            entries.add(updatedSnapshot);
                            if (!completed(shards.values())) {
                                accepted = true;
                            }
                        } else {
                            entries.add(entry);
                        }
                    }
                    return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(entries.build())).build();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.warn("[{}] failed to create snapshot", t, snapshot.snapshotId());
                    removeSnapshotFromClusterState(snapshot.snapshotId(), null, t);
                    try {
                        repositoriesService.repository(snapshot.snapshotId().getRepository()).finalizeSnapshot(
                                snapshot.snapshotId(), snapshot.indices(), snapshot.startTime(), ExceptionsHelper.detailedMessage(t), 0, ImmutableList.<SnapshotShardFailure>of());
                    } catch (Throwable t2) {
                        logger.warn("[{}] failed to close snapshot in repository", snapshot.snapshotId());
                    }
                    userCreateSnapshotListener.onFailure(t);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    // The userCreateSnapshotListener.onResponse() notifies caller that the snapshot was accepted
                    // for processing. If client wants to wait for the snapshot completion, it can register snapshot
                    // completion listener in this method. For the snapshot completion to work properly, the snapshot
                    // should still exist when listener is registered.
                    userCreateSnapshotListener.onResponse();

                    // Now that snapshot completion listener is registered we can end the snapshot if needed
                    // We should end snapshot only if 1) we didn't accept it for processing (which happens when there
                    // is nothing to do) and 2) there was a snapshot in metadata that we should end. Otherwise we should
                    // go ahead and continue working on this snapshot rather then end here.
                    if (!accepted && updatedSnapshot != null) {
                        endSnapshot(updatedSnapshot, failure);
                    }
                }
            });
        } catch (Throwable t) {
            logger.warn("failed to create snapshot [{}]", t, snapshot.snapshotId());
            removeSnapshotFromClusterState(snapshot.snapshotId(), null, t);
            if (snapshotCreated) {
                try {
                    repositoriesService.repository(snapshot.snapshotId().getRepository()).finalizeSnapshot(snapshot.snapshotId(), snapshot.indices(), snapshot.startTime(),
                            ExceptionsHelper.detailedMessage(t), 0, ImmutableList.<SnapshotShardFailure>of());
                } catch (Throwable t2) {
                    logger.warn("[{}] failed to close snapshot in repository", snapshot.snapshotId());
                }
            }
            userCreateSnapshotListener.onFailure(t);
        }
    }

    /**
     * Calculates the list of shards that should be included into the current snapshot
     *
     * @param clusterState cluster state
     * @param indices      list of indices to be snapshotted
     * @return list of shard to be included into current snapshot
     */
    private ImmutableMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards(ClusterState clusterState, ImmutableList<String> indices) {
        ImmutableMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableMap.builder();
        MetaData metaData = clusterState.metaData();
        for (String index : indices) {
            IndexMetaData indexMetaData = metaData.index(index);
            if (indexMetaData == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                builder.put(new ShardId(index, 0), new SnapshotsInProgress.ShardSnapshotStatus(null, SnapshotsInProgress.State.MISSING, "missing index"));
            } else if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                for (int i = 0; i < indexMetaData.numberOfShards(); i++) {
                    ShardId shardId = new ShardId(index, i);
                    builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(null, SnapshotsInProgress.State.MISSING, "index is closed"));
                }
            } else {
                IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(index);
                for (int i = 0; i < indexMetaData.numberOfShards(); i++) {
                    ShardId shardId = new ShardId(index, i);
                    if (indexRoutingTable != null) {
                        ShardRouting primary = indexRoutingTable.shard(i).primaryShard();
                        if (primary == null || !primary.assignedToNode()) {
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(null, SnapshotsInProgress.State.MISSING, "primary shard is not allocated"));
                        } else if (primary.relocating() || primary.initializing()) {
                            // The WAITING state was introduced in V1.2.0 - don't use it if there are nodes with older version in the cluster
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), SnapshotsInProgress.State.WAITING));
                        } else if (!primary.started()) {
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), SnapshotsInProgress.State.MISSING, "primary shard hasn't been started yet"));
                        } else {
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId()));
                        }
                    } else {
                        builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(null, SnapshotsInProgress.State.MISSING, "missing routing table"));
                    }
                }
            }
        }

        return builder.build();
    }

    /**
     * Deletes snapshot from repository.
     * <p/>
     * If the snapshot is still running cancels the snapshot first and then deletes it from the repository.
     *
     * @param snapshotId snapshot id
     * @param listener   listener
     */
    public void deleteSnapshot(final SnapshotId snapshotId, final SnapshotsService.DeleteSnapshotListener listener) {
        clusterService.submitStateUpdateTask("delete snapshot", new ProcessedClusterStateUpdateTask() {

            boolean waitForSnapshot = false;

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots == null) {
                    // No snapshots running - we can continue
                    return currentState;
                }
                SnapshotsInProgress.Entry snapshot = snapshots.snapshot(snapshotId);
                if (snapshot == null) {
                    // This snapshot is not running - continue
                    if (!snapshots.entries().isEmpty()) {
                        // However other snapshots are running - cannot continue
                        throw new ConcurrentSnapshotExecutionException(snapshotId, "another snapshot is currently running cannot delete");
                    }
                    return currentState;
                } else {
                    // This snapshot is currently running - stopping shards first
                    waitForSnapshot = true;
                    ImmutableMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards;
                    if (snapshot.state() == SnapshotsInProgress.State.STARTED && snapshot.shards() != null) {
                        // snapshot is currently running - stop started shards
                        ImmutableMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsBuilder = ImmutableMap.builder();
                        for (ImmutableMap.Entry<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : snapshot.shards().entrySet()) {
                            SnapshotsInProgress.ShardSnapshotStatus status = shardEntry.getValue();
                            if (!status.state().completed()) {
                                shardsBuilder.put(shardEntry.getKey(), new SnapshotsInProgress.ShardSnapshotStatus(status.nodeId(), SnapshotsInProgress.State.ABORTED));
                            } else {
                                shardsBuilder.put(shardEntry.getKey(), status);
                            }
                        }
                        shards = shardsBuilder.build();
                    } else if (snapshot.state() == SnapshotsInProgress.State.INIT) {
                        // snapshot hasn't started yet - end it
                        shards = snapshot.shards();
                        endSnapshot(snapshot);
                    } else {
                        boolean hasUncompletedShards = false;
                        // Cleanup in case a node gone missing and snapshot wasn't updated for some reason
                        for (SnapshotsInProgress.ShardSnapshotStatus shardStatus : snapshot.shards().values()) {
                            // Check if we still have shard running on existing nodes
                            if (shardStatus.state().completed() == false && shardStatus.nodeId() != null && currentState.nodes().get(shardStatus.nodeId()) != null) {
                                hasUncompletedShards = true;
                                break;
                            }
                        }
                        if (hasUncompletedShards) {
                            // snapshot is being finalized - wait for shards to complete finalization process
                            logger.debug("trying to delete completed snapshot - should wait for shards to finalize on all nodes");
                            return currentState;
                        } else {
                            // no shards to wait for - finish the snapshot
                            logger.debug("trying to delete completed snapshot with no finalizing shards - can delete immediately");
                            shards = snapshot.shards();
                            endSnapshot(snapshot);
                        }
                    }
                    SnapshotsInProgress.Entry newSnapshot = new SnapshotsInProgress.Entry(snapshot, SnapshotsInProgress.State.ABORTED, shards);
                    snapshots = new SnapshotsInProgress(newSnapshot);
                    return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
                }
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (waitForSnapshot) {
                    logger.trace("adding snapshot completion listener to wait for deleted snapshot to finish");
                    addListener(new SnapshotCompletionListener() {
                        @Override
                        public void onSnapshotCompletion(SnapshotId completedSnapshotId, SnapshotInfo snapshot) {
                            if (completedSnapshotId.equals(snapshotId)) {
                                logger.trace("deleted snapshot completed - deleting files");
                                removeListener(this);
                                deleteSnapshotFromRepository(snapshotId, listener);
                            }
                        }

                        @Override
                        public void onSnapshotFailure(SnapshotId failedSnapshotId, Throwable t) {
                            if (failedSnapshotId.equals(snapshotId)) {
                                logger.trace("deleted snapshot failed - deleting files", t);
                                removeListener(this);
                                deleteSnapshotFromRepository(snapshotId, listener);
                            }
                        }
                    });
                } else {
                    logger.trace("deleted snapshot is not running - deleting files");
                    deleteSnapshotFromRepository(snapshotId, listener);
                }
            }
        });
    }


    /**
     * Adds snapshot completion listener
     *
     * @param listener listener
     */
    public void addListener(SnapshotCompletionListener listener) {
        this.snapshotCompletionListeners.add(listener);
    }

    /**
     * Removes snapshot completion listener
     *
     * @param listener listener
     */
    public void removeListener(SnapshotCompletionListener listener) {
        this.snapshotCompletionListeners.remove(listener);
    }


    /**
     * Deletes snapshot from repository
     *
     * @param snapshotId snapshot id
     * @param listener   listener
     */
    private void deleteSnapshotFromRepository(final SnapshotId snapshotId, final SnapshotsService.DeleteSnapshotListener listener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Repository repository = repositoriesService.repository(snapshotId.getRepository());
                    repository.deleteSnapshot(snapshotId);
                    listener.onResponse();
                } catch (Throwable t) {
                    listener.onFailure(t);
                }
            }
        });
    }

    /**
     * Finalizes the shard in repository and then removes it from cluster state
     * <p/>
     * This is non-blocking method that runs on a thread from SNAPSHOT thread pool
     *
     * @param entry snapshot
     */
    public void endSnapshot(SnapshotsInProgress.Entry entry) {
        endSnapshot(entry, null);
    }


    /**
     * Finalizes the shard in repository and then removes it from cluster state
     * <p/>
     * This is non-blocking method that runs on a thread from SNAPSHOT thread pool
     *
     * @param entry   snapshot
     * @param failure failure reason or null if snapshot was successful
     */
    private void endSnapshot(final SnapshotsInProgress.Entry entry, final String failure) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new Runnable() {
            @Override
            public void run() {
                SnapshotId snapshotId = entry.snapshotId();
                try {
                    final Repository repository = repositoriesService.repository(snapshotId.getRepository());
                    logger.trace("[{}] finalizing snapshot in repository, state: [{}], failure[{}]", snapshotId, entry.state(), failure);
                    ArrayList<ShardSearchFailure> failures = newArrayList();
                    ArrayList<SnapshotShardFailure> shardFailures = newArrayList();
                    for (Map.Entry<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardStatus : entry.shards().entrySet()) {
                        ShardId shardId = shardStatus.getKey();
                        SnapshotsInProgress.ShardSnapshotStatus status = shardStatus.getValue();
                        if (status.state().failed()) {
                            failures.add(new ShardSearchFailure(status.reason(), new SearchShardTarget(status.nodeId(), shardId.getIndex(), shardId.id())));
                            shardFailures.add(new SnapshotShardFailure(status.nodeId(), shardId.getIndex(), shardId.id(), status.reason()));
                        }
                    }
                    Snapshot snapshot = repository.finalizeSnapshot(snapshotId, entry.indices(), entry.startTime(), failure, entry.shards().size(), ImmutableList.copyOf(shardFailures));
                    removeSnapshotFromClusterState(snapshotId, new SnapshotInfo(snapshot), null);
                } catch (Throwable t) {
                    logger.warn("[{}] failed to finalize snapshot", t, snapshotId);
                    removeSnapshotFromClusterState(snapshotId, null, t);
                }
            }
        });
    }

    /**
     * Removes record of running snapshot from cluster state
     *
     * @param snapshotId snapshot id
     * @param snapshot   snapshot info if snapshot was successful
     * @param t          exception if snapshot failed
     */
    private void removeSnapshotFromClusterState(final SnapshotId snapshotId, final SnapshotInfo snapshot, final Throwable t) {
        clusterService.submitStateUpdateTask("remove snapshot metadata", new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots != null) {
                    boolean changed = false;
                    ArrayList<SnapshotsInProgress.Entry> entries = newArrayList();
                    for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                        if (entry.snapshotId().equals(snapshotId)) {
                            changed = true;
                        } else {
                            entries.add(entry);
                        }
                    }
                    if (changed) {
                        snapshots = new SnapshotsInProgress(entries.toArray(new SnapshotsInProgress.Entry[entries.size()]));
                        return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("[{}][{}] failed to remove snapshot metadata", t, snapshotId);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                for (SnapshotCompletionListener listener : snapshotCompletionListeners) {
                    try {
                        if (snapshot != null) {
                            listener.onSnapshotCompletion(snapshotId, snapshot);
                        } else {
                            listener.onSnapshotFailure(snapshotId, t);
                        }
                    } catch (Throwable t) {
                        logger.warn("failed to notify listener [{}]", t, listener);
                    }
                }

            }
        });
    }

    /**
     * Returns list of indices with missing shards, and list of indices that are closed
     *
     * @param shards list of shard statuses
     * @return list of failed and closed indices
     */
    private Tuple<Set<String>, Set<String>> indicesWithMissingShards(ImmutableMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards, MetaData metaData) {
        Set<String> missing = newHashSet();
        Set<String> closed = newHashSet();
        for (ImmutableMap.Entry<ShardId, SnapshotsInProgress.ShardSnapshotStatus> entry : shards.entrySet()) {
            if (entry.getValue().state() == SnapshotsInProgress.State.MISSING) {
                if (metaData.hasIndex(entry.getKey().getIndex()) && metaData.index(entry.getKey().getIndex()).getState() == IndexMetaData.State.CLOSE) {
                    closed.add(entry.getKey().getIndex());
                } else {
                    missing.add(entry.getKey().getIndex());
                }
            }
        }
        return new Tuple<>(missing, closed);
    }

}
