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
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.metadata.SnapshotMetaData.ShardSnapshotStatus;
import org.elasticsearch.cluster.metadata.SnapshotMetaData.State;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.index.snapshots.IndexShardSnapshotAndRestoreService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

/**
 * Service responsible for creating snapshots
 * <p/>
 * A typical snapshot creating process looks like this:
 * <ul>
 * <li>On the master node the {@link #createSnapshot(SnapshotRequest, CreateSnapshotListener)} is called and makes sure that no snapshots is currently running
 * and registers the new snapshot in cluster state</li>
 * <li>When cluster state is updated the {@link #beginSnapshot(ClusterState, SnapshotMetaData.Entry, boolean, CreateSnapshotListener)} method
 * kicks in and initializes the snapshot in the repository and then populates list of shards that needs to be snapshotted in cluster state</li>
 * <li>Each data node is watching for these shards and when new shards scheduled for snapshotting appear in the cluster state, data nodes
 * start processing them through {@link #processIndexShardSnapshots(SnapshotMetaData)} method</li>
 * <li>Once shard snapshot is created data node updates state of the shard in the cluster state using the {@link #updateIndexShardSnapshotStatus(UpdateIndexShardSnapshotStatusRequest)} method</li>
 * <li>When last shard is completed master node in {@link #innerUpdateSnapshotState} method marks the snapshot as completed</li>
 * <li>After cluster state is updated, the {@link #endSnapshot(SnapshotMetaData.Entry)} finalizes snapshot in the repository,
 * notifies all {@link #snapshotCompletionListeners} that snapshot is completed, and finally calls {@link #removeSnapshotFromClusterState(SnapshotId, SnapshotInfo, Throwable)} to remove snapshot from cluster state</li>
 * </ul>
 */
public class SnapshotsService extends AbstractLifecycleComponent<SnapshotsService> implements ClusterStateListener {

    public static final String UPDATE_SNAPSHOT_ACTION_NAME = "internal:cluster/snapshot/update_snapshot";

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final ThreadPool threadPool;

    private final IndicesService indicesService;

    private final TransportService transportService;

    private volatile ImmutableMap<SnapshotId, SnapshotShards> shardSnapshots = ImmutableMap.of();

    private final Lock shutdownLock = new ReentrantLock();

    private final Condition shutdownCondition = shutdownLock.newCondition();

    private final CopyOnWriteArrayList<SnapshotCompletionListener> snapshotCompletionListeners = new CopyOnWriteArrayList<>();


    @Inject
    public SnapshotsService(Settings settings, ClusterService clusterService, RepositoriesService repositoriesService, ThreadPool threadPool,
                            IndicesService indicesService, TransportService transportService) {
        super(settings);
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        this.indicesService = indicesService;
        this.transportService = transportService;

        transportService.registerRequestHandler(UPDATE_SNAPSHOT_ACTION_NAME, UpdateIndexShardSnapshotStatusRequest.class, ThreadPool.Names.SAME, new UpdateSnapshotStateRequestHandler());

        // addLast to make sure that Repository will be created before snapshot
        clusterService.addLast(this);
    }

    /**
     * Retrieves snapshot from repository
     *
     * @param snapshotId snapshot id
     * @return snapshot
     * @throws SnapshotMissingException if snapshot is not found
     */
    public Snapshot snapshot(SnapshotId snapshotId) {
        ImmutableList<SnapshotMetaData.Entry> entries = currentSnapshots(snapshotId.getRepository(), new String[]{snapshotId.getSnapshot()});
        if (!entries.isEmpty()) {
            return inProgressSnapshot(entries.iterator().next());
        }
        return repositoriesService.repository(snapshotId.getRepository()).readSnapshot(snapshotId);
    }

    /**
     * Returns a list of snapshots from repository sorted by snapshot creation date
     *
     * @param repositoryName repository name
     * @return list of snapshots
     */
    public ImmutableList<Snapshot> snapshots(String repositoryName) {
        Set<Snapshot> snapshotSet = newHashSet();
        ImmutableList<SnapshotMetaData.Entry> entries = currentSnapshots(repositoryName, null);
        for (SnapshotMetaData.Entry entry : entries) {
            snapshotSet.add(inProgressSnapshot(entry));
        }
        Repository repository = repositoriesService.repository(repositoryName);
        ImmutableList<SnapshotId> snapshotIds = repository.snapshots();
        for (SnapshotId snapshotId : snapshotIds) {
            snapshotSet.add(repository.readSnapshot(snapshotId));
        }
        ArrayList<Snapshot> snapshotList = newArrayList(snapshotSet);
        CollectionUtil.timSort(snapshotList);
        return ImmutableList.copyOf(snapshotList);
    }

    /**
     * Returns a list of currently running snapshots from repository sorted by snapshot creation date
     *
     * @param repositoryName repository name
     * @return list of snapshots
     */
    public ImmutableList<Snapshot> currentSnapshots(String repositoryName) {
        List<Snapshot> snapshotList = newArrayList();
        ImmutableList<SnapshotMetaData.Entry> entries = currentSnapshots(repositoryName, null);
        for (SnapshotMetaData.Entry entry : entries) {
            snapshotList.add(inProgressSnapshot(entry));
        }
        CollectionUtil.timSort(snapshotList);
        return ImmutableList.copyOf(snapshotList);
    }

    /**
     * Initializes the snapshotting process.
     * <p/>
     * This method is used by clients to start snapshot. It makes sure that there is no snapshots are currently running and
     * creates a snapshot record in cluster state metadata.
     *
     * @param request  snapshot request
     * @param listener snapshot creation listener
     */
    public void createSnapshot(final SnapshotRequest request, final CreateSnapshotListener listener) {
        final SnapshotId snapshotId = new SnapshotId(request.repository(), request.name());
        clusterService.submitStateUpdateTask(request.cause(), new TimeoutClusterStateUpdateTask() {

            private SnapshotMetaData.Entry newSnapshot = null;

            @Override
            public ClusterState execute(ClusterState currentState) {
                validate(request, currentState);

                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                SnapshotMetaData snapshots = metaData.custom(SnapshotMetaData.TYPE);
                if (snapshots == null || snapshots.entries().isEmpty()) {
                    // Store newSnapshot here to be processed in clusterStateProcessed
                    ImmutableList<String> indices = ImmutableList.copyOf(metaData.concreteIndices(request.indicesOptions(), request.indices()));
                    logger.trace("[{}][{}] creating snapshot for indices [{}]", request.repository(), request.name(), indices);
                    newSnapshot = new SnapshotMetaData.Entry(snapshotId, request.includeGlobalState(), State.INIT, indices, System.currentTimeMillis(), null);
                    snapshots = new SnapshotMetaData(newSnapshot);
                } else {
                    // TODO: What should we do if a snapshot is already running?
                    throw new ConcurrentSnapshotExecutionException(snapshotId, "a snapshot is already running");
                }
                mdBuilder.putCustom(SnapshotMetaData.TYPE, snapshots);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
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
                            beginSnapshot(newState, newSnapshot, request.partial, listener);
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
    private void validate(SnapshotRequest request, ClusterState state) throws ElasticsearchException {
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
    private void beginSnapshot(ClusterState clusterState, final SnapshotMetaData.Entry snapshot, final boolean partial, final CreateSnapshotListener userCreateSnapshotListener) {
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
            clusterService.submitStateUpdateTask("update_snapshot [" + snapshot + "]", new ProcessedClusterStateUpdateTask() {
                boolean accepted = false;
                SnapshotMetaData.Entry updatedSnapshot;
                String failure = null;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    MetaData metaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    SnapshotMetaData snapshots = metaData.custom(SnapshotMetaData.TYPE);
                    ImmutableList.Builder<SnapshotMetaData.Entry> entries = ImmutableList.builder();
                    for (SnapshotMetaData.Entry entry : snapshots.entries()) {
                        if (entry.snapshotId().equals(snapshot.snapshotId())) {
                            // Replace the snapshot that was just created
                            ImmutableMap<ShardId, SnapshotMetaData.ShardSnapshotStatus> shards = shards(currentState, entry.indices());
                            if (!partial) {
                                Tuple<Set<String>, Set<String>> indicesWithMissingShards = indicesWithMissingShards(shards, currentState.metaData());
                                Set<String> missing = indicesWithMissingShards.v1();
                                Set<String> closed = indicesWithMissingShards.v2();
                                if (missing.isEmpty() == false || closed.isEmpty() == false) {
                                    StringBuilder failureMessage = new StringBuilder();
                                    updatedSnapshot = new SnapshotMetaData.Entry(entry, State.FAILED, shards);
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
                            updatedSnapshot = new SnapshotMetaData.Entry(entry, State.STARTED, shards);
                            entries.add(updatedSnapshot);
                            if (!completed(shards.values())) {
                                accepted = true;
                            }
                        } else {
                            entries.add(entry);
                        }
                    }
                    mdBuilder.putCustom(SnapshotMetaData.TYPE, new SnapshotMetaData(entries.build()));
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
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

    private Snapshot inProgressSnapshot(SnapshotMetaData.Entry entry) {
        return new Snapshot(entry.snapshotId().getSnapshot(), entry.indices(), entry.startTime());
    }

    /**
     * Returns status of the currently running snapshots
     * <p>
     * This method is executed on master node
     * </p>
     *
     * @param repository repository id
     * @param snapshots  optional list of snapshots that will be used as a filter
     * @return list of metadata for currently running snapshots
     */
    public ImmutableList<SnapshotMetaData.Entry> currentSnapshots(String repository, String[] snapshots) {
        MetaData metaData = clusterService.state().metaData();
        SnapshotMetaData snapshotMetaData = metaData.custom(SnapshotMetaData.TYPE);
        if (snapshotMetaData == null || snapshotMetaData.entries().isEmpty()) {
            return ImmutableList.of();
        }
        if ("_all".equals(repository)) {
            return snapshotMetaData.entries();
        }
        if (snapshotMetaData.entries().size() == 1) {
            // Most likely scenario - one snapshot is currently running
            // Check this snapshot against the query
            SnapshotMetaData.Entry entry = snapshotMetaData.entries().get(0);
            if (!entry.snapshotId().getRepository().equals(repository)) {
                return ImmutableList.of();
            }
            if (snapshots != null && snapshots.length > 0) {
                for (String snapshot : snapshots) {
                    if (entry.snapshotId().getSnapshot().equals(snapshot)) {
                        return snapshotMetaData.entries();
                    }
                }
                return ImmutableList.of();
            } else {
                return snapshotMetaData.entries();
            }
        }
        ImmutableList.Builder<SnapshotMetaData.Entry> builder = ImmutableList.builder();
        for (SnapshotMetaData.Entry entry : snapshotMetaData.entries()) {
            if (!entry.snapshotId().getRepository().equals(repository)) {
                continue;
            }
            if (snapshots != null && snapshots.length > 0) {
                for (String snapshot : snapshots) {
                    if (entry.snapshotId().getSnapshot().equals(snapshot)) {
                        builder.add(entry);
                        break;
                    }
                }
            } else {
                builder.add(entry);
            }
        }
        return builder.build();
    }

    /**
     * Returns status of shards that are snapshotted on the node and belong to the given snapshot
     * <p>
     * This method is executed on data node
     * </p>
     *
     * @param snapshotId snapshot id
     * @return map of shard id to snapshot status
     */
    public ImmutableMap<ShardId, IndexShardSnapshotStatus> currentSnapshotShards(SnapshotId snapshotId) {
        SnapshotShards snapshotShards = shardSnapshots.get(snapshotId);
        if (snapshotShards == null) {
            return null;
        } else {
            return snapshotShards.shards;
        }
    }

    /**
     * Returns status of shards  currently finished snapshots
     * <p>
     * This method is executed on master node and it's complimentary to the {@link #currentSnapshotShards(SnapshotId)} because it
     * returns simliar information but for already finished snapshots.
     * </p>
     *
     * @param snapshotId snapshot id
     * @return map of shard id to snapshot status
     */
    public ImmutableMap<ShardId, IndexShardSnapshotStatus> snapshotShards(SnapshotId snapshotId) throws IOException {
        ImmutableMap.Builder<ShardId, IndexShardSnapshotStatus> shardStatusBuilder = ImmutableMap.builder();
        Repository repository = repositoriesService.repository(snapshotId.getRepository());
        IndexShardRepository indexShardRepository = repositoriesService.indexShardRepository(snapshotId.getRepository());
        Snapshot snapshot = repository.readSnapshot(snapshotId);
        MetaData metaData = repository.readSnapshotMetaData(snapshotId, snapshot.indices());
        for (String index : snapshot.indices()) {
            IndexMetaData indexMetaData = metaData.indices().get(index);
            if (indexMetaData != null) {
                int numberOfShards = indexMetaData.getNumberOfShards();
                for (int i = 0; i < numberOfShards; i++) {
                    ShardId shardId = new ShardId(index, i);
                    SnapshotShardFailure shardFailure = findShardFailure(snapshot.shardFailures(), shardId);
                    if (shardFailure != null) {
                        IndexShardSnapshotStatus shardSnapshotStatus = new IndexShardSnapshotStatus();
                        shardSnapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.FAILURE);
                        shardSnapshotStatus.failure(shardFailure.reason());
                        shardStatusBuilder.put(shardId, shardSnapshotStatus);
                    } else {
                        IndexShardSnapshotStatus shardSnapshotStatus = indexShardRepository.snapshotStatus(snapshotId, shardId);
                        shardStatusBuilder.put(shardId, shardSnapshotStatus);
                    }
                }
            }
        }
        return shardStatusBuilder.build();
    }


    private SnapshotShardFailure findShardFailure(ImmutableList<SnapshotShardFailure> shardFailures, ShardId shardId) {
        for (SnapshotShardFailure shardFailure : shardFailures) {
            if (shardId.getIndex().equals(shardFailure.index()) && shardId.getId() == shardFailure.shardId()) {
                return shardFailure;
            }
        }
        return null;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                if (event.nodesRemoved()) {
                    processSnapshotsOnRemovedNodes(event);
                }
                if (event.routingTableChanged()) {
                    processStartedShards(event);
                }
            }
            SnapshotMetaData prev = event.previousState().metaData().custom(SnapshotMetaData.TYPE);
            SnapshotMetaData curr = event.state().metaData().custom(SnapshotMetaData.TYPE);

            if (prev == null) {
                if (curr != null) {
                    processIndexShardSnapshots(curr);
                }
            } else {
                if (!prev.equals(curr)) {
                    processIndexShardSnapshots(curr);
                }
            }
        } catch (Throwable t) {
            logger.warn("Failed to update snapshot state ", t);
        }
    }

    /**
     * Cleans up shard snapshots that were running on removed nodes
     *
     * @param event cluster changed event
     */
    private void processSnapshotsOnRemovedNodes(ClusterChangedEvent event) {
        if (removedNodesCleanupNeeded(event)) {
            // Check if we just became the master
            final boolean newMaster = !event.previousState().nodes().localNodeMaster();
            clusterService.submitStateUpdateTask("update snapshot state after node removal", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    DiscoveryNodes nodes = currentState.nodes();
                    MetaData metaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    SnapshotMetaData snapshots = metaData.custom(SnapshotMetaData.TYPE);
                    if (snapshots == null) {
                        return currentState;
                    }
                    boolean changed = false;
                    ArrayList<SnapshotMetaData.Entry> entries = newArrayList();
                    for (final SnapshotMetaData.Entry snapshot : snapshots.entries()) {
                        SnapshotMetaData.Entry updatedSnapshot = snapshot;
                        boolean snapshotChanged = false;
                        if (snapshot.state() == State.STARTED || snapshot.state() == State.ABORTED) {
                            ImmutableMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableMap.builder();
                            for (ImmutableMap.Entry<ShardId, ShardSnapshotStatus> shardEntry : snapshot.shards().entrySet()) {
                                ShardSnapshotStatus shardStatus = shardEntry.getValue();
                                if (!shardStatus.state().completed() && shardStatus.nodeId() != null) {
                                    if (nodes.nodeExists(shardStatus.nodeId())) {
                                        shards.put(shardEntry);
                                    } else {
                                        // TODO: Restart snapshot on another node?
                                        snapshotChanged = true;
                                        logger.warn("failing snapshot of shard [{}] on closed node [{}]", shardEntry.getKey(), shardStatus.nodeId());
                                        shards.put(shardEntry.getKey(), new ShardSnapshotStatus(shardStatus.nodeId(), State.FAILED, "node shutdown"));
                                    }
                                }
                            }
                            if (snapshotChanged) {
                                changed = true;
                                ImmutableMap<ShardId, ShardSnapshotStatus> shardsMap = shards.build();
                                if (!snapshot.state().completed() && completed(shardsMap.values())) {
                                    updatedSnapshot = new SnapshotMetaData.Entry(snapshot, State.SUCCESS, shardsMap);
                                    endSnapshot(updatedSnapshot);
                                } else {
                                    updatedSnapshot = new SnapshotMetaData.Entry(snapshot, snapshot.state(), shardsMap);
                                }
                            }
                            entries.add(updatedSnapshot);
                        } else if (snapshot.state() == State.INIT && newMaster) {
                            // Clean up the snapshot that failed to start from the old master
                            deleteSnapshot(snapshot.snapshotId(), new DeleteSnapshotListener() {
                                @Override
                                public void onResponse() {
                                    logger.debug("cleaned up abandoned snapshot {} in INIT state", snapshot.snapshotId());
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    logger.warn("failed to clean up abandoned snapshot {} in INIT state", snapshot.snapshotId());
                                }
                            });
                        } else if (snapshot.state() == State.SUCCESS && newMaster) {
                            // Finalize the snapshot
                            endSnapshot(snapshot);
                        }
                    }
                    if (changed) {
                        snapshots = new SnapshotMetaData(entries.toArray(new SnapshotMetaData.Entry[entries.size()]));
                        mdBuilder.putCustom(SnapshotMetaData.TYPE, snapshots);
                        return ClusterState.builder(currentState).metaData(mdBuilder).build();
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.warn("failed to update snapshot state after node removal");
                }
            });
        }
    }

    private void processStartedShards(ClusterChangedEvent event) {
        if (waitingShardsStartedOrUnassigned(event)) {
            clusterService.submitStateUpdateTask("update snapshot state after shards started", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData metaData = currentState.metaData();
                    RoutingTable routingTable = currentState.routingTable();
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    SnapshotMetaData snapshots = metaData.custom(SnapshotMetaData.TYPE);
                    if (snapshots != null) {
                        boolean changed = false;
                        ArrayList<SnapshotMetaData.Entry> entries = newArrayList();
                        for (final SnapshotMetaData.Entry snapshot : snapshots.entries()) {
                            SnapshotMetaData.Entry updatedSnapshot = snapshot;
                            if (snapshot.state() == State.STARTED) {
                                ImmutableMap<ShardId, ShardSnapshotStatus> shards = processWaitingShards(snapshot.shards(), routingTable);
                                if (shards != null) {
                                    changed = true;
                                    if (!snapshot.state().completed() && completed(shards.values())) {
                                        updatedSnapshot = new SnapshotMetaData.Entry(snapshot, State.SUCCESS, shards);
                                        endSnapshot(updatedSnapshot);
                                    } else {
                                        updatedSnapshot = new SnapshotMetaData.Entry(snapshot, shards);
                                    }
                                }
                                entries.add(updatedSnapshot);
                            }
                        }
                        if (changed) {
                            snapshots = new SnapshotMetaData(entries.toArray(new SnapshotMetaData.Entry[entries.size()]));
                            mdBuilder.putCustom(SnapshotMetaData.TYPE, snapshots);
                            return ClusterState.builder(currentState).metaData(mdBuilder).build();
                        }
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.warn("failed to update snapshot state after shards started from [{}] ", t, source);
                }
            });
        }
    }

    private ImmutableMap<ShardId, ShardSnapshotStatus> processWaitingShards(ImmutableMap<ShardId, ShardSnapshotStatus> snapshotShards, RoutingTable routingTable) {
        boolean snapshotChanged = false;
        ImmutableMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableMap.builder();
        for (ImmutableMap.Entry<ShardId, ShardSnapshotStatus> shardEntry : snapshotShards.entrySet()) {
            ShardSnapshotStatus shardStatus = shardEntry.getValue();
            if (shardStatus.state() == State.WAITING) {
                ShardId shardId = shardEntry.getKey();
                IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                if (indexShardRoutingTable != null) {
                    IndexShardRoutingTable shardRouting = indexShardRoutingTable.shard(shardId.id());
                    if (shardRouting != null && shardRouting.primaryShard() != null) {
                        if (shardRouting.primaryShard().started()) {
                            // Shard that we were waiting for has started on a node, let's process it
                            snapshotChanged = true;
                            logger.trace("starting shard that we were waiting for [{}] on node [{}]", shardEntry.getKey(), shardStatus.nodeId());
                            shards.put(shardEntry.getKey(), new ShardSnapshotStatus(shardRouting.primaryShard().currentNodeId()));
                            continue;
                        } else if (shardRouting.primaryShard().initializing() || shardRouting.primaryShard().relocating()) {
                            // Shard that we were waiting for hasn't started yet or still relocating - will continue to wait
                            shards.put(shardEntry);
                            continue;
                        }
                    }
                }
                // Shard that we were waiting for went into unassigned state or disappeared - giving up
                snapshotChanged = true;
                logger.warn("failing snapshot of shard [{}] on unassigned shard [{}]", shardEntry.getKey(), shardStatus.nodeId());
                shards.put(shardEntry.getKey(), new ShardSnapshotStatus(shardStatus.nodeId(), State.FAILED, "shard is unassigned"));
            } else {
                shards.put(shardEntry);
            }
        }
        if (snapshotChanged) {
            return shards.build();
        } else {
            return null;
        }
    }

    private boolean waitingShardsStartedOrUnassigned(ClusterChangedEvent event) {
        SnapshotMetaData curr = event.state().metaData().custom(SnapshotMetaData.TYPE);
        if (curr != null) {
            for (SnapshotMetaData.Entry entry : curr.entries()) {
                if (entry.state() == State.STARTED && !entry.waitingIndices().isEmpty()) {
                    for (String index : entry.waitingIndices().keySet()) {
                        if (event.indexRoutingTableChanged(index)) {
                            IndexRoutingTable indexShardRoutingTable = event.state().getRoutingTable().index(index);
                            for (ShardId shardId : entry.waitingIndices().get(index)) {
                                ShardRouting shardRouting = indexShardRoutingTable.shard(shardId.id()).primaryShard();
                                if (shardRouting != null && (shardRouting.started() || shardRouting.unassigned())) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean removedNodesCleanupNeeded(ClusterChangedEvent event) {
        // Check if we just became the master
        boolean newMaster = !event.previousState().nodes().localNodeMaster();
        SnapshotMetaData snapshotMetaData = event.state().getMetaData().custom(SnapshotMetaData.TYPE);
        if (snapshotMetaData == null) {
            return false;
        }
        for (SnapshotMetaData.Entry snapshot : snapshotMetaData.entries()) {
            if (newMaster && (snapshot.state() == State.SUCCESS || snapshot.state() == State.INIT)) {
                // We just replaced old master and snapshots in intermediate states needs to be cleaned
                return true;
            }
            for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                for (ShardSnapshotStatus shardStatus : snapshot.shards().values()) {
                    if (!shardStatus.state().completed() && node.getId().equals(shardStatus.nodeId())) {
                        // At least one shard was running on the removed node - we need to fail it
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Checks if any new shards should be snapshotted on this node
     *
     * @param snapshotMetaData snapshot metadata to be processed
     */
    private void processIndexShardSnapshots(SnapshotMetaData snapshotMetaData) {
        Map<SnapshotId, SnapshotShards> survivors = newHashMap();
        // First, remove snapshots that are no longer there
        for (Map.Entry<SnapshotId, SnapshotShards> entry : shardSnapshots.entrySet()) {
            if (snapshotMetaData != null && snapshotMetaData.snapshot(entry.getKey()) != null) {
                survivors.put(entry.getKey(), entry.getValue());
            }
        }

        // For now we will be mostly dealing with a single snapshot at a time but might have multiple simultaneously running
        // snapshots in the future
        Map<SnapshotId, Map<ShardId, IndexShardSnapshotStatus>> newSnapshots = newHashMap();
        // Now go through all snapshots and update existing or create missing
        final String localNodeId = clusterService.localNode().id();
        if (snapshotMetaData != null) {
            for (SnapshotMetaData.Entry entry : snapshotMetaData.entries()) {
                if (entry.state() == State.STARTED) {
                    Map<ShardId, IndexShardSnapshotStatus> startedShards = newHashMap();
                    SnapshotShards snapshotShards = shardSnapshots.get(entry.snapshotId());
                    for (Map.Entry<ShardId, SnapshotMetaData.ShardSnapshotStatus> shard : entry.shards().entrySet()) {
                        // Add all new shards to start processing on
                        if (localNodeId.equals(shard.getValue().nodeId())) {
                            if (shard.getValue().state() == State.INIT && (snapshotShards == null || !snapshotShards.shards.containsKey(shard.getKey()))) {
                                logger.trace("[{}] - Adding shard to the queue", shard.getKey());
                                startedShards.put(shard.getKey(), new IndexShardSnapshotStatus());
                            }
                        }
                    }
                    if (!startedShards.isEmpty()) {
                        newSnapshots.put(entry.snapshotId(), startedShards);
                        if (snapshotShards != null) {
                            // We already saw this snapshot but we need to add more started shards
                            ImmutableMap.Builder<ShardId, IndexShardSnapshotStatus> shards = ImmutableMap.builder();
                            // Put all shards that were already running on this node
                            shards.putAll(snapshotShards.shards);
                            // Put all newly started shards
                            shards.putAll(startedShards);
                            survivors.put(entry.snapshotId(), new SnapshotShards(shards.build()));
                        } else {
                            // Brand new snapshot that we haven't seen before
                            survivors.put(entry.snapshotId(), new SnapshotShards(ImmutableMap.copyOf(startedShards)));
                        }
                    }
                } else if (entry.state() == State.ABORTED) {
                    // Abort all running shards for this snapshot
                    SnapshotShards snapshotShards = shardSnapshots.get(entry.snapshotId());
                    if (snapshotShards != null) {
                        for (Map.Entry<ShardId, SnapshotMetaData.ShardSnapshotStatus> shard : entry.shards().entrySet()) {
                            IndexShardSnapshotStatus snapshotStatus = snapshotShards.shards.get(shard.getKey());
                            if (snapshotStatus != null) {
                                snapshotStatus.abort();
                            }
                        }
                    }
                }
            }
        }

        // Update the list of snapshots that we saw and tried to started
        // If startup of these shards fails later, we don't want to try starting these shards again
        shutdownLock.lock();
        try {
            shardSnapshots = ImmutableMap.copyOf(survivors);
            if (shardSnapshots.isEmpty()) {
                // Notify all waiting threads that no more snapshots
                shutdownCondition.signalAll();
            }
        } finally {
            shutdownLock.unlock();
        }

        // We have new shards to starts
        if (!newSnapshots.isEmpty()) {
            for (final Map.Entry<SnapshotId, Map<ShardId, IndexShardSnapshotStatus>> entry : newSnapshots.entrySet()) {
                for (final Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : entry.getValue().entrySet()) {
                    try {
                        final IndexShardSnapshotAndRestoreService shardSnapshotService = indicesService.indexServiceSafe(shardEntry.getKey().getIndex()).shardInjectorSafe(shardEntry.getKey().id())
                                .getInstance(IndexShardSnapshotAndRestoreService.class);
                        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    shardSnapshotService.snapshot(entry.getKey(), shardEntry.getValue());
                                    updateIndexShardSnapshotStatus(new UpdateIndexShardSnapshotStatusRequest(entry.getKey(), shardEntry.getKey(), new ShardSnapshotStatus(localNodeId, SnapshotMetaData.State.SUCCESS)));
                                } catch (Throwable t) {
                                    logger.warn("[{}] [{}] failed to create snapshot", t, shardEntry.getKey(), entry.getKey());
                                    updateIndexShardSnapshotStatus(new UpdateIndexShardSnapshotStatusRequest(entry.getKey(), shardEntry.getKey(), new ShardSnapshotStatus(localNodeId, SnapshotMetaData.State.FAILED, ExceptionsHelper.detailedMessage(t))));
                                }
                            }
                        });
                    } catch (Throwable t) {
                        updateIndexShardSnapshotStatus(new UpdateIndexShardSnapshotStatusRequest(entry.getKey(), shardEntry.getKey(), new ShardSnapshotStatus(localNodeId, SnapshotMetaData.State.FAILED, ExceptionsHelper.detailedMessage(t))));
                    }
                }
            }
        }
    }

    /**
     * Updates the shard status
     *
     * @param request update shard status request
     */
    private void updateIndexShardSnapshotStatus(UpdateIndexShardSnapshotStatusRequest request) {
        try {
            if (clusterService.state().nodes().localNodeMaster()) {
                innerUpdateSnapshotState(request);
            } else {
                transportService.sendRequest(clusterService.state().nodes().masterNode(),
                        UPDATE_SNAPSHOT_ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
            }
        } catch (Throwable t) {
            logger.warn("[{}] [{}] failed to update snapshot state", t, request.snapshotId(), request.status());
        }
    }

    /**
     * Checks if all shards in the list have completed
     *
     * @param shards list of shard statuses
     * @return true if all shards have completed (either successfully or failed), false otherwise
     */
    private boolean completed(Collection<SnapshotMetaData.ShardSnapshotStatus> shards) {
        for (ShardSnapshotStatus status : shards) {
            if (!status.state().completed()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns list of indices with missing shards, and list of indices that are closed
     *
     * @param shards list of shard statuses
     * @return list of failed and closed indices
     */
    private Tuple<Set<String>, Set<String>> indicesWithMissingShards(ImmutableMap<ShardId, SnapshotMetaData.ShardSnapshotStatus> shards, MetaData metaData) {
        Set<String> missing = newHashSet();
        Set<String> closed = newHashSet();
        for (ImmutableMap.Entry<ShardId, SnapshotMetaData.ShardSnapshotStatus> entry : shards.entrySet()) {
            if (entry.getValue().state() == State.MISSING) {
                if (metaData.hasIndex(entry.getKey().getIndex()) && metaData.index(entry.getKey().getIndex()).getState() == IndexMetaData.State.CLOSE) {
                    closed.add(entry.getKey().getIndex());
                } else {
                    missing.add(entry.getKey().getIndex());
                }
            }
        }
        return new Tuple<>(missing, closed);
    }

    /**
     * Updates the shard status on master node
     *
     * @param request update shard status request
     */
    private void innerUpdateSnapshotState(final UpdateIndexShardSnapshotStatusRequest request) {
        clusterService.submitStateUpdateTask("update snapshot state", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                SnapshotMetaData snapshots = metaData.custom(SnapshotMetaData.TYPE);
                if (snapshots != null) {
                    boolean changed = false;
                    ArrayList<SnapshotMetaData.Entry> entries = newArrayList();
                    for (SnapshotMetaData.Entry entry : snapshots.entries()) {
                        if (entry.snapshotId().equals(request.snapshotId())) {
                            HashMap<ShardId, ShardSnapshotStatus> shards = newHashMap(entry.shards());
                            logger.trace("[{}] Updating shard [{}] with status [{}]", request.snapshotId(), request.shardId(), request.status().state());
                            shards.put(request.shardId(), request.status());
                            if (!completed(shards.values())) {
                                entries.add(new SnapshotMetaData.Entry(entry, ImmutableMap.copyOf(shards)));
                            } else {
                                // Snapshot is finished - mark it as done
                                // TODO: Add PARTIAL_SUCCESS status?
                                SnapshotMetaData.Entry updatedEntry = new SnapshotMetaData.Entry(entry, State.SUCCESS, ImmutableMap.copyOf(shards));
                                entries.add(updatedEntry);
                                // Finalize snapshot in the repository
                                endSnapshot(updatedEntry);
                                logger.info("snapshot [{}] is done", updatedEntry.snapshotId());
                            }
                            changed = true;
                        } else {
                            entries.add(entry);
                        }
                    }
                    if (changed) {
                        snapshots = new SnapshotMetaData(entries.toArray(new SnapshotMetaData.Entry[entries.size()]));
                        mdBuilder.putCustom(SnapshotMetaData.TYPE, snapshots);
                        return ClusterState.builder(currentState).metaData(mdBuilder).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("[{}][{}] failed to update snapshot status to [{}]", t, request.snapshotId(), request.shardId(), request.status());
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
    private void endSnapshot(SnapshotMetaData.Entry entry) {
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
    private void endSnapshot(final SnapshotMetaData.Entry entry, final String failure) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new Runnable() {
            @Override
            public void run() {
                SnapshotId snapshotId = entry.snapshotId();
                try {
                    final Repository repository = repositoriesService.repository(snapshotId.getRepository());
                    logger.trace("[{}] finalizing snapshot in repository, state: [{}], failure[{}]", snapshotId, entry.state(), failure);
                    ArrayList<ShardSearchFailure> failures = newArrayList();
                    ArrayList<SnapshotShardFailure> shardFailures = newArrayList();
                    for (Map.Entry<ShardId, ShardSnapshotStatus> shardStatus : entry.shards().entrySet()) {
                        ShardId shardId = shardStatus.getKey();
                        ShardSnapshotStatus status = shardStatus.getValue();
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
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                SnapshotMetaData snapshots = metaData.custom(SnapshotMetaData.TYPE);
                if (snapshots != null) {
                    boolean changed = false;
                    ArrayList<SnapshotMetaData.Entry> entries = newArrayList();
                    for (SnapshotMetaData.Entry entry : snapshots.entries()) {
                        if (entry.snapshotId().equals(snapshotId)) {
                            changed = true;
                        } else {
                            entries.add(entry);
                        }
                    }
                    if (changed) {
                        snapshots = new SnapshotMetaData(entries.toArray(new SnapshotMetaData.Entry[entries.size()]));
                        mdBuilder.putCustom(SnapshotMetaData.TYPE, snapshots);
                        return ClusterState.builder(currentState).metaData(mdBuilder).build();
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
     * Deletes snapshot from repository.
     * <p/>
     * If the snapshot is still running cancels the snapshot first and then deletes it from the repository.
     *
     * @param snapshotId snapshot id
     * @param listener   listener
     */
    public void deleteSnapshot(final SnapshotId snapshotId, final DeleteSnapshotListener listener) {
        clusterService.submitStateUpdateTask("delete snapshot", new ProcessedClusterStateUpdateTask() {

            boolean waitForSnapshot = false;

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                SnapshotMetaData snapshots = metaData.custom(SnapshotMetaData.TYPE);
                if (snapshots == null) {
                    // No snapshots running - we can continue
                    return currentState;
                }
                SnapshotMetaData.Entry snapshot = snapshots.snapshot(snapshotId);
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
                    ImmutableMap<ShardId, ShardSnapshotStatus> shards;
                    if (snapshot.state() == State.STARTED && snapshot.shards() != null) {
                        // snapshot is currently running - stop started shards
                        ImmutableMap.Builder<ShardId, ShardSnapshotStatus> shardsBuilder = ImmutableMap.builder();
                        for (ImmutableMap.Entry<ShardId, ShardSnapshotStatus> shardEntry : snapshot.shards().entrySet()) {
                            ShardSnapshotStatus status = shardEntry.getValue();
                            if (!status.state().completed()) {
                                shardsBuilder.put(shardEntry.getKey(), new ShardSnapshotStatus(status.nodeId(), State.ABORTED));
                            } else {
                                shardsBuilder.put(shardEntry.getKey(), status);
                            }
                        }
                        shards = shardsBuilder.build();
                    } else if (snapshot.state() == State.INIT) {
                        // snapshot hasn't started yet - end it
                        shards = snapshot.shards();
                        endSnapshot(snapshot);
                    } else {
                        boolean hasUncompletedShards = false;
                        // Cleanup in case a node gone missing and snapshot wasn't updated for some reason
                        for (ShardSnapshotStatus shardStatus : snapshot.shards().values()) {
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
                    SnapshotMetaData.Entry newSnapshot = new SnapshotMetaData.Entry(snapshot, State.ABORTED, shards);
                    snapshots = new SnapshotMetaData(newSnapshot);
                    mdBuilder.putCustom(SnapshotMetaData.TYPE, snapshots);
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
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
     * Checks if a repository is currently in use by one of the snapshots
     *
     * @param clusterState cluster state
     * @param repository   repository id
     * @return true if repository is currently in use by one of the running snapshots
     */
    public static boolean isRepositoryInUse(ClusterState clusterState, String repository) {
        MetaData metaData = clusterState.metaData();
        SnapshotMetaData snapshots = metaData.custom(SnapshotMetaData.TYPE);
        if (snapshots != null) {
            for (SnapshotMetaData.Entry snapshot : snapshots.entries()) {
                if (repository.equals(snapshot.snapshotId().getRepository())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Deletes snapshot from repository
     *
     * @param snapshotId snapshot id
     * @param listener   listener
     */
    private void deleteSnapshotFromRepository(final SnapshotId snapshotId, final DeleteSnapshotListener listener) {
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
     * Calculates the list of shards that should be included into the current snapshot
     *
     * @param clusterState cluster state
     * @param indices      list of indices to be snapshotted
     * @return list of shard to be included into current snapshot
     */
    private ImmutableMap<ShardId, SnapshotMetaData.ShardSnapshotStatus> shards(ClusterState clusterState, ImmutableList<String> indices) {
        ImmutableMap.Builder<ShardId, SnapshotMetaData.ShardSnapshotStatus> builder = ImmutableMap.builder();
        MetaData metaData = clusterState.metaData();
        for (String index : indices) {
            IndexMetaData indexMetaData = metaData.index(index);
            if (indexMetaData == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                builder.put(new ShardId(index, 0), new SnapshotMetaData.ShardSnapshotStatus(null, State.MISSING, "missing index"));
            } else if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                for (int i = 0; i < indexMetaData.numberOfShards(); i++) {
                    ShardId shardId = new ShardId(index, i);
                    builder.put(shardId, new SnapshotMetaData.ShardSnapshotStatus(null, State.MISSING, "index is closed"));
                }
            } else {
                IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(index);
                for (int i = 0; i < indexMetaData.numberOfShards(); i++) {
                    ShardId shardId = new ShardId(index, i);
                    if (indexRoutingTable != null) {
                        ShardRouting primary = indexRoutingTable.shard(i).primaryShard();
                        if (primary == null || !primary.assignedToNode()) {
                            builder.put(shardId, new SnapshotMetaData.ShardSnapshotStatus(null, State.MISSING, "primary shard is not allocated"));
                        } else if (primary.relocating() || primary.initializing()) {
                            // The WAITING state was introduced in V1.2.0 - don't use it if there are nodes with older version in the cluster
                            builder.put(shardId, new SnapshotMetaData.ShardSnapshotStatus(primary.currentNodeId(), State.WAITING));
                        } else if (!primary.started()) {
                            builder.put(shardId, new SnapshotMetaData.ShardSnapshotStatus(primary.currentNodeId(), State.MISSING, "primary shard hasn't been started yet"));
                        } else {
                            builder.put(shardId, new SnapshotMetaData.ShardSnapshotStatus(primary.currentNodeId()));
                        }
                    } else {
                        builder.put(shardId, new SnapshotMetaData.ShardSnapshotStatus(null, State.MISSING, "missing routing table"));
                    }
                }
            }
        }

        return builder.build();
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

    @Override
    protected void doStart() throws ElasticsearchException {

    }

    @Override
    protected void doStop() throws ElasticsearchException {
        shutdownLock.lock();
        try {
            while(!shardSnapshots.isEmpty() && shutdownCondition.await(5, TimeUnit.SECONDS)) {
                // Wait for at most 5 second for locally running snapshots to finish
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            shutdownLock.unlock();
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {

    }

    /**
     * Listener for create snapshot operation
     */
    public static interface CreateSnapshotListener {

        /**
         * Called when snapshot has successfully started
         */
        void onResponse();

        /**
         * Called if a snapshot operation couldn't start
         */
        void onFailure(Throwable t);
    }

    /**
     * Listener for delete snapshot operation
     */
    public static interface DeleteSnapshotListener {

        /**
         * Called if delete operation was successful
         */
        void onResponse();

        /**
         * Called if delete operation failed
         */
        void onFailure(Throwable t);
    }

    public static interface SnapshotCompletionListener {

        void onSnapshotCompletion(SnapshotId snapshotId, SnapshotInfo snapshot);

        void onSnapshotFailure(SnapshotId snapshotId, Throwable t);
    }

    /**
     * Snapshot creation request
     */
    public static class SnapshotRequest {

        private String cause;

        private String name;

        private String repository;

        private String[] indices;

        private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

        private boolean partial;

        private Settings settings;

        private boolean includeGlobalState;

        private TimeValue masterNodeTimeout;

        /**
         * Constructs new snapshot creation request
         *
         * @param cause      cause for snapshot operation
         * @param name       name of the snapshot
         * @param repository name of the repository
         */
        public SnapshotRequest(String cause, String name, String repository) {
            this.cause = cause;
            this.name = name;
            this.repository = repository;
        }

        /**
         * Sets the list of indices to be snapshotted
         *
         * @param indices list of indices
         * @return this request
         */
        public SnapshotRequest indices(String[] indices) {
            this.indices = indices;
            return this;
        }

        /**
         * Sets repository-specific snapshot settings
         *
         * @param settings snapshot settings
         * @return this request
         */
        public SnapshotRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        /**
         * Set to true if global state should be stored as part of the snapshot
         *
         * @param includeGlobalState true if global state should be stored as part of the snapshot
         * @return this request
         */
        public SnapshotRequest includeGlobalState(boolean includeGlobalState) {
            this.includeGlobalState = includeGlobalState;
            return this;
        }

        /**
         * Sets master node timeout
         *
         * @param masterNodeTimeout master node timeout
         * @return this request
         */
        public SnapshotRequest masterNodeTimeout(TimeValue masterNodeTimeout) {
            this.masterNodeTimeout = masterNodeTimeout;
            return this;
        }

        /**
         * Sets the indices options
         *
         * @param indicesOptions indices options
         * @return this request
         */
        public SnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        /**
         * Set to true if partial snapshot should be allowed
         *
         * @param partial true if partial snapshots should be allowed
         * @return this request
         */
        public SnapshotRequest partial(boolean partial) {
            this.partial = partial;
            return this;
        }

        /**
         * Returns cause for snapshot operation
         *
         * @return cause for snapshot operation
         */
        public String cause() {
            return cause;
        }

        /**
         * Returns snapshot name
         *
         * @return snapshot name
         */
        public String name() {
            return name;
        }

        /**
         * Returns snapshot repository
         *
         * @return snapshot repository
         */
        public String repository() {
            return repository;
        }

        /**
         * Returns the list of indices to be snapshotted
         *
         * @return the list of indices
         */
        public String[] indices() {
            return indices;
        }

        /**
         * Returns indices options
         *
         * @return indices options
         */
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        /**
         * Returns repository-specific settings for the snapshot operation
         *
         * @return repository-specific settings
         */
        public Settings settings() {
            return settings;
        }

        /**
         * Returns true if global state should be stored as part of the snapshot
         *
         * @return true if global state should be stored as part of the snapshot
         */
        public boolean includeGlobalState() {
            return includeGlobalState;
        }

        /**
         * Returns master node timeout
         *
         * @return master node timeout
         */
        public TimeValue masterNodeTimeout() {
            return masterNodeTimeout;
        }

    }

    /**
     * Stores the list of shards that has to be snapshotted on this node
     */
    private static class SnapshotShards {
        private final ImmutableMap<ShardId, IndexShardSnapshotStatus> shards;

        private SnapshotShards(ImmutableMap<ShardId, IndexShardSnapshotStatus> shards) {
            this.shards = shards;
        }
    }

    /**
     * Internal request that is used to send changes in snapshot status to master
     */
    private static class UpdateIndexShardSnapshotStatusRequest extends TransportRequest {
        private SnapshotId snapshotId;
        private ShardId shardId;
        private SnapshotMetaData.ShardSnapshotStatus status;

        private UpdateIndexShardSnapshotStatusRequest() {

        }

        private UpdateIndexShardSnapshotStatusRequest(SnapshotId snapshotId, ShardId shardId, SnapshotMetaData.ShardSnapshotStatus status) {
            this.snapshotId = snapshotId;
            this.shardId = shardId;
            this.status = status;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            snapshotId = SnapshotId.readSnapshotId(in);
            shardId = ShardId.readShardId(in);
            status = SnapshotMetaData.ShardSnapshotStatus.readShardSnapshotStatus(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshotId.writeTo(out);
            shardId.writeTo(out);
            status.writeTo(out);
        }

        public SnapshotId snapshotId() {
            return snapshotId;
        }

        public ShardId shardId() {
            return shardId;
        }

        public SnapshotMetaData.ShardSnapshotStatus status() {
            return status;
        }
    }

    /**
     * Transport request handler that is used to send changes in snapshot status to master
     */
    class UpdateSnapshotStateRequestHandler implements TransportRequestHandler<UpdateIndexShardSnapshotStatusRequest> {
        @Override
        public void messageReceived(UpdateIndexShardSnapshotStatusRequest request, final TransportChannel channel) throws Exception {
            innerUpdateSnapshotState(request);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }
}

