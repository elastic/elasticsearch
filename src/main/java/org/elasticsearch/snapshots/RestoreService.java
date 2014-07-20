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

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.metadata.RestoreMetaData.ShardRestoreStatus;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.cluster.metadata.MetaDataIndexStateService.INDEX_CLOSED_BLOCK;

/**
 * Service responsible for restoring snapshots
 * <p/>
 * Restore operation is performed in several stages.
 * <p/>
 * First {@link #restoreSnapshot(RestoreRequest, RestoreSnapshotListener)}
 * method reads information about snapshot and metadata from repository. In update cluster state task it checks restore
 * preconditions, restores global state if needed, creates {@link RestoreMetaData} record with list of shards that needs
 * to be restored and adds this shard to the routing table using {@link RoutingTable.Builder#addAsRestore(IndexMetaData, RestoreSource)}
 * method.
 * <p/>
 * Individual shards are getting restored as part of normal recovery process in
 * {@link org.elasticsearch.index.gateway.IndexShardGatewayService#recover(boolean, org.elasticsearch.index.gateway.IndexShardGatewayService.RecoveryListener)}
 * method, which detects that shard should be restored from snapshot rather than recovered from gateway by looking
 * at the {@link org.elasticsearch.cluster.routing.ShardRouting#restoreSource()} property. If this property is not null
 * {@code recover} method uses {@link org.elasticsearch.index.snapshots.IndexShardSnapshotAndRestoreService#restore(org.elasticsearch.indices.recovery.RecoveryState)}
 * method to start shard restore process.
 * <p/>
 * At the end of the successful restore process {@code IndexShardSnapshotAndRestoreService} calls {@link #indexShardRestoreCompleted(SnapshotId, ShardId)},
 * which updates {@link RestoreMetaData} in cluster state or removes it when all shards are completed. In case of
 * restore failure a normal recovery fail-over process kicks in.
 */
public class RestoreService extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final TransportService transportService;

    private final AllocationService allocationService;

    private final MetaDataCreateIndexService createIndexService;

    private final CopyOnWriteArrayList<RestoreCompletionListener> listeners = new CopyOnWriteArrayList<>();

    @Inject
    public RestoreService(Settings settings, ClusterService clusterService, RepositoriesService repositoriesService, TransportService transportService, AllocationService allocationService, MetaDataCreateIndexService createIndexService) {
        super(settings);
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.transportService = transportService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        transportService.registerHandler(UpdateRestoreStateRequestHandler.ACTION, new UpdateRestoreStateRequestHandler());
        clusterService.add(this);
    }

    /**
     * Restores snapshot specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     */
    public void restoreSnapshot(final RestoreRequest request, final RestoreSnapshotListener listener) {
        try {
            // Read snapshot info and metadata from the repository
            Repository repository = repositoriesService.repository(request.repository());
            final SnapshotId snapshotId = new SnapshotId(request.repository(), request.name());
            final Snapshot snapshot = repository.readSnapshot(snapshotId);
            ImmutableList<String> filteredIndices = SnapshotUtils.filterIndices(snapshot.indices(), request.indices(), request.indicesOptions());
            final MetaData metaData = repository.readSnapshotMetaData(snapshotId, filteredIndices);

            // Make sure that we can restore from this snapshot
            validateSnapshotRestorable(snapshotId, snapshot);

            // Find list of indices that we need to restore
            final Map<String, String> renamedIndices = renamedIndices(request, filteredIndices);

            // Now we can start the actual restore process by adding shards to be recovered in the cluster state
            // and updating cluster metadata (global and index) as needed
            clusterService.submitStateUpdateTask(request.cause(), new TimeoutClusterStateUpdateTask() {
                RestoreInfo restoreInfo = null;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    // Check if another restore process is already running - cannot run two restore processes at the
                    // same time
                    RestoreMetaData restoreMetaData = currentState.metaData().custom(RestoreMetaData.TYPE);
                    if (restoreMetaData != null && !restoreMetaData.entries().isEmpty()) {
                        throw new ConcurrentSnapshotExecutionException(snapshotId, "Restore process is already running in this cluster");
                    }

                    // Updating cluster state
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                    final ImmutableMap<ShardId, RestoreMetaData.ShardRestoreStatus> shards;
                    if (!renamedIndices.isEmpty()) {
                        // We have some indices to restore
                        ImmutableMap.Builder<ShardId, RestoreMetaData.ShardRestoreStatus> shardsBuilder = ImmutableMap.builder();
                        for (Map.Entry<String, String> indexEntry : renamedIndices.entrySet()) {
                            String index = indexEntry.getValue();
                            boolean partial = checkPartial(index);
                            RestoreSource restoreSource = new RestoreSource(snapshotId, index);
                            String renamedIndex = indexEntry.getKey();
                            IndexMetaData snapshotIndexMetaData = metaData.index(index);
                            // Check that the index is closed or doesn't exist
                            IndexMetaData currentIndexMetaData = currentState.metaData().index(renamedIndex);
                            IntSet ignoreShards = new IntOpenHashSet();
                            if (currentIndexMetaData == null) {
                                // Index doesn't exist - create it and start recovery
                                // Make sure that the index we are about to create has a validate name
                                createIndexService.validateIndexName(renamedIndex, currentState);
                                IndexMetaData.Builder indexMdBuilder = IndexMetaData.builder(snapshotIndexMetaData).state(IndexMetaData.State.OPEN).index(renamedIndex);
                                if (!request.includeAliases() && !snapshotIndexMetaData.aliases().isEmpty()) {
                                    // Remove all aliases - they shouldn't be restored
                                    indexMdBuilder.removeAllAliases();
                                }
                                IndexMetaData updatedIndexMetaData = indexMdBuilder.build();
                                if (partial) {
                                    populateIgnoredShards(index, ignoreShards);
                                }
                                rtBuilder.addAsNewRestore(updatedIndexMetaData, restoreSource, ignoreShards);
                                mdBuilder.put(updatedIndexMetaData, true);
                            } else {
                                validateExistingIndex(currentIndexMetaData, snapshotIndexMetaData, renamedIndex, partial);
                                // Index exists and it's closed - open it in metadata and start recovery
                                IndexMetaData.Builder indexMdBuilder = IndexMetaData.builder(snapshotIndexMetaData).state(IndexMetaData.State.OPEN);
                                indexMdBuilder.version(Math.max(snapshotIndexMetaData.version(), currentIndexMetaData.version() + 1));
                                if (!request.includeAliases()) {
                                    // Remove all snapshot aliases
                                    if (!snapshotIndexMetaData.aliases().isEmpty()) {
                                        indexMdBuilder.removeAllAliases();
                                    }
                                    /// Add existing aliases
                                    for (ObjectCursor<AliasMetaData> alias : currentIndexMetaData.aliases().values()) {
                                        indexMdBuilder.putAlias(alias.value);
                                    }
                                }
                                IndexMetaData updatedIndexMetaData = indexMdBuilder.index(renamedIndex).build();
                                rtBuilder.addAsRestore(updatedIndexMetaData, restoreSource);
                                blocks.removeIndexBlock(renamedIndex, INDEX_CLOSED_BLOCK);
                                mdBuilder.put(updatedIndexMetaData, true);
                            }
                            for (int shard = 0; shard < snapshotIndexMetaData.getNumberOfShards(); shard++) {
                                if (!ignoreShards.contains(shard)) {
                                    shardsBuilder.put(new ShardId(renamedIndex, shard), new RestoreMetaData.ShardRestoreStatus(clusterService.state().nodes().localNodeId()));
                                } else {
                                    shardsBuilder.put(new ShardId(renamedIndex, shard), new RestoreMetaData.ShardRestoreStatus(clusterService.state().nodes().localNodeId(), RestoreMetaData.State.FAILURE));
                                }
                            }
                        }

                        shards = shardsBuilder.build();
                        RestoreMetaData.Entry restoreEntry = new RestoreMetaData.Entry(snapshotId, RestoreMetaData.State.INIT, ImmutableList.copyOf(renamedIndices.keySet()), shards);
                        mdBuilder.putCustom(RestoreMetaData.TYPE, new RestoreMetaData(restoreEntry));
                    } else {
                        shards = ImmutableMap.of();
                    }

                    // Restore global state if needed
                    restoreGlobalStateIfRequested(mdBuilder);

                    if (completed(shards)) {
                        // We don't have any indices to restore - we are done
                        restoreInfo = new RestoreInfo(request.name(), ImmutableList.<String>copyOf(renamedIndices.keySet()),
                                shards.size(), shards.size() - failedShards(shards));
                    }

                    ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocks).routingTable(rtBuilder).build();
                    RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(updatedState).routingTable(rtBuilder).build());
                    return ClusterState.builder(updatedState).routingResult(routingResult).build();
                }

                private void populateIgnoredShards(String index, IntSet ignoreShards) {
                    for (SnapshotShardFailure failure : snapshot.shardFailures()) {
                        if (index.equals(failure.index())) {
                            ignoreShards.add(failure.shardId());
                        }
                    }
                }

                private boolean checkPartial(String index) {
                    // Make sure that index was fully snapshotted
                    if (failed(snapshot, index)) {
                        if (request.partial()) {
                            return true;
                        } else {
                            throw new SnapshotRestoreException(snapshotId, "index [" + index + "] wasn't fully snapshotted - cannot restore");
                        }
                    } else {
                        return false;
                    }
                }

                private void validateExistingIndex(IndexMetaData currentIndexMetaData, IndexMetaData snapshotIndexMetaData, String renamedIndex, boolean partial) {
                    // Index exist - checking that it's closed
                    if (currentIndexMetaData.state() != IndexMetaData.State.CLOSE) {
                        // TODO: Enable restore for open indices
                        throw new SnapshotRestoreException(snapshotId, "cannot restore index [" + renamedIndex + "] because it's open");
                    }
                    // Index exist - checking if it's partial restore
                    if (partial) {
                        throw new SnapshotRestoreException(snapshotId, "cannot restore partial index [" + renamedIndex + "] because such index already exists");
                    }
                    // Make sure that the number of shards is the same. That's the only thing that we cannot change
                    if (currentIndexMetaData.getNumberOfShards() != snapshotIndexMetaData.getNumberOfShards()) {
                        throw new SnapshotRestoreException(snapshotId, "cannot restore index [" + renamedIndex + "] with [" + currentIndexMetaData.getNumberOfShards() +
                                "] shard from snapshot with [" + snapshotIndexMetaData.getNumberOfShards() + "] shards");
                    }
                }

                private void restoreGlobalStateIfRequested(MetaData.Builder mdBuilder) {
                    if (request.includeGlobalState()) {
                        if (metaData.persistentSettings() != null) {
                            mdBuilder.persistentSettings(metaData.persistentSettings());
                        }
                        if (metaData.templates() != null) {
                            // TODO: Should all existing templates be deleted first?
                            for (ObjectCursor<IndexTemplateMetaData> cursor : metaData.templates().values()) {
                                mdBuilder.put(cursor.value);
                            }
                        }
                        if (metaData.customs() != null) {
                            for (ObjectObjectCursor<String, MetaData.Custom> cursor : metaData.customs()) {
                                if (!RepositoriesMetaData.TYPE.equals(cursor.key)) {
                                    // Don't restore repositories while we are working with them
                                    // TODO: Should we restore them at the end?
                                    mdBuilder.putCustom(cursor.key, cursor.value);
                                }
                            }
                        }
                    }
                }


                @Override
                public void onFailure(String source, Throwable t) {
                    logger.warn("[{}] failed to restore snapshot", t, snapshotId);
                    listener.onFailure(t);
                }

                @Override
                public TimeValue timeout() {
                    return request.masterNodeTimeout();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(restoreInfo);
                }
            });


        } catch (Throwable e) {
            logger.warn("[{}][{}] failed to restore snapshot", e, request.repository(), request.name());
            listener.onFailure(e);
        }
    }

    /**
     * This method is used by {@link org.elasticsearch.index.snapshots.IndexShardSnapshotAndRestoreService} to notify
     * {@code RestoreService} about shard restore completion.
     *
     * @param snapshotId snapshot id
     * @param shardId    shard id
     */
    public void indexShardRestoreCompleted(SnapshotId snapshotId, ShardId shardId) {
        logger.trace("[{}] successfully restored shard  [{}]", snapshotId, shardId);
        UpdateIndexShardRestoreStatusRequest request = new UpdateIndexShardRestoreStatusRequest(snapshotId, shardId,
                new ShardRestoreStatus(clusterService.state().nodes().localNodeId(), RestoreMetaData.State.SUCCESS));
        if (clusterService.state().nodes().localNodeMaster()) {
            innerUpdateRestoreState(request);
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    UpdateRestoreStateRequestHandler.ACTION, request, EmptyTransportResponseHandler.INSTANCE_SAME);
        }
    }

    /**
     * Updates shard restore record in the cluster state.
     *
     * @param request update shard status request
     */
    private void innerUpdateRestoreState(final UpdateIndexShardRestoreStatusRequest request) {
        clusterService.submitStateUpdateTask("update snapshot state", new ProcessedClusterStateUpdateTask() {

            private RestoreInfo restoreInfo = null;

            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                RestoreMetaData restore = metaData.custom(RestoreMetaData.TYPE);
                if (restore != null) {
                    boolean changed = false;
                    ArrayList<RestoreMetaData.Entry> entries = newArrayList();
                    for (RestoreMetaData.Entry entry : restore.entries()) {
                        if (entry.snapshotId().equals(request.snapshotId())) {
                            HashMap<ShardId, ShardRestoreStatus> shards = newHashMap(entry.shards());
                            logger.trace("[{}] Updating shard [{}] with status [{}]", request.snapshotId(), request.shardId(), request.status().state());
                            shards.put(request.shardId(), request.status());
                            if (!completed(shards)) {
                                entries.add(new RestoreMetaData.Entry(entry.snapshotId(), RestoreMetaData.State.STARTED, entry.indices(), ImmutableMap.copyOf(shards)));
                            } else {
                                logger.info("restore [{}] is done", request.snapshotId());
                                restoreInfo = new RestoreInfo(entry.snapshotId().getSnapshot(), entry.indices(), shards.size(), shards.size() - failedShards(shards));
                            }
                            changed = true;
                        } else {
                            entries.add(entry);
                        }
                    }
                    if (changed) {
                        restore = new RestoreMetaData(entries.toArray(new RestoreMetaData.Entry[entries.size()]));
                        mdBuilder.putCustom(RestoreMetaData.TYPE, restore);
                        return ClusterState.builder(currentState).metaData(mdBuilder).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("[{}][{}] failed to update snapshot status to [{}]", t, request.snapshotId(), request.shardId(), request.status());
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (restoreInfo != null) {
                    for (RestoreCompletionListener listener : listeners) {
                        try {
                            listener.onRestoreCompletion(request.snapshotId, restoreInfo);
                        } catch (Throwable e) {
                            logger.warn("failed to update snapshot status for [{}]", e, listener);
                        }
                    }
                }
            }
        });
    }

    private boolean completed(Map<ShardId, RestoreMetaData.ShardRestoreStatus> shards) {
        for (RestoreMetaData.ShardRestoreStatus status : shards.values()) {
            if (!status.state().completed()) {
                return false;
            }
        }
        return true;
    }

    private int failedShards(Map<ShardId, RestoreMetaData.ShardRestoreStatus> shards) {
        int failedShards = 0;
        for (RestoreMetaData.ShardRestoreStatus status : shards.values()) {
            if (status.state() == RestoreMetaData.State.FAILURE) {
                failedShards++;
            }
        }
        return failedShards;
    }

    private Map<String, String> renamedIndices(RestoreRequest request, ImmutableList<String> filteredIndices) {
        Map<String, String> renamedIndices = newHashMap();
        for (String index : filteredIndices) {
            String renamedIndex = index;
            if (request.renameReplacement() != null && request.renamePattern() != null) {
                renamedIndex = index.replaceAll(request.renamePattern(), request.renameReplacement());
            }
            String previousIndex = renamedIndices.put(renamedIndex, index);
            if (previousIndex != null) {
                throw new SnapshotRestoreException(new SnapshotId(request.repository(), request.name()),
                        "indices [" + index + "] and [" + previousIndex + "] are renamed into the same index [" + renamedIndex + "]");
            }
        }
        return renamedIndices;
    }

    /**
     * Checks that snapshots can be restored and have compatible version
     *
     * @param snapshotId snapshot id
     * @param snapshot   snapshot metadata
     */
    private void validateSnapshotRestorable(SnapshotId snapshotId, Snapshot snapshot) {
        if (!snapshot.state().restorable()) {
            throw new SnapshotRestoreException(snapshotId, "unsupported snapshot state [" + snapshot.state() + "]");
        }
        if (Version.CURRENT.before(snapshot.version())) {
            throw new SnapshotRestoreException(snapshotId, "the snapshot was created with Elasticsearch version [" +
                    snapshot.version() + "] which is higher than the version of this node [" + Version.CURRENT + "]");
        }
    }

    /**
     * Checks if any of the deleted indices are still recovering and fails recovery on the shards of these indices
     *
     * @param event cluster changed event
     */
    private void processDeletedIndices(ClusterChangedEvent event) {
        MetaData metaData = event.state().metaData();
        RestoreMetaData restore = metaData.custom(RestoreMetaData.TYPE);
        if (restore == null) {
            // Not restoring - nothing to do
            return;
        }

        if (!event.indicesDeleted().isEmpty()) {
            // Some indices were deleted, let's make sure all indices that we are restoring still exist
            for (RestoreMetaData.Entry entry : restore.entries()) {
                List<ShardId> shardsToFail = null;
                for (ImmutableMap.Entry<ShardId, ShardRestoreStatus> shard : entry.shards().entrySet()) {
                    if (!shard.getValue().state().completed()) {
                        if (!event.state().metaData().hasIndex(shard.getKey().getIndex())) {
                            if (shardsToFail == null) {
                                shardsToFail = newArrayList();
                            }
                            shardsToFail.add(shard.getKey());
                        }
                    }
                }
                if (shardsToFail != null) {
                    for (ShardId shardId : shardsToFail) {
                        logger.trace("[{}] failing running shard restore [{}]", entry.snapshotId(), shardId);
                        innerUpdateRestoreState(new UpdateIndexShardRestoreStatusRequest(entry.snapshotId(), shardId, new ShardRestoreStatus(null, RestoreMetaData.State.FAILURE, "index was deleted")));
                    }
                }
            }
        }
    }

    /**
     * Fails the given snapshot restore operation for the given shard
     */
    public void failRestore(SnapshotId snapshotId, ShardId shardId) {
        logger.debug("[{}] failed to restore shard  [{}]", snapshotId, shardId);
        UpdateIndexShardRestoreStatusRequest request = new UpdateIndexShardRestoreStatusRequest(snapshotId, shardId,
                new ShardRestoreStatus(clusterService.state().nodes().localNodeId(), RestoreMetaData.State.FAILURE));
        if (clusterService.state().nodes().localNodeMaster()) {
            innerUpdateRestoreState(request);
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    UpdateRestoreStateRequestHandler.ACTION, request, EmptyTransportResponseHandler.INSTANCE_SAME);
        }
    }

    private boolean failed(Snapshot snapshot, String index) {
        for (SnapshotShardFailure failure : snapshot.shardFailures()) {
            if (index.equals(failure.index())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds restore completion listener
     * <p/>
     * This listener is called for each snapshot that finishes restore operation in the cluster. It's responsibility of
     * the listener to decide if it's called for the appropriate snapshot or not.
     *
     * @param listener restore completion listener
     */
    public void addListener(RestoreCompletionListener listener) {
        this.listeners.add(listener);
    }

    /**
     * Removes restore completion listener
     * <p/>
     * This listener is called for each snapshot that finishes restore operation in the cluster.
     *
     * @param listener restore completion listener
     */
    public void removeListener(RestoreCompletionListener listener) {
        this.listeners.remove(listener);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                processDeletedIndices(event);
            }
        } catch (Throwable t) {
            logger.warn("Failed to update restore state ", t);
        }
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
        RestoreMetaData snapshots = metaData.custom(RestoreMetaData.TYPE);
        if (snapshots != null) {
            for (RestoreMetaData.Entry snapshot : snapshots.entries()) {
                if (repository.equals(snapshot.snapshotId().getRepository())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Restore snapshot request
     */
    public static class RestoreRequest {

        final private String cause;

        final private String name;

        final private String repository;

        final private String[] indices;

        final private String renamePattern;

        final private String renameReplacement;

        final private IndicesOptions indicesOptions;

        final private Settings settings;

        final private TimeValue masterNodeTimeout;

        final private boolean includeGlobalState;

        final private boolean partial;

        final private boolean includeAliases;

        /**
         * Constructs new restore request
         *
         * @param cause              cause for restoring the snapshot
         * @param repository         repository name
         * @param name               snapshot name
         * @param indices            list of indices to restore
         * @param indicesOptions     indices options
         * @param renamePattern      pattern to rename indices
         * @param renameReplacement  replacement for renamed indices
         * @param settings           repository specific restore settings
         * @param masterNodeTimeout  master node timeout
         * @param includeGlobalState include global state into restore
         * @param partial            allow partial restore
         */
        public RestoreRequest(String cause, String repository, String name, String[] indices, IndicesOptions indicesOptions,
                              String renamePattern, String renameReplacement, Settings settings,
                              TimeValue masterNodeTimeout, boolean includeGlobalState, boolean partial, boolean includeAliases) {
            this.cause = cause;
            this.name = name;
            this.repository = repository;
            this.indices = indices;
            this.renamePattern = renamePattern;
            this.renameReplacement = renameReplacement;
            this.indicesOptions = indicesOptions;
            this.settings = settings;
            this.masterNodeTimeout = masterNodeTimeout;
            this.includeGlobalState = includeGlobalState;
            this.partial = partial;
            this.includeAliases = includeAliases;
        }

        /**
         * Returns restore operation cause
         *
         * @return restore operation cause
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
         * Returns repository name
         *
         * @return repository name
         */
        public String repository() {
            return repository;
        }

        /**
         * Return the list of indices to be restored
         *
         * @return the list of indices
         */
        public String[] indices() {
            return indices;
        }

        /**
         * Returns indices option flags
         *
         * @return indices options flags
         */
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        /**
         * Returns rename pattern
         *
         * @return rename pattern
         */
        public String renamePattern() {
            return renamePattern;
        }

        /**
         * Returns replacement pattern
         *
         * @return replacement pattern
         */
        public String renameReplacement() {
            return renameReplacement;
        }

        /**
         * Returns repository-specific restore settings
         *
         * @return restore settings
         */
        public Settings settings() {
            return settings;
        }

        /**
         * Returns true if global state should be restore during this restore operation
         *
         * @return restore global state flag
         */
        public boolean includeGlobalState() {
            return includeGlobalState;
        }

        /**
         * Returns true if incomplete indices will be restored
         *
         * @return partial indices restore flag
         */
        public boolean partial() {
            return partial;
        }

        /**
         * Returns true if aliases should be restore during this restore operation
         *
         * @return restore aliases state flag
         */
        public boolean includeAliases() {
            return includeAliases;
        }

        /**
         * Return master node timeout
         *
         * @return master node timeout
         */
        public TimeValue masterNodeTimeout() {
            return masterNodeTimeout;
        }

    }


    /**
     * This listener is called as soon as restore operation starts in the cluster.
     * <p/>
     * To receive notifications about  when operation ends in the cluster use {@link RestoreCompletionListener}
     */
    public static interface RestoreSnapshotListener {
        /**
         * Called when restore operations successfully starts in the cluster. Not null value of {@code snapshot} parameter
         * means that restore operation didn't involve any shards and therefore has already completed.
         *
         * @param restoreInfo if restore operation finished, contains information about restore operation, null otherwise
         */
        void onResponse(RestoreInfo restoreInfo);

        /**
         * Called when restore operation failed to start
         *
         * @param t exception that prevented the restore operation to start
         */
        void onFailure(Throwable t);
    }

    /**
     * This listener is called every time a snapshot is restored in the cluster
     */
    public static interface RestoreCompletionListener {
        /**
         * Called for every snapshot that is completed in the cluster
         *
         * @param snapshotId  snapshot id
         * @param restoreInfo restore completion information
         */
        void onRestoreCompletion(SnapshotId snapshotId, RestoreInfo restoreInfo);
    }

    /**
     * Internal class that is used to send notifications about finished shard restore operations to master node
     */
    private static class UpdateIndexShardRestoreStatusRequest extends TransportRequest {
        private SnapshotId snapshotId;
        private ShardId shardId;
        private ShardRestoreStatus status;

        private UpdateIndexShardRestoreStatusRequest() {

        }

        private UpdateIndexShardRestoreStatusRequest(SnapshotId snapshotId, ShardId shardId, ShardRestoreStatus status) {
            this.snapshotId = snapshotId;
            this.shardId = shardId;
            this.status = status;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            snapshotId = SnapshotId.readSnapshotId(in);
            shardId = ShardId.readShardId(in);
            status = ShardRestoreStatus.readShardRestoreStatus(in);
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

        public ShardRestoreStatus status() {
            return status;
        }
    }

    /**
     * Internal class that is used to send notifications about finished shard restore operations to master node
     */
    private class UpdateRestoreStateRequestHandler extends BaseTransportRequestHandler<UpdateIndexShardRestoreStatusRequest> {

        static final String ACTION = "cluster/snapshot/update_restore";

        @Override
        public UpdateIndexShardRestoreStatusRequest newInstance() {
            return new UpdateIndexShardRestoreStatusRequest();
        }

        @Override
        public void messageReceived(UpdateIndexShardRestoreStatusRequest request, final TransportChannel channel) throws Exception {
            innerUpdateRestoreState(request);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
