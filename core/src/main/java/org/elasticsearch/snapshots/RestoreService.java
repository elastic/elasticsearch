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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.RestoreInProgress.ShardRestoreStatus;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.settings.ClusterDynamicSettings;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

/**
 * Service responsible for restoring snapshots
 * <p>
 * Restore operation is performed in several stages.
 * <p>
 * First {@link #restoreSnapshot(RestoreRequest, org.elasticsearch.action.ActionListener)}
 * method reads information about snapshot and metadata from repository. In update cluster state task it checks restore
 * preconditions, restores global state if needed, creates {@link RestoreInProgress} record with list of shards that needs
 * to be restored and adds this shard to the routing table using {@link org.elasticsearch.cluster.routing.RoutingTable.Builder#addAsRestore(IndexMetaData, RestoreSource)}
 * method.
 * <p>
 * Individual shards are getting restored as part of normal recovery process in
 * {@link IndexShard#restoreFromRepository(IndexShardRepository, DiscoveryNode)} )}
 * method, which detects that shard should be restored from snapshot rather than recovered from gateway by looking
 * at the {@link org.elasticsearch.cluster.routing.ShardRouting#restoreSource()} property.
 * <p>
 * At the end of the successful restore process {@code IndexShardSnapshotAndRestoreService} calls {@link #indexShardRestoreCompleted(SnapshotId, ShardId)},
 * which updates {@link RestoreInProgress} in cluster state or removes it when all shards are completed. In case of
 * restore failure a normal recovery fail-over process kicks in.
 */
public class RestoreService extends AbstractComponent implements ClusterStateListener {

    public static final String UPDATE_RESTORE_ACTION_NAME = "internal:cluster/snapshot/update_restore";

    private static final Set<String> UNMODIFIABLE_SETTINGS = unmodifiableSet(newHashSet(
            SETTING_NUMBER_OF_SHARDS,
            SETTING_VERSION_CREATED,
            SETTING_INDEX_UUID,
            SETTING_CREATION_DATE));

    // It's OK to change some settings, but we shouldn't allow simply removing them
    private static final Set<String> UNREMOVABLE_SETTINGS;

    static {
        Set<String> unremovable = new HashSet<>(UNMODIFIABLE_SETTINGS.size() + 4);
        unremovable.addAll(UNMODIFIABLE_SETTINGS);
        unremovable.add(SETTING_NUMBER_OF_REPLICAS);
        unremovable.add(SETTING_AUTO_EXPAND_REPLICAS);
        unremovable.add(SETTING_VERSION_UPGRADED);
        unremovable.add(SETTING_VERSION_MINIMUM_COMPATIBLE);
        UNREMOVABLE_SETTINGS = unmodifiableSet(unremovable);
    }

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final TransportService transportService;

    private final AllocationService allocationService;

    private final MetaDataCreateIndexService createIndexService;

    private final DynamicSettings dynamicSettings;

    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;

    private final CopyOnWriteArrayList<ActionListener<RestoreCompletionResponse>> listeners = new CopyOnWriteArrayList<>();

    private final BlockingQueue<UpdateIndexShardRestoreStatusRequest> updatedSnapshotStateQueue = ConcurrentCollections.newBlockingQueue();

    @Inject
    public RestoreService(Settings settings, ClusterService clusterService, RepositoriesService repositoriesService, TransportService transportService,
                          AllocationService allocationService, MetaDataCreateIndexService createIndexService, @ClusterDynamicSettings DynamicSettings dynamicSettings,
                          MetaDataIndexUpgradeService metaDataIndexUpgradeService) {
        super(settings);
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.transportService = transportService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.dynamicSettings = dynamicSettings;
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
        transportService.registerRequestHandler(UPDATE_RESTORE_ACTION_NAME, UpdateIndexShardRestoreStatusRequest::new, ThreadPool.Names.SAME, new UpdateRestoreStateRequestHandler());
        clusterService.add(this);
    }

    /**
     * Restores snapshot specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     */
    public void restoreSnapshot(final RestoreRequest request, final ActionListener<RestoreInfo> listener) {
        try {
            // Read snapshot info and metadata from the repository
            Repository repository = repositoriesService.repository(request.repository());
            final SnapshotId snapshotId = new SnapshotId(request.repository(), request.name());
            final Snapshot snapshot = repository.readSnapshot(snapshotId);
            List<String> filteredIndices = SnapshotUtils.filterIndices(snapshot.indices(), request.indices(), request.indicesOptions());
            MetaData metaDataIn = repository.readSnapshotMetaData(snapshotId, snapshot, filteredIndices);

            final MetaData metaData;
            if (snapshot.version().before(Version.V_2_0_0_beta1)) {
                // ES 2.0 now requires units for all time and byte-sized settings, so we add the default unit if it's missing in this snapshot:
                metaData = MetaData.addDefaultUnitsIfNeeded(logger, metaDataIn);
            } else {
                // Units are already enforced:
                metaData = metaDataIn;
            }

            // Make sure that we can restore from this snapshot
            validateSnapshotRestorable(snapshotId, snapshot);

            // Find list of indices that we need to restore
            final Map<String, String> renamedIndices = renamedIndices(request, filteredIndices);

            // Now we can start the actual restore process by adding shards to be recovered in the cluster state
            // and updating cluster metadata (global and index) as needed
            clusterService.submitStateUpdateTask(request.cause(), new ClusterStateUpdateTask() {
                RestoreInfo restoreInfo = null;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    // Check if another restore process is already running - cannot run two restore processes at the
                    // same time
                    RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE);
                    if (restoreInProgress != null && !restoreInProgress.entries().isEmpty()) {
                        throw new ConcurrentSnapshotExecutionException(snapshotId, "Restore process is already running in this cluster");
                    }

                    // Updating cluster state
                    ClusterState.Builder builder = ClusterState.builder(currentState);
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                    ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards;
                    Set<String> aliases = new HashSet<>();
                    if (!renamedIndices.isEmpty()) {
                        // We have some indices to restore
                        ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder();
                        for (Map.Entry<String, String> indexEntry : renamedIndices.entrySet()) {
                            String index = indexEntry.getValue();
                            boolean partial = checkPartial(index);
                            RestoreSource restoreSource = new RestoreSource(snapshotId, snapshot.version(), index);
                            String renamedIndex = indexEntry.getKey();
                            IndexMetaData snapshotIndexMetaData = metaData.index(index);
                            snapshotIndexMetaData = updateIndexSettings(snapshotIndexMetaData, request.indexSettings, request.ignoreIndexSettings);
                            try {
                                snapshotIndexMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(snapshotIndexMetaData);
                            } catch (Exception ex) {
                                throw new SnapshotRestoreException(snapshotId, "cannot restore index [" + index + "] because it cannot be upgraded", ex);
                            }
                            // Check that the index is closed or doesn't exist
                            IndexMetaData currentIndexMetaData = currentState.metaData().index(renamedIndex);
                            IntSet ignoreShards = new IntHashSet();
                            if (currentIndexMetaData == null) {
                                // Index doesn't exist - create it and start recovery
                                // Make sure that the index we are about to create has a validate name
                                createIndexService.validateIndexName(renamedIndex, currentState);
                                createIndexService.validateIndexSettings(renamedIndex, snapshotIndexMetaData.getSettings());
                                IndexMetaData.Builder indexMdBuilder = IndexMetaData.builder(snapshotIndexMetaData).state(IndexMetaData.State.OPEN).index(renamedIndex);
                                indexMdBuilder.settings(Settings.settingsBuilder().put(snapshotIndexMetaData.getSettings()).put(IndexMetaData.SETTING_INDEX_UUID, Strings.randomBase64UUID()));
                                if (!request.includeAliases() && !snapshotIndexMetaData.getAliases().isEmpty()) {
                                    // Remove all aliases - they shouldn't be restored
                                    indexMdBuilder.removeAllAliases();
                                } else {
                                    for (ObjectCursor<String> alias : snapshotIndexMetaData.getAliases().keys()) {
                                        aliases.add(alias.value);
                                    }
                                }
                                IndexMetaData updatedIndexMetaData = indexMdBuilder.build();
                                if (partial) {
                                    populateIgnoredShards(index, ignoreShards);
                                }
                                rtBuilder.addAsNewRestore(updatedIndexMetaData, restoreSource, ignoreShards);
                                blocks.addBlocks(updatedIndexMetaData);
                                mdBuilder.put(updatedIndexMetaData, true);
                            } else {
                                validateExistingIndex(currentIndexMetaData, snapshotIndexMetaData, renamedIndex, partial);
                                // Index exists and it's closed - open it in metadata and start recovery
                                IndexMetaData.Builder indexMdBuilder = IndexMetaData.builder(snapshotIndexMetaData).state(IndexMetaData.State.OPEN);
                                indexMdBuilder.version(Math.max(snapshotIndexMetaData.getVersion(), currentIndexMetaData.getVersion() + 1));
                                if (!request.includeAliases()) {
                                    // Remove all snapshot aliases
                                    if (!snapshotIndexMetaData.getAliases().isEmpty()) {
                                        indexMdBuilder.removeAllAliases();
                                    }
                                    /// Add existing aliases
                                    for (ObjectCursor<AliasMetaData> alias : currentIndexMetaData.getAliases().values()) {
                                        indexMdBuilder.putAlias(alias.value);
                                    }
                                } else {
                                    for (ObjectCursor<String> alias : snapshotIndexMetaData.getAliases().keys()) {
                                        aliases.add(alias.value);
                                    }
                                }
                                indexMdBuilder.settings(Settings.settingsBuilder().put(snapshotIndexMetaData.getSettings()).put(IndexMetaData.SETTING_INDEX_UUID, currentIndexMetaData.getIndexUUID()));
                                IndexMetaData updatedIndexMetaData = indexMdBuilder.index(renamedIndex).build();
                                rtBuilder.addAsRestore(updatedIndexMetaData, restoreSource);
                                blocks.updateBlocks(updatedIndexMetaData);
                                mdBuilder.put(updatedIndexMetaData, true);
                            }

                            for (int shard = 0; shard < snapshotIndexMetaData.getNumberOfShards(); shard++) {
                                if (!ignoreShards.contains(shard)) {
                                    shardsBuilder.put(new ShardId(renamedIndex, shard), new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().localNodeId()));
                                } else {
                                    shardsBuilder.put(new ShardId(renamedIndex, shard), new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().localNodeId(), RestoreInProgress.State.FAILURE));
                                }
                            }
                        }

                        shards = shardsBuilder.build();
                        RestoreInProgress.Entry restoreEntry = new RestoreInProgress.Entry(snapshotId, RestoreInProgress.State.INIT, Collections.unmodifiableList(new ArrayList<>(renamedIndices.keySet())), shards);
                        builder.putCustom(RestoreInProgress.TYPE, new RestoreInProgress(restoreEntry));
                    } else {
                        shards = ImmutableOpenMap.of();
                    }

                    checkAliasNameConflicts(renamedIndices, aliases);

                    // Restore global state if needed
                    restoreGlobalStateIfRequested(mdBuilder);

                    if (completed(shards)) {
                        // We don't have any indices to restore - we are done
                        restoreInfo = new RestoreInfo(request.name(), Collections.unmodifiableList(new ArrayList<>(renamedIndices.keySet())),
                                shards.size(), shards.size() - failedShards(shards));
                    }

                    RoutingTable rt = rtBuilder.build();
                    ClusterState updatedState = builder.metaData(mdBuilder).blocks(blocks).routingTable(rt).build();
                    RoutingAllocation.Result routingResult = allocationService.reroute(
                            ClusterState.builder(updatedState).routingTable(rt).build(),
                            "restored snapshot [" + snapshotId + "]");
                    return ClusterState.builder(updatedState).routingResult(routingResult).build();
                }

                private void checkAliasNameConflicts(Map<String, String> renamedIndices, Set<String> aliases) {
                    for (Map.Entry<String, String> renamedIndex : renamedIndices.entrySet()) {
                        if (aliases.contains(renamedIndex.getKey())) {
                            throw new SnapshotRestoreException(snapshotId, "cannot rename index [" + renamedIndex.getValue() + "] into [" + renamedIndex.getKey() + "] because of conflict with an alias with the same name");
                        }
                    }
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
                    if (currentIndexMetaData.getState() != IndexMetaData.State.CLOSE) {
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

                /**
                 * Optionally updates index settings in indexMetaData by removing settings listed in ignoreSettings and
                 * merging them with settings in changeSettings.
                 */
                private IndexMetaData updateIndexSettings(IndexMetaData indexMetaData, Settings changeSettings, String[] ignoreSettings) {
                    if (changeSettings.names().isEmpty() && ignoreSettings.length == 0) {
                        return indexMetaData;
                    }
                    Settings normalizedChangeSettings = Settings.settingsBuilder().put(changeSettings).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX).build();
                    IndexMetaData.Builder builder = IndexMetaData.builder(indexMetaData);
                    Map<String, String> settingsMap = new HashMap<>(indexMetaData.getSettings().getAsMap());
                    List<String> simpleMatchPatterns = new ArrayList<>();
                    for (String ignoredSetting : ignoreSettings) {
                        if (!Regex.isSimpleMatchPattern(ignoredSetting)) {
                            if (UNREMOVABLE_SETTINGS.contains(ignoredSetting)) {
                                throw new SnapshotRestoreException(snapshotId, "cannot remove setting [" + ignoredSetting + "] on restore");
                            } else {
                                settingsMap.remove(ignoredSetting);
                            }
                        } else {
                            simpleMatchPatterns.add(ignoredSetting);
                        }
                    }
                    if (!simpleMatchPatterns.isEmpty()) {
                        String[] removePatterns = simpleMatchPatterns.toArray(new String[simpleMatchPatterns.size()]);
                        Iterator<Map.Entry<String, String>> iterator = settingsMap.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<String, String> entry = iterator.next();
                            if (UNREMOVABLE_SETTINGS.contains(entry.getKey()) == false) {
                                if (Regex.simpleMatch(removePatterns, entry.getKey())) {
                                    iterator.remove();
                                }
                            }
                        }
                    }
                    for(Map.Entry<String, String> entry : normalizedChangeSettings.getAsMap().entrySet()) {
                        if (UNMODIFIABLE_SETTINGS.contains(entry.getKey())) {
                            throw new SnapshotRestoreException(snapshotId, "cannot modify setting [" + entry.getKey() + "] on restore");
                        } else {
                            settingsMap.put(entry.getKey(), entry.getValue());
                        }
                    }

                    return builder.settings(Settings.builder().put(settingsMap)).build();
                }

                private void restoreGlobalStateIfRequested(MetaData.Builder mdBuilder) {
                    if (request.includeGlobalState()) {
                        if (metaData.persistentSettings() != null) {
                            boolean changed = false;
                            Settings.Builder persistentSettings = Settings.settingsBuilder().put();
                            for (Map.Entry<String, String> entry : metaData.persistentSettings().getAsMap().entrySet()) {
                                if (dynamicSettings.isDynamicOrLoggingSetting(entry.getKey())) {
                                    String error = dynamicSettings.validateDynamicSetting(entry.getKey(), entry.getValue(), clusterService.state());
                                    if (error == null) {
                                        persistentSettings.put(entry.getKey(), entry.getValue());
                                        changed = true;
                                    } else {
                                        logger.warn("ignoring persistent setting [{}], [{}]", entry.getKey(), error);
                                    }
                                } else {
                                    logger.warn("ignoring persistent setting [{}], not dynamically updateable", entry.getKey());
                                }
                            }
                            if (changed) {
                                mdBuilder.persistentSettings(persistentSettings.build());
                            }
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
     * This method is used by {@link IndexShard} to notify
     * {@code RestoreService} about shard restore completion.
     *
     * @param snapshotId snapshot id
     * @param shardId    shard id
     */
    public void indexShardRestoreCompleted(SnapshotId snapshotId, ShardId shardId) {
        logger.trace("[{}] successfully restored shard  [{}]", snapshotId, shardId);
        UpdateIndexShardRestoreStatusRequest request = new UpdateIndexShardRestoreStatusRequest(snapshotId, shardId,
                new ShardRestoreStatus(clusterService.state().nodes().localNodeId(), RestoreInProgress.State.SUCCESS));
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    UPDATE_RESTORE_ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
    }

    public final static class RestoreCompletionResponse {
        private final SnapshotId snapshotId;
        private final RestoreInfo restoreInfo;

        private RestoreCompletionResponse(SnapshotId snapshotId, RestoreInfo restoreInfo) {
            this.snapshotId = snapshotId;
            this.restoreInfo = restoreInfo;
        }

        public SnapshotId getSnapshotId() {
            return snapshotId;
        }

        public RestoreInfo getRestoreInfo() {
            return restoreInfo;
        }
    }

    /**
     * Updates shard restore record in the cluster state.
     *
     * @param request update shard status request
     */
    private void updateRestoreStateOnMaster(final UpdateIndexShardRestoreStatusRequest request) {
        logger.trace("received updated snapshot restore state [{}]", request);
        updatedSnapshotStateQueue.add(request);

        clusterService.submitStateUpdateTask("update snapshot state", new ClusterStateUpdateTask() {
            private final List<UpdateIndexShardRestoreStatusRequest> drainedRequests = new ArrayList<>();
            private Map<SnapshotId, Tuple<RestoreInfo, ImmutableOpenMap<ShardId, ShardRestoreStatus>>> batchedRestoreInfo = null;

            @Override
            public ClusterState execute(ClusterState currentState) {

                if (request.processed) {
                    return currentState;
                }

                updatedSnapshotStateQueue.drainTo(drainedRequests);

                final int batchSize = drainedRequests.size();

                // nothing to process (a previous event has processed it already)
                if (batchSize == 0) {
                    return currentState;
                }

                final RestoreInProgress restore = currentState.custom(RestoreInProgress.TYPE);
                if (restore != null) {
                    int changedCount = 0;
                    final List<RestoreInProgress.Entry> entries = new ArrayList<>();
                    for (RestoreInProgress.Entry entry : restore.entries()) {
                        ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = null;

                        for (int i = 0; i < batchSize; i++) {
                            final UpdateIndexShardRestoreStatusRequest updateSnapshotState = drainedRequests.get(i);
                            updateSnapshotState.processed = true;

                            if (entry.snapshotId().equals(updateSnapshotState.snapshotId())) {
                                logger.trace("[{}] Updating shard [{}] with status [{}]", updateSnapshotState.snapshotId(), updateSnapshotState.shardId(), updateSnapshotState.status().state());
                                if (shardsBuilder == null) {
                                    shardsBuilder = ImmutableOpenMap.builder(entry.shards());
                                }
                                shardsBuilder.put(updateSnapshotState.shardId(), updateSnapshotState.status());
                                changedCount++;
                            }
                        }

                        if (shardsBuilder != null) {
                            ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                            if (!completed(shards)) {
                                entries.add(new RestoreInProgress.Entry(entry.snapshotId(), RestoreInProgress.State.STARTED, entry.indices(), shards));
                            } else {
                                logger.info("restore [{}] is done", entry.snapshotId());
                                if (batchedRestoreInfo == null) {
                                    batchedRestoreInfo = new HashMap<>();
                                }
                                assert !batchedRestoreInfo.containsKey(entry.snapshotId());
                                batchedRestoreInfo.put(entry.snapshotId(),
                                    new Tuple<>(
                                        new RestoreInfo(entry.snapshotId().getSnapshot(), entry.indices(), shards.size(), shards.size() - failedShards(shards)),
                                        shards));
                            }
                        } else {
                            entries.add(entry);
                        }
                    }

                    if (changedCount > 0) {
                        logger.trace("changed cluster state triggered by {} snapshot restore state updates", changedCount);

                        final RestoreInProgress updatedRestore = new RestoreInProgress(entries.toArray(new RestoreInProgress.Entry[entries.size()]));
                        return ClusterState.builder(currentState).putCustom(RestoreInProgress.TYPE, updatedRestore).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {
                for (UpdateIndexShardRestoreStatusRequest request : drainedRequests) {
                    logger.warn("[{}][{}] failed to update snapshot status to [{}]", t, request.snapshotId(), request.shardId(), request.status());
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (batchedRestoreInfo != null) {
                    for (final Entry<SnapshotId, Tuple<RestoreInfo, ImmutableOpenMap<ShardId, ShardRestoreStatus>>> entry : batchedRestoreInfo.entrySet()) {
                        final SnapshotId snapshotId = entry.getKey();
                        final RestoreInfo restoreInfo = entry.getValue().v1();
                        final ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = entry.getValue().v2();
                        RoutingTable routingTable = newState.getRoutingTable();
                        final List<ShardId> waitForStarted = new ArrayList<>();
                        for (ObjectObjectCursor<ShardId, ShardRestoreStatus> shard : shards) {
                            if (shard.value.state() == RestoreInProgress.State.SUCCESS ) {
                                ShardId shardId = shard.key;
                                ShardRouting shardRouting = findPrimaryShard(routingTable, shardId);
                                if (shardRouting != null && !shardRouting.active()) {
                                    logger.trace("[{}][{}] waiting for the shard to start", snapshotId, shardId);
                                    waitForStarted.add(shardId);
                                }
                            }
                        }
                        if (waitForStarted.isEmpty()) {
                            notifyListeners(snapshotId, restoreInfo);
                        } else {
                            clusterService.addLast(new ClusterStateListener() {
                                @Override
                                public void clusterChanged(ClusterChangedEvent event) {
                                    if (event.routingTableChanged()) {
                                        RoutingTable routingTable = event.state().getRoutingTable();
                                        for (Iterator<ShardId> iterator = waitForStarted.iterator(); iterator.hasNext();) {
                                            ShardId shardId = iterator.next();
                                            ShardRouting shardRouting = findPrimaryShard(routingTable, shardId);
                                            // Shard disappeared (index deleted) or became active
                                            if (shardRouting == null || shardRouting.active()) {
                                                iterator.remove();
                                                logger.trace("[{}][{}] shard disappeared or started - removing", snapshotId, shardId);
                                            }
                                        }
                                    }
                                    if (waitForStarted.isEmpty()) {
                                        notifyListeners(snapshotId, restoreInfo);
                                        clusterService.remove(this);
                                    }
                                }
                            });
                        }
                    }
                }
            }

            private ShardRouting findPrimaryShard(RoutingTable routingTable, ShardId shardId) {
                IndexRoutingTable indexRoutingTable = routingTable.index(shardId.getIndex());
                if (indexRoutingTable != null) {
                    IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId.id());
                    if (indexShardRoutingTable != null) {
                        return indexShardRoutingTable.primaryShard();
                    }
                }
                return null;
            }

            private void notifyListeners(SnapshotId snapshotId, RestoreInfo restoreInfo) {
                for (ActionListener<RestoreCompletionResponse> listener : listeners) {
                    try {
                        listener.onResponse(new RestoreCompletionResponse(snapshotId, restoreInfo));
                    } catch (Throwable e) {
                        logger.warn("failed to update snapshot status for [{}]", e, listener);
                    }
                }
            }
        });
    }

    private boolean completed(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (!status.value.state().completed()) {
                return false;
            }
        }
        return true;
    }

    private int failedShards(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        int failedShards = 0;
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (status.value.state() == RestoreInProgress.State.FAILURE) {
                failedShards++;
            }
        }
        return failedShards;
    }

    private Map<String, String> renamedIndices(RestoreRequest request, List<String> filteredIndices) {
        Map<String, String> renamedIndices = new HashMap<>();
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
        RestoreInProgress restore = event.state().custom(RestoreInProgress.TYPE);
        if (restore == null) {
            // Not restoring - nothing to do
            return;
        }

        if (!event.indicesDeleted().isEmpty()) {
            // Some indices were deleted, let's make sure all indices that we are restoring still exist
            for (RestoreInProgress.Entry entry : restore.entries()) {
                List<ShardId> shardsToFail = null;
                for (ObjectObjectCursor<ShardId, ShardRestoreStatus> shard : entry.shards()) {
                    if (!shard.value.state().completed()) {
                        if (!event.state().metaData().hasIndex(shard.key.getIndex())) {
                            if (shardsToFail == null) {
                                shardsToFail = new ArrayList<>();
                            }
                            shardsToFail.add(shard.key);
                        }
                    }
                }
                if (shardsToFail != null) {
                    for (ShardId shardId : shardsToFail) {
                        logger.trace("[{}] failing running shard restore [{}]", entry.snapshotId(), shardId);
                        updateRestoreStateOnMaster(new UpdateIndexShardRestoreStatusRequest(entry.snapshotId(), shardId, new ShardRestoreStatus(null, RestoreInProgress.State.FAILURE, "index was deleted")));
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
                new ShardRestoreStatus(clusterService.state().nodes().localNodeId(), RestoreInProgress.State.FAILURE));
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    UPDATE_RESTORE_ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
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
     * <p>
     * This listener is called for each snapshot that finishes restore operation in the cluster. It's responsibility of
     * the listener to decide if it's called for the appropriate snapshot or not.
     *
     * @param listener restore completion listener
     */
    public void addListener(ActionListener<RestoreCompletionResponse> listener) {
        this.listeners.add(listener);
    }

    /**
     * Removes restore completion listener
     * <p>
     * This listener is called for each snapshot that finishes restore operation in the cluster.
     *
     * @param listener restore completion listener
     */
    public void removeListener(ActionListener<RestoreCompletionResponse> listener) {
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
        RestoreInProgress snapshots = clusterState.custom(RestoreInProgress.TYPE);
        if (snapshots != null) {
            for (RestoreInProgress.Entry snapshot : snapshots.entries()) {
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

        final private Settings indexSettings;

        final private String[] ignoreIndexSettings;

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
         * @param indexSettings      index settings that should be changed on restore
         * @param ignoreIndexSettings index settings that shouldn't be restored
         */
        public RestoreRequest(String cause, String repository, String name, String[] indices, IndicesOptions indicesOptions,
                              String renamePattern, String renameReplacement, Settings settings,
                              TimeValue masterNodeTimeout, boolean includeGlobalState, boolean partial, boolean includeAliases,
                              Settings indexSettings, String[] ignoreIndexSettings ) {
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
            this.indexSettings = indexSettings;
            this.ignoreIndexSettings = ignoreIndexSettings;

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
         * Returns index settings that should be changed on restore
         *
         * @return restore aliases state flag
         */
        public Settings indexSettings() {
            return indexSettings;
        }

        /**
         * Returns index settings that that shouldn't be restored
         *
         * @return restore aliases state flag
         */
        public String[] ignoreIndexSettings() {
            return ignoreIndexSettings;
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
     * Internal class that is used to send notifications about finished shard restore operations to master node
     */
    public static class UpdateIndexShardRestoreStatusRequest extends TransportRequest {
        private SnapshotId snapshotId;
        private ShardId shardId;
        private ShardRestoreStatus status;

        volatile boolean processed; // state field, no need to serialize

        public UpdateIndexShardRestoreStatusRequest() {

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

        @Override
        public String toString() {
            return "" + snapshotId + ", shardId [" + shardId + "], status [" + status.state() + "]";
        }
    }

    /**
     * Internal class that is used to send notifications about finished shard restore operations to master node
     */
    class UpdateRestoreStateRequestHandler implements TransportRequestHandler<UpdateIndexShardRestoreStatusRequest> {
        @Override
        public void messageReceived(UpdateIndexShardRestoreStatusRequest request, final TransportChannel channel) throws Exception {
            updateRestoreStateOnMaster(request);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }
}
