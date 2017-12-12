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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.RestoreInProgress.ShardRestoreStatus;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_UPGRADED;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

/**
 * Service responsible for restoring snapshots
 * <p>
 * Restore operation is performed in several stages.
 * <p>
 * First {@link #restoreSnapshot(RestoreRequest, org.elasticsearch.action.ActionListener)}
 * method reads information about snapshot and metadata from repository. In update cluster state task it checks restore
 * preconditions, restores global state if needed, creates {@link RestoreInProgress} record with list of shards that needs
 * to be restored and adds this shard to the routing table using {@link RoutingTable.Builder#addAsRestore(IndexMetaData, SnapshotRecoverySource)}
 * method.
 * <p>
 * Individual shards are getting restored as part of normal recovery process in
 * {@link IndexShard#restoreFromRepository(Repository)} )}
 * method, which detects that shard should be restored from snapshot rather than recovered from gateway by looking
 * at the {@link ShardRouting#recoverySource()} property.
 * <p>
 * At the end of the successful restore process {@code RestoreService} calls {@link #cleanupRestoreState(ClusterChangedEvent)},
 * which removes {@link RestoreInProgress} when all shards are completed. In case of
 * restore failure a normal recovery fail-over process kicks in.
 */
public class RestoreService extends AbstractComponent implements ClusterStateApplier {

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
        UNREMOVABLE_SETTINGS = unmodifiableSet(unremovable);
    }

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final AllocationService allocationService;

    private final MetaDataCreateIndexService createIndexService;

    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;

    private final ClusterSettings clusterSettings;

    private final CleanRestoreStateTaskExecutor cleanRestoreStateTaskExecutor;

    @Inject
    public RestoreService(Settings settings, ClusterService clusterService, RepositoriesService repositoriesService,
                          AllocationService allocationService, MetaDataCreateIndexService createIndexService,
                          MetaDataIndexUpgradeService metaDataIndexUpgradeService, ClusterSettings clusterSettings) {
        super(settings);
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
        clusterService.addStateApplier(this);
        this.clusterSettings = clusterSettings;
        this.cleanRestoreStateTaskExecutor = new CleanRestoreStateTaskExecutor(logger);
    }

    /**
     * Restores snapshot specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     */
    public void restoreSnapshot(final RestoreRequest request, final ActionListener<RestoreCompletionResponse> listener) {
        try {
            // Read snapshot info and metadata from the repository
            Repository repository = repositoriesService.repository(request.repositoryName);
            final RepositoryData repositoryData = repository.getRepositoryData();
            final Optional<SnapshotId> incompatibleSnapshotId =
                repositoryData.getIncompatibleSnapshotIds().stream().filter(s -> request.snapshotName.equals(s.getName())).findFirst();
            if (incompatibleSnapshotId.isPresent()) {
                throw new SnapshotRestoreException(request.repositoryName, request.snapshotName, "cannot restore incompatible snapshot");
            }
            final Optional<SnapshotId> matchingSnapshotId = repositoryData.getSnapshotIds().stream()
                .filter(s -> request.snapshotName.equals(s.getName())).findFirst();
            if (matchingSnapshotId.isPresent() == false) {
                throw new SnapshotRestoreException(request.repositoryName, request.snapshotName, "snapshot does not exist");
            }
            final SnapshotId snapshotId = matchingSnapshotId.get();
            final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
            final Snapshot snapshot = new Snapshot(request.repositoryName, snapshotId);
            List<String> filteredIndices = SnapshotUtils.filterIndices(snapshotInfo.indices(), request.indices(), request.indicesOptions());
            final MetaData metaData = repository.getSnapshotMetaData(snapshotInfo, repositoryData.resolveIndices(filteredIndices));

            // Make sure that we can restore from this snapshot
            validateSnapshotRestorable(request.repositoryName, snapshotInfo);

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
                        throw new ConcurrentSnapshotExecutionException(snapshot, "Restore process is already running in this cluster");
                    }
                    // Check if the snapshot to restore is currently being deleted
                    SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(SnapshotDeletionsInProgress.TYPE);
                    if (deletionsInProgress != null && deletionsInProgress.hasDeletionsInProgress()) {
                        throw new ConcurrentSnapshotExecutionException(snapshot,
                            "cannot restore a snapshot while a snapshot deletion is in-progress [" +
                                deletionsInProgress.getEntries().get(0).getSnapshot() + "]");
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
                        final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion()
                            .minimumIndexCompatibilityVersion();
                        for (Map.Entry<String, String> indexEntry : renamedIndices.entrySet()) {
                            String index = indexEntry.getValue();
                            boolean partial = checkPartial(index);
                            SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(snapshot, snapshotInfo.version(), index);
                            String renamedIndexName = indexEntry.getKey();
                            IndexMetaData snapshotIndexMetaData = metaData.index(index);
                            snapshotIndexMetaData = updateIndexSettings(snapshotIndexMetaData, request.indexSettings, request.ignoreIndexSettings);
                            try {
                                snapshotIndexMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(snapshotIndexMetaData,
                                    minIndexCompatibilityVersion);
                            } catch (Exception ex) {
                                throw new SnapshotRestoreException(snapshot, "cannot restore index [" + index + "] because it cannot be upgraded", ex);
                            }
                            // Check that the index is closed or doesn't exist
                            IndexMetaData currentIndexMetaData = currentState.metaData().index(renamedIndexName);
                            IntSet ignoreShards = new IntHashSet();
                            final Index renamedIndex;
                            if (currentIndexMetaData == null) {
                                // Index doesn't exist - create it and start recovery
                                // Make sure that the index we are about to create has a validate name
                                MetaDataCreateIndexService.validateIndexName(renamedIndexName, currentState);
                                createIndexService.validateIndexSettings(renamedIndexName, snapshotIndexMetaData.getSettings());
                                IndexMetaData.Builder indexMdBuilder = IndexMetaData.builder(snapshotIndexMetaData).state(IndexMetaData.State.OPEN).index(renamedIndexName);
                                indexMdBuilder.settings(Settings.builder().put(snapshotIndexMetaData.getSettings()).put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()));
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
                                rtBuilder.addAsNewRestore(updatedIndexMetaData, recoverySource, ignoreShards);
                                blocks.addBlocks(updatedIndexMetaData);
                                mdBuilder.put(updatedIndexMetaData, true);
                                renamedIndex = updatedIndexMetaData.getIndex();
                            } else {
                                validateExistingIndex(currentIndexMetaData, snapshotIndexMetaData, renamedIndexName, partial);
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
                                indexMdBuilder.settings(Settings.builder().put(snapshotIndexMetaData.getSettings()).put(IndexMetaData.SETTING_INDEX_UUID, currentIndexMetaData.getIndexUUID()));
                                IndexMetaData updatedIndexMetaData = indexMdBuilder.index(renamedIndexName).build();
                                rtBuilder.addAsRestore(updatedIndexMetaData, recoverySource);
                                blocks.updateBlocks(updatedIndexMetaData);
                                mdBuilder.put(updatedIndexMetaData, true);
                                renamedIndex = updatedIndexMetaData.getIndex();
                            }

                            for (int shard = 0; shard < snapshotIndexMetaData.getNumberOfShards(); shard++) {
                                if (!ignoreShards.contains(shard)) {
                                    shardsBuilder.put(new ShardId(renamedIndex, shard), new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId()));
                                } else {
                                    shardsBuilder.put(new ShardId(renamedIndex, shard), new RestoreInProgress.ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId(), RestoreInProgress.State.FAILURE));
                                }
                            }
                        }

                        shards = shardsBuilder.build();
                        RestoreInProgress.Entry restoreEntry = new RestoreInProgress.Entry(snapshot, overallState(RestoreInProgress.State.INIT, shards), Collections.unmodifiableList(new ArrayList<>(renamedIndices.keySet())), shards);
                        builder.putCustom(RestoreInProgress.TYPE, new RestoreInProgress(restoreEntry));
                    } else {
                        shards = ImmutableOpenMap.of();
                    }

                    checkAliasNameConflicts(renamedIndices, aliases);

                    // Restore global state if needed
                    restoreGlobalStateIfRequested(mdBuilder);

                    if (completed(shards)) {
                        // We don't have any indices to restore - we are done
                        restoreInfo = new RestoreInfo(snapshotId.getName(),
                                                      Collections.unmodifiableList(new ArrayList<>(renamedIndices.keySet())),
                                                      shards.size(),
                                                      shards.size() - failedShards(shards));
                    }

                    RoutingTable rt = rtBuilder.build();
                    ClusterState updatedState = builder.metaData(mdBuilder).blocks(blocks).routingTable(rt).build();
                    return allocationService.reroute(updatedState, "restored snapshot [" + snapshot + "]");
                }

                private void checkAliasNameConflicts(Map<String, String> renamedIndices, Set<String> aliases) {
                    for (Map.Entry<String, String> renamedIndex : renamedIndices.entrySet()) {
                        if (aliases.contains(renamedIndex.getKey())) {
                            throw new SnapshotRestoreException(snapshot, "cannot rename index [" + renamedIndex.getValue() + "] into [" + renamedIndex.getKey() + "] because of conflict with an alias with the same name");
                        }
                    }
                }

                private void populateIgnoredShards(String index, IntSet ignoreShards) {
                    for (SnapshotShardFailure failure : snapshotInfo.shardFailures()) {
                        if (index.equals(failure.index())) {
                            ignoreShards.add(failure.shardId());
                        }
                    }
                }

                private boolean checkPartial(String index) {
                    // Make sure that index was fully snapshotted
                    if (failed(snapshotInfo, index)) {
                        if (request.partial()) {
                            return true;
                        } else {
                            throw new SnapshotRestoreException(snapshot, "index [" + index + "] wasn't fully snapshotted - cannot restore");
                        }
                    } else {
                        return false;
                    }
                }

                private void validateExistingIndex(IndexMetaData currentIndexMetaData, IndexMetaData snapshotIndexMetaData, String renamedIndex, boolean partial) {
                    // Index exist - checking that it's closed
                    if (currentIndexMetaData.getState() != IndexMetaData.State.CLOSE) {
                        // TODO: Enable restore for open indices
                        throw new SnapshotRestoreException(snapshot, "cannot restore index [" + renamedIndex + "] because it's open");
                    }
                    // Index exist - checking if it's partial restore
                    if (partial) {
                        throw new SnapshotRestoreException(snapshot, "cannot restore partial index [" + renamedIndex + "] because such index already exists");
                    }
                    // Make sure that the number of shards is the same. That's the only thing that we cannot change
                    if (currentIndexMetaData.getNumberOfShards() != snapshotIndexMetaData.getNumberOfShards()) {
                        throw new SnapshotRestoreException(snapshot, "cannot restore index [" + renamedIndex + "] with [" + currentIndexMetaData.getNumberOfShards() +
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
                    Settings normalizedChangeSettings = Settings.builder().put(changeSettings).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX).build();
                    IndexMetaData.Builder builder = IndexMetaData.builder(indexMetaData);
                    Settings settings = indexMetaData.getSettings();
                    Set<String> keyFilters = new HashSet<>();
                    List<String> simpleMatchPatterns = new ArrayList<>();
                    for (String ignoredSetting : ignoreSettings) {
                        if (!Regex.isSimpleMatchPattern(ignoredSetting)) {
                            if (UNREMOVABLE_SETTINGS.contains(ignoredSetting)) {
                                throw new SnapshotRestoreException(snapshot, "cannot remove setting [" + ignoredSetting + "] on restore");
                            } else {
                                keyFilters.add(ignoredSetting);
                            }
                        } else {
                            simpleMatchPatterns.add(ignoredSetting);
                        }
                    }
                    Predicate<String> settingsFilter = k -> {
                        if (UNREMOVABLE_SETTINGS.contains(k) == false) {
                            for (String filterKey : keyFilters) {
                                if (k.equals(filterKey)) {
                                    return false;
                                }
                            }
                            for (String pattern : simpleMatchPatterns) {
                                if (Regex.simpleMatch(pattern, k)) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    };
                    Settings.Builder settingsBuilder = Settings.builder()
                        .put(settings.filter(settingsFilter))
                        .put(normalizedChangeSettings.filter(k -> {
                            if (UNMODIFIABLE_SETTINGS.contains(k)) {
                                throw new SnapshotRestoreException(snapshot, "cannot modify setting [" + k + "] on restore");
                            } else {
                                return true;
                            }
                        }));
                    return builder.settings(settingsBuilder).build();
                }

                private void restoreGlobalStateIfRequested(MetaData.Builder mdBuilder) {
                    if (request.includeGlobalState()) {
                        if (metaData.persistentSettings() != null) {
                            Settings settings = metaData.persistentSettings();
                            clusterSettings.validateUpdate(settings);
                            mdBuilder.persistentSettings(settings);
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
                public void onFailure(String source, Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to restore snapshot", snapshotId), e);
                    listener.onFailure(e);
                }

                @Override
                public TimeValue timeout() {
                    return request.masterNodeTimeout();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new RestoreCompletionResponse(snapshot, restoreInfo));
                }
            });


        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to restore snapshot", request.repositoryName + ":" + request.snapshotName), e);
            listener.onFailure(e);
        }
    }

    public static RestoreInProgress updateRestoreStateWithDeletedIndices(RestoreInProgress oldRestore, Set<Index> deletedIndices) {
        boolean changesMade = false;
        final List<RestoreInProgress.Entry> entries = new ArrayList<>();
        for (RestoreInProgress.Entry entry : oldRestore.entries()) {
            ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = null;
            for (ObjectObjectCursor<ShardId, ShardRestoreStatus> cursor : entry.shards()) {
                ShardId shardId = cursor.key;
                if (deletedIndices.contains(shardId.getIndex())) {
                    changesMade = true;
                    if (shardsBuilder == null) {
                        shardsBuilder = ImmutableOpenMap.builder(entry.shards());
                    }
                    shardsBuilder.put(shardId, new ShardRestoreStatus(null, RestoreInProgress.State.FAILURE, "index was deleted"));
                }
            }
            if (shardsBuilder != null) {
                ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                entries.add(new RestoreInProgress.Entry(entry.snapshot(), overallState(RestoreInProgress.State.STARTED, shards), entry.indices(), shards));
            } else {
                entries.add(entry);
            }
        }
        if (changesMade) {
            return new RestoreInProgress(entries.toArray(new RestoreInProgress.Entry[entries.size()]));
        } else {
            return oldRestore;
        }
    }

    public static final class RestoreCompletionResponse {
        private final Snapshot snapshot;
        private final RestoreInfo restoreInfo;

        private RestoreCompletionResponse(final Snapshot snapshot, final RestoreInfo restoreInfo) {
            this.snapshot = snapshot;
            this.restoreInfo = restoreInfo;
        }

        public Snapshot getSnapshot() {
            return snapshot;
        }

        public RestoreInfo getRestoreInfo() {
            return restoreInfo;
        }
    }

    public static class RestoreInProgressUpdater extends RoutingChangesObserver.AbstractRoutingChangesObserver {
        private final Map<Snapshot, Updates> shardChanges = new HashMap<>();

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            // mark snapshot as completed
            if (initializingShard.primary()) {
                RecoverySource recoverySource = initializingShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    Snapshot snapshot = ((SnapshotRecoverySource) recoverySource).snapshot();
                    changes(snapshot).shards.put(initializingShard.shardId(),
                        new ShardRestoreStatus(initializingShard.currentNodeId(), RestoreInProgress.State.SUCCESS));
                }
            }
        }

        @Override
        public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
            if (failedShard.primary() && failedShard.initializing()) {
                RecoverySource recoverySource = failedShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    Snapshot snapshot = ((SnapshotRecoverySource) recoverySource).snapshot();
                    // mark restore entry for this shard as failed when it's due to a file corruption. There is no need wait on retries
                    // to restore this shard on another node if the snapshot files are corrupt. In case where a node just left or crashed,
                    // however, we only want to acknowledge the restore operation once it has been successfully restored on another node.
                    if (unassignedInfo.getFailure() != null && Lucene.isCorruptionException(unassignedInfo.getFailure().getCause())) {
                        changes(snapshot).shards.put(failedShard.shardId(), new ShardRestoreStatus(failedShard.currentNodeId(),
                            RestoreInProgress.State.FAILURE, unassignedInfo.getFailure().getCause().getMessage()));
                    }
                }
            }
        }

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            // if we force an empty primary, we should also fail the restore entry
            if (unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT &&
                initializedShard.recoverySource().getType() != RecoverySource.Type.SNAPSHOT) {
                Snapshot snapshot = ((SnapshotRecoverySource) unassignedShard.recoverySource()).snapshot();
                changes(snapshot).shards.put(unassignedShard.shardId(), new ShardRestoreStatus(null,
                    RestoreInProgress.State.FAILURE, "recovery source type changed from snapshot to " + initializedShard.recoverySource()));
            }
        }

        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
            RecoverySource recoverySource = unassignedShard.recoverySource();
            if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                if (newUnassignedInfo.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO) {
                    Snapshot snapshot = ((SnapshotRecoverySource) recoverySource).snapshot();
                    String reason = "shard could not be allocated to any of the nodes";
                    changes(snapshot).shards.put(unassignedShard.shardId(),
                        new ShardRestoreStatus(unassignedShard.currentNodeId(), RestoreInProgress.State.FAILURE, reason));
                }
            }
        }

        /**
         * Helper method that creates update entry for the given shard id if such an entry does not exist yet.
         */
        private Updates changes(Snapshot snapshot) {
            return shardChanges.computeIfAbsent(snapshot, k -> new Updates());
        }

        private static class Updates {
            private Map<ShardId, ShardRestoreStatus> shards = new HashMap<>();
        }

        public RestoreInProgress applyChanges(final RestoreInProgress oldRestore) {
            if (shardChanges.isEmpty() == false) {
                final List<RestoreInProgress.Entry> entries = new ArrayList<>();
                for (RestoreInProgress.Entry entry : oldRestore.entries()) {
                    Snapshot snapshot = entry.snapshot();
                    Updates updates = shardChanges.get(snapshot);
                    if (updates.shards.isEmpty() == false) {
                        ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder(entry.shards());
                        for (Map.Entry<ShardId, ShardRestoreStatus> shard : updates.shards.entrySet()) {
                            shardsBuilder.put(shard.getKey(), shard.getValue());
                        }

                        ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                        RestoreInProgress.State newState = overallState(RestoreInProgress.State.STARTED, shards);
                        entries.add(new RestoreInProgress.Entry(entry.snapshot(), newState, entry.indices(), shards));
                    } else {
                        entries.add(entry);
                    }
                }
                return new RestoreInProgress(entries.toArray(new RestoreInProgress.Entry[entries.size()]));
            } else {
                return oldRestore;
            }
        }
    }

    public static RestoreInProgress.Entry restoreInProgress(ClusterState state, Snapshot snapshot) {
        final RestoreInProgress restoreInProgress = state.custom(RestoreInProgress.TYPE);
        if (restoreInProgress != null) {
            for (RestoreInProgress.Entry e : restoreInProgress.entries()) {
                if (e.snapshot().equals(snapshot)) {
                    return e;
                }
            }
        }
        return null;
    }

    static class CleanRestoreStateTaskExecutor implements ClusterStateTaskExecutor<CleanRestoreStateTaskExecutor.Task>, ClusterStateTaskListener {

        static class Task {
            final Snapshot snapshot;

            Task(Snapshot snapshot) {
                this.snapshot = snapshot;
            }

            @Override
            public String toString() {
                return "clean restore state for restoring snapshot " + snapshot;
            }
        }

        private final Logger logger;

        CleanRestoreStateTaskExecutor(Logger logger) {
            this.logger = logger;
        }

        @Override
        public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) throws Exception {
            final ClusterTasksResult.Builder<Task> resultBuilder = ClusterTasksResult.<Task>builder().successes(tasks);
            Set<Snapshot> completedSnapshots = tasks.stream().map(e -> e.snapshot).collect(Collectors.toSet());
            final List<RestoreInProgress.Entry> entries = new ArrayList<>();
            final RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE);
            boolean changed = false;
            if (restoreInProgress != null) {
                for (RestoreInProgress.Entry entry : restoreInProgress.entries()) {
                    if (completedSnapshots.contains(entry.snapshot()) == false) {
                        entries.add(entry);
                    } else {
                        changed = true;
                    }
                }
            }
            if (changed == false) {
                return resultBuilder.build(currentState);
            }
            RestoreInProgress updatedRestoreInProgress = new RestoreInProgress(entries.toArray(new RestoreInProgress.Entry[entries.size()]));
            ImmutableOpenMap.Builder<String, ClusterState.Custom> builder = ImmutableOpenMap.builder(currentState.getCustoms());
            builder.put(RestoreInProgress.TYPE, updatedRestoreInProgress);
            ImmutableOpenMap<String, ClusterState.Custom> customs = builder.build();
            return resultBuilder.build(ClusterState.builder(currentState).customs(customs).build());
        }

        @Override
        public void onFailure(final String source, final Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
        }

        @Override
        public void onNoLongerMaster(String source) {
            logger.debug("no longer master while processing restore state update [{}]", source);
        }

    }

    private void cleanupRestoreState(ClusterChangedEvent event) {
        ClusterState state = event.state();

        RestoreInProgress restoreInProgress = state.custom(RestoreInProgress.TYPE);
        if (restoreInProgress != null) {
            for (RestoreInProgress.Entry entry : restoreInProgress.entries()) {
                if (entry.state().completed()) {
                    assert completed(entry.shards()) : "state says completed but restore entries are not";
                    clusterService.submitStateUpdateTask(
                        "clean up snapshot restore state",
                        new CleanRestoreStateTaskExecutor.Task(entry.snapshot()),
                        ClusterStateTaskConfig.build(Priority.URGENT),
                        cleanRestoreStateTaskExecutor,
                        cleanRestoreStateTaskExecutor);
                }
            }
        }
    }

    public static RestoreInProgress.State overallState(RestoreInProgress.State nonCompletedState,
                                                       ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        boolean hasFailed = false;
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (!status.value.state().completed()) {
                return nonCompletedState;
            }
            if (status.value.state() == RestoreInProgress.State.FAILURE) {
                hasFailed = true;
            }
        }
        if (hasFailed) {
            return RestoreInProgress.State.FAILURE;
        } else {
            return RestoreInProgress.State.SUCCESS;
        }
    }

    public static boolean completed(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        for (ObjectCursor<RestoreInProgress.ShardRestoreStatus> status : shards.values()) {
            if (!status.value.state().completed()) {
                return false;
            }
        }
        return true;
    }

    public static int failedShards(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
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
                throw new SnapshotRestoreException(request.repositoryName, request.snapshotName,
                        "indices [" + index + "] and [" + previousIndex + "] are renamed into the same index [" + renamedIndex + "]");
            }
        }
        return renamedIndices;
    }

    /**
     * Checks that snapshots can be restored and have compatible version
     *
     * @param repository      repository name
     * @param snapshotInfo    snapshot metadata
     */
    private void validateSnapshotRestorable(final String repository, final SnapshotInfo snapshotInfo) {
        if (!snapshotInfo.state().restorable()) {
            throw new SnapshotRestoreException(new Snapshot(repository, snapshotInfo.snapshotId()),
                                               "unsupported snapshot state [" + snapshotInfo.state() + "]");
        }
        if (Version.CURRENT.before(snapshotInfo.version())) {
            throw new SnapshotRestoreException(new Snapshot(repository, snapshotInfo.snapshotId()),
                                               "the snapshot was created with Elasticsearch version [" + snapshotInfo.version() +
                                                   "] which is higher than the version of this node [" + Version.CURRENT + "]");
        }
    }

    private boolean failed(SnapshotInfo snapshot, String index) {
        for (SnapshotShardFailure failure : snapshot.shardFailures()) {
            if (index.equals(failure.index())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if any of the indices to be closed are currently being restored from a snapshot and fail closing if such an index
     * is found as closing an index that is being restored makes the index unusable (it cannot be recovered).
     */
    public static void checkIndexClosing(ClusterState currentState, Set<IndexMetaData> indices) {
        RestoreInProgress restore = currentState.custom(RestoreInProgress.TYPE);
        if (restore != null) {
            Set<Index> indicesToFail = null;
            for (RestoreInProgress.Entry entry : restore.entries()) {
                for (ObjectObjectCursor<ShardId, RestoreInProgress.ShardRestoreStatus> shard : entry.shards()) {
                    if (!shard.value.state().completed()) {
                        IndexMetaData indexMetaData = currentState.metaData().index(shard.key.getIndex());
                        if (indexMetaData != null && indices.contains(indexMetaData)) {
                            if (indicesToFail == null) {
                                indicesToFail = new HashSet<>();
                            }
                            indicesToFail.add(shard.key.getIndex());
                        }
                    }
                }
            }
            if (indicesToFail != null) {
                throw new IllegalArgumentException("Cannot close indices that are being restored: " + indicesToFail);
            }
        }
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                cleanupRestoreState(event);
            }
        } catch (Exception t) {
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
                if (repository.equals(snapshot.snapshot().getRepository())) {
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

        private final String cause;

        private final String repositoryName;

        private final String snapshotName;

        private final String[] indices;

        private final String renamePattern;

        private final String renameReplacement;

        private final IndicesOptions indicesOptions;

        private final Settings settings;

        private final TimeValue masterNodeTimeout;

        private final boolean includeGlobalState;

        private final boolean partial;

        private final boolean includeAliases;

        private final Settings indexSettings;

        private final String[] ignoreIndexSettings;

        /**
         * Constructs new restore request
         *
         * @param repositoryName     repositoryName
         * @param snapshotName       snapshotName
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
         * @param cause              cause for restoring the snapshot
         */
        public RestoreRequest(String repositoryName, String snapshotName, String[] indices, IndicesOptions indicesOptions,
                              String renamePattern, String renameReplacement, Settings settings,
                              TimeValue masterNodeTimeout, boolean includeGlobalState, boolean partial, boolean includeAliases,
                              Settings indexSettings, String[] ignoreIndexSettings, String cause) {
            this.repositoryName = Objects.requireNonNull(repositoryName);
            this.snapshotName = Objects.requireNonNull(snapshotName);
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
            this.cause = cause;
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
         * Returns repository name
         *
         * @return repository name
         */
        public String repositoryName() {
            return repositoryName;
        }

        /**
         * Returns snapshot name
         *
         * @return snapshot name
         */
        public String snapshotName() {
            return snapshotName;
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
}
