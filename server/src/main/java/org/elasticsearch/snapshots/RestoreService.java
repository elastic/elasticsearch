/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotClusterManagerException;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.RestoreInProgress.ShardRestoreStatus;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener;
import org.elasticsearch.cluster.service.ClusterManagerTaskThrottler;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.FeatureFlags;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.remote.filecache.FileCacheStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.node.remotestore.RemoteStoreNodeAttribute;
import org.elasticsearch.node.remotestore.RemoteStoreNodeService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_HISTORY_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.common.util.FeatureFlags.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.index.store.remote.directory.RemoteSnapshotDirectory.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION;
import static org.elasticsearch.node.Node.NODE_SEARCH_CACHE_SIZE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY;
import static org.elasticsearch.snapshots.SnapshotUtils.filterIndices;
import static org.elasticsearch.snapshots.SnapshotsService.NO_FEATURE_STATES_VALUE;

/**
 * Service responsible for restoring snapshots
 * <p>
 * Restore operation is performed in several stages.
 * <p>
 * First {@link #restoreSnapshot(RestoreSnapshotRequest, org.elasticsearch.action.ActionListener)}
 * method reads information about snapshot and metadata from repository. In update cluster state task it checks restore
 * preconditions, restores global state if needed, creates {@link RestoreInProgress} record with list of shards that needs
 * to be restored and adds this shard to the routing table using {@link RoutingTable.Builder#addAsRestore} method.
 * <p>
 * Individual shards are getting restored as part of normal recovery process in
 * {@link IndexShard#restoreFromRepository} )}
 * method, which detects that shard should be restored from snapshot rather than recovered from gateway by looking
 * at the {@link ShardRouting#recoverySource()} property.
 * <p>
 * At the end of the successful restore process {@code RestoreService} calls {@link #removeCompletedRestoresFromClusterState()},
 * which removes {@link RestoreInProgress} when all shards are completed. In case of
 * restore failure a normal recovery fail-over process kicks in.
 */
public final class RestoreService implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(RestoreService.class);

    public static final Setting<Boolean> REFRESH_REPO_UUID_ON_RESTORE_SETTING = Setting.boolSetting(
        "snapshot.refresh_repo_uuid_on_restore",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Set<String> USER_UNMODIFIABLE_SETTINGS = unmodifiableSet(
        newHashSet(
            SETTING_NUMBER_OF_SHARDS,
            SETTING_VERSION_CREATED,
            SETTING_INDEX_UUID,
            SETTING_CREATION_DATE,
            SETTING_HISTORY_UUID,
            SETTING_REMOTE_STORE_ENABLED,
            SETTING_REMOTE_SEGMENT_STORE_REPOSITORY,
            SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY
        )
    );

    private static final Set<String> UNMODIFIABLE_SETTINGS = Set.of(
        SETTING_NUMBER_OF_SHARDS,
        SETTING_VERSION_CREATED,
        SETTING_INDEX_UUID,
        SETTING_CREATION_DATE,
        SETTING_HISTORY_UUID
    );

    // It's OK to change some settings, but we shouldn't allow simply removing them
    private static final Set<String> UNREMOVABLE_SETTINGS;
    private static final Set<String> USER_UNREMOVABLE_SETTINGS;


    static {
        Set<String> unremovable = Sets.newHashSetWithExpectedSize(UNMODIFIABLE_SETTINGS.size() + 4);
        unremovable.addAll(UNMODIFIABLE_SETTINGS);
        unremovable.add(SETTING_NUMBER_OF_REPLICAS);
        unremovable.add(SETTING_AUTO_EXPAND_REPLICAS);
        UNREMOVABLE_SETTINGS = unmodifiableSet(unremovable);
        USER_UNREMOVABLE_SETTINGS = unmodifiableSet(unremovable);

    }

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final AllocationService allocationService;


    private final MetadataCreateIndexService createIndexService;

    private final IndexMetadataVerifier indexMetadataVerifier;

    private final ShardLimitValidator shardLimitValidator;

    private final ClusterSettings clusterSettings;

    private final SystemIndices systemIndices;

    private final IndicesService indicesService;

    private final FileSettingsService fileSettingsService;

    private final ThreadPool threadPool;

    private volatile boolean refreshRepositoryUuidOnRestore;
    private final ClusterManagerTaskThrottler.ThrottlingKey restoreSnapshotTaskKey;
    private MetadataIndexUpgradeService metadataIndexUpgradeService;
    private final Supplier<Double> dataToFileCacheSizeRatioSupplier;


    public RestoreService(
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        AllocationService allocationService,
        MetadataCreateIndexService createIndexService,
        IndexMetadataVerifier indexMetadataVerifier,
        ShardLimitValidator shardLimitValidator,
        SystemIndices systemIndices,
        IndicesService indicesService,
        FileSettingsService fileSettingsService,
        ThreadPool threadPool,
        ClusterManagerTaskThrottler.ThrottlingKey restoreSnapshotTaskKey, MetadataIndexUpgradeService metadataIndexUpgradeService,
        Supplier<Double> dataToFileCacheSizeRatioSupplier, Supplier<ClusterInfo> clusterInfoSupplier) {
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.indexMetadataVerifier = indexMetadataVerifier;
        this.restoreSnapshotTaskKey = restoreSnapshotTaskKey;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        this.dataToFileCacheSizeRatioSupplier = dataToFileCacheSizeRatioSupplier;
        this.clusterInfoSupplier = clusterInfoSupplier;
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            clusterService.addStateApplier(this);
        }
        this.clusterSettings = clusterService.getClusterSettings();
        this.shardLimitValidator = shardLimitValidator;
        this.systemIndices = systemIndices;
        this.indicesService = indicesService;
        this.fileSettingsService = fileSettingsService;
        this.threadPool = threadPool;
        this.refreshRepositoryUuidOnRestore = REFRESH_REPO_UUID_ON_RESTORE_SETTING.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(REFRESH_REPO_UUID_ON_RESTORE_SETTING, this::setRefreshRepositoryUuidOnRestore);
    }

    private static final String REMOTE_STORE_INDEX_SETTINGS_REGEX = "index.remote_store.*";
    private final Supplier<ClusterInfo> clusterInfoSupplier;



    /**
     * Restores snapshot specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     */
    public void restoreSnapshot(final RestoreSnapshotRequest request, final ActionListener<RestoreCompletionResponse> listener) {
        restoreSnapshot(request, listener, (clusterState, builder) -> {});
    }

    /**
     * Restores snapshot specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     * @param updater  handler that allows callers to make modifications to {@link Metadata}
     *                 in the same cluster state update as the restore operation
     */
//    public void restoreSnapshot(
//        final RestoreSnapshotRequest request,
//        final ActionListener<RestoreCompletionResponse> listener,
//        final BiConsumer<ClusterState, Metadata.Builder> updater
//    ) {
//        try {
//            // Try and fill in any missing repository UUIDs in case they're needed during the restore
//            final var repositoryUuidRefreshStep = new ListenableFuture<Void>();
//            refreshRepositoryUuids(refreshRepositoryUuidOnRestore, repositoriesService, () -> repositoryUuidRefreshStep.onResponse(null));
//
//            // Read snapshot info and metadata from the repository
//            final String repositoryName = request.repository();
//            Repository repository = repositoriesService.repository(repositoryName);
//            final ListenableFuture<RepositoryData> repositoryDataListener = new ListenableFuture<>();
//            repository.getRepositoryData(
//                EsExecutors.DIRECT_EXECUTOR_SERVICE, // TODO contemplate threading here, do we need to fork, see #101445?
//                repositoryDataListener
//            );
//
//            repositoryDataListener.addListener(
//                listener.delegateFailureAndWrap(
//                    (delegate, repositoryData) -> repositoryUuidRefreshStep.addListener(
//                        delegate.delegateFailureAndWrap((subDelegate, ignored) -> {
//                            final String snapshotName = request.snapshot();
//                            final Optional<SnapshotId> matchingSnapshotId = repositoryData.getSnapshotIds()
//                                .stream()
//                                .filter(s -> snapshotName.equals(s.getName()))
//                                .findFirst();
//                            if (matchingSnapshotId.isPresent() == false) {
//                                throw new SnapshotRestoreException(repositoryName, snapshotName, "snapshot does not exist");
//                            }
//
//                            final SnapshotId snapshotId = matchingSnapshotId.get();
//                            if (request.snapshotUuid() != null && request.snapshotUuid().equals(snapshotId.getUUID()) == false) {
//                                throw new SnapshotRestoreException(
//                                    repositoryName,
//                                    snapshotName,
//                                    "snapshot UUID mismatch: expected ["
//                                        + request.snapshotUuid()
//                                        + "] but got ["
//                                        + snapshotId.getUUID()
//                                        + "]"
//                                );
//                            }
//                            repository.getSnapshotInfo(
//                                snapshotId,
//                                subDelegate.delegateFailureAndWrap(
//                                    (l, snapshotInfo) -> startRestore(snapshotInfo, repository, request, repositoryData, updater, l)
//                                )
//                            );
//                        })
//                    )
//                )
//            );
//        } catch (Exception e) {
//            logger.warn(() -> "[" + request.repository() + ":" + request.snapshot() + "] failed to restore snapshot", e);
//            listener.onFailure(e);
//        }
//    }


    @SuppressWarnings("checkstyle:DescendantToken")
    public void  restoreSnapshot(final RestoreSnapshotRequest request,
                                 final ActionListener<RestoreCompletionResponse> listener,
                                 final BiConsumer<ClusterState, Metadata.Builder> updater) {
        try {
            // Setting INDEX_STORE_TYPE_SETTING as REMOTE_SNAPSHOT is intended to be a system-managed index setting that is configured when
            // restoring a snapshot and should not be manually set by user.
            String storeTypeSetting = request.indexSettings().get(INDEX_STORE_TYPE_SETTING.getKey());
            if (storeTypeSetting != null && storeTypeSetting.equals(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT.toString())) {
                throw new SnapshotRestoreException(
                    request.repository(),
                    request.snapshot(),
                    "cannot restore remote snapshot with index settings \"index.store.type\"" +
                        " set to \"remote_snapshot\". Instead use \"storage_type\": \"remote_snapshot\" as argument to restore."
                );
            }
            // Read snapshot info and metadata from the repository
            final String repositoryName = request.repository();
            Repository repository = repositoriesService.repository(repositoryName);
            final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
            repository.getRepositoryData(repositoryDataListener);
            repositoryDataListener.whenComplete(repositoryData -> {
                final String snapshotName = request.snapshot();
                final Optional<SnapshotId> matchingSnapshotId = repositoryData.getSnapshotIds()
                    .stream()
                    .filter(s -> snapshotName.equals(s.getName()))
                    .findFirst();
                if (matchingSnapshotId.isPresent() == false) {
                    throw new SnapshotRestoreException(repositoryName, snapshotName, "snapshot does not exist");
                }

                final SnapshotId snapshotId = matchingSnapshotId.get();
                if (request.snapshotUuid() != null && request.snapshotUuid().equals(snapshotId.getUUID()) == false) {
                    throw new SnapshotRestoreException(
                        repositoryName,
                        snapshotName,
                        "snapshot UUID mismatch: expected [" + request.snapshotUuid() + "] but got [" + snapshotId.getUUID() + "]"
                    );
                }

                final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
                final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);

                // Make sure that we can restore from this snapshot
                validateSnapshotRestorable(repositoryName, snapshotInfo);

                Metadata globalMetadata = null;
                // Resolve the indices from the snapshot that need to be restored
                Map<String, DataStream> dataStreams;
                List<String> requestIndices = new ArrayList<>(Arrays.asList(request.indices()));

                List<String> requestedDataStreams = filterIndices(
                    snapshotInfo.dataStreams(),
                    requestIndices.toArray(new String[0]),
                    IndicesOptions.fromOptions(true, true, true, true)
                );
                if (requestedDataStreams.isEmpty()) {
                    dataStreams = new HashMap<>();
                } else {
                    globalMetadata = repository.getSnapshotGlobalMetadata(snapshotId);
                    final Map<String, DataStream> dataStreamsInSnapshot = globalMetadata.dataStreams();
                    dataStreams = new HashMap<>(requestedDataStreams.size());
                    for (String requestedDataStream : requestedDataStreams) {
                        final DataStream dataStreamInSnapshot = dataStreamsInSnapshot.get(requestedDataStream);
                        assert dataStreamInSnapshot != null : "DataStream [" + requestedDataStream + "] not found in snapshot";
                        dataStreams.put(requestedDataStream, dataStreamInSnapshot);
                    }
                }
                requestIndices.removeAll(dataStreams.keySet());
                Set<String> dataStreamIndices = dataStreams.values()
                    .stream()
                    .flatMap(ds -> ds.getIndices().stream())
                    .map(Index::getName)
                    .collect(Collectors.toSet());
                requestIndices.addAll(dataStreamIndices);

                final List<String> indicesInSnapshot = filterIndices(
                    snapshotInfo.indices(),
                    requestIndices.toArray(new String[0]),
                    request.indicesOptions()
                );

                final Metadata.Builder metadataBuilder;
                if (request.includeGlobalState()) {
                    if (globalMetadata == null) {
                        globalMetadata = repository.getSnapshotGlobalMetadata(snapshotId);
                    }
                    metadataBuilder = Metadata.builder(globalMetadata);
                } else {
                    metadataBuilder = Metadata.builder();
                }

                final List<IndexId> indexIdsInSnapshot = (List<IndexId>) repositoryData.resolveIndices(indicesInSnapshot);
                for (IndexId indexId : indexIdsInSnapshot) {
                    metadataBuilder.put(repository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId), false);
                }

                final Metadata metadata = metadataBuilder.build();

                // Apply renaming on index names, returning a map of names where
                // the key is the renamed index and the value is the original name
                final Map<String, String> indices = renamedIndices(request, indicesInSnapshot, dataStreamIndices);

                // Now we can start the actual restore process by adding shards to be recovered in the cluster state
                // and updating cluster metadata (global and index) as needed
                clusterService.submitStateUpdateTask("restore_snapshot[" + snapshotName + ']', new ClusterStateUpdateTask() {
                    @Override
                    public void onFailure(Exception e) {}

                    @Override
                    public void onFailure(String source, ProcessClusterEventTimeoutException e) {}

                    @Override
                    public void onFailure(String source, NotClusterManagerException e) {

                    }

                    @Override
                    public void onFailure(String source, FailedToCommitClusterStateException t) {}

                    final String restoreUUID = UUIDs.randomBase64UUID();
                    RestoreInfo restoreInfo = null;

                    @Override
                    public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                        return restoreSnapshotTaskKey;
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        // Check if the snapshot to restore is currently being deleted
                        SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                            SnapshotDeletionsInProgress.TYPE,
                            SnapshotDeletionsInProgress.EMPTY
                        );
                        if (deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(snapshotId))) {
                            throw new ConcurrentSnapshotExecutionException(
                                snapshot,
                                "cannot restore a snapshot while a snapshot deletion is in-progress ["
                                    + deletionsInProgress.getEntries().get(0)
                                    + "]"
                            );
                        }

                        // Updating cluster state
                        ClusterState.Builder builder = ClusterState.builder(currentState);
                        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                        RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                        final Map<ShardId, ShardRestoreStatus> shards;
                        final boolean isRemoteSnapshot = IndexModule.Type.REMOTE_SNAPSHOT.match(request.storageType().toString());
                        Set<String> aliases = new HashSet<>();
                        long totalRestorableRemoteIndexesSize = 0;

                        if (indices.isEmpty() == false) {
                            // We have some indices to restore
                            Map<ShardId, ShardRestoreStatus> shardsBuilder = new HashMap<>();

                            for (Map.Entry<String, String> indexEntry : indices.entrySet()) {
                                String renamedIndexName = indexEntry.getKey();
                                String index = indexEntry.getValue();
                                boolean partial = checkPartial(index);

                                IndexId snapshotIndexId = repositoryData.resolveIndexId(index);

                                final Settings overrideSettingsInternal = getOverrideSettingsInternal();
                                final String[] ignoreSettingsInternal = getIgnoreSettingsInternal();

                                IndexMetadata snapshotIndexMetadata = updateIndexSettings(
                                    metadata.index(index),
                                    request.indexSettings(),
                                    request.ignoreIndexSettings(),
                                    overrideSettingsInternal,
                                    ignoreSettingsInternal
                                );
                                if (isRemoteSnapshot) {
                                    snapshotIndexMetadata = addSnapshotToIndexSettings(snapshotIndexMetadata, snapshot, snapshotIndexId);
                                }
                                final boolean isSearchableSnapshot = snapshotIndexMetadata.isRemoteSnapshot();
                                final boolean isRemoteStoreShallowCopy = Boolean.TRUE.equals(
                                    snapshotInfo.isRemoteStoreIndexShallowCopyEnabled()
                                ) && metadata.index(index).getSettings().getAsBoolean(SETTING_REMOTE_STORE_ENABLED, false);
                                if (isSearchableSnapshot && isRemoteStoreShallowCopy) {
                                    throw new SnapshotRestoreException(
                                        snapshot,
                                        "Shallow copy snapshot cannot be restored as searchable snapshot."
                                    );
                                }
                                if (isRemoteStoreShallowCopy && !currentState.getNodes().getMinNodeVersion()
                                    .onOrAfter(Version.V_2_9_0.minimumCompatibilityVersion())) {
                                    throw new SnapshotRestoreException(
                                        snapshot,
                                        "cannot restore shallow copy snapshot for index ["
                                            + index
                                            + "] as some of the nodes in cluster have version less than 2.9"
                                    );
                                }
                                final SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(
                                    restoreUUID,
                                    snapshot,
                                    snapshotInfo.version(),
                                    snapshotIndexId,
                                    isSearchableSnapshot,
                                    isRemoteStoreShallowCopy,
                                    (String) request.getSourceRemoteStoreRepository());
                                final Version minIndexCompatibilityVersion;
                                if (isSearchableSnapshot && isSearchableSnapshotsExtendedCompatibilityEnabled()) {
                                    minIndexCompatibilityVersion = SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION
                                        .minimumIndexCompatibilityVersion();
                                } else {
                                    minIndexCompatibilityVersion = currentState.getNodes()
                                        .getMaxNodeVersion()
                                        .minimumIndexCompatibilityVersion();
                                }
                                try {
                                    snapshotIndexMetadata = metadataIndexUpgradeService.upgradeIndexMetadata(
                                        snapshotIndexMetadata,
                                        minIndexCompatibilityVersion
                                    );
                                } catch (Exception ex) {
                                    throw new SnapshotRestoreException(
                                        snapshot,
                                        "cannot restore index [" + index + "] because it cannot be upgraded",
                                        ex
                                    );
                                }
                                // Check that the index is closed or doesn't exist
                                IndexMetadata currentIndexMetadata = currentState.metadata().index(renamedIndexName);
                                Set<Integer> ignoreShards = new HashSet<>();
                                final Index renamedIndex;
                                if (currentIndexMetadata == null) {
                                    // Index doesn't exist - create it and start recovery
                                    // Make sure that the index we are about to create has valid name
                                    boolean isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(snapshotIndexMetadata.getSettings());
                                    createIndexService.validateIndexName(renamedIndexName, currentState);
                                    createIndexService.validateDotIndex(renamedIndexName, isHidden);
                                    createIndexService.validateIndexSettings(renamedIndexName, snapshotIndexMetadata.getSettings(), false);
                                    MetadataCreateIndexService.validateRefreshIntervalSettings(
                                        snapshotIndexMetadata.getSettings(),
                                        clusterSettings
                                    );
                                    IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata)
                                        .state(IndexMetadata.State.OPEN)
                                        .index(renamedIndexName);
                                    indexMdBuilder.settings(
                                        Settings.builder()
                                            .put(snapshotIndexMetadata.getSettings())
                                            .put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                                    );
                                    createIndexService.addRemoteStoreCustomMetadata(indexMdBuilder, false);
                                    shardLimitValidator.validateShardLimit(
                                        renamedIndexName,
                                        snapshotIndexMetadata.getSettings(),
                                        currentState
                                    );
                                    if (!request.includeAliases() && !snapshotIndexMetadata.getAliases().isEmpty()) {
                                        // Remove all aliases - they shouldn't be restored
                                        indexMdBuilder.removeAllAliases();
                                    } else {
                                        for (final String alias : snapshotIndexMetadata.getAliases().keySet()) {
                                            aliases.add(alias);
                                        }
                                    }
                                    IndexMetadata updatedIndexMetadata = indexMdBuilder.build();
                                    if (partial) {
                                        populateIgnoredShards(index, ignoreShards);
                                    }
                                    rtBuilder.addAsNewRestore(updatedIndexMetadata, recoverySource, ignoreShards);
                                    blocks.addBlocks(updatedIndexMetadata);
                                    mdBuilder.put(updatedIndexMetadata, true);
                                    renamedIndex = updatedIndexMetadata.getIndex();
                                } else {
                                    validateExistingIndex(currentIndexMetadata, snapshotIndexMetadata, renamedIndexName, partial);
                                    // Index exists and it's closed - open it in metadata and start recovery
                                    IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata)
                                        .state(IndexMetadata.State.OPEN);
                                    indexMdBuilder.version(
                                        Math.max(snapshotIndexMetadata.getVersion(), 1 + currentIndexMetadata.getVersion())
                                    );
                                    indexMdBuilder.mappingVersion(
                                        Math.max(snapshotIndexMetadata.getMappingVersion(), 1 + currentIndexMetadata.getMappingVersion())
                                    );
                                    indexMdBuilder.settingsVersion(
                                        Math.max(snapshotIndexMetadata.getSettingsVersion(), 1 + currentIndexMetadata.getSettingsVersion())
                                    );
                                    indexMdBuilder.aliasesVersion(
                                        Math.max(snapshotIndexMetadata.getAliasesVersion(), 1 + currentIndexMetadata.getAliasesVersion())
                                    );

                                    for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                                        indexMdBuilder.primaryTerm(
                                            shard,
                                            Math.max(snapshotIndexMetadata.primaryTerm(shard), currentIndexMetadata.primaryTerm(shard))
                                        );
                                    }

                                    if (!request.includeAliases()) {
                                        // Remove all snapshot aliases
                                        if (!snapshotIndexMetadata.getAliases().isEmpty()) {
                                            indexMdBuilder.removeAllAliases();
                                        }
                                        /// Add existing aliases
                                        for (final AliasMetadata alias : currentIndexMetadata.getAliases().values()) {
                                            indexMdBuilder.putAlias(alias);
                                        }
                                    } else {
                                        for (final String alias : snapshotIndexMetadata.getAliases().keySet()) {
                                            aliases.add(alias);
                                        }
                                    }
                                    final Settings.Builder indexSettingsBuilder = Settings.builder()
                                        .put(snapshotIndexMetadata.getSettings())
                                        .put(SETTING_INDEX_UUID, currentIndexMetadata.getIndexUUID());
                                    // add a restore uuid
                                    indexSettingsBuilder.put(SETTING_HISTORY_UUID, UUIDs.randomBase64UUID());
                                    indexMdBuilder.settings(indexSettingsBuilder);
                                    IndexMetadata updatedIndexMetadata = indexMdBuilder.index(renamedIndexName).build();
                                    rtBuilder.addAsRestore(updatedIndexMetadata, recoverySource);
                                    blocks.updateBlocks(updatedIndexMetadata);
                                    mdBuilder.put(updatedIndexMetadata, true);
                                    renamedIndex = updatedIndexMetadata.getIndex();
                                }

                                for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                                    if (isRemoteSnapshot) {
                                        IndexShardSnapshotStatus.Copy shardStatus = repository.getShardSnapshotStatus(
                                            snapshotInfo.snapshotId(),
                                            snapshotIndexId,
                                            new ShardId(metadata.index(index).getIndex(), shard)
                                        ).asCopy();
                                        totalRestorableRemoteIndexesSize += shardStatus.getTotalSize();
                                    }
                                    if (!ignoreShards.contains(shard)) {
                                        shardsBuilder.put(
                                            new ShardId(renamedIndex, shard),
                                            new ShardRestoreStatus(clusterService.state().nodes().getLocalNodeId())
                                        );
                                    } else {
                                        shardsBuilder.put(
                                            new ShardId(renamedIndex, shard),
                                            new ShardRestoreStatus(
                                                clusterService.state().nodes().getLocalNodeId(),
                                                RestoreInProgress.State.FAILURE
                                            )
                                        );
                                    }
                                }
                            }

                            shards = Collections.unmodifiableMap(shardsBuilder);
                            RestoreInProgress.Entry restoreEntry = new RestoreInProgress.Entry(
                                restoreUUID,
                                snapshot,
                                overallState(RestoreInProgress.State.INIT, shards),
                                Collections.unmodifiableList(new ArrayList<>(indices.keySet())),
                                shards
                            );
                            builder.putCustom(
                                RestoreInProgress.TYPE,
                                new RestoreInProgress.Builder(currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).add(
                                    restoreEntry
                                ).build()
                            );
                        } else {
                            shards = Map.of();
                        }

                        checkAliasNameConflicts(indices, aliases);
                        if (isRemoteSnapshot) {
                            validateSearchableSnapshotRestorable(totalRestorableRemoteIndexesSize);
                        }

                        Map<String, DataStream> updatedDataStreams = new HashMap<>(currentState.metadata().dataStreams());
                        updatedDataStreams.putAll(
                            dataStreams.values()
                                .stream()
                                .map(ds -> updateDataStream(ds, mdBuilder, request))
                                .collect(Collectors.toMap(DataStream::getName, Function.identity()))
                        );
                        mdBuilder.dataStreams(updatedDataStreams);

                        // Restore global state if needed
                        if (request.includeGlobalState()) {
                            if (metadata.persistentSettings() != null) {
                                Settings settings = metadata.persistentSettings();
                                clusterSettings.validateUpdate(settings);
                                mdBuilder.persistentSettings(settings);
                            }
                            if (metadata.templates() != null) {
                                // TODO: Should all existing templates be deleted first?
                                for (final IndexTemplateMetadata cursor : metadata.templates().values()) {
                                    MetadataCreateIndexService.validateRefreshIntervalSettings(cursor.settings(), clusterSettings);
                                    mdBuilder.put(cursor);
                                }
                            }
                            if (metadata.customs() != null) {
                                for (final Map.Entry<String, Metadata.Custom> cursor : metadata.customs().entrySet()) {
                                    if (RepositoriesMetadata.TYPE.equals(cursor.getKey()) == false
                                        && DataStreamMetadata.TYPE.equals(cursor.getKey()) == false) {
                                        // Don't restore repositories while we are working with them
                                        // TODO: Should we restore them at the end?
                                        // Also, don't restore data streams here, we already added them to the metadata builder above
                                        mdBuilder.putCustom(cursor.getKey(), cursor.getValue());
                                    }
                                }
                            }
                        }

                        if (completed(shards)) {
                            // We don't have any indices to restore - we are done
                            restoreInfo = new RestoreInfo(
                                snapshotId.getName(),
                                Collections.unmodifiableList(new ArrayList<>(indices.keySet())),
                                shards.size(),
                                shards.size() - failedShards(shards)
                            );
                        }

                        RoutingTable rt = rtBuilder.build();
                        ClusterState updatedState = builder.metadata(mdBuilder).blocks(blocks).routingTable(rt).build();
                        return allocationService.reroute(updatedState, "restored snapshot [" + snapshot + "]");
                    }

                    @Override
                    public ClusterStateTaskExecutor.ClusterTasksResult<RestoreSnapshotStateTask.Task>
                        execute(ClusterState currentState, List<RestoreSnapshotStateTask.Task> tasks) {
                        return null;
                    }

                    private void checkAliasNameConflicts(Map<String, String> renamedIndices, Set<String> aliases) {
                        for (Map.Entry<String, String> renamedIndex : renamedIndices.entrySet()) {
                            if (aliases.contains(renamedIndex.getKey())) {
                                throw new SnapshotRestoreException(
                                    snapshot,
                                    "cannot rename index ["
                                        + renamedIndex.getValue()
                                        + "] into ["
                                        + renamedIndex.getKey()
                                        + "] because of conflict with an alias with the same name"
                                );
                            }
                        }
                    }

                    private String[] getIgnoreSettingsInternal() {
                        // for non-remote store enabled domain, we will remove all the remote store
                        // related index settings present in the snapshot.
                        String[] indexSettingsToBeIgnored = new String[] {};
                        if (false == RemoteStoreNodeAttribute.isRemoteStoreAttributePresent(clusterService.getSettings())) {
                            indexSettingsToBeIgnored = ArrayUtils.concat(
                                indexSettingsToBeIgnored,
                                new String[] { REMOTE_STORE_INDEX_SETTINGS_REGEX }
                            );
                        }
                        return indexSettingsToBeIgnored;
                    }

                    private Settings getOverrideSettingsInternal() {
                        final Settings.Builder settingsBuilder = Settings.builder();

                        // We will use whatever replication strategy provided by user or from snapshot metadata unless
                        // cluster is remote store enabled or user have restricted a specific replication type in the
                        // cluster. If cluster is undergoing remote store migration, replication strategy is strictly SEGMENT type
                        if (RemoteStoreNodeAttribute.isRemoteStoreAttributePresent(clusterService.getSettings())
                            || clusterSettings.get(IndicesService.CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING)
                            || RemoteStoreNodeService.isMigratingToRemoteStore(clusterSettings)) {
                            MetadataCreateIndexService.updateReplicationStrategy(
                                settingsBuilder,
                                request.indexSettings(),
                                clusterService.getSettings(),
                                null,
                                clusterSettings
                            );
                        }
                        // remote store settings needs to be overridden if the remote store feature is enabled in the
                        // cluster where snapshot is being restored.
                        MetadataCreateIndexService.updateRemoteStoreSettings(
                            settingsBuilder,
                            clusterService.state(),
                            clusterSettings,
                            clusterService.getSettings(),
                            String.join(",", request.indices())
                        );
                        return settingsBuilder.build();
                    }

                    private void populateIgnoredShards(String index, final Set<Integer> ignoreShards) {
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
                                throw new SnapshotRestoreException(
                                    snapshot,
                                    "index [" + index + "] wasn't fully snapshotted - cannot " + "restore"
                                );
                            }
                        } else {
                            return false;
                        }
                    }

                    private void validateExistingIndex(
                        IndexMetadata currentIndexMetadata,
                        IndexMetadata snapshotIndexMetadata,
                        String renamedIndex,
                        boolean partial
                    ) {
                        // Index exist - checking that it's closed
                        if (currentIndexMetadata.getState() != IndexMetadata.State.CLOSE) {
                            // TODO: Enable restore for open indices
                            throw new SnapshotRestoreException(
                                snapshot,
                                "cannot restore index ["
                                    + renamedIndex
                                    + "] because an open index "
                                    + "with same name already exists in the cluster. Either close or delete" +
                                    " the existing index or restore the "
                                    + "index under a different name by providing a rename pattern and replacement name"
                            );
                        }
                        // Index exist - checking if it's partial restore
                        if (partial) {
                            throw new SnapshotRestoreException(
                                snapshot,
                                "cannot restore partial index [" + renamedIndex + "] because such index already exists"
                            );
                        }
                        // Make sure that the number of shards is the same. That's the only thing that we cannot change
                        if (currentIndexMetadata.getNumberOfShards() != snapshotIndexMetadata.getNumberOfShards()) {
                            throw new SnapshotRestoreException(
                                snapshot,
                                "cannot restore index ["
                                    + renamedIndex
                                    + "] with ["
                                    + currentIndexMetadata.getNumberOfShards()
                                    + "] shards from a snapshot of index ["
                                    + snapshotIndexMetadata.getIndex().getName()
                                    + "] with ["
                                    + snapshotIndexMetadata.getNumberOfShards()
                                    + "] shards"
                            );
                        }
                    }

                    /**
                     * Optionally updates index settings in indexMetadata by removing settings listed in ignoreSettings and
                     * merging them with settings in changeSettings.
                     */
                    private IndexMetadata updateIndexSettings(
                        IndexMetadata indexMetadata,
                        Settings overrideSettings,
                        String[] ignoreSettings,
                        Settings overrideSettingsInternal,
                        String[] ignoreSettingsInternal
                    ) {
                        Settings normalizedChangeSettings = Settings.builder()
                            .put(overrideSettings)
                            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
                            .build();
                        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexMetadata.getSettings())
                            && IndexSettings.INDEX_SOFT_DELETES_SETTING.exists(overrideSettings)
                            && IndexSettings.INDEX_SOFT_DELETES_SETTING.get(overrideSettings) == false) {
                            throw new SnapshotRestoreException(
                                snapshot,
                                "cannot disable setting [" + IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey() + "] on restore"
                            );
                        }
                        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
                        Settings settings = indexMetadata.getSettings();
                        Set<String> keyFilters = new HashSet<>();
                        List<String> simpleMatchPatterns = new ArrayList<>();
                        for (String ignoredSetting : ignoreSettings) {
                            if (!Regex.isSimpleMatchPattern(ignoredSetting)) {
                                if (USER_UNREMOVABLE_SETTINGS.contains(ignoredSetting)) {
                                    throw new SnapshotRestoreException(
                                        snapshot,
                                        "cannot remove setting [" + ignoredSetting + "] on restore"
                                    );
                                } else {
                                    keyFilters.add(ignoredSetting);
                                }
                            } else {
                                simpleMatchPatterns.add(ignoredSetting);
                            }
                        }

                        // add internal settings to ignore settings list
                        for (String ignoredSetting : ignoreSettingsInternal) {
                            if (!Regex.isSimpleMatchPattern(ignoredSetting)) {
                                keyFilters.add(ignoredSetting);
                            } else {
                                simpleMatchPatterns.add(ignoredSetting);
                            }
                        }

                        Predicate<String> settingsFilter = k -> {
                            if (USER_UNREMOVABLE_SETTINGS.contains(k) == false) {
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
                                if (USER_UNMODIFIABLE_SETTINGS.contains(k)) {
                                    throw new SnapshotRestoreException(snapshot, "cannot modify setting [" + k + "] on restore");
                                } else {
                                    return true;
                                }
                            }));

                        // override internal settings
                        if (overrideSettingsInternal != null) {
                            settingsBuilder.put(overrideSettingsInternal).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
                        }
                        settingsBuilder.remove(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey());
                        return builder.settings(settingsBuilder).build();
                    }

                    private void validateSearchableSnapshotRestorable(long totalRestorableRemoteIndexesSize) {
                        ClusterInfo clusterInfo = clusterInfoSupplier.get();
                        final double remoteDataToFileCacheRatio = dataToFileCacheSizeRatioSupplier.get();
                        Map<String, FileCacheStats> nodeFileCacheStats = clusterInfo.getNodeFileCacheStats();
                        if (nodeFileCacheStats.isEmpty() || remoteDataToFileCacheRatio <= 0.01f) {
                            return;
                        }

                        long totalNodeFileCacheSize = clusterInfo.getNodeFileCacheStats()
                            .values()
                            .stream()
                            .map(fileCacheStats -> fileCacheStats.getTotal().getBytes())
                            .mapToLong(Long::longValue)
                            .sum();

                        Predicate<ShardRouting> isRemoteSnapshotShard = shardRouting -> shardRouting.primary()
                            && clusterService.state().getMetadata().getIndexSafe(shardRouting.index()).isRemoteSnapshot();

                        ShardsIterator shardsIterator = clusterService.state()
                            .routingTable()
                            .allShardsSatisfyingPredicate(isRemoteSnapshotShard);

                        long totalRestoredRemoteIndexesSize = shardsIterator.getShardRoutings()
                            .stream()
                            .map(clusterInfo::getShardSize)
                            .mapToLong(Long::longValue)
                            .sum();

                        if (totalRestoredRemoteIndexesSize + totalRestorableRemoteIndexesSize > remoteDataToFileCacheRatio
                            * totalNodeFileCacheSize) {
                            throw new SnapshotRestoreException(
                                snapshot,
                                "Size of the indexes to be restored exceeds the file cache bounds. " +
                                    "Increase the file cache capacity on the cluster nodes using "
                                    + NODE_SEARCH_CACHE_SIZE_SETTING.getKey()
                                    + " setting."
                            );
                        }
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.warn(() -> new ParameterizedMessage("[{}] failed to restore snapshot", snapshotId), e);
                        listener.onFailure(e);
                    }

                    @Override
                    public Comparable<TimeValue> timeout() {
                        return request.clusterManagerNodeTimeout();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        listener.onResponse(new RestoreCompletionResponse(restoreUUID, snapshot, restoreInfo));
                    }
                });
            }, listener::onFailure);
        } catch (Exception e) {
            logger.warn(
                () -> new ParameterizedMessage("[{}] failed to restore snapshot", request.repository() + ":" + request.snapshot()),
                e
            );
            listener.onFailure(e);
        }
    }

    private boolean isSearchableSnapshotsExtendedCompatibilityEnabled() {
        return org.elasticsearch.Version.CURRENT.after(org.elasticsearch.Version.V_2_4_0)
            && FeatureFlags.isEnabled(SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY);
    }

    private static IndexMetadata addSnapshotToIndexSettings(IndexMetadata metadata, Snapshot snapshot, IndexId indexId) {
        final Settings newSettings = Settings.builder()
            .put(metadata.getSettings())
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY.getKey(), snapshot.getRepository())
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_ID_UUID.getKey(), snapshot.getSnapshotId().getUUID())
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_ID_NAME.getKey(), snapshot.getSnapshotId().getName())
            .put(IndexSettings.SEARCHABLE_SNAPSHOT_INDEX_ID.getKey(), indexId.getId())
            .build();
        return IndexMetadata.builder(metadata).settings(newSettings).build();
    }

    private static Map<String, String> renamedIndices(
        RestoreSnapshotRequest request,
        List<String> filteredIndices,
        Set<String> dataStreamIndices
    ) {
        Map<String, String> renamedIndices = new HashMap<>();
        for (String index : filteredIndices) {
            String renamedIndex = renameIndex(index, request, dataStreamIndices.contains(index));
            String previousIndex = renamedIndices.put(renamedIndex, index);
            if (previousIndex != null) {
                throw new SnapshotRestoreException(
                    request.repository(),
                    request.snapshot(),
                    "indices [" + index + "] and [" + previousIndex + "] are renamed into the same index [" + renamedIndex + "]"
                );
            }
        }
        return Collections.unmodifiableMap(renamedIndices);
    }

    private static String renameIndex(String index, RestoreSnapshotRequest request, boolean partOfDataStream) {
        String renamedIndex = index;
        if (request.renameReplacement() != null && request.renamePattern() != null) {
            partOfDataStream = partOfDataStream && index.startsWith(DataStream.BACKING_INDEX_PREFIX);
            if (partOfDataStream) {
                index = index.substring(DataStream.BACKING_INDEX_PREFIX.length());
            }
            renamedIndex = index.replaceAll(request.renamePattern(), request.renameReplacement());
            if (partOfDataStream) {
                renamedIndex = DataStream.BACKING_INDEX_PREFIX + renamedIndex;
            }
        }
        return renamedIndex;
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    private static void validateSnapshotRestorable(final String repository, final SnapshotInfo snapshotInfo) {
        if (!snapshotInfo.state().restorable()) {
            throw new SnapshotRestoreException(
                new Snapshot(repository, snapshotInfo.snapshotId()),
                "unsupported snapshot state [" + snapshotInfo.state() + "]"
            );
        }
        if (Version.CURRENT.before(snapshotInfo.version())) {
            throw new SnapshotRestoreException(
                new Snapshot(repository, snapshotInfo.snapshotId()),
                "the snapshot was created with OpenSearch version ["
                    + snapshotInfo.version()
                    + "] which is higher than the version of this node ["
                    + Version.CURRENT
                    + "]"
            );
        }
    }


    /**
     * Start the snapshot restore process. First validate that the snapshot can be restored based on the contents of the repository and
     * the restore request. If it can be restored, compute the metadata to be restored for the current restore request and submit the
     * cluster state update request to start the restore.
     *
     * @param snapshotInfo   snapshot info for the snapshot to restore
     * @param repository     the repository to restore from
     * @param request        restore request
     * @param repositoryData current repository data for the repository to restore from
     * @param updater        handler that allows callers to make modifications to {@link Metadata} in the same cluster state update as the
     *                       restore operation
     * @param listener       listener to resolve once restore has been started
     * @throws IOException   on failure to load metadata from the repository
     */
    private void startRestore(
        SnapshotInfo snapshotInfo,
        Repository repository,
        RestoreSnapshotRequest request,
        RepositoryData repositoryData,
        BiConsumer<ClusterState, Metadata.Builder> updater,
        ActionListener<RestoreCompletionResponse> listener
    ) throws IOException {
        assert Repository.assertSnapshotMetaThread();
        final SnapshotId snapshotId = snapshotInfo.snapshotId();
        final String repositoryName = repository.getMetadata().name();
        final Snapshot snapshot = new Snapshot(repositoryName, snapshotId);

        // Make sure that we can restore from this snapshot
        validateSnapshotRestorable(request, repository.getMetadata(), snapshotInfo, repositoriesService.getPreRestoreVersionChecks());

        // Get the global state if necessary
        Metadata globalMetadata = null;
        final Metadata.Builder metadataBuilder;
        if (request.includeGlobalState()) {
            globalMetadata = repository.getSnapshotGlobalMetadata(snapshotId);
            metadataBuilder = Metadata.builder(globalMetadata);
        } else {
            metadataBuilder = Metadata.builder();
        }

        final String[] indicesInRequest = request.indices();
        List<String> requestIndices = new ArrayList<>(indicesInRequest.length);
        if (indicesInRequest.length == 0) {
            // no specific indices request means restore everything
            requestIndices.add("*");
        } else {
            Collections.addAll(requestIndices, indicesInRequest);
        }

        // Determine system indices to restore from requested feature states
        final Map<String, List<String>> featureStatesToRestore = getFeatureStatesToRestore(request, snapshotInfo, snapshot);
        final Set<String> featureStateIndices = featureStatesToRestore.values()
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        final Set<String> featureStateDataStreams = featureStatesToRestore.keySet().stream().filter(featureName -> {
            if (systemIndices.getFeatureNames().contains(featureName)) {
                return true;
            }
            logger.warn(
                () -> format(
                    "Restoring snapshot[%s] skipping feature [%s] because it is not available in this cluster",
                    snapshotInfo.snapshotId(),
                    featureName
                )
            );
            return false;
        })
            .map(systemIndices::getFeature)
            .flatMap(feature -> feature.getDataStreamDescriptors().stream())
            .map(SystemDataStreamDescriptor::getDataStreamName)
            .collect(Collectors.toSet());

        // Get data stream metadata for requested data streams
        Tuple<Map<String, DataStream>, Map<String, DataStreamAlias>> result = getDataStreamsToRestore(
            repository,
            snapshotId,
            snapshotInfo,
            globalMetadata,
            requestIndices,
            featureStateDataStreams,
            request.includeAliases()
        );
        Map<String, DataStream> dataStreamsToRestore = result.v1();
        Map<String, DataStreamAlias> dataStreamAliasesToRestore = result.v2();

        // Remove the data streams from the list of requested indices
        requestIndices.removeAll(dataStreamsToRestore.keySet());

        // And add the backing indices and failure indices of data streams (the distinction is important for renaming)
        final Set<String> systemDataStreamIndices;
        final Set<String> nonSystemDataStreamBackingIndices;
        final Set<String> nonSystemDataStreamFailureIndices;
        {
            Map<Boolean, Set<String>> backingIndices = dataStreamsToRestore.values()
                .stream()
                .flatMap(ds -> ds.getIndices().stream().map(idx -> new Tuple<>(ds.isSystem(), idx.getName())))
                .collect(Collectors.partitioningBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toSet())));
            Map<Boolean, Set<String>> failureIndices = Map.of();
            if (DataStream.isFailureStoreFeatureFlagEnabled()) {
                failureIndices = dataStreamsToRestore.values()
                    .stream()
                    .flatMap(ds -> ds.getFailureIndices().getIndices().stream().map(idx -> new Tuple<>(ds.isSystem(), idx.getName())))
                    .collect(Collectors.partitioningBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toSet())));
            }
            systemDataStreamIndices = Sets.union(backingIndices.getOrDefault(true, Set.of()), failureIndices.getOrDefault(true, Set.of()));
            nonSystemDataStreamBackingIndices = backingIndices.getOrDefault(false, Set.of());
            nonSystemDataStreamFailureIndices = failureIndices.getOrDefault(false, Set.of());
        }
        requestIndices.addAll(nonSystemDataStreamBackingIndices);
        requestIndices.addAll(nonSystemDataStreamFailureIndices);
        final Set<String> allSystemIndicesToRestore = Stream.of(systemDataStreamIndices, featureStateIndices)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

        // Strip system indices out of the list of "available" indices - these should only come from feature states.
        List<String> availableNonSystemIndices;
        {
            Set<String> systemIndicesInSnapshot = new HashSet<>();
            snapshotInfo.featureStates().stream().flatMap(state -> state.getIndices().stream()).forEach(systemIndicesInSnapshot::add);
            // And the system data stream backing indices too
            snapshotInfo.indices().stream().filter(systemIndices::isSystemIndexBackingDataStream).forEach(systemIndicesInSnapshot::add);

            Set<String> explicitlyRequestedSystemIndices = new HashSet<>(requestIndices);
            explicitlyRequestedSystemIndices.retainAll(systemIndicesInSnapshot);

            if (explicitlyRequestedSystemIndices.size() > 0) {
                throw new IllegalArgumentException(
                    format(
                        "requested system indices %s, but system indices can only be restored as part of a feature state",
                        explicitlyRequestedSystemIndices
                    )
                );
            }

            availableNonSystemIndices = snapshotInfo.indices()
                .stream()
                .filter(idxName -> systemIndicesInSnapshot.contains(idxName) == false)
                .toList();
        }

        // Resolve the indices that were directly requested
        final List<String> requestedIndicesInSnapshot = filterIndices(
            availableNonSystemIndices,
            requestIndices.toArray(String[]::new),
            request.indicesOptions()
        );

        // Combine into the final list of indices to be restored
        final List<String> requestedIndicesIncludingSystem = Stream.of(
            requestedIndicesInSnapshot,
            featureStateIndices,
            systemDataStreamIndices
        ).flatMap(Collection::stream).distinct().toList();

        final Set<String> explicitlyRequestedSystemIndices = new HashSet<>();
        for (IndexId indexId : repositoryData.resolveIndices(requestedIndicesIncludingSystem).values()) {
            IndexMetadata snapshotIndexMetaData = repository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId);
            if (snapshotIndexMetaData.isSystem()) {
                if (requestIndices.contains(indexId.getName())) {
                    explicitlyRequestedSystemIndices.add(indexId.getName());
                }
            }
            metadataBuilder.put(snapshotIndexMetaData, false);
        }

        assert explicitlyRequestedSystemIndices.size() == 0
            : "it should be impossible to reach this point with explicitly requested system indices, but got: "
                + explicitlyRequestedSystemIndices;

        // Now we can start the actual restore process by adding shards to be recovered in the cluster state
        // and updating cluster metadata (global and index) as needed
        submitUnbatchedTask(
            "restore_snapshot[" + snapshotId.getName() + ']',
            new RestoreSnapshotStateTask(
                request,
                snapshot,
                featureStatesToRestore.keySet(),
                // Apply renaming on index names, returning a map of names where
                // the key is the renamed index and the value is the original name
                renamedIndices(
                    request,
                    requestedIndicesIncludingSystem,
                    nonSystemDataStreamBackingIndices,
                    nonSystemDataStreamFailureIndices,
                    allSystemIndicesToRestore,
                    repositoryData
                ),
                snapshotInfo,
                metadataBuilder.dataStreams(dataStreamsToRestore, dataStreamAliasesToRestore).build(),
                dataStreamsToRestore.values(),
                updater,
                clusterService.getSettings(),
                listener
            )
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    private void setRefreshRepositoryUuidOnRestore(boolean refreshRepositoryUuidOnRestore) {
        this.refreshRepositoryUuidOnRestore = refreshRepositoryUuidOnRestore;
    }

    /**
     * Best-effort attempt to make sure that we know all the repository UUIDs. Calls {@link Repository#getRepositoryData} on every
     * {@link BlobStoreRepository} with a missing UUID.
     *
     * @param enabled If {@code false} this method completes the listener immediately
     * @param repositoriesService Supplies the repositories to check
     * @param onCompletion Action that is executed when all repositories have been refreshed.
     */
    // Exposed for tests
    static void refreshRepositoryUuids(boolean enabled, RepositoriesService repositoriesService, Runnable onCompletion) {
        try (var refs = new RefCountingRunnable(onCompletion)) {
            if (enabled == false) {
                logger.debug("repository UUID refresh is disabled");
                return;
            }

            for (Repository repository : repositoriesService.getRepositories().values()) {
                // We only care about BlobStoreRepositories because they're the only ones that can contain a searchable snapshot, and we
                // only care about ones with missing UUIDs. It's possible to have the UUID change from under us if, e.g., the repository was
                // wiped by an external force, but in this case any searchable snapshots are lost anyway so it doesn't really matter.
                if (repository instanceof BlobStoreRepository && repository.getMetadata().uuid().equals(RepositoryData.MISSING_UUID)) {
                    final var repositoryName = repository.getMetadata().name();
                    logger.info("refreshing repository UUID for repository [{}]", repositoryName);
                    repository.getRepositoryData(
                        EsExecutors.DIRECT_EXECUTOR_SERVICE, // TODO contemplate threading here, do we need to fork, see #101445?
                        ActionListener.releaseAfter(new ActionListener<>() {
                            @Override
                            public void onResponse(RepositoryData repositoryData) {
                                logger.debug(() -> format("repository UUID [%s] refresh completed", repositoryName));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.debug(() -> format("repository UUID [%s] refresh failed", repositoryName), e);
                            }
                        }, refs.acquire())
                    );
                }
            }
        }
    }

    private boolean isSystemIndex(IndexMetadata indexMetadata) {
        return indexMetadata.isSystem() || systemIndices.isSystemName(indexMetadata.getIndex().getName());
    }

    private static Tuple<Map<String, DataStream>, Map<String, DataStreamAlias>> getDataStreamsToRestore(
        Repository repository,
        SnapshotId snapshotId,
        SnapshotInfo snapshotInfo,
        Metadata globalMetadata,
        List<String> requestIndices,
        Collection<String> featureStateDataStreams,
        boolean includeAliases
    ) {
        Map<String, DataStream> dataStreams;
        Map<String, DataStreamAlias> dataStreamAliases;
        List<String> requestedDataStreams = filterIndices(
            snapshotInfo.dataStreams(),
            Stream.of(requestIndices, featureStateDataStreams).flatMap(Collection::stream).toArray(String[]::new),
            IndicesOptions.lenientExpand()
        );
        if (requestedDataStreams.isEmpty()) {
            dataStreams = Map.of();
            dataStreamAliases = Map.of();
        } else {
            if (globalMetadata == null) {
                globalMetadata = repository.getSnapshotGlobalMetadata(snapshotId);
            }
            final Map<String, DataStream> dataStreamsInSnapshot = globalMetadata.dataStreams();
            dataStreams = Maps.newMapWithExpectedSize(requestedDataStreams.size());
            for (String requestedDataStream : requestedDataStreams) {
                final DataStream dataStreamInSnapshot = dataStreamsInSnapshot.get(requestedDataStream);
                assert dataStreamInSnapshot != null : "DataStream [" + requestedDataStream + "] not found in snapshot";

                if (dataStreamInSnapshot.isSystem() == false) {
                    dataStreams.put(requestedDataStream, dataStreamInSnapshot);
                } else if (requestIndices.contains(requestedDataStream)) {
                    throw new IllegalArgumentException(
                        format(
                            "requested system data stream [%s], but system data streams can only be restored as part of a feature state",
                            requestedDataStream
                        )
                    );
                } else if (featureStateDataStreams.contains(requestedDataStream)) {
                    dataStreams.put(requestedDataStream, dataStreamInSnapshot);
                } else {
                    logger.debug(
                        "omitting system data stream [{}] from snapshot restoration because its feature state was not requested",
                        requestedDataStream
                    );
                }
            }
            if (includeAliases) {
                dataStreamAliases = new HashMap<>();
                final Map<String, DataStreamAlias> dataStreamAliasesInSnapshot = globalMetadata.dataStreamAliases();
                for (DataStreamAlias alias : dataStreamAliasesInSnapshot.values()) {
                    DataStreamAlias copy = alias.intersect(dataStreams.keySet()::contains);
                    if (copy.getDataStreams().isEmpty() == false) {
                        dataStreamAliases.put(alias.getName(), copy);
                    }
                }
            } else {
                dataStreamAliases = Map.of();
            }
        }
        return new Tuple<>(dataStreams, dataStreamAliases);
    }

    private Map<String, List<String>> getFeatureStatesToRestore(
        RestoreSnapshotRequest request,
        SnapshotInfo snapshotInfo,
        Snapshot snapshot
    ) {
        if (snapshotInfo.featureStates() == null) {
            return Collections.emptyMap();
        }
        final Map<String, List<String>> snapshotFeatureStates = snapshotInfo.featureStates()
            .stream()
            .collect(Collectors.toMap(SnapshotFeatureInfo::getPluginName, SnapshotFeatureInfo::getIndices));

        final Map<String, List<String>> featureStatesToRestore;
        final String[] requestedFeatureStates = request.featureStates();

        if (requestedFeatureStates == null || requestedFeatureStates.length == 0) {
            // Handle the default cases - defer to the global state value
            if (request.includeGlobalState()) {
                featureStatesToRestore = new HashMap<>(snapshotFeatureStates);
            } else {
                featureStatesToRestore = Collections.emptyMap();
            }
        } else if (requestedFeatureStates.length == 1 && NO_FEATURE_STATES_VALUE.equalsIgnoreCase(requestedFeatureStates[0])) {
            // If there's exactly one value and it's "none", include no states
            featureStatesToRestore = Collections.emptyMap();
        } else {
            // Otherwise, handle the list of requested feature states
            final Set<String> requestedStates = Set.of(requestedFeatureStates);
            if (requestedStates.contains(NO_FEATURE_STATES_VALUE)) {
                throw new SnapshotRestoreException(
                    snapshot,
                    "the feature_states value ["
                        + NO_FEATURE_STATES_VALUE
                        + "] indicates that no feature states should be restored, but other feature states were requested: "
                        + requestedStates
                );
            }
            if (snapshotFeatureStates.keySet().containsAll(requestedStates) == false) {
                Set<String> nonExistingRequestedStates = new HashSet<>(requestedStates);
                nonExistingRequestedStates.removeAll(snapshotFeatureStates.keySet());
                throw new SnapshotRestoreException(
                    snapshot,
                    "requested feature states [" + nonExistingRequestedStates + "] are not present in snapshot"
                );
            }
            featureStatesToRestore = new HashMap<>(snapshotFeatureStates);
            featureStatesToRestore.keySet().retainAll(requestedStates);
        }

        final List<String> featuresNotOnThisNode = featureStatesToRestore.keySet()
            .stream()
            .filter(s -> systemIndices.getFeatureNames().contains(s) == false)
            .toList();
        if (featuresNotOnThisNode.isEmpty() == false) {
            throw new SnapshotRestoreException(
                snapshot,
                "requested feature states "
                    + featuresNotOnThisNode
                    + " are present in "
                    + "snapshot but those features are not installed on the current master node"
            );
        }
        return featureStatesToRestore;
    }

    /**
     * Resolves a set of index names that currently exist in the cluster that are part of a feature state which is about to be restored,
     * and should therefore be removed prior to restoring those feature states from the snapshot.
     *
     * @param currentState The current cluster state
     * @param featureStatesToRestore A set of feature state names that are about to be restored
     * @return A set of index names that should be removed based on the feature states being restored
     */
    private Set<Index> resolveSystemIndicesToDelete(ClusterState currentState, Set<String> featureStatesToRestore) {
        if (featureStatesToRestore == null) {
            return Collections.emptySet();
        }

        return featureStatesToRestore.stream()
            .map(systemIndices::getFeature)
            .filter(Objects::nonNull) // Features that aren't present on this node will be warned about in `getFeatureStatesToRestore`
            .flatMap(feature -> feature.getIndexDescriptors().stream())
            .flatMap(descriptor -> descriptor.getMatchingIndices(currentState.metadata()).stream())
            .map(indexName -> {
                assert currentState.metadata().hasIndex(indexName) : "index [" + indexName + "] not found in metadata but must be present";
                return currentState.metadata().getIndices().get(indexName).getIndex();
            })
            .collect(Collectors.toUnmodifiableSet());
    }

    // visible for testing
    static DataStream updateDataStream(DataStream dataStream, Metadata.Builder metadata, RestoreSnapshotRequest request) {
        String dataStreamName = dataStream.getName();
        if (request.renamePattern() != null && request.renameReplacement() != null) {
            dataStreamName = dataStreamName.replaceAll(request.renamePattern(), request.renameReplacement());
        }
        List<Index> updatedIndices = dataStream.getIndices()
            .stream()
            .map(i -> metadata.get(renameIndex(i.getName(), request, true, false)).getIndex())
            .toList();
        List<Index> updatedFailureIndices = DataStream.isFailureStoreFeatureFlagEnabled()
            ? dataStream.getFailureIndices()
                .getIndices()
                .stream()
                .map(i -> metadata.get(renameIndex(i.getName(), request, false, true)).getIndex())
                .toList()
            : List.of();
        return dataStream.copy()
            .setName(dataStreamName)
            .setBackingIndices(dataStream.getBackingIndices().copy().setIndices(updatedIndices).build())
            .setFailureIndices(dataStream.getFailureIndices().copy().setIndices(updatedFailureIndices).build())
            .build();
    }

    public static RestoreInProgress updateRestoreStateWithDeletedIndices(RestoreInProgress oldRestore, Set<Index> deletedIndices) {
        boolean changesMade = false;
        RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
        for (RestoreInProgress.Entry entry : oldRestore) {
            ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = null;
            for (Map.Entry<ShardId, ShardRestoreStatus> cursor : entry.shards().entrySet()) {
                ShardId shardId = cursor.getKey();
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
                builder.add(
                    new RestoreInProgress.Entry(
                        entry.uuid(),
                        entry.snapshot(),
                        overallState(RestoreInProgress.State.STARTED, shards),
                        entry.indices(),
                        shards
                    )
                );
            } else {
                builder.add(entry);
            }
        }
        if (changesMade) {
            return builder.build();
        } else {
            return oldRestore;
        }
    }

    public record RestoreCompletionResponse(String uuid, Snapshot snapshot, RestoreInfo restoreInfo) {}

    public static class RestoreInProgressUpdater implements RoutingChangesObserver {
        // Map of RestoreUUID to a of changes to the shards' restore statuses
        private final Map<String, Map<ShardId, ShardRestoreStatus>> shardChanges = new HashMap<>();

        @Override
        public void shardStarted(ShardRouting initializingShard, ShardRouting startedShard) {
            // mark snapshot as completed
            if (initializingShard.primary()) {
                RecoverySource recoverySource = initializingShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    changes(recoverySource).put(
                        initializingShard.shardId(),
                        new ShardRestoreStatus(initializingShard.currentNodeId(), RestoreInProgress.State.SUCCESS)
                    );
                }
            }
        }

        @Override
        public void shardFailed(ShardRouting failedShard, UnassignedInfo unassignedInfo) {
            if (failedShard.primary() && failedShard.initializing()) {
                RecoverySource recoverySource = failedShard.recoverySource();
                if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                    // mark restore entry for this shard as failed when it's due to a file corruption. There is no need wait on retries
                    // to restore this shard on another node if the snapshot files are corrupt. In case where a node just left or crashed,
                    // however, we only want to acknowledge the restore operation once it has been successfully restored on another node.
                    if (unassignedInfo.failure() != null && Lucene.isCorruptionException(unassignedInfo.failure().getCause())) {
                        changes(recoverySource).put(
                            failedShard.shardId(),
                            new ShardRestoreStatus(
                                failedShard.currentNodeId(),
                                RestoreInProgress.State.FAILURE,
                                unassignedInfo.failure().getCause().getMessage()
                            )
                        );
                    }
                }
            }
        }

        @Override
        public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
            // if we force an empty primary, we should also fail the restore entry
            if (unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT
                && initializedShard.recoverySource().getType() != RecoverySource.Type.SNAPSHOT) {
                changes(unassignedShard.recoverySource()).put(
                    unassignedShard.shardId(),
                    new ShardRestoreStatus(
                        null,
                        RestoreInProgress.State.FAILURE,
                        "recovery source type changed from snapshot to " + initializedShard.recoverySource()
                    )
                );
            }
        }

        @Override
        public void unassignedInfoUpdated(ShardRouting unassignedShard, UnassignedInfo newUnassignedInfo) {
            RecoverySource recoverySource = unassignedShard.recoverySource();
            if (recoverySource.getType() == RecoverySource.Type.SNAPSHOT) {
                if (newUnassignedInfo.lastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO) {
                    String reason = "shard could not be allocated to any of the nodes";
                    changes(recoverySource).put(
                        unassignedShard.shardId(),
                        new ShardRestoreStatus(unassignedShard.currentNodeId(), RestoreInProgress.State.FAILURE, reason)
                    );
                }
            }
        }

        /**
         * Helper method that creates update entry for the given recovery source's restore uuid
         * if such an entry does not exist yet.
         */
        private Map<ShardId, ShardRestoreStatus> changes(RecoverySource recoverySource) {
            assert recoverySource.getType() == RecoverySource.Type.SNAPSHOT;
            return shardChanges.computeIfAbsent(((SnapshotRecoverySource) recoverySource).restoreUUID(), k -> new HashMap<>());
        }

        public RestoreInProgress applyChanges(final RestoreInProgress oldRestore) {
            if (shardChanges.isEmpty() == false) {
                RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
                for (RestoreInProgress.Entry entry : oldRestore) {
                    Map<ShardId, ShardRestoreStatus> updates = shardChanges.get(entry.uuid());
                    Map<ShardId, ShardRestoreStatus> shardStates = entry.shards();
                    if (updates != null && updates.isEmpty() == false) {
                        Map<ShardId, ShardRestoreStatus> shardsBuilder = new HashMap<>(shardStates);
                        for (Map.Entry<ShardId, ShardRestoreStatus> shard : updates.entrySet()) {
                            ShardId shardId = shard.getKey();
                            ShardRestoreStatus status = shardStates.get(shardId);
                            if (status == null || status.state().completed() == false) {
                                shardsBuilder.put(shardId, shard.getValue());
                            }
                        }

                        Map<ShardId, ShardRestoreStatus> shards = Map.copyOf(shardsBuilder);
                        RestoreInProgress.State newState = overallState(RestoreInProgress.State.STARTED, shards);
                        builder.add(
                            new RestoreInProgress.Entry(entry.uuid(), entry.snapshot(), newState, entry.indices(), shards)
                        );
                    } else {
                        builder.add(entry);
                    }
                }
                return builder.build();
            } else {
                return oldRestore;
            }
        }
    }

    private static RestoreInProgress.State overallState(
        RestoreInProgress.State nonCompletedState,
        Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards
    ) {
        boolean hasFailed = false;
        for (RestoreInProgress.ShardRestoreStatus status : shards.values()) {
            if (status.state().completed() == false) {
                return nonCompletedState;
            }
            if (status.state() == RestoreInProgress.State.FAILURE) {
                hasFailed = true;
            }
        }
        if (hasFailed) {
            return RestoreInProgress.State.FAILURE;
        } else {
            return RestoreInProgress.State.SUCCESS;
        }
    }

    public static boolean completed(Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        for (RestoreInProgress.ShardRestoreStatus status : shards.values()) {
            if (status.state().completed() == false) {
                return false;
            }
        }
        return true;
    }

    public static int failedShards(Map<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        int failedShards = 0;
        for (RestoreInProgress.ShardRestoreStatus status : shards.values()) {
            if (status.state() == RestoreInProgress.State.FAILURE) {
                failedShards++;
            }
        }
        return failedShards;
    }

    private static String renameIndex(String index, RestoreSnapshotRequest request, boolean isBackingIndex, boolean isFailureStore) {
        if (request.renameReplacement() == null || request.renamePattern() == null) {
            return index;
        }
        String prefix = null;
        if (isBackingIndex && index.startsWith(DataStream.BACKING_INDEX_PREFIX)) {
            prefix = DataStream.BACKING_INDEX_PREFIX;
        }
        if (isFailureStore && index.startsWith(DataStream.FAILURE_STORE_PREFIX)) {
            prefix = DataStream.FAILURE_STORE_PREFIX;
        }
        String renamedIndex;
        if (prefix != null) {
            index = index.substring(prefix.length());
        }
        renamedIndex = index.replaceAll(request.renamePattern(), request.renameReplacement());
        if (prefix != null) {
            renamedIndex = prefix + renamedIndex;
        }
        return renamedIndex;
    }

    private static Map<String, IndexId> renamedIndices(
        RestoreSnapshotRequest request,
        List<String> filteredIndices,
        Set<String> dataStreamBackingIndices,
        Set<String> dataStreamFailureIndices,
        Set<String> featureIndices,
        RepositoryData repositoryData
    ) {
        Map<String, IndexId> renamedIndices = new HashMap<>();
        for (String index : filteredIndices) {
            String renamedIndex;
            if (featureIndices.contains(index)) {
                // Don't rename system indices
                renamedIndex = index;
            } else {
                renamedIndex = renameIndex(
                    index,
                    request,
                    dataStreamBackingIndices.contains(index),
                    dataStreamFailureIndices.contains(index)
                );
            }
            IndexId previousIndex = renamedIndices.put(renamedIndex, repositoryData.resolveIndexId(index));
            if (previousIndex != null) {
                throw new SnapshotRestoreException(
                    request.repository(),
                    request.snapshot(),
                    "indices [" + index + "] and [" + previousIndex.getName() + "] are renamed into the same index [" + renamedIndex + "]"
                );
            }
        }
        return Collections.unmodifiableMap(renamedIndices);
    }

    /**
     * Checks that snapshots can be restored and have compatible version
     * @param repository      repository name
     * @param snapshotInfo    snapshot metadata
     * @param preRestoreVersionChecks
     */
    static void validateSnapshotRestorable(
        RestoreSnapshotRequest request,
        RepositoryMetadata repository,
        SnapshotInfo snapshotInfo,
        List<BiConsumer<Snapshot, IndexVersion>> preRestoreVersionChecks
    ) {
        if (snapshotInfo.state().restorable() == false) {
            throw new SnapshotRestoreException(
                new Snapshot(repository.name(), snapshotInfo.snapshotId()),
                "unsupported snapshot state [" + snapshotInfo.state() + "]"
            );
        }
        if (Version.CURRENT.before(snapshotInfo.version())) {
            throw new SnapshotRestoreException(
                new Snapshot(repository.name(), snapshotInfo.snapshotId()),
                "the snapshot was created with version ["
                    + snapshotInfo.version()
                    + "] which is higher than the version of this node ["
                    + Version.CURRENT
                    + "]"
            );
        }

    }

    public static boolean failed(SnapshotInfo snapshot, String index) {
        for (SnapshotShardFailure failure : snapshot.shardFailures()) {
            if (index.equals(failure.index())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the indices that are currently being restored and that are contained in the indices-to-check set.
     */
    public static Set<Index> restoringIndices(final ClusterState currentState, final Set<Index> indicesToCheck) {
        final Set<Index> indices = new HashSet<>();
        for (RestoreInProgress.Entry entry : RestoreInProgress.get(currentState)) {
            for (Map.Entry<ShardId, RestoreInProgress.ShardRestoreStatus> shard : entry.shards().entrySet()) {
                Index index = shard.getKey().getIndex();
                if (indicesToCheck.contains(index)
                    && shard.getValue().state().completed() == false
                    && currentState.getMetadata().index(index) != null) {
                    indices.add(index);
                }
            }
        }
        return indices;
    }

    public static RestoreInProgress.Entry restoreInProgress(ClusterState state, String restoreUUID) {
        return RestoreInProgress.get(state).get(restoreUUID);
    }

    /**
     * Set to true if {@link #removeCompletedRestoresFromClusterState()} already has an in-flight state update running that will clean up
     * all completed restores from the cluster state.
     */
    private volatile boolean cleanupInProgress = false;

    // run a cluster state update that removes all completed restores from the cluster state
    private void removeCompletedRestoresFromClusterState() {
        submitUnbatchedTask("clean up snapshot restore status", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public void onFailure(String source, Exception e) {

            }

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return null;
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                RestoreInProgress.Builder restoreInProgressBuilder = new RestoreInProgress.Builder();
                boolean changed = false;
                for (RestoreInProgress.Entry entry : RestoreInProgress.get(currentState)) {
                    if (entry.state().completed()) {
                        logger.log(
                            Level.DEBUG,
                            "completed restore of snapshot [{}] with state [{}]",
                            entry.snapshot(),
                            entry.state()
                        );
                        changed = true;
                    } else {
                        restoreInProgressBuilder.add(entry);
                    }
                }
                return changed == false
                    ? currentState
                    : ClusterState.builder(currentState).putCustom(RestoreInProgress.TYPE, restoreInProgressBuilder.build()).build();
            }

            @Override
            public ClusterStateTaskExecutor.ClusterTasksResult<RestoreSnapshotStateTask.Task>
                execute(ClusterState currentState, List<RestoreSnapshotStateTask.Task> tasks) {
                return null;
            }

            @Override
            public void onFailure(final Exception e) {
                cleanupInProgress = false;
                logger.log(
                    MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.WARN,
                    "failed to remove completed restores from cluster state",
                    e
                );
            }

            @Override
            public void onFailure(String source, ProcessClusterEventTimeoutException e) {

            }

            @Override
            public void onFailure(String source, NotClusterManagerException e) {

            }

            @Override
            public void onFailure(String source, FailedToCommitClusterStateException t) {

            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                cleanupInProgress = false;
            }
        });
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster() && cleanupInProgress == false) {
                for (RestoreInProgress.Entry entry : RestoreInProgress.get(event.state())) {
                    if (entry.state().completed()) {
                        assert completed(entry.shards()) : "state says completed but restore entries are not";
                        removeCompletedRestoresFromClusterState();
                        cleanupInProgress = true;
                        // the above method cleans up all completed restores, no need to keep looping
                        break;
                    }
                }
            }
        } catch (Exception t) {
            assert false : t;
            logger.warn("Failed to update restore state ", t);
        }
    }

    /**
     * Optionally updates index settings in indexMetadata by removing settings listed in ignoreSettings and
     * merging them with settings in changeSettings.
     */
    private static IndexMetadata updateIndexSettings(
        Snapshot snapshot,
        IndexMetadata indexMetadata,
        Settings changeSettings,
        String[] ignoreSettings
    ) {
        final Settings settings = indexMetadata.getSettings();
        Settings normalizedChangeSettings = Settings.builder()
            .put(changeSettings)
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
            .build();
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(settings)
            && IndexSettings.INDEX_SOFT_DELETES_SETTING.exists(changeSettings)
            && IndexSettings.INDEX_SOFT_DELETES_SETTING.get(changeSettings) == false) {
            throw new SnapshotRestoreException(
                snapshot,
                "cannot disable setting [" + IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey() + "] on restore"
            );
        }
        if ("snapshot".equals(INDEX_STORE_TYPE_SETTING.get(settings))) {
            final Boolean changed = changeSettings.getAsBoolean(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, null);
            if (changed != null) {
                final Boolean previous = settings.getAsBoolean(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, null);
                if (Objects.equals(previous, changed) == false) {
                    throw new SnapshotRestoreException(
                        snapshot,
                        format(
                            "cannot change value of [%s] when restoring searchable snapshot [%s:%s] as index %s",
                            SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION,
                            snapshot.getRepository(),
                            snapshot.getSnapshotId().getName(),
                            indexMetadata.getIndex()
                        )
                    );
                }
            }
        }
        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
        Set<String> keyFilters = new HashSet<>();
        List<String> simpleMatchPatterns = new ArrayList<>();
        for (String ignoredSetting : ignoreSettings) {
            if (Regex.isSimpleMatchPattern(ignoredSetting) == false) {
                if (UNREMOVABLE_SETTINGS.contains(ignoredSetting)) {
                    throw new SnapshotRestoreException(snapshot, "cannot remove setting [" + ignoredSetting + "] on restore");
                } else {
                    keyFilters.add(ignoredSetting);
                }
            } else {
                simpleMatchPatterns.add(ignoredSetting);
            }
        }
        Settings.Builder settingsBuilder = Settings.builder().put(settings.filter(k -> {
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
        })).put(normalizedChangeSettings.filter(k -> {
            if (UNMODIFIABLE_SETTINGS.contains(k)) {
                throw new SnapshotRestoreException(snapshot, "cannot modify setting [" + k + "] on restore");
            } else {
                return true;
            }
        }));
        settingsBuilder.remove(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey());
        return builder.settings(settingsBuilder).build();
    }

    /**
     * Cluster state update task that is executed to start a restore operation.
     */
    private final class RestoreSnapshotStateTask extends ClusterStateUpdateTask {

        /**
         * UUID to use for this restore, as returned by {@link RestoreInProgress.Entry#uuid()}.
         */
        private final String restoreUUID = UUIDs.randomBase64UUID();

        /**
         * The restore request that triggered this restore task.
         */
        private final RestoreSnapshotRequest request;

        /**
         * Feature states to restore.
         */
        private final Set<String> featureStatesToRestore;

        /**
         * Map of index names to restore to the repository index id to restore them from.
         */
        private final Map<String, IndexId> indicesToRestore;

        private final Snapshot snapshot;

        /**
         * Snapshot info of the snapshot to restore
         */
        private final SnapshotInfo snapshotInfo;

        /**
         * Metadata loaded from the snapshot
         */
        private final Metadata metadata;

        private final Collection<DataStream> dataStreamsToRestore;

        private final BiConsumer<ClusterState, Metadata.Builder> updater;

        private final AllocationActionListener<RestoreCompletionResponse> listener;
        private final Settings settings;

        @Nullable
        private RestoreInfo restoreInfo;

        RestoreSnapshotStateTask(
            RestoreSnapshotRequest request,
            Snapshot snapshot,
            Set<String> featureStatesToRestore,
            Map<String, IndexId> indicesToRestore,
            SnapshotInfo snapshotInfo,
            Metadata metadata,
            Collection<DataStream> dataStreamsToRestore,
            BiConsumer<ClusterState, Metadata.Builder> updater,
            Settings settings,
            ActionListener<RestoreCompletionResponse> listener
        ) {
            super(request.masterNodeTimeout());
            this.request = request;
            this.snapshot = snapshot;
            this.featureStatesToRestore = featureStatesToRestore;
            this.indicesToRestore = indicesToRestore;
            this.snapshotInfo = snapshotInfo;
            this.metadata = metadata;
            this.dataStreamsToRestore = dataStreamsToRestore;
            this.updater = updater;
            this.settings = settings;
            this.listener = new AllocationActionListener<>(listener, threadPool.getThreadContext());
        }

        @Override
        public void onFailure(String source, Exception e) {

        }

        static class Task {
            final String uuid;

            Task(String uuid) {
                this.uuid = uuid;
            }

            @Override
            public String toString() {
                return "clean restore state for restore " + uuid;
            }
        }

        @Override
        public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
            return null;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            return null;
        }

        @Override
        public ClusterStateTaskExecutor.ClusterTasksResult<Task> execute(final ClusterState currentState, final List<Task> tasks) {
            final ClusterStateTaskExecutor.ClusterTasksResult.Builder<Task> resultBuilder =
                    ClusterStateTaskExecutor.ClusterTasksResult.<Task>builder().successes(tasks);
            Set<String> completedRestores = tasks.stream().map(e -> e.uuid).collect(Collectors.toSet());
            RestoreInProgress.Builder restoreInProgressBuilder = new RestoreInProgress.Builder();
            boolean changed = false;
            for (RestoreInProgress.Entry entry : currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
                if (completedRestores.contains(entry.uuid())) {
                    changed = true;
                } else {
                    restoreInProgressBuilder.add(entry);
                }
            }
            if (changed == false) {
                return resultBuilder.build(currentState);
            }
            final Map<String, ClusterState.Custom> builder = new HashMap<>(currentState.getCustoms());
            builder.put(RestoreInProgress.TYPE, restoreInProgressBuilder.build());
            final Map<String, ClusterState.Custom> customs = Collections.unmodifiableMap(builder);
            return resultBuilder.build(ClusterState.builder(currentState).customs(customs).build());
        }


        private void applyDataStreamRestores(ClusterState currentState, Metadata.Builder mdBuilder) {
            final Map<String, DataStream> updatedDataStreams = new HashMap<>(currentState.metadata().dataStreams());
            updatedDataStreams.putAll(
                dataStreamsToRestore.stream()
                    .map(ds -> updateDataStream(ds, mdBuilder, request))
                    .collect(Collectors.toMap(DataStream::getName, Function.identity()))
            );
            final Map<String, DataStreamAlias> updatedDataStreamAliases = new HashMap<>(currentState.metadata().dataStreamAliases());
            for (DataStreamAlias alias : metadata.dataStreamAliases().values()) {
                // Merge data stream alias from snapshot with an existing data stream aliases in target cluster:
                updatedDataStreamAliases.compute(
                    alias.getName(),
                    (key, previous) -> alias.restore(previous, request.renamePattern(), request.renameReplacement())
                );
            }
            mdBuilder.dataStreams(updatedDataStreams, updatedDataStreamAliases);
        }

        private void ensureSnapshotNotDeleted(ClusterState currentState) {
            SnapshotDeletionsInProgress deletionsInProgress = SnapshotDeletionsInProgress.get(currentState);
            if (deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.snapshots().contains(snapshot.getSnapshotId()))) {
                throw new ConcurrentSnapshotExecutionException(
                    snapshot,
                    "cannot restore a snapshot while a snapshot deletion is in-progress [" + deletionsInProgress.getEntries().get(0) + "]"
                );
            }
        }

        private void applyGlobalStateRestore(ClusterState currentState, Metadata.Builder mdBuilder) {
            if (metadata.persistentSettings() != null) {
                Settings settings = metadata.persistentSettings();
                if (request.skipOperatorOnlyState()) {
                    // Skip any operator-only settings from the snapshot. This happens when operator privileges are enabled
                    final Set<String> operatorSettingKeys = Stream.concat(
                        settings.keySet().stream(),
                        currentState.metadata().persistentSettings().keySet().stream()
                    ).filter(k -> {
                        final Setting<?> setting = clusterSettings.get(k);
                        return setting != null && setting.isOperatorOnly();
                    }).collect(Collectors.toSet());
                    if (false == operatorSettingKeys.isEmpty()) {
                        settings = Settings.builder()
                            .put(settings.filter(k -> false == operatorSettingKeys.contains(k)))
                            .put(currentState.metadata().persistentSettings().filter(operatorSettingKeys::contains))
                            .build();
                    }
                }
                clusterSettings.validateUpdate(settings);
                mdBuilder.persistentSettings(settings);
            }
            if (metadata.templates() != null) {
                // TODO: Should all existing templates be deleted first?
                for (IndexTemplateMetadata cursor : metadata.templates().values()) {
                    mdBuilder.put(cursor);
                }
            }

            // override existing restorable customs (as there might be nothing in snapshot to override them)
            mdBuilder.removeCustomIf((key, value) -> value.isRestorable());

            // restore customs from the snapshot
            if (metadata.customs() != null) {
                for (var entry : metadata.customs().entrySet()) {
                    if (entry.getValue().isRestorable()) {
                        // TODO: Check request.skipOperatorOnly for Autoscaling policies (NonRestorableCustom)
                        // Don't restore repositories while we are working with them
                        // TODO: Should we restore them at the end?
                        // Also, don't restore data streams here, we already added them to the metadata builder above
                        mdBuilder.putCustom(entry.getKey(), entry.getValue());
                    }
                }
            }
        }

        private void ensureNoAliasNameConflicts(IndexMetadata snapshotIndexMetadata) {
            for (String aliasName : snapshotIndexMetadata.getAliases().keySet()) {
                final IndexId indexId = indicesToRestore.get(aliasName);
                if (indexId != null) {
                    throw new SnapshotRestoreException(
                        snapshot,
                        "cannot rename index ["
                            + indexId
                            + "] into ["
                            + aliasName
                            + "] because of conflict with an alias with the same name"
                    );
                }
            }
        }

        private void populateIgnoredShards(String index, Set<Integer> ignoreShards) {
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

        private void validateExistingClosedIndex(
            IndexMetadata currentIndexMetadata,
            IndexMetadata snapshotIndexMetadata,
            String renamedIndex,
            boolean partial
        ) {
            // Index exist - checking that it's closed
            if (currentIndexMetadata.getState() != IndexMetadata.State.CLOSE) {
                // TODO: Enable restore for open indices
                throw new SnapshotRestoreException(
                    snapshot,
                    "cannot restore index ["
                        + renamedIndex
                        + "] because an open index "
                        + "with same name already exists in the cluster. Either close or delete the existing index or restore the "
                        + "index under a different name by providing a rename pattern and replacement name"
                );
            }
            // Index exist - checking if it's partial restore
            if (partial) {
                throw new SnapshotRestoreException(
                    snapshot,
                    "cannot restore partial index [" + renamedIndex + "] because such index already exists"
                );
            }
            // Make sure that the number of shards is the same. That's the only thing that we cannot change
            if (currentIndexMetadata.getNumberOfShards() != snapshotIndexMetadata.getNumberOfShards()) {
                throw new SnapshotRestoreException(
                    snapshot,
                    "cannot restore index ["
                        + renamedIndex
                        + "] with ["
                        + currentIndexMetadata.getNumberOfShards()
                        + "] shards from a snapshot of index ["
                        + snapshotIndexMetadata.getIndex().getName()
                        + "] with ["
                        + snapshotIndexMetadata.getNumberOfShards()
                        + "] shards"
                );
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(() -> "[" + snapshot + "] failed to restore snapshot", e);
            listener.clusterStateUpdate().onFailure(e);
        }

        @Override
        public void onFailure(String source, ProcessClusterEventTimeoutException e) {

        }

        @Override
        public void onFailure(String source, NotClusterManagerException e) {

        }

        @Override
        public void onFailure(String source, FailedToCommitClusterStateException t) {

        }

        @Override
        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
            logger.log(
                request.quiet() ? Level.DEBUG : Level.INFO,
                "started restore of snapshot [{}] for indices {}",
                snapshot,
                snapshotInfo.indices()
            );
            listener.clusterStateUpdate().onResponse(new RestoreCompletionResponse(restoreUUID, snapshot, restoreInfo));
        }
    }

    private static IndexMetadata convertLegacyIndex(
        IndexMetadata snapshotIndexMetadata,
        ClusterState clusterState,
        IndicesService indicesService
    ) {
        if (snapshotIndexMetadata.getCreationVersion().before(IndexVersion.fromId(5000099))) {
            throw new IllegalArgumentException("can't restore an index created before version 5.0.0");
        }
        IndexMetadata.Builder convertedIndexMetadataBuilder = IndexMetadata.builder(snapshotIndexMetadata);
        convertedIndexMetadataBuilder.settings(
            Settings.builder()
                .put(snapshotIndexMetadata.getSettings())
                .put(IndexMetadata.SETTING_INDEX_VERSION_COMPATIBILITY.getKey(), clusterState.getNodes().getMinSupportedIndexVersion())
                .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
        );
        snapshotIndexMetadata = convertedIndexMetadataBuilder.build();

        convertedIndexMetadataBuilder = IndexMetadata.builder(snapshotIndexMetadata);

        MappingMetadata mappingMetadata = snapshotIndexMetadata.mapping();
        if (mappingMetadata != null) {
            Map<String, Object> loadedMappingSource = mappingMetadata.rawSourceAsMap();

            // store old mapping under _meta/legacy_mappings
            Map<String, Object> legacyMapping = new LinkedHashMap<>();
            boolean sourceOnlySnapshot = snapshotIndexMetadata.getSettings().getAsBoolean("index.source_only", false);
            if (sourceOnlySnapshot) {
                // actual mapping is under "_meta" (but strip type first)
                Object sourceOnlyMeta = mappingMetadata.sourceAsMap().get("_meta");
                if (sourceOnlyMeta instanceof Map<?, ?> sourceOnlyMetaMap) {
                    legacyMapping.put("legacy_mappings", sourceOnlyMetaMap);
                }
            } else {
                legacyMapping.put("legacy_mappings", loadedMappingSource);
            }

            Map<String, Object> newMappingSource = new LinkedHashMap<>();

            // mappings keyed by type
            Map<String, Object> mergedMapping = new LinkedHashMap<>();
            // bring to single type by merging maps
            for (Map.Entry<String, Object> typeMapping : loadedMappingSource.entrySet()) {
                if (typeMapping.getValue() instanceof Map<?, ?>) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> mapping = ((Map<String, Object>) typeMapping.getValue());
                    if (mergedMapping.isEmpty()) {
                        mergedMapping.putAll(mapping);
                    } else {
                        XContentHelper.mergeDefaults(mergedMapping, mapping);
                    }
                }
            }

            // reorder top-level map so that _meta appears in right place
            // the order is type, dynamic, enabled, _meta, and then the rest
            if (mergedMapping.containsKey("type")) {
                newMappingSource.put("type", mergedMapping.remove("type"));
            }
            if (mergedMapping.containsKey("dynamic")) {
                newMappingSource.put("dynamic", mergedMapping.remove("dynamic"));
            }
            if (mergedMapping.containsKey("enabled")) {
                newMappingSource.put("enabled", mergedMapping.remove("enabled"));
            }

            // if existing mapping already has a _meta section, merge it with new _meta/legacy_mappings
            if (sourceOnlySnapshot == false && mergedMapping.containsKey("_meta") && mergedMapping.get("_meta") instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<String, Object> oldMeta = (Map<String, Object>) mergedMapping.remove("_meta");
                Map<String, Object> newMeta = new LinkedHashMap<>();
                newMeta.putAll(oldMeta);
                newMeta.putAll(legacyMapping);
                newMappingSource.put("_meta", newMeta);
            } else {
                newMappingSource.put("_meta", legacyMapping);
            }

            // now add the actual mapping
            if (sourceOnlySnapshot == false) {
                newMappingSource.putAll(mergedMapping);
            } else {
                // TODO: automatically add runtime field definitions for source-only snapshots
            }

            Map<String, Object> newMapping = new LinkedHashMap<>();
            newMapping.put(mappingMetadata.type(), newMappingSource);

            MappingMetadata updatedMappingMetadata = new MappingMetadata(mappingMetadata.type(), newMapping);
            convertedIndexMetadataBuilder.putMapping(updatedMappingMetadata);
            IndexMetadata convertedIndexMetadata = convertedIndexMetadataBuilder.build();

            try {
                Mapping mapping;
                try (MapperService mapperService = indicesService.createIndexMapperServiceForValidation(convertedIndexMetadata)) {
                    // create and validate in-memory mapping
                    mapperService.merge(convertedIndexMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
                    mapping = mapperService.documentMapper().mapping();
                }
                if (mapping != null) {
                    convertedIndexMetadataBuilder = IndexMetadata.builder(convertedIndexMetadata);
                    // using the recomputed mapping allows stripping some fields that we no longer support (e.g. include_in_all)
                    convertedIndexMetadataBuilder.putMapping(new MappingMetadata(mapping.toCompressedXContent()));
                    return convertedIndexMetadataBuilder.build();
                }
            } catch (Exception e) {
                final var metadata = snapshotIndexMetadata;
                logger.warn(() -> "could not import mappings for legacy index " + metadata.getIndex().getName(), e);
                // put mapping into _meta/legacy_mappings instead without adding anything else
                convertedIndexMetadataBuilder = IndexMetadata.builder(snapshotIndexMetadata);

                newMappingSource.clear();
                newMappingSource.put("_meta", legacyMapping);

                newMapping = new LinkedHashMap<>();
                newMapping.put(mappingMetadata.type(), newMappingSource);

                updatedMappingMetadata = new MappingMetadata(mappingMetadata.type(), newMapping);
                convertedIndexMetadataBuilder.putMapping(updatedMappingMetadata);
                throw new IllegalArgumentException(e);
            }
        }

        // TODO: _routing? Perhaps we don't need to obey any routing here as stuff is read-only anyway and get API will be disabled
        return convertedIndexMetadataBuilder.build();
    }

    private static IndexMetadata.Builder restoreToCreateNewIndex(IndexMetadata snapshotIndexMetadata, String renamedIndexName) {
        return IndexMetadata.builder(snapshotIndexMetadata)
            .state(IndexMetadata.State.OPEN)
            .index(renamedIndexName)
            .settings(
                Settings.builder().put(snapshotIndexMetadata.getSettings()).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            )
            .timestampRange(IndexLongFieldRange.NO_SHARDS);
    }

    private static IndexMetadata.Builder restoreOverClosedIndex(IndexMetadata snapshotIndexMetadata, IndexMetadata currentIndexMetadata) {
        final IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata)
            .state(IndexMetadata.State.OPEN)
            .version(Math.max(snapshotIndexMetadata.getVersion(), 1 + currentIndexMetadata.getVersion()))
            .mappingVersion(Math.max(snapshotIndexMetadata.getMappingVersion(), 1 + currentIndexMetadata.getMappingVersion()))
            .mappingsUpdatedVersion(snapshotIndexMetadata.getMappingsUpdatedVersion())
            .settingsVersion(Math.max(snapshotIndexMetadata.getSettingsVersion(), 1 + currentIndexMetadata.getSettingsVersion()))
            .aliasesVersion(Math.max(snapshotIndexMetadata.getAliasesVersion(), 1 + currentIndexMetadata.getAliasesVersion()))
            .timestampRange(IndexLongFieldRange.NO_SHARDS)
            .index(currentIndexMetadata.getIndex().getName())
            .settings(
                Settings.builder()
                    .put(snapshotIndexMetadata.getSettings())
                    .put(IndexMetadata.SETTING_INDEX_UUID, currentIndexMetadata.getIndexUUID())
                    .put(IndexMetadata.SETTING_HISTORY_UUID, UUIDs.randomBase64UUID())
            );
        for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
            indexMdBuilder.primaryTerm(shard, Math.max(snapshotIndexMetadata.primaryTerm(shard), currentIndexMetadata.primaryTerm(shard)));
        }
        return indexMdBuilder;
    }

    private void ensureValidIndexName(ClusterState currentState, IndexMetadata snapshotIndexMetadata, String renamedIndexName) {
        final boolean isHidden = snapshotIndexMetadata.isHidden();
        MetadataCreateIndexService.validateIndexName(renamedIndexName, currentState);
        createIndexService.validateDotIndex(renamedIndexName, isHidden);
        createIndexService.validateIndexSettings(renamedIndexName, snapshotIndexMetadata.getSettings(), false);
    }

    private static void ensureSearchableSnapshotsRestorable(
        final ClusterState currentState,
        final SnapshotInfo snapshotInfo,
        final Set<Index> indices
    ) {
        final Metadata metadata = currentState.metadata();
        for (Index index : indices) {
            final Settings indexSettings = metadata.getIndexSafe(index).getSettings();
            assert "snapshot".equals(INDEX_STORE_TYPE_SETTING.get(indexSettings)) : "not a snapshot backed index: " + index;

            final String repositoryUuid = indexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY);
            final String repositoryName = indexSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY);
            final String snapshotUuid = indexSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY);

            final boolean deleteSnapshot = indexSettings.getAsBoolean(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, false);
            if (deleteSnapshot && snapshotInfo.indices().size() != 1 && Objects.equals(snapshotUuid, snapshotInfo.snapshotId().getUUID())) {
                throw new SnapshotRestoreException(
                    repositoryName,
                    snapshotInfo.snapshotId().getName(),
                    format(
                        "cannot mount snapshot [%s/%s:%s] as index [%s] with the deletion of snapshot on index removal enabled "
                            + "[index.store.snapshot.delete_searchable_snapshot: true]; snapshot contains [%d] indices instead of 1.",
                        repositoryName,
                        repositoryUuid,
                        snapshotInfo.snapshotId().getName(),
                        index.getName(),
                        snapshotInfo.indices().size()
                    )
                );
            }

            for (IndexMetadata other : metadata) {
                if (other.getIndex().equals(index)) {
                    continue; // do not check the searchable snapshot index against itself
                }
                final Settings otherSettings = other.getSettings();
                if ("snapshot".equals(INDEX_STORE_TYPE_SETTING.get(otherSettings)) == false) {
                    continue; // other index is not a searchable snapshot index, skip
                }
                final String otherSnapshotUuid = otherSettings.get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_UUID_SETTING_KEY);
                if (Objects.equals(snapshotUuid, otherSnapshotUuid) == false) {
                    continue; // other index is backed by a different snapshot, skip
                }
                final String otherRepositoryUuid = otherSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_UUID_SETTING_KEY);
                final String otherRepositoryName = otherSettings.get(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY);
                if (matchRepository(repositoryUuid, repositoryName, otherRepositoryUuid, otherRepositoryName) == false) {
                    continue; // other index is backed by a snapshot from a different repository, skip
                }
                final boolean otherDeleteSnap = otherSettings.getAsBoolean(SEARCHABLE_SNAPSHOTS_DELETE_SNAPSHOT_ON_INDEX_DELETION, false);
                if (deleteSnapshot != otherDeleteSnap) {
                    throw new SnapshotRestoreException(
                        repositoryName,
                        snapshotInfo.snapshotId().getName(),
                        format(
                            "cannot mount snapshot [%s/%s:%s] as index [%s] with [index.store.snapshot.delete_searchable_snapshot: %b]; "
                                + "another index %s is mounted with [index.store.snapshot.delete_searchable_snapshot: %b].",
                            repositoryName,
                            repositoryUuid,
                            snapshotInfo.snapshotId().getName(),
                            index.getName(),
                            deleteSnapshot,
                            other.getIndex(),
                            otherDeleteSnap
                        )
                    );
                }
            }
        }
    }

    private static boolean matchRepository(
        String repositoryUuid,
        String repositoryName,
        String otherRepositoryUuid,
        String otherRepositoryName
    ) {
        if (Strings.hasLength(repositoryUuid) && Strings.hasLength(otherRepositoryUuid)) {
            return Objects.equals(repositoryUuid, otherRepositoryUuid);
        } else {
            return Objects.equals(repositoryName, otherRepositoryName);
        }
    }
}
