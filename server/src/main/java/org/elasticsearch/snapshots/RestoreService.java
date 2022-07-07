/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.RestoreInProgress.ShardRestoreStatus;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_HISTORY_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.snapshots.SnapshotUtils.filterIndices;
import static org.elasticsearch.snapshots.SnapshotsService.FEATURE_STATES_VERSION;
import static org.elasticsearch.snapshots.SnapshotsService.NO_FEATURE_STATES_VALUE;

/**
 * Service responsible for restoring snapshots
 * <p>
 * Restore operation is performed in several stages.
 * <p>
 * First {@link #restoreSnapshot(RestoreSnapshotRequest, org.elasticsearch.action.ActionListener)}
 * method reads information about snapshot and metadata from repository. In update cluster state task it checks restore
 * preconditions, restores global state if needed, creates {@link RestoreInProgress} record with list of shards that needs
 * to be restored and adds this shard to the routing table using
 * {@link RoutingTable.Builder#addAsRestore(IndexMetadata, SnapshotRecoverySource)} method.
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
public class RestoreService implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(RestoreService.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestoreService.class);

    public static final Setting<Boolean> REFRESH_REPO_UUID_ON_RESTORE_SETTING = Setting.boolSetting(
        "snapshot.refresh_repo_uuid_on_restore",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Set<String> UNMODIFIABLE_SETTINGS = unmodifiableSet(
        newHashSet(SETTING_NUMBER_OF_SHARDS, SETTING_VERSION_CREATED, SETTING_INDEX_UUID, SETTING_CREATION_DATE, SETTING_HISTORY_UUID)
    );

    // It's OK to change some settings, but we shouldn't allow simply removing them
    private static final Set<String> UNREMOVABLE_SETTINGS;

    static {
        Set<String> unremovable = new HashSet<>(UNMODIFIABLE_SETTINGS.size() + 4);
        unremovable.addAll(UNMODIFIABLE_SETTINGS);
        unremovable.add(SETTING_NUMBER_OF_REPLICAS);
        unremovable.add(SETTING_AUTO_EXPAND_REPLICAS);
        UNREMOVABLE_SETTINGS = unmodifiableSet(unremovable);
    }

    private final ClusterService clusterService;

    private final RepositoriesService repositoriesService;

    private final AllocationService allocationService;

    private final MetadataCreateIndexService createIndexService;

    private final IndexMetadataVerifier indexMetadataVerifier;

    private final MetadataDeleteIndexService metadataDeleteIndexService;

    private final ShardLimitValidator shardLimitValidator;

    private final ClusterSettings clusterSettings;

    private final SystemIndices systemIndices;

    private volatile boolean refreshRepositoryUuidOnRestore;

    public RestoreService(
        ClusterService clusterService,
        RepositoriesService repositoriesService,
        AllocationService allocationService,
        MetadataCreateIndexService createIndexService,
        MetadataDeleteIndexService metadataDeleteIndexService,
        IndexMetadataVerifier indexMetadataVerifier,
        ShardLimitValidator shardLimitValidator,
        SystemIndices systemIndices
    ) {
        this.clusterService = clusterService;
        this.repositoriesService = repositoriesService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.indexMetadataVerifier = indexMetadataVerifier;
        this.metadataDeleteIndexService = metadataDeleteIndexService;
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            clusterService.addStateApplier(this);
        }
        this.clusterSettings = clusterService.getClusterSettings();
        this.shardLimitValidator = shardLimitValidator;
        this.systemIndices = systemIndices;
        this.refreshRepositoryUuidOnRestore = REFRESH_REPO_UUID_ON_RESTORE_SETTING.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(REFRESH_REPO_UUID_ON_RESTORE_SETTING, this::setRefreshRepositoryUuidOnRestore);
    }

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
    public void restoreSnapshot(
        final RestoreSnapshotRequest request,
        final ActionListener<RestoreCompletionResponse> listener,
        final BiConsumer<ClusterState, Metadata.Builder> updater
    ) {
        try {

            // Try and fill in any missing repository UUIDs in case they're needed during the restore
            final StepListener<Void> repositoryUuidRefreshListener = new StepListener<>();
            refreshRepositoryUuids(refreshRepositoryUuidOnRestore, repositoriesService, repositoryUuidRefreshListener);

            // Read snapshot info and metadata from the repository
            final String repositoryName = request.repository();
            Repository repository = repositoriesService.repository(repositoryName);
            final StepListener<RepositoryData> repositoryDataListener = new StepListener<>();
            repository.getRepositoryData(repositoryDataListener);

            repositoryDataListener.whenComplete(repositoryData -> repositoryUuidRefreshListener.whenComplete(ignored -> {
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
                repository.getSnapshotInfo(
                    snapshotId,
                    ActionListener.wrap(
                        snapshotInfo -> startRestore(snapshotInfo, repository, request, repositoryData, updater, listener),
                        listener::onFailure
                    )
                );
            }, listener::onFailure), listener::onFailure);
        } catch (Exception e) {
            logger.warn(
                () -> new ParameterizedMessage("[{}] failed to restore snapshot", request.repository() + ":" + request.snapshot()),
                e
            );
            listener.onFailure(e);
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
        validateSnapshotRestorable(repositoryName, snapshotInfo);

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
                () -> new ParameterizedMessage(
                    "Restoring snapshot[{}] skipping feature [{}] because it is not available in this cluster",
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
            // include system data stream names in argument to this method
            Stream.concat(requestIndices.stream(), featureStateDataStreams.stream()).collect(Collectors.toList()),
            request.includeAliases()
        );
        Map<String, DataStream> dataStreamsToRestore = result.v1();
        Map<String, DataStreamAlias> dataStreamAliasesToRestore = result.v2();

        // Remove the data streams from the list of requested indices
        requestIndices.removeAll(dataStreamsToRestore.keySet());

        // And add the backing indices
        Set<String> dataStreamIndices = dataStreamsToRestore.values()
            .stream()
            .flatMap(ds -> ds.getIndices().stream())
            .map(Index::getName)
            .collect(Collectors.toSet());
        requestIndices.addAll(dataStreamIndices);

        // Resolve the indices that were directly requested
        final List<String> requestedIndicesInSnapshot = filterIndices(
            snapshotInfo.indices(),
            requestIndices.toArray(new String[] {}),
            request.indicesOptions()
        );

        // Combine into the final list of indices to be restored
        final List<String> requestedIndicesIncludingSystem = Stream.concat(
            requestedIndicesInSnapshot.stream(),
            featureStateIndices.stream()
        ).distinct().collect(Collectors.toList());

        final Set<String> explicitlyRequestedSystemIndices = new HashSet<>();
        for (IndexId indexId : repositoryData.resolveIndices(requestedIndicesIncludingSystem).values()) {
            IndexMetadata snapshotIndexMetaData = repository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId);
            if (snapshotIndexMetaData.isSystem()) {
                if (requestedIndicesInSnapshot.contains(indexId.getName())) {
                    explicitlyRequestedSystemIndices.add(indexId.getName());
                }
            }
            metadataBuilder.put(snapshotIndexMetaData, false);
        }

        // log a deprecation warning if the any of the indexes to delete were included in the request and the snapshot
        // is from a version that should have feature states
        if (snapshotInfo.version().onOrAfter(FEATURE_STATES_VERSION) && explicitlyRequestedSystemIndices.isEmpty() == false) {
            deprecationLogger.critical(
                DeprecationCategory.API,
                "restore-system-index-from-snapshot",
                "Restoring system indices by name is deprecated. Use feature states instead. System indices: "
                    + explicitlyRequestedSystemIndices
            );
        }

        // Now we can start the actual restore process by adding shards to be recovered in the cluster state
        // and updating cluster metadata (global and index) as needed
        clusterService.submitStateUpdateTask(
            "restore_snapshot[" + snapshotId.getName() + ']',
            new RestoreSnapshotStateTask(
                request,
                snapshot,
                featureStatesToRestore.keySet(),
                // Apply renaming on index names, returning a map of names where
                // the key is the renamed index and the value is the original name
                renamedIndices(request, requestedIndicesIncludingSystem, dataStreamIndices, featureStateIndices, repositoryData),
                snapshotInfo,
                metadataBuilder.dataStreams(dataStreamsToRestore, dataStreamAliasesToRestore).build(),
                dataStreamsToRestore.values(),
                updater,
                listener
            )
        );
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
     * @param refreshListener Listener that is completed when all repositories have been refreshed.
     */
    // Exposed for tests
    static void refreshRepositoryUuids(boolean enabled, RepositoriesService repositoriesService, ActionListener<Void> refreshListener) {

        if (enabled == false) {
            logger.debug("repository UUID refresh is disabled");
            refreshListener.onResponse(null);
            return;
        }

        // We only care about BlobStoreRepositories because they're the only ones that can contain a searchable snapshot, and we only care
        // about ones with missing UUIDs. It's possible to have the UUID change from under us if, e.g., the repository was wiped by an
        // external force, but in this case any searchable snapshots are lost anyway so it doesn't really matter.
        final List<Repository> repositories = repositoriesService.getRepositories()
            .values()
            .stream()
            .filter(
                repository -> repository instanceof BlobStoreRepository
                    && repository.getMetadata().uuid().equals(RepositoryData.MISSING_UUID)
            )
            .collect(Collectors.toList());
        if (repositories.isEmpty()) {
            logger.debug("repository UUID refresh is not required");
            refreshListener.onResponse(null);
            return;
        }

        logger.info(
            "refreshing repository UUIDs for repositories [{}]",
            repositories.stream().map(repository -> repository.getMetadata().name()).collect(Collectors.joining(","))
        );
        final ActionListener<RepositoryData> groupListener = new GroupedActionListener<>(new ActionListener<Collection<Void>>() {
            @Override
            public void onResponse(Collection<Void> ignored) {
                logger.debug("repository UUID refresh completed");
                refreshListener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("repository UUID refresh failed", e);
                refreshListener.onResponse(null); // this refresh is best-effort, the restore should proceed either way
            }
        }, repositories.size()).map(repositoryData -> null /* don't collect the RepositoryData */);

        for (Repository repository : repositories) {
            repository.getRepositoryData(groupListener);
        }

    }

    private boolean isSystemIndex(IndexMetadata indexMetadata) {
        return indexMetadata.isSystem() || systemIndices.isSystemName(indexMetadata.getIndex().getName());
    }

    private Tuple<Map<String, DataStream>, Map<String, DataStreamAlias>> getDataStreamsToRestore(
        Repository repository,
        SnapshotId snapshotId,
        SnapshotInfo snapshotInfo,
        Metadata globalMetadata,
        List<String> requestIndices,
        boolean includeAliases
    ) {
        Map<String, DataStream> dataStreams;
        Map<String, DataStreamAlias> dataStreamAliases;
        List<String> requestedDataStreams = filterIndices(
            snapshotInfo.dataStreams(),
            requestIndices.toArray(new String[] {}),
            IndicesOptions.fromOptions(true, true, true, true)
        );
        if (requestedDataStreams.isEmpty()) {
            dataStreams = Collections.emptyMap();
            dataStreamAliases = Collections.emptyMap();
        } else {
            if (globalMetadata == null) {
                globalMetadata = repository.getSnapshotGlobalMetadata(snapshotId);
            }
            final Map<String, DataStream> dataStreamsInSnapshot = globalMetadata.dataStreams();
            dataStreams = new HashMap<>(requestedDataStreams.size());
            for (String requestedDataStream : requestedDataStreams) {
                final DataStream dataStreamInSnapshot = dataStreamsInSnapshot.get(requestedDataStream);
                assert dataStreamInSnapshot != null : "DataStream [" + requestedDataStream + "] not found in snapshot";
                dataStreams.put(requestedDataStream, dataStreamInSnapshot);
            }
            if (includeAliases) {
                dataStreamAliases = new HashMap<>();
                final Map<String, DataStreamAlias> dataStreamAliasesInSnapshot = globalMetadata.dataStreamAliases();
                for (DataStreamAlias alias : dataStreamAliasesInSnapshot.values()) {
                    DataStreamAlias copy = alias.intersect(requestedDataStreams::contains);
                    if (copy.getDataStreams().isEmpty() == false) {
                        dataStreamAliases.put(alias.getName(), copy);
                    }
                }
            } else {
                dataStreamAliases = Collections.emptyMap();
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
            final Set<String> requestedStates = org.elasticsearch.core.Set.of(requestedFeatureStates);
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
            .collect(Collectors.toList());
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

        return Collections.unmodifiableSet(
            featureStatesToRestore.stream()
                .map(systemIndices::getFeature)
                .filter(Objects::nonNull) // Features that aren't present on this node will be warned about in `getFeatureStatesToRestore`
                .flatMap(feature -> feature.getIndexDescriptors().stream())
                .flatMap(descriptor -> descriptor.getMatchingIndices(currentState.metadata()).stream())
                .map(indexName -> {
                    assert currentState.metadata().hasIndex(indexName)
                        : "index [" + indexName + "] not found in metadata but must be present";
                    return currentState.metadata().getIndices().get(indexName).getIndex();
                })
                .collect(Collectors.toSet())
        );
    }

    // visible for testing
    static DataStream updateDataStream(DataStream dataStream, Metadata.Builder metadata, RestoreSnapshotRequest request) {
        String dataStreamName = dataStream.getName();
        if (request.renamePattern() != null && request.renameReplacement() != null) {
            dataStreamName = dataStreamName.replaceAll(request.renamePattern(), request.renameReplacement());
        }
        List<Index> updatedIndices = dataStream.getIndices()
            .stream()
            .map(i -> metadata.get(renameIndex(i.getName(), request, true)).getIndex())
            .collect(Collectors.toList());
        return new DataStream(
            dataStreamName,
            dataStream.getTimeStampField(),
            updatedIndices,
            dataStream.getGeneration(),
            dataStream.getMetadata(),
            dataStream.isHidden(),
            dataStream.isReplicated(),
            dataStream.isSystem()
        );
    }

    public static RestoreInProgress updateRestoreStateWithDeletedIndices(RestoreInProgress oldRestore, Set<Index> deletedIndices) {
        boolean changesMade = false;
        RestoreInProgress.Builder builder = new RestoreInProgress.Builder();
        for (RestoreInProgress.Entry entry : oldRestore) {
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

    public static final class RestoreCompletionResponse {
        private final String uuid;
        private final Snapshot snapshot;
        private final RestoreInfo restoreInfo;

        private RestoreCompletionResponse(final String uuid, final Snapshot snapshot, final RestoreInfo restoreInfo) {
            this.uuid = uuid;
            this.snapshot = snapshot;
            this.restoreInfo = restoreInfo;
        }

        public String getUuid() {
            return uuid;
        }

        public Snapshot getSnapshot() {
            return snapshot;
        }

        public RestoreInfo getRestoreInfo() {
            return restoreInfo;
        }
    }

    public static class RestoreInProgressUpdater extends RoutingChangesObserver.AbstractRoutingChangesObserver {
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
                    if (unassignedInfo.getFailure() != null && Lucene.isCorruptionException(unassignedInfo.getFailure().getCause())) {
                        changes(recoverySource).put(
                            failedShard.shardId(),
                            new ShardRestoreStatus(
                                failedShard.currentNodeId(),
                                RestoreInProgress.State.FAILURE,
                                unassignedInfo.getFailure().getCause().getMessage()
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
                if (newUnassignedInfo.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO) {
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
                    ImmutableOpenMap<ShardId, ShardRestoreStatus> shardStates = entry.shards();
                    if (updates != null && updates.isEmpty() == false) {
                        ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder(shardStates);
                        for (Map.Entry<ShardId, ShardRestoreStatus> shard : updates.entrySet()) {
                            ShardId shardId = shard.getKey();
                            ShardRestoreStatus status = shardStates.get(shardId);
                            if (status == null || status.state().completed() == false) {
                                shardsBuilder.put(shardId, shard.getValue());
                            }
                        }

                        ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
                        RestoreInProgress.State newState = overallState(RestoreInProgress.State.STARTED, shards);
                        builder.add(new RestoreInProgress.Entry(entry.uuid(), entry.snapshot(), newState, entry.indices(), shards));
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
        ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards
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

    public static boolean completed(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        for (RestoreInProgress.ShardRestoreStatus status : shards.values()) {
            if (status.state().completed() == false) {
                return false;
            }
        }
        return true;
    }

    public static int failedShards(ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards) {
        int failedShards = 0;
        for (RestoreInProgress.ShardRestoreStatus status : shards.values()) {
            if (status.state() == RestoreInProgress.State.FAILURE) {
                failedShards++;
            }
        }
        return failedShards;
    }

    private static Map<String, IndexId> renamedIndices(
        RestoreSnapshotRequest request,
        List<String> filteredIndices,
        Set<String> dataStreamIndices,
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
                renamedIndex = renameIndex(index, request, dataStreamIndices.contains(index));
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

    /**
     * Checks that snapshots can be restored and have compatible version
     *
     * @param repository      repository name
     * @param snapshotInfo    snapshot metadata
     */
    private static void validateSnapshotRestorable(final String repository, final SnapshotInfo snapshotInfo) {
        if (snapshotInfo.state().restorable() == false) {
            throw new SnapshotRestoreException(
                new Snapshot(repository, snapshotInfo.snapshotId()),
                "unsupported snapshot state [" + snapshotInfo.state() + "]"
            );
        }
        if (Version.CURRENT.before(snapshotInfo.version())) {
            throw new SnapshotRestoreException(
                new Snapshot(repository, snapshotInfo.snapshotId()),
                "the snapshot was created with Elasticsearch version ["
                    + snapshotInfo.version()
                    + "] which is higher than the version of this node ["
                    + Version.CURRENT
                    + "]"
            );
        }
        if (snapshotInfo.version().before(Version.CURRENT.minimumIndexCompatibilityVersion())) {
            throw new SnapshotRestoreException(
                new Snapshot(repository, snapshotInfo.snapshotId()),
                "the snapshot was created with Elasticsearch version ["
                    + snapshotInfo.version()
                    + "] which is below the current versions minimum index compatibility version ["
                    + Version.CURRENT.minimumIndexCompatibilityVersion()
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
        for (RestoreInProgress.Entry entry : currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            for (ObjectObjectCursor<ShardId, RestoreInProgress.ShardRestoreStatus> shard : entry.shards()) {
                Index index = shard.key.getIndex();
                if (indicesToCheck.contains(index)
                    && shard.value.state().completed() == false
                    && currentState.getMetadata().index(index) != null) {
                    indices.add(index);
                }
            }
        }
        return indices;
    }

    public static RestoreInProgress.Entry restoreInProgress(ClusterState state, String restoreUUID) {
        return state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).get(restoreUUID);
    }

    /**
     * Set to true if {@link #removeCompletedRestoresFromClusterState()} already has an in-flight state update running that will clean up
     * all completed restores from the cluster state.
     */
    private volatile boolean cleanupInProgress = false;

    // run a cluster state update that removes all completed restores from the cluster state
    private void removeCompletedRestoresFromClusterState() {
        clusterService.submitStateUpdateTask("clean up snapshot restore status", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                RestoreInProgress.Builder restoreInProgressBuilder = new RestoreInProgress.Builder();
                boolean changed = false;
                for (RestoreInProgress.Entry entry : currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
                    if (entry.state().completed()) {
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
            public void onFailure(final String source, final Exception e) {
                cleanupInProgress = false;
                logger.warn(() -> new ParameterizedMessage("failed to remove completed restores from cluster state"), e);
            }

            @Override
            public void onNoLongerMaster(String source) {
                cleanupInProgress = false;
                logger.debug("no longer master while removing completed restores from cluster state");
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                cleanupInProgress = false;
            }
        });
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster() && cleanupInProgress == false) {
                for (RestoreInProgress.Entry entry : event.state().custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
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
        Settings normalizedChangeSettings = Settings.builder()
            .put(changeSettings)
            .normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX)
            .build();
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(indexMetadata.getSettings())
            && IndexSettings.INDEX_SOFT_DELETES_SETTING.exists(changeSettings)
            && IndexSettings.INDEX_SOFT_DELETES_SETTING.get(changeSettings) == false) {
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

        private final ActionListener<RestoreCompletionResponse> listener;

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
            this.listener = listener;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
            if (currentState.getNodes().getMinNodeVersion().before(Version.V_7_0_0)) {
                // Check if another restore process is already running - cannot run two restore processes at the
                // same time in versions prior to 7.0
                if (restoreInProgress.isEmpty() == false) {
                    throw new ConcurrentSnapshotExecutionException(snapshot, "Restore process is already running in this cluster");
                }
            }

            // Check if the snapshot to restore is currently being deleted
            ensureSnapshotNotDeleted(currentState);

            // Clear out all existing indices which fall within a system index pattern being restored
            currentState = metadataDeleteIndexService.deleteIndices(
                currentState,
                resolveSystemIndicesToDelete(currentState, featureStatesToRestore)
            );

            // Updating cluster state
            final Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
            final ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
            final RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());

            final ImmutableOpenMap.Builder<ShardId, ShardRestoreStatus> shardsBuilder = ImmutableOpenMap.builder();

            final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion().minimumIndexCompatibilityVersion();
            final String localNodeId = clusterService.state().nodes().getLocalNodeId();
            for (Map.Entry<String, IndexId> indexEntry : indicesToRestore.entrySet()) {
                final IndexId index = indexEntry.getValue();
                IndexMetadata snapshotIndexMetadata = updateIndexSettings(
                    snapshot,
                    metadata.index(index.getName()),
                    request.indexSettings(),
                    request.ignoreIndexSettings()
                );
                try {
                    snapshotIndexMetadata = indexMetadataVerifier.verifyIndexMetadata(snapshotIndexMetadata, minIndexCompatibilityVersion);
                } catch (Exception ex) {
                    throw new SnapshotRestoreException(snapshot, "cannot restore index [" + index + "] because it cannot be upgraded", ex);
                }
                final String renamedIndexName = indexEntry.getKey();
                final IndexMetadata currentIndexMetadata = currentState.metadata().index(renamedIndexName);
                final SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(
                    restoreUUID,
                    snapshot,
                    snapshotInfo.version(),
                    index
                );
                final boolean partial = checkPartial(index.getName());
                final IntSet ignoreShards = new IntHashSet();
                final IndexMetadata updatedIndexMetadata;

                // different paths depending on whether we are restoring to create a new index or restoring over an existing closed index
                // that will be opened by the restore
                if (currentIndexMetadata == null) {
                    // Index doesn't exist - create it and start recovery
                    // Make sure that the index we are about to create has a validate name
                    ensureValidIndexName(currentState, snapshotIndexMetadata, renamedIndexName);
                    shardLimitValidator.validateShardLimit(snapshotIndexMetadata.getSettings(), currentState);

                    final IndexMetadata.Builder indexMdBuilder = restoreToCreateNewIndex(snapshotIndexMetadata, renamedIndexName);
                    if (request.includeAliases() == false
                        && snapshotIndexMetadata.getAliases().isEmpty() == false
                        && isSystemIndex(snapshotIndexMetadata) == false) {
                        // Remove all aliases - they shouldn't be restored
                        indexMdBuilder.removeAllAliases();
                    } else {
                        ensureNoAliasNameConflicts(snapshotIndexMetadata);
                    }
                    updatedIndexMetadata = indexMdBuilder.build();
                    if (partial) {
                        populateIgnoredShards(index.getName(), ignoreShards);
                    }
                    rtBuilder.addAsNewRestore(updatedIndexMetadata, recoverySource, ignoreShards);
                    blocks.addBlocks(updatedIndexMetadata);
                } else {
                    // Index exists and it's closed - open it in metadata and start recovery
                    validateExistingClosedIndex(currentIndexMetadata, snapshotIndexMetadata, renamedIndexName, partial);
                    final IndexMetadata.Builder indexMdBuilder = restoreOverClosedIndex(
                        snapshotIndexMetadata,
                        currentIndexMetadata,
                        currentState
                    );

                    if (request.includeAliases() == false && isSystemIndex(snapshotIndexMetadata) == false) {
                        // Remove all snapshot aliases
                        if (snapshotIndexMetadata.getAliases().isEmpty() == false) {
                            indexMdBuilder.removeAllAliases();
                        }
                        // Add existing aliases
                        for (AliasMetadata alias : currentIndexMetadata.getAliases().values()) {
                            indexMdBuilder.putAlias(alias);
                        }
                    } else {
                        ensureNoAliasNameConflicts(snapshotIndexMetadata);
                    }
                    updatedIndexMetadata = indexMdBuilder.build();
                    rtBuilder.addAsRestore(updatedIndexMetadata, recoverySource);
                    blocks.updateBlocks(updatedIndexMetadata);
                }

                mdBuilder.put(updatedIndexMetadata, true);
                final Index renamedIndex = updatedIndexMetadata.getIndex();
                for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
                    shardsBuilder.put(
                        new ShardId(renamedIndex, shard),
                        ignoreShards.contains(shard)
                            ? new ShardRestoreStatus(localNodeId, RestoreInProgress.State.FAILURE)
                            : new ShardRestoreStatus(localNodeId)
                    );
                }
            }

            final ClusterState.Builder builder = ClusterState.builder(currentState);
            final ImmutableOpenMap<ShardId, ShardRestoreStatus> shards = shardsBuilder.build();
            if (shards.isEmpty() == false) {
                builder.putCustom(
                    RestoreInProgress.TYPE,
                    new RestoreInProgress.Builder(currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).add(
                        new RestoreInProgress.Entry(
                            restoreUUID,
                            snapshot,
                            overallState(RestoreInProgress.State.INIT, shards),
                            org.elasticsearch.core.List.copyOf(indicesToRestore.keySet()),
                            shards
                        )
                    ).build()
                );
            }

            applyDataStreamRestores(currentState, mdBuilder);

            // Restore global state if needed
            if (request.includeGlobalState()) {
                applyGlobalStateRestore(currentState, mdBuilder);
            }

            if (completed(shards)) {
                // We don't have any indices to restore - we are done
                restoreInfo = new RestoreInfo(
                    snapshot.getSnapshotId().getName(),
                    org.elasticsearch.core.List.copyOf(indicesToRestore.keySet()),
                    shards.size(),
                    shards.size() - failedShards(shards)
                );
            }

            updater.accept(currentState, mdBuilder);
            return allocationService.reroute(
                builder.metadata(mdBuilder).blocks(blocks).routingTable(rtBuilder.build()).build(),
                "restored snapshot [" + snapshot + "]"
            );
        }

        private void applyDataStreamRestores(ClusterState currentState, Metadata.Builder mdBuilder) {
            final Map<String, DataStream> updatedDataStreams = new HashMap<>(currentState.metadata().dataStreams());
            updatedDataStreams.putAll(
                dataStreamsToRestore.stream()
                    .map(ds -> updateDataStream(ds, mdBuilder, request))
                    .collect(Collectors.toMap(DataStream::getName, Function.identity()))
            );
            final Map<String, DataStreamAlias> updatedDataStreamAliases = new HashMap<>(currentState.metadata().dataStreamAliases());
            metadata.dataStreamAliases()
                .values()
                .stream()
                // Optionally rename the data stream names for each alias
                .map(alias -> {
                    if (request.renamePattern() != null && request.renameReplacement() != null) {
                        return alias.renameDataStreams(request.renamePattern(), request.renameReplacement());
                    } else {
                        return alias;
                    }
                })
                .forEach(alias -> {
                    final DataStreamAlias current = updatedDataStreamAliases.putIfAbsent(alias.getName(), alias);
                    if (current != null) {
                        // Merge data stream alias from snapshot with an existing data stream aliases in target cluster:
                        DataStreamAlias newInstance = alias.merge(current);
                        updatedDataStreamAliases.put(alias.getName(), newInstance);
                    }
                });
            mdBuilder.dataStreams(updatedDataStreams, updatedDataStreamAliases);
        }

        private void ensureSnapshotNotDeleted(ClusterState currentState) {
            SnapshotDeletionsInProgress deletionsInProgress = currentState.custom(
                SnapshotDeletionsInProgress.TYPE,
                SnapshotDeletionsInProgress.EMPTY
            );
            if (deletionsInProgress.getEntries().stream().anyMatch(entry -> entry.getSnapshots().contains(snapshot.getSnapshotId()))) {
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
            if (metadata.customs() != null) {
                for (ObjectObjectCursor<String, Metadata.Custom> cursor : metadata.customs()) {
                    if (RepositoriesMetadata.TYPE.equals(cursor.key) == false
                        && DataStreamMetadata.TYPE.equals(cursor.key) == false
                        && cursor.value instanceof Metadata.NonRestorableCustom == false) {
                        // TODO: Check request.skipOperatorOnly for Autoscaling policies (NonRestorableCustom)
                        // Don't restore repositories while we are working with them
                        // TODO: Should we restore them at the end?
                        // Also, don't restore data streams here, we already added them to the metadata builder above
                        mdBuilder.putCustom(cursor.key, cursor.value);
                    }
                }
            }
        }

        private void ensureNoAliasNameConflicts(IndexMetadata snapshotIndexMetadata) {
            for (ObjectCursor<String> alias : snapshotIndexMetadata.getAliases().keys()) {
                final String aliasName = alias.value;
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
        public void onFailure(String source, Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to restore snapshot", snapshot), e);
            listener.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            listener.onResponse(new RestoreCompletionResponse(restoreUUID, snapshot, restoreInfo));
        }
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

    private static IndexMetadata.Builder restoreOverClosedIndex(
        IndexMetadata snapshotIndexMetadata,
        IndexMetadata currentIndexMetadata,
        ClusterState currentState
    ) {
        final Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(snapshotIndexMetadata.getSettings())
            .put(IndexMetadata.SETTING_INDEX_UUID, currentIndexMetadata.getIndexUUID());
        // Only add a restore uuid if either all nodes in the cluster support it (version >= 7.9) or if the
        // index itself was created after 7.9 and thus won't be restored to a node that doesn't support the
        // setting anyway
        if (snapshotIndexMetadata.getCreationVersion().onOrAfter(Version.V_7_9_0)
            || currentState.nodes().getMinNodeVersion().onOrAfter(Version.V_7_9_0)) {
            indexSettingsBuilder.put(SETTING_HISTORY_UUID, UUIDs.randomBase64UUID());
        }
        final IndexMetadata.Builder indexMdBuilder = IndexMetadata.builder(snapshotIndexMetadata)
            .state(IndexMetadata.State.OPEN)
            .version(Math.max(snapshotIndexMetadata.getVersion(), 1 + currentIndexMetadata.getVersion()))
            .mappingVersion(Math.max(snapshotIndexMetadata.getMappingVersion(), 1 + currentIndexMetadata.getMappingVersion()))
            .settingsVersion(Math.max(snapshotIndexMetadata.getSettingsVersion(), 1 + currentIndexMetadata.getSettingsVersion()))
            .aliasesVersion(Math.max(snapshotIndexMetadata.getAliasesVersion(), 1 + currentIndexMetadata.getAliasesVersion()))
            .timestampRange(IndexLongFieldRange.NO_SHARDS)
            .index(currentIndexMetadata.getIndex().getName())
            .settings(indexSettingsBuilder);
        for (int shard = 0; shard < snapshotIndexMetadata.getNumberOfShards(); shard++) {
            indexMdBuilder.primaryTerm(shard, Math.max(snapshotIndexMetadata.primaryTerm(shard), currentIndexMetadata.primaryTerm(shard)));
        }
        return indexMdBuilder;
    }

    private void ensureValidIndexName(ClusterState currentState, IndexMetadata snapshotIndexMetadata, String renamedIndexName) {
        final boolean isHidden = snapshotIndexMetadata.isHidden();
        createIndexService.validateIndexName(renamedIndexName, currentState);
        createIndexService.validateDotIndex(renamedIndexName, isHidden);
        createIndexService.validateIndexSettings(renamedIndexName, snapshotIndexMetadata.getSettings(), false);
    }
}
