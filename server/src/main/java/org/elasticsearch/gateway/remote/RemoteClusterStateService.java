/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.gateway.remote;


import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.remote.RemoteRoutingTableService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlags;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.translog.transfer.BlobStoreTransferService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.remotestore.RemoteStoreNodeAttribute;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL;
import static org.elasticsearch.gateway.PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD;

/**
 * A Service which provides APIs to upload and download cluster metadata from remote store.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateService implements Closeable {

    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    /**
     * Used to specify if cluster state metadata should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    public static final TimeValue REMOTE_STATE_READ_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> REMOTE_STATE_READ_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.read_timeout",
        REMOTE_STATE_READ_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private TimeValue remoteStateReadTimeout;
    private final String nodeId;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final LongSupplier relativeTimeNanosSupplier;
    private final ThreadPool threadpool;
    private final List<IndexMetadataUploadListener> indexMetadataUploadListeners;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private final RemoteRoutingTableService remoteRoutingTableService;
    private volatile TimeValue slowWriteLoggingThreshold;

    private final RemotePersistenceStats remoteStateStats;
    private RemoteClusterStateCleanupManager remoteClusterStateCleanupManager;
    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private RemoteGlobalMetadataManager remoteGlobalMetadataManager;
    private RemoteClusterStateAttributesManager remoteClusterStateAttributesManager;
    private RemoteManifestManager remoteManifestManager;
    private ClusterSettings clusterSettings;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final String CLUSTER_STATE_UPLOAD_TIME_LOG_STRING = "writing cluster state for version [{}] took [{}ms]";
    private final String METADATA_UPDATE_LOG_STRING = "wrote metadata for [{}] indices and skipped [{}] unchanged "
        + "indices, coordination metadata updated : [{}], settings metadata updated : [{}], templates metadata "
        + "updated : [{}], custom metadata updated : [{}], indices routing updated : [{}]";
    private final boolean isPublicationEnabled;

    // ToXContent Params with gateway mode.
    // We are using gateway context mode to persist all custom metadata.
    public static final ToXContent.Params FORMAT_PARAMS;

    static {
        Map<String, String> params = new HashMap<>(1);
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new ToXContent.MapParams(params);
    }

    public RemoteClusterStateService(
        String nodeId,
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterService clusterService,
        LongSupplier relativeTimeNanosSupplier,
        ThreadPool threadPool,
        List<IndexMetadataUploadListener> indexMetadataUploadListeners,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        assert isRemoteStoreClusterStateEnabled(settings) : "Remote cluster state is not enabled";
        this.nodeId = nodeId;
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.relativeTimeNanosSupplier = relativeTimeNanosSupplier;
        this.threadpool = threadPool;
        clusterSettings = clusterService.getClusterSettings();
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
        this.remoteStateReadTimeout = clusterSettings.get(REMOTE_STATE_READ_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_STATE_READ_TIMEOUT_SETTING, this::setRemoteStateReadTimeout);
        this.remoteStateStats = new RemotePersistenceStats();
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.indexMetadataUploadListeners = indexMetadataUploadListeners;
        this.remoteRoutingTableService = RemoteRoutingTableServiceFactory.getService(
            repositoriesService,
            settings,
            clusterSettings,
            threadPool
        );
        this.remoteClusterStateCleanupManager = new RemoteClusterStateCleanupManager(this, clusterService, remoteRoutingTableService);
        this.isPublicationEnabled = FeatureFlags.isEnabled(REMOTE_PUBLICATION_EXPERIMENTAL)
            && RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled(settings)
            && RemoteStoreNodeAttribute.isRemoteRoutingTableEnabled(settings);
    }

    /**
     * This method uploads entire cluster state metadata to the configured blob store. For now only index metadata upload is supported. This method should be
     * invoked by the elected cluster manager when the remote cluster state is enabled.
     *
     * @return A manifest object which contains the details of uploaded entity metadata.
     */
    @Nullable
    public RemoteClusterStateManifestInfo writeFullMetadata(ClusterState clusterState, String previousClusterUUID) throws IOException {
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }

        UploadedMetadataResults uploadedMetadataResults = writeMetadataInParallel(
            clusterState,
            new ArrayList<>(clusterState.metadata().indices().values()),
            emptyMap(),
            RemoteGlobalMetadataManager.filterCustoms(clusterState.metadata().customs(), isPublicationEnabled),
            true,
            true,
            true,
            isPublicationEnabled,
            isPublicationEnabled,
            isPublicationEnabled,
            isPublicationEnabled ? clusterState.customs() : Collections.emptyMap(),
            isPublicationEnabled,
            remoteRoutingTableService.getIndicesRouting(clusterState.getRoutingTable())
        );
        final RemoteClusterStateManifestInfo manifestDetails = remoteManifestManager.uploadManifest(
            clusterState,
            uploadedMetadataResults,
            previousClusterUUID,
            new ClusterStateDiffManifest(clusterState, ClusterState.EMPTY_STATE),
            false
        );

        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateSucceeded();
        remoteStateStats.stateTook(durationMillis);
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; "
                    + "wrote full state with [{}] indices and [{}] indicesRouting",
                durationMillis,
                slowWriteLoggingThreshold,
                uploadedMetadataResults.uploadedIndexMetadata.size(),
                uploadedMetadataResults.uploadedIndicesRoutingMetadata.size()
            );
        } else {
            logger.info(
                "writing cluster state took [{}ms]; " + "wrote full state with [{}] indices, [{}] indicesRouting and global metadata",
                durationMillis,
                uploadedMetadataResults.uploadedIndexMetadata.size(),
                uploadedMetadataResults.uploadedIndicesRoutingMetadata.size()
            );
        }
        return manifestDetails;
    }

    /**
     * This method uploads the diff between the previous cluster state and the current cluster state. The previous manifest file is needed to create the new
     * manifest. The new manifest file is created by using the unchanged metadata from the previous manifest and the new metadata changes from the current
     * cluster state.
     *
     * @return {@link RemoteClusterStateManifestInfo} object containing uploaded manifest detail
     */
    @SuppressWarnings("checkstyle:DescendantToken")
    @Nullable
    public RemoteClusterStateManifestInfo writeIncrementalMetadata(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataManifest previousManifest
    ) throws IOException {
        logger.trace("WRITING INCREMENTAL STATE");

        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousClusterState.metadata().coordinationMetadata().term() == clusterState.metadata().coordinationMetadata().term();

        boolean firstUploadForSplitGlobalMetadata = !previousManifest.hasMetadataAttributesFiles();

        final DiffableUtils.MapDiff<String, Metadata.Custom, Map<String, Metadata.Custom>> customsDiff = remoteGlobalMetadataManager
            .getCustomsDiff(clusterState, previousClusterState, firstUploadForSplitGlobalMetadata, isPublicationEnabled);
        final DiffableUtils.MapDiff<String, ClusterState.Custom, Map<String, ClusterState.Custom>> clusterStateCustomsDiff =
            remoteClusterStateAttributesManager.getUpdatedCustoms(clusterState, previousClusterState, isPublicationEnabled, false);
        final Map<String, UploadedMetadataAttribute> allUploadedCustomMap = new HashMap<>(previousManifest.getCustomMetadataMap());
        final Map<String, UploadedMetadataAttribute> allUploadedClusterStateCustomsMap = new HashMap<>(
            previousManifest.getClusterStateCustomMap()
        );
        final Map<String, IndexMetadata> indicesToBeDeletedFromRemote = new HashMap<>(previousClusterState.metadata().indices());
        int numIndicesUpdated = 0;
        int numIndicesUnchanged = 0;
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndexMetadata = previousManifest.getIndices()
            .stream()
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));

        List<IndexMetadata> toUpload = new ArrayList<>();
        // We prepare a map that contains the previous index metadata for the indexes for which version has changed.
        Map<String, IndexMetadata> prevIndexMetadataByName = new HashMap<>();
        for (final IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            String indexName = indexMetadata.getIndex().getName();
            final IndexMetadata prevIndexMetadata = indicesToBeDeletedFromRemote.get(indexName);
            Long previousVersion = prevIndexMetadata != null ? prevIndexMetadata.getVersion() : null;
            if (previousVersion == null || indexMetadata.getVersion() != previousVersion) {
                logger.debug(
                    "updating metadata for [{}], changing version from [{}] to [{}]",
                    indexMetadata.getIndex(),
                    previousVersion,
                    indexMetadata.getVersion()
                );
                numIndicesUpdated++;
                toUpload.add(indexMetadata);
                prevIndexMetadataByName.put(indexName, prevIndexMetadata);
            } else {
                numIndicesUnchanged++;
            }
            // index present in current cluster state
            indicesToBeDeletedFromRemote.remove(indexMetadata.getIndex().getName());
        }

        final DiffableUtils.MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> routingTableDiff = remoteRoutingTableService
            .getIndicesRoutingMapDiff(previousClusterState.getRoutingTable(), clusterState.getRoutingTable());
        final List<IndexRoutingTable> indicesRoutingToUpload = new ArrayList<>();
        routingTableDiff.getUpserts().forEach((k, v) -> indicesRoutingToUpload.add(v));

        UploadedMetadataResults uploadedMetadataResults;
        // For migration case from codec V0 or V1 to V2, we have added null check on metadata attribute files,
        // If file is empty and codec is 1 then write global metadata.
        boolean updateCoordinationMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isCoordinationMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;
        ;
        boolean updateSettingsMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isSettingsMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;
        boolean updateTransientSettingsMetadata = Metadata.isTransientSettingsMetadataEqual(
            previousClusterState.metadata(),
            clusterState.metadata()
        ) == false;
        boolean updateTemplatesMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isTemplatesMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;

        final boolean updateDiscoveryNodes = isPublicationEnabled
            && clusterState.getNodes().delta(previousClusterState.getNodes()).hasChanges();
        final boolean updateClusterBlocks = isPublicationEnabled && !clusterState.blocks().equals(previousClusterState.blocks());
        final boolean updateHashesOfConsistentSettings = isPublicationEnabled
            || Metadata.isHashesOfConsistentSettingsEqual(previousClusterState.metadata(), clusterState.metadata()) == false;

        uploadedMetadataResults = writeMetadataInParallel(
            clusterState,
            toUpload,
            prevIndexMetadataByName,
            customsDiff.getUpserts(),
            updateCoordinationMetadata,
            updateSettingsMetadata,
            updateTemplatesMetadata,
            updateDiscoveryNodes,
            updateClusterBlocks,
            updateTransientSettingsMetadata,
            clusterStateCustomsDiff.getUpserts(),
            updateHashesOfConsistentSettings,
            indicesRoutingToUpload
        );

        // update the map if the metadata was uploaded
        uploadedMetadataResults.uploadedIndexMetadata.forEach(
            uploadedIndexMetadata -> allUploadedIndexMetadata.put(uploadedIndexMetadata.getIndexName(), uploadedIndexMetadata)
        );
        allUploadedCustomMap.putAll(uploadedMetadataResults.uploadedCustomMetadataMap);
        allUploadedClusterStateCustomsMap.putAll(uploadedMetadataResults.uploadedClusterStateCustomMetadataMap);
        // remove the data for removed custom/indices
        customsDiff.getDeletes().forEach(allUploadedCustomMap::remove);
        indicesToBeDeletedFromRemote.keySet().forEach(allUploadedIndexMetadata::remove);
        clusterStateCustomsDiff.getDeletes().forEach(allUploadedClusterStateCustomsMap::remove);

        if (!updateCoordinationMetadata) {
            uploadedMetadataResults.uploadedCoordinationMetadata = previousManifest.getCoordinationMetadata();
        }
        if (!updateSettingsMetadata) {
            uploadedMetadataResults.uploadedSettingsMetadata = previousManifest.getSettingsMetadata();
        }
        if (!updateTransientSettingsMetadata) {
            uploadedMetadataResults.uploadedTransientSettingsMetadata = previousManifest.getTransientSettingsMetadata();
        }
        if (!updateTemplatesMetadata) {
            uploadedMetadataResults.uploadedTemplatesMetadata = previousManifest.getTemplatesMetadata();
        }
        if (!updateDiscoveryNodes) {
            uploadedMetadataResults.uploadedDiscoveryNodes = previousManifest.getDiscoveryNodesMetadata();
        }
        if (!updateClusterBlocks) {
            uploadedMetadataResults.uploadedClusterBlocks = previousManifest.getClusterBlocksMetadata();
        }
        if (!updateHashesOfConsistentSettings) {
            uploadedMetadataResults.uploadedHashesOfConsistentSettings = previousManifest.getHashesOfConsistentSettings();
        }
        uploadedMetadataResults.uploadedCustomMetadataMap = allUploadedCustomMap;
        uploadedMetadataResults.uploadedClusterStateCustomMetadataMap = allUploadedClusterStateCustomsMap;
        uploadedMetadataResults.uploadedIndexMetadata = new ArrayList<>(allUploadedIndexMetadata.values());

        uploadedMetadataResults.uploadedIndicesRoutingMetadata = remoteRoutingTableService.getAllUploadedIndicesRouting(
            previousManifest,
            uploadedMetadataResults.uploadedIndicesRoutingMetadata,
            routingTableDiff.getDeletes()
        );

        final RemoteClusterStateManifestInfo manifestDetails = remoteManifestManager.uploadManifest(
            clusterState,
            uploadedMetadataResults,
            previousManifest.getPreviousClusterUUID(),
            new ClusterStateDiffManifest(clusterState, previousClusterState),
            false
        );

        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateSucceeded();
        remoteStateStats.stateTook(durationMillis);
        ParameterizedMessage clusterStateUploadTimeMessage = new ParameterizedMessage(
            CLUSTER_STATE_UPLOAD_TIME_LOG_STRING,
            manifestDetails.getClusterMetadataManifest().getStateVersion(),
            durationMillis
        );
        ParameterizedMessage metadataUpdateMessage = new ParameterizedMessage(
            METADATA_UPDATE_LOG_STRING,
            numIndicesUpdated,
            numIndicesUnchanged,
            updateCoordinationMetadata,
            updateSettingsMetadata,
            updateTemplatesMetadata,
            customsDiff.getUpserts().size(),
            indicesRoutingToUpload.size()
        );
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            // TODO update logs to add more details about objects uploaded
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; "
                    + "wrote  metadata for [{}] indices and skipped [{}] unchanged indices, coordination metadata updated : [{}], "
                    + "settings metadata updated : [{}], templates metadata updated : [{}], custom metadata updated : [{}]",
                durationMillis,
                slowWriteLoggingThreshold,
                numIndicesUpdated,
                numIndicesUnchanged,
                updateCoordinationMetadata,
                updateSettingsMetadata,
                updateTemplatesMetadata,
                customsDiff.getUpserts().size()
            );
        } else {
            logger.info("{}; {}", clusterStateUploadTimeMessage, metadataUpdateMessage);
            logger.info(
                "writing cluster state for version [{}] took [{}ms]; "
                    + "wrote metadata for [{}] indices and skipped [{}] unchanged indices, coordination metadata updated : [{}], "
                    + "settings metadata updated : [{}], templates metadata updated : [{}], custom metadata updated : [{}]",
                manifestDetails.getClusterMetadataManifest().getStateVersion(),
                durationMillis,
                numIndicesUpdated,
                numIndicesUnchanged,
                updateCoordinationMetadata,
                updateSettingsMetadata,
                updateTemplatesMetadata,
                customsDiff.getUpserts().size()
            );
        }
        return manifestDetails;
    }

    private UploadedMetadataResults writeMetadataInParallel(
        ClusterState clusterState,
        List<IndexMetadata> indexToUpload,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        Map<String, Metadata.Custom> customToUpload,
        boolean uploadCoordinationMetadata,
        boolean uploadSettingsMetadata,
        boolean uploadTemplateMetadata,
        boolean uploadDiscoveryNodes,
        boolean uploadClusterBlock,
        boolean uploadTransientSettingMetadata,
        Map<String, ClusterState.Custom> clusterStateCustomToUpload,
        boolean uploadHashesOfConsistentSettings,
        List<IndexRoutingTable> indicesRoutingToUpload
    ) throws IOException {
        assert Objects.nonNull(indexMetadataUploadListeners) : "indexMetadataUploadListeners can not be null";
        int totalUploadTasks = indexToUpload.size() + indexMetadataUploadListeners.size() + customToUpload.size()
            + (uploadCoordinationMetadata ? 1 : 0) + (uploadSettingsMetadata ? 1 : 0) + (uploadTemplateMetadata ? 1 : 0)
            + (uploadDiscoveryNodes ? 1 : 0) + (uploadClusterBlock ? 1 : 0) + (uploadTransientSettingMetadata ? 1 : 0)
            + clusterStateCustomToUpload.size() + (uploadHashesOfConsistentSettings ? 1 : 0) + indicesRoutingToUpload.size();
        CountDownLatch latch = new CountDownLatch(totalUploadTasks);
        Map<String, CheckedRunnable<IOException>> uploadTasks = new ConcurrentHashMap<>(totalUploadTasks);
        Map<String, ClusterMetadataManifest.UploadedMetadata> results = new ConcurrentHashMap<>(totalUploadTasks);
        List<Exception> exceptionList = Collections.synchronizedList(new ArrayList<>(totalUploadTasks));

        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = new LatchedActionListener<>(
            ActionListener.wrap((ClusterMetadataManifest.UploadedMetadata uploadedMetadata) -> {
                logger.trace(String.format(Locale.ROOT, "Metadata component %s uploaded successfully.", uploadedMetadata.getComponent()));
                results.put(uploadedMetadata.getComponent(), uploadedMetadata);
            }, ex -> {
                logger.error(
                    () -> new ParameterizedMessage("Exception during transfer of Metadata Fragment to Remote {}", ex.getMessage()),
                    ex
                );
                exceptionList.add(ex);
            }),
            latch
        );

        if (uploadSettingsMetadata) {
            uploadTasks.put(
                SETTING_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    new RemotePersistentSettingsMetadata(
                        clusterState.metadata().persistentSettings(),
                        clusterState.metadata().version(),
                        clusterState.metadata().clusterUUID(),
                        blobStoreRepository.getCompressor(),
                        blobStoreRepository.getNamedXContentRegistry()
                    ),
                    listener
                )
            );
        }
        if (uploadTransientSettingMetadata) {
            uploadTasks.put(
                TRANSIENT_SETTING_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    new RemoteTransientSettingsMetadata(
                        clusterState.metadata().transientSettings(),
                        clusterState.metadata().version(),
                        clusterState.metadata().clusterUUID(),
                        blobStoreRepository.getCompressor(),
                        blobStoreRepository.getNamedXContentRegistry()
                    ),
                    listener
                )
            );
        }
        if (uploadCoordinationMetadata) {
            uploadTasks.put(
                COORDINATION_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    new RemoteCoordinationMetadata(
                        clusterState.metadata().coordinationMetadata(),
                        clusterState.metadata().version(),
                        clusterState.metadata().clusterUUID(),
                        blobStoreRepository.getCompressor(),
                        blobStoreRepository.getNamedXContentRegistry()
                    ),
                    listener
                )
            );
        }
        if (uploadTemplateMetadata) {
            uploadTasks.put(
                TEMPLATES_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    new RemoteTemplatesMetadata(
                        clusterState.metadata().templatesMetadata(),
                        clusterState.metadata().version(),
                        clusterState.metadata().clusterUUID(),
                        blobStoreRepository.getCompressor(),
                        blobStoreRepository.getNamedXContentRegistry()
                    ),
                    listener
                )
            );
        }
        if (uploadDiscoveryNodes) {
            uploadTasks.put(
                DISCOVERY_NODES,
                remoteClusterStateAttributesManager.getAsyncMetadataWriteAction(
                    RemoteDiscoveryNodes.DISCOVERY_NODES,
                    new RemoteDiscoveryNodes(
                        clusterState.nodes(),
                        clusterState.version(),
                        clusterState.stateUUID(),
                        blobStoreRepository.getCompressor()
                    ),
                    listener
                )
            );
        }
        if (uploadClusterBlock) {
            uploadTasks.put(
                CLUSTER_BLOCKS,
                remoteClusterStateAttributesManager.getAsyncMetadataWriteAction(
                    RemoteClusterBlocks.CLUSTER_BLOCKS,
                    new RemoteClusterBlocks(
                        clusterState.blocks(),
                        clusterState.version(),
                        clusterState.metadata().clusterUUID(),
                        blobStoreRepository.getCompressor()
                    ),
                    listener
                )
            );
        }
        if (uploadHashesOfConsistentSettings) {
            uploadTasks.put(
                HASHES_OF_CONSISTENT_SETTINGS,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    new RemoteHashesOfConsistentSettings(
                        (DiffableStringMap) clusterState.metadata().hashesOfConsistentSettings(),
                        clusterState.metadata().version(),
                        clusterState.metadata().clusterUUID(),
                        blobStoreRepository.getCompressor()
                    ),
                    listener
                )
            );
        }
        customToUpload.forEach((key, value) -> {
            String customComponent = String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, key);
            uploadTasks.put(
                customComponent,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    new RemoteCustomMetadata(
                        value,
                        key,
                        clusterState.metadata().version(),
                        clusterState.metadata().clusterUUID(),
                        blobStoreRepository.getCompressor(),
                        namedWriteableRegistry
                    ),
                    listener
                )
            );
        });
        indexToUpload.forEach(indexMetadata -> {
            uploadTasks.put(
                indexMetadata.getIndex().getName(),
                remoteIndexMetadataManager.getAsyncIndexMetadataWriteAction(indexMetadata, clusterState.metadata().clusterUUID(), listener)
            );
        });

        clusterStateCustomToUpload.forEach((key, value) -> {
            uploadTasks.put(
                key,
                remoteClusterStateAttributesManager.getAsyncMetadataWriteAction(
                    CLUSTER_STATE_CUSTOM,
                    new RemoteClusterStateCustoms(
                        value,
                        key,
                        clusterState.version(),
                        clusterState.metadata().clusterUUID(),
                        blobStoreRepository.getCompressor(),
                        namedWriteableRegistry
                    ),
                    listener
                )
            );
        });
        indicesRoutingToUpload.forEach(indexRoutingTable -> {
            uploadTasks.put(
                InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX + indexRoutingTable.getIndex().getName(),
                remoteRoutingTableService.getIndexRoutingAsyncAction(
                    clusterState,
                    indexRoutingTable,
                    listener,
                    getClusterMetadataBasePath(
                        blobStoreRepository,
                        clusterState.getClusterName().value(),
                        clusterState.metadata().clusterUUID()
                    )
                )
            );
        });

        // start async upload of all required metadata files
        for (CheckedRunnable<IOException> uploadTask : uploadTasks.values()) {
            uploadTask.run();
        }
        invokeIndexMetadataUploadListeners(indexToUpload, prevIndexMetadataByName, latch, exceptionList);

        try {
            if (latch.await(remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                // TODO: We should add metrics where transfer is timing out. [Issue: #10687]
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(
                        Locale.ROOT,
                        "Timed out waiting for transfer of following metadata to complete - %s",
                        String.join(", ", uploadTasks.keySet())
                    )
                );
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (InterruptedException ex) {
            exceptionList.forEach(ex::addSuppressed);
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(
                    Locale.ROOT,
                    "Timed out waiting for transfer of metadata to complete - %s",
                    String.join(", ", uploadTasks.keySet())
                ),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (!exceptionList.isEmpty()) {
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(
                    Locale.ROOT,
                    "Exception during transfer of following metadata to Remote - %s",
                    String.join(", ", uploadTasks.keySet())
                )
            );
            exceptionList.forEach(exception::addSuppressed);
            throw exception;
        }
        UploadedMetadataResults response = new UploadedMetadataResults();
        results.forEach((name, uploadedMetadata) -> {
            if (uploadedMetadata.getClass().equals(UploadedIndexMetadata.class)
                && uploadedMetadata.getComponent().contains(InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX)) {
                response.uploadedIndicesRoutingMetadata.add((UploadedIndexMetadata) uploadedMetadata);
            } else if (name.startsWith(CUSTOM_METADATA)) {
                // component name for custom metadata will look like custom--<metadata-attribute>
                String custom = name.split(DELIMITER)[0].split(CUSTOM_DELIMITER)[1];
                response.uploadedCustomMetadataMap.put(
                    custom,
                    new UploadedMetadataAttribute(custom, uploadedMetadata.getUploadedFilename())
                );
            } else if (name.startsWith(CLUSTER_STATE_CUSTOM)) {
                String custom = name.split(DELIMITER)[0].split(CUSTOM_DELIMITER)[1];
                response.uploadedClusterStateCustomMetadataMap.put(
                    custom,
                    new UploadedMetadataAttribute(custom, uploadedMetadata.getUploadedFilename())
                );
            } else if (COORDINATION_METADATA.equals(name)) {
                response.uploadedCoordinationMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (RemotePersistentSettingsMetadata.SETTING_METADATA.equals(name)) {
                response.uploadedSettingsMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (TEMPLATES_METADATA.equals(name)) {
                response.uploadedTemplatesMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (name.contains(UploadedIndexMetadata.COMPONENT_PREFIX)) {
                response.uploadedIndexMetadata.add((UploadedIndexMetadata) uploadedMetadata);
            } else if (RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA.equals(name)) {
                response.uploadedTransientSettingsMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (RemoteDiscoveryNodes.DISCOVERY_NODES.equals(uploadedMetadata.getComponent())) {
                response.uploadedDiscoveryNodes = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (RemoteClusterBlocks.CLUSTER_BLOCKS.equals(uploadedMetadata.getComponent())) {
                response.uploadedClusterBlocks = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS.equals(uploadedMetadata.getComponent())) {
                response.uploadedHashesOfConsistentSettings = (UploadedMetadataAttribute) uploadedMetadata;
            } else {
                throw new IllegalStateException("Unknown metadata component name " + name);
            }
        });
        logger.trace("response {}", response.uploadedIndicesRoutingMetadata.toString());
        return response;
    }

    /**
     * Invokes the index metadata upload listener but does not wait for the execution to complete.
     */
    private void invokeIndexMetadataUploadListeners(
        List<IndexMetadata> updatedIndexMetadataList,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        CountDownLatch latch,
        List<Exception> exceptionList
    ) {
        for (IndexMetadataUploadListener listener : indexMetadataUploadListeners) {
            String listenerName = listener.getClass().getSimpleName();
            listener.onUpload(
                updatedIndexMetadataList,
                prevIndexMetadataByName,
                getIndexMetadataUploadActionListener(updatedIndexMetadataList, prevIndexMetadataByName, latch, exceptionList, listenerName)
            );
        }

    }

    private ActionListener<Void> getIndexMetadataUploadActionListener(
        List<IndexMetadata> newIndexMetadataList,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        CountDownLatch latch,
        List<Exception> exceptionList,
        String listenerName
    ) {
        long startTime = System.nanoTime();
        return new LatchedActionListener<>(
            ActionListener.wrap(
                ignored -> logger.trace(
                    new ParameterizedMessage(
                        "listener={} : Invoked successfully with indexMetadataList={} prevIndexMetadataList={} tookTimeNs={}",
                        listenerName,
                        newIndexMetadataList,
                        prevIndexMetadataByName.values(),
                        (System.nanoTime() - startTime)
                    )
                ),
                ex -> {
                    logger.error(
                        new ParameterizedMessage(
                            "listener={} : Exception during invocation with indexMetadataList={} prevIndexMetadataList={} tookTimeNs={}",
                            listenerName,
                            newIndexMetadataList,
                            prevIndexMetadataByName.values(),
                            (System.nanoTime() - startTime)
                        ),
                        ex
                    );
                    exceptionList.add(ex);
                }
            ),
            latch
        );
    }

    public RemoteManifestManager getRemoteManifestManager() {
        return remoteManifestManager;
    }

    public RemoteClusterStateCleanupManager getCleanupManager() {
        return remoteClusterStateCleanupManager;
    }

    @Nullable
    public RemoteClusterStateManifestInfo markLastStateAsCommitted(ClusterState clusterState, ClusterMetadataManifest previousManifest)
        throws IOException {
        assert clusterState != null : "Last accepted cluster state is not set";
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousManifest != null : "Last cluster metadata manifest is not set";
        UploadedMetadataResults uploadedMetadataResults = new UploadedMetadataResults(
            previousManifest.getIndices(),
            previousManifest.getCustomMetadataMap(),
            previousManifest.getCoordinationMetadata(),
            previousManifest.getSettingsMetadata(),
            previousManifest.getTemplatesMetadata(),
            previousManifest.getTransientSettingsMetadata(),
            previousManifest.getDiscoveryNodesMetadata(),
            previousManifest.getClusterBlocksMetadata(),
            previousManifest.getIndicesRouting(),
            previousManifest.getHashesOfConsistentSettings(),
            previousManifest.getClusterStateCustomMap()
        );

        RemoteClusterStateManifestInfo committedManifestDetails = remoteManifestManager.uploadManifest(
            clusterState,
            uploadedMetadataResults,
            previousManifest.getPreviousClusterUUID(),
            previousManifest.getDiffManifest(),
            true
        );
        if (!previousManifest.isClusterUUIDCommitted() && committedManifestDetails.getClusterMetadataManifest().isClusterUUIDCommitted()) {
            remoteClusterStateCleanupManager.deleteStaleClusterUUIDs(clusterState, committedManifestDetails.getClusterMetadataManifest());
        }

        return committedManifestDetails;
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        return remoteManifestManager.getLatestClusterMetadataManifest(clusterName, clusterUUID);
    }

    public ClusterMetadataManifest getClusterMetadataManifestByFileName(String clusterUUID, String fileName) {
        return remoteManifestManager.getRemoteClusterMetadataManifestByFileName(clusterUUID, fileName);
    }

    @Override
    public void close() throws IOException {
        remoteClusterStateCleanupManager.close();
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
        this.remoteRoutingTableService.close();
    }

    public void start() {
        assert isRemoteStoreClusterStateEnabled(settings) == true : "Remote cluster state is not enabled";
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote Cluster State repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
        this.remoteRoutingTableService.start();
        blobStoreTransferService = new BlobStoreTransferService(getBlobStore(), threadpool);
        String clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings).value();

        remoteGlobalMetadataManager = new RemoteGlobalMetadataManager(
            clusterSettings,
            clusterName,
            blobStoreRepository,
            blobStoreTransferService,
            namedWriteableRegistry,
            threadpool
        );
        remoteIndexMetadataManager = new RemoteIndexMetadataManager(
            clusterSettings,
            clusterName,
            blobStoreRepository,
            blobStoreTransferService,
            threadpool
        );
        remoteManifestManager = new RemoteManifestManager(
            clusterSettings,
            clusterName,
            nodeId,
            blobStoreRepository,
            blobStoreTransferService,
            threadpool
        );
        remoteClusterStateAttributesManager = new RemoteClusterStateAttributesManager(
            clusterName,
            blobStoreRepository,
            blobStoreTransferService,
            namedWriteableRegistry,
            threadpool
        );
        remoteClusterStateCleanupManager.start();
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    // Package private for unit test
    RemoteRoutingTableService getRemoteRoutingTableService() {
        return this.remoteRoutingTableService;
    }

    ThreadPool getThreadpool() {
        return threadpool;
    }

    BlobStoreRepository getBlobStoreRepository() {
        return blobStoreRepository;
    }

    BlobStore getBlobStore() {
        return blobStoreRepository.blobStore();
    }

    /**
     * Fetch latest ClusterState from remote, including global metadata, index metadata and cluster state version
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return {@link IndexMetadata}
     */
    public ClusterState getLatestClusterState(String clusterName, String clusterUUID, boolean includeEphemeral) throws IOException {
        Optional<ClusterMetadataManifest> clusterMetadataManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            clusterName,
            clusterUUID
        );
        if (clusterMetadataManifest.isEmpty()) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Latest cluster metadata manifest is not present for the provided clusterUUID: %s", clusterUUID)
            );
        }

        return getClusterStateForManifest(clusterName, clusterMetadataManifest.get(), nodeId, includeEphemeral);
    }

    private ClusterState readClusterStateInParallel(
        ClusterState previousState,
        ClusterMetadataManifest manifest,
        String clusterUUID,
        String localNodeId,
        List<UploadedIndexMetadata> indicesToRead,
        Map<String, UploadedMetadataAttribute> customToRead,
        boolean readCoordinationMetadata,
        boolean readSettingsMetadata,
        boolean readTransientSettingsMetadata,
        boolean readTemplatesMetadata,
        boolean readDiscoveryNodes,
        boolean readClusterBlocks,
        List<UploadedIndexMetadata> indicesRoutingToRead,
        boolean readHashesOfConsistentSettings,
        Map<String, UploadedMetadataAttribute> clusterStateCustomToRead,
        boolean includeEphemeral
    ) throws IOException {
        int totalReadTasks = indicesToRead.size() + customToRead.size() + (readCoordinationMetadata ? 1 : 0) + (readSettingsMetadata
            ? 1
            : 0) + (readTemplatesMetadata ? 1 : 0) + (readDiscoveryNodes ? 1 : 0) + (readClusterBlocks ? 1 : 0)
            + (readTransientSettingsMetadata ? 1 : 0) + (readHashesOfConsistentSettings ? 1 : 0) + clusterStateCustomToRead.size()
            + indicesRoutingToRead.size();
        CountDownLatch latch = new CountDownLatch(totalReadTasks);
        List<CheckedRunnable<IOException>> asyncMetadataReadActions = new ArrayList<>();
        List<RemoteReadResult> readResults = Collections.synchronizedList(new ArrayList<>());
        List<IndexRoutingTable> readIndexRoutingTableResults = Collections.synchronizedList(new ArrayList<>());
        List<Exception> exceptionList = Collections.synchronizedList(new ArrayList<>(totalReadTasks));

        LatchedActionListener<RemoteReadResult> listener = new LatchedActionListener<>(ActionListener.wrap(response -> {
            logger.debug("Successfully read cluster state component from remote");
            readResults.add(response);
        }, ex -> {
            logger.error("Failed to read cluster state from remote", ex);
            exceptionList.add(ex);
        }), latch);

        for (UploadedIndexMetadata indexMetadata : indicesToRead) {
            asyncMetadataReadActions.add(
                remoteIndexMetadataManager.getAsyncIndexMetadataReadAction(clusterUUID, indexMetadata.getUploadedFilename(), listener)
            );
        }

        LatchedActionListener<IndexRoutingTable> routingTableLatchedActionListener = new LatchedActionListener<>(
            ActionListener.wrap(response -> {
                logger.debug("Successfully read cluster state component from remote");
                readIndexRoutingTableResults.add(response);
            }, ex -> {
                logger.error("Failed to read cluster state from remote", ex);
                exceptionList.add(ex);
            }),
            latch
        );

        for (UploadedIndexMetadata indexRouting : indicesRoutingToRead) {
            asyncMetadataReadActions.add(
                remoteRoutingTableService.getAsyncIndexRoutingReadAction(
                    indexRouting.getUploadedFilename(),
                    new Index(indexRouting.getIndexName(), indexRouting.getIndexUUID()),
                    routingTableLatchedActionListener
                )
            );
        }

        for (Map.Entry<String, UploadedMetadataAttribute> entry : customToRead.entrySet()) {
            asyncMetadataReadActions.add(
                remoteGlobalMetadataManager.getAsyncMetadataReadAction(
                    new RemoteCustomMetadata(
                        entry.getValue().getUploadedFilename(),
                        entry.getKey(),
                        clusterUUID,
                        blobStoreRepository.getCompressor(),
                        namedWriteableRegistry
                    ),
                    entry.getValue().getAttributeName(),
                    listener
                )
            );
        }

        if (readCoordinationMetadata) {
            asyncMetadataReadActions.add(
                remoteGlobalMetadataManager.getAsyncMetadataReadAction(
                    new RemoteCoordinationMetadata(
                        manifest.getCoordinationMetadata().getUploadedFilename(),
                        clusterUUID,
                        blobStoreRepository.getCompressor(),
                        blobStoreRepository.getNamedXContentRegistry()
                    ),
                    COORDINATION_METADATA,
                    listener
                )
            );
        }

        if (readSettingsMetadata) {
            asyncMetadataReadActions.add(
                remoteGlobalMetadataManager.getAsyncMetadataReadAction(
                    new RemotePersistentSettingsMetadata(
                        manifest.getSettingsMetadata().getUploadedFilename(),
                        clusterUUID,
                        blobStoreRepository.getCompressor(),
                        blobStoreRepository.getNamedXContentRegistry()
                    ),
                    SETTING_METADATA,
                    listener
                )
            );
        }

        if (readTransientSettingsMetadata) {
            asyncMetadataReadActions.add(
                remoteGlobalMetadataManager.getAsyncMetadataReadAction(
                    new RemoteTransientSettingsMetadata(
                        manifest.getTransientSettingsMetadata().getUploadedFilename(),
                        clusterUUID,
                        blobStoreRepository.getCompressor(),
                        blobStoreRepository.getNamedXContentRegistry()
                    ),
                    TRANSIENT_SETTING_METADATA,
                    listener
                )
            );
        }

        if (readTemplatesMetadata) {
            asyncMetadataReadActions.add(
                remoteGlobalMetadataManager.getAsyncMetadataReadAction(
                    new RemoteTemplatesMetadata(
                        manifest.getTemplatesMetadata().getUploadedFilename(),
                        clusterUUID,
                        blobStoreRepository.getCompressor(),
                        blobStoreRepository.getNamedXContentRegistry()
                    ),
                    TEMPLATES_METADATA,
                    listener
                )
            );
        }

        if (readDiscoveryNodes) {
            asyncMetadataReadActions.add(
                remoteClusterStateAttributesManager.getAsyncMetadataReadAction(
                    DISCOVERY_NODES,
                    new RemoteDiscoveryNodes(
                        manifest.getDiscoveryNodesMetadata().getUploadedFilename(),
                        clusterUUID,
                        blobStoreRepository.getCompressor()
                    ),
                    listener
                )
            );
        }

        if (readClusterBlocks) {
            asyncMetadataReadActions.add(
                remoteClusterStateAttributesManager.getAsyncMetadataReadAction(
                    CLUSTER_BLOCKS,
                    new RemoteClusterBlocks(
                        manifest.getClusterBlocksMetadata().getUploadedFilename(),
                        clusterUUID,
                        blobStoreRepository.getCompressor()
                    ),
                    listener
                )
            );
        }

        if (readHashesOfConsistentSettings) {
            asyncMetadataReadActions.add(
                remoteGlobalMetadataManager.getAsyncMetadataReadAction(
                    new RemoteHashesOfConsistentSettings(
                        manifest.getHashesOfConsistentSettings().getUploadedFilename(),
                        clusterUUID,
                        blobStoreRepository.getCompressor()
                    ),
                    HASHES_OF_CONSISTENT_SETTINGS,
                    listener
                )
            );
        }

        for (Map.Entry<String, UploadedMetadataAttribute> entry : clusterStateCustomToRead.entrySet()) {
            asyncMetadataReadActions.add(
                remoteClusterStateAttributesManager.getAsyncMetadataReadAction(
                    CLUSTER_STATE_CUSTOM,
                    new RemoteClusterStateCustoms(
                        entry.getValue().getUploadedFilename(),
                        entry.getValue().getAttributeName(),
                        clusterUUID,
                        blobStoreRepository.getCompressor(),
                        namedWriteableRegistry
                    ),
                    listener
                )
            );
        }

        for (CheckedRunnable<IOException> asyncMetadataReadAction : asyncMetadataReadActions) {
            asyncMetadataReadAction.run();
        }

        try {
            if (latch.await(this.remoteStateReadTimeout.getMillis(), TimeUnit.MILLISECONDS) == false) {
                RemoteStateTransferException exception = new RemoteStateTransferException(
                    "Timed out waiting to read cluster state from remote within timeout " + this.remoteStateReadTimeout
                );
                exceptionList.forEach(exception::addSuppressed);
                throw exception;
            }
        } catch (InterruptedException e) {
            exceptionList.forEach(e::addSuppressed);
            RemoteStateTransferException ex = new RemoteStateTransferException(
                "Interrupted while waiting to read cluster state from metadata"
            );
            Thread.currentThread().interrupt();
            throw ex;
        }

        if (!exceptionList.isEmpty()) {
            RemoteStateTransferException exception = new RemoteStateTransferException("Exception during reading cluster state from remote");
            exceptionList.forEach(exception::addSuppressed);
            throw exception;
        }

        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(previousState);
        AtomicReference<Builder> discoveryNodesBuilder = new AtomicReference<>(DiscoveryNodes.builder());
        Metadata.Builder metadataBuilder = Metadata.builder(previousState.metadata());
        metadataBuilder.version(manifest.getMetadataVersion());
        metadataBuilder.clusterUUID(manifest.getClusterUUID());
        metadataBuilder.clusterUUIDCommitted(manifest.isClusterUUIDCommitted());
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        Map<String, IndexRoutingTable> indicesRouting = new HashMap<>(previousState.routingTable().getIndicesRouting());

        readResults.forEach(remoteReadResult -> {
            switch (remoteReadResult.getComponent()) {
                case RemoteIndexMetadata.INDEX:
                    IndexMetadata indexMetadata = (IndexMetadata) remoteReadResult.getObj();
                    indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata);
                    break;
                case CUSTOM_METADATA:
                    Metadata.Custom metadataCustom = (Metadata.Custom) remoteReadResult.getObj();
                    if (includeEphemeral || (!includeEphemeral && metadataCustom.context().contains(XContentContext.GATEWAY))) {
                        metadataBuilder.putCustom(remoteReadResult.getComponentName(), (Metadata.Custom) remoteReadResult.getObj());
                    }
                    break;
                case COORDINATION_METADATA:
                    metadataBuilder.coordinationMetadata((CoordinationMetadata) remoteReadResult.getObj());
                    break;
                case SETTING_METADATA:
                    metadataBuilder.persistentSettings((Settings) remoteReadResult.getObj());
                    break;
                case TRANSIENT_SETTING_METADATA:
                    metadataBuilder.transientSettings((Settings) remoteReadResult.getObj());
                    break;
                case TEMPLATES_METADATA:
                    metadataBuilder.templates((TemplatesMetadata) remoteReadResult.getObj());
                    break;
                case HASHES_OF_CONSISTENT_SETTINGS:
                    metadataBuilder.hashesOfConsistentSettings((DiffableStringMap) remoteReadResult.getObj());
                    break;
                case CLUSTER_STATE_ATTRIBUTE:
                    if (remoteReadResult.getComponentName().equals(DISCOVERY_NODES)) {
                        discoveryNodesBuilder.set(DiscoveryNodes.builder((DiscoveryNodes) remoteReadResult.getObj()));
                    } else if (remoteReadResult.getComponentName().equals(CLUSTER_BLOCKS)) {
                        clusterStateBuilder.blocks((ClusterBlocks) remoteReadResult.getObj());
                    } else if (remoteReadResult.getComponentName().startsWith(CLUSTER_STATE_CUSTOM)) {
                        // component name for mat is "cluster-state-custom--custom_name"
                        String custom = remoteReadResult.getComponentName().split(CUSTOM_DELIMITER)[1];
                        clusterStateBuilder.putCustom(custom, (ClusterState.Custom) remoteReadResult.getObj());
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown component: " + remoteReadResult.getComponent());
            }
        });

        metadataBuilder.indices(indexMetadataMap);
        if (readDiscoveryNodes) {
            clusterStateBuilder.nodes(discoveryNodesBuilder.get().localNodeId(localNodeId));
        }

        clusterStateBuilder.metadata(metadataBuilder).version(manifest.getStateVersion()).stateUUID(manifest.getStateUUID());

        readIndexRoutingTableResults.forEach(
            indexRoutingTable -> indicesRouting.put(indexRoutingTable.getIndex().getName(), indexRoutingTable)
        );
        clusterStateBuilder.routingTable(new RoutingTable(manifest.getRoutingTableVersion(), indicesRouting));

        return clusterStateBuilder.build();
    }

    public ClusterState getClusterStateForManifest(
        String clusterName,
        ClusterMetadataManifest manifest,
        String localNodeId,
        boolean includeEphemeral
    ) throws IOException {
        if (manifest.onOrAfterCodecVersion(CODEC_V2)) {
            return readClusterStateInParallel(
                ClusterState.builder(new ClusterName(clusterName)).build(),
                manifest,
                manifest.getClusterUUID(),
                localNodeId,
                manifest.getIndices(),
                manifest.getCustomMetadataMap(),
                manifest.getCoordinationMetadata() != null,
                manifest.getSettingsMetadata() != null,
                manifest.getTransientSettingsMetadata() != null,
                manifest.getTemplatesMetadata() != null,
                includeEphemeral && manifest.getDiscoveryNodesMetadata() != null,
                includeEphemeral && manifest.getClusterBlocksMetadata() != null,
                includeEphemeral ? manifest.getIndicesRouting() : emptyList(),
                includeEphemeral && manifest.getHashesOfConsistentSettings() != null,
                includeEphemeral ? manifest.getClusterStateCustomMap() : emptyMap(),
                includeEphemeral
            );
        } else {
            ClusterState clusterState = readClusterStateInParallel(
                ClusterState.builder(new ClusterName(clusterName)).build(),
                manifest,
                manifest.getClusterUUID(),
                localNodeId,
                manifest.getIndices(),
                // for manifest codec V1, we don't have the following objects to read, so not passing anything
                emptyMap(),
                false,
                false,
                false,
                false,
                false,
                false,
                emptyList(),
                false,
                emptyMap(),
                false
            );
            Metadata.Builder mb = Metadata.builder(remoteGlobalMetadataManager.getGlobalMetadata(manifest.getClusterUUID(), manifest));
            mb.indices(clusterState.metadata().indices());
            return ClusterState.builder(clusterState).metadata(mb).build();
        }

    }

    public ClusterState getClusterStateUsingDiff(
        String clusterName,
        ClusterMetadataManifest manifest,
        ClusterState previousState,
        String localNodeId
    ) throws IOException {
        assert manifest.getDiffManifest() != null;
        ClusterStateDiffManifest diff = manifest.getDiffManifest();
        List<UploadedIndexMetadata> updatedIndices = diff.getIndicesUpdated().stream().map(idx -> {
            Optional<UploadedIndexMetadata> uploadedIndexMetadataOptional = manifest.getIndices()
                .stream()
                .filter(idx2 -> idx2.getIndexName().equals(idx))
                .findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            return uploadedIndexMetadataOptional.get();
        }).collect(Collectors.toList());

        Map<String, UploadedMetadataAttribute> updatedCustomMetadata = new HashMap<>();
        if (diff.getCustomMetadataUpdated() != null) {
            for (String customType : diff.getCustomMetadataUpdated()) {
                updatedCustomMetadata.put(customType, manifest.getCustomMetadataMap().get(customType));
            }
        }
        Map<String, UploadedMetadataAttribute> updatedClusterStateCustom = new HashMap<>();
        if (diff.getClusterStateCustomUpdated() != null) {
            for (String customType : diff.getClusterStateCustomUpdated()) {
                updatedClusterStateCustom.put(customType, manifest.getClusterStateCustomMap().get(customType));
            }
        }

        List<UploadedIndexMetadata> updatedIndexRouting = new ArrayList<>();
        updatedIndexRouting.addAll(
            remoteRoutingTableService.getUpdatedIndexRoutingTableMetadata(diff.getIndicesRoutingUpdated(), manifest.getIndicesRouting())
        );

        ClusterState updatedClusterState = readClusterStateInParallel(
            previousState,
            manifest,
            manifest.getClusterUUID(),
            localNodeId,
            updatedIndices,
            updatedCustomMetadata,
            diff.isCoordinationMetadataUpdated(),
            diff.isSettingsMetadataUpdated(),
            diff.isTransientSettingsMetadataUpdated(),
            diff.isTemplatesMetadataUpdated(),
            diff.isDiscoveryNodesUpdated(),
            diff.isClusterBlocksUpdated(),
            updatedIndexRouting,
            diff.isHashesOfConsistentSettingsUpdated(),
            updatedClusterStateCustom,
            true
        );
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(updatedClusterState);
        Metadata.Builder metadataBuilder = Metadata.builder(updatedClusterState.metadata());
        // remove the deleted indices from the metadata
        for (String index : diff.getIndicesDeleted()) {
            metadataBuilder.remove(index);
        }
        // remove the deleted metadata customs from the metadata
        if (diff.getCustomMetadataDeleted() != null) {
            for (String customType : diff.getCustomMetadataDeleted()) {
                metadataBuilder.removeCustom(customType);
            }
        }

        // remove the deleted cluster state customs from the metadata
        if (diff.getClusterStateCustomDeleted() != null) {
            for (String customType : diff.getClusterStateCustomDeleted()) {
                clusterStateBuilder.removeCustom(customType);
            }
        }

        HashMap<String, IndexRoutingTable> indexRoutingTables = new HashMap<>(updatedClusterState.getRoutingTable().getIndicesRouting());

        for (String indexName : diff.getIndicesRoutingDeleted()) {
            indexRoutingTables.remove(indexName);
        }

        return clusterStateBuilder.stateUUID(manifest.getStateUUID())
            .version(manifest.getStateVersion())
            .metadata(metadataBuilder)
            .routingTable(new RoutingTable(manifest.getRoutingTableVersion(), indexRoutingTables))
            .build();
    }

    /**
     * Fetch the previous cluster UUIDs from remote state store and return the most recent valid cluster UUID
     *
     * @param clusterName The cluster name for which previous cluster UUID is to be fetched
     * @return Last valid cluster UUID
     */
    public String getLastKnownUUIDFromRemote(String clusterName) {
        try {
            Set<String> clusterUUIDs = getAllClusterUUIDs(clusterName);
            Map<String, ClusterMetadataManifest> latestManifests = remoteManifestManager.getLatestManifestForAllClusterUUIDs(
                clusterName,
                clusterUUIDs
            );
            List<String> validChain = createClusterChain(latestManifests, clusterName);
            if (validChain.isEmpty()) {
                return ClusterState.UNKNOWN_UUID;
            }
            return validChain.get(0);
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while fetching previous UUIDs from remote store for cluster name: %s", clusterName),
                e
            );
        }
    }

    public void setRemoteStateReadTimeout(TimeValue remoteStateReadTimeout) {
        this.remoteStateReadTimeout = remoteStateReadTimeout;
    }

    private BlobStoreTransferService getBlobStoreTransferService() {
        if (blobStoreTransferService == null) {
            blobStoreTransferService = new BlobStoreTransferService(getBlobStore(), threadpool);
        }
        return blobStoreTransferService;
    }

    Set<String> getAllClusterUUIDs(String clusterName) throws IOException {
        Map<String, BlobContainer> clusterUUIDMetadata = clusterUUIDContainer(blobStoreRepository, clusterName).children();
        if (clusterUUIDMetadata == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(clusterUUIDMetadata.keySet());
    }

    private Map<String, ClusterMetadataManifest> getLatestManifestForAllClusterUUIDs(String clusterName, Set<String> clusterUUIDs) {
        Map<String, ClusterMetadataManifest> manifestsByClusterUUID = new HashMap<>();
        for (String clusterUUID : clusterUUIDs) {
            try {
                Optional<ClusterMetadataManifest> manifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
                manifest.ifPresent(clusterMetadataManifest -> manifestsByClusterUUID.put(clusterUUID, clusterMetadataManifest));
            } catch (Exception e) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "Exception in fetching manifest for clusterUUID: %s", clusterUUID),
                    e
                );
            }
        }
        return manifestsByClusterUUID;
    }

    /**
     * This method creates a valid cluster UUID chain.
     *
     * @param manifestsByClusterUUID Map of latest ClusterMetadataManifest for every cluster UUID
     * @return List of cluster UUIDs. The first element is the most recent cluster UUID in the chain
     */
    private List<String> createClusterChain(final Map<String, ClusterMetadataManifest> manifestsByClusterUUID, final String clusterName) {
        final List<ClusterMetadataManifest> validClusterManifests = manifestsByClusterUUID.values()
            .stream()
            .filter(this::isValidClusterUUID)
            .collect(Collectors.toList());
        final Map<String, String> clusterUUIDGraph = validClusterManifests.stream()
            .collect(Collectors.toMap(ClusterMetadataManifest::getClusterUUID, ClusterMetadataManifest::getPreviousClusterUUID));
        final List<String> topLevelClusterUUIDs = validClusterManifests.stream()
            .map(ClusterMetadataManifest::getClusterUUID)
            .filter(clusterUUID -> !clusterUUIDGraph.containsValue(clusterUUID))
            .collect(Collectors.toList());

        if (topLevelClusterUUIDs.isEmpty()) {
            // This can occur only when there are no valid cluster UUIDs
            assert validClusterManifests.isEmpty() : "There are no top level cluster UUIDs even when there are valid cluster UUIDs";
            logger.info("There is no valid previous cluster UUID. All cluster UUIDs evaluated are: {}", manifestsByClusterUUID.keySet());
            return emptyList();
        }
        if (topLevelClusterUUIDs.size() > 1) {
            logger.info("Top level cluster UUIDs: {}", topLevelClusterUUIDs);
            // If the valid cluster UUIDs are more that 1, it means there was some race condition where
            // more then 2 cluster manager nodes tried to become active cluster manager and published
            // 2 cluster UUIDs which followed the same previous UUID.
            final Map<String, ClusterMetadataManifest> manifestsByClusterUUIDTrimmed = trimClusterUUIDs(
                manifestsByClusterUUID,
                topLevelClusterUUIDs,
                clusterName
            );
            if (manifestsByClusterUUID.size() == manifestsByClusterUUIDTrimmed.size()) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "The system has ended into multiple valid cluster states in the remote store. "
                            + "Please check their latest manifest to decide which one you want to keep. Valid Cluster UUIDs: - %s",
                        topLevelClusterUUIDs
                    )
                );
            }
            return createClusterChain(manifestsByClusterUUIDTrimmed, clusterName);
        }
        final List<String> validChain = new ArrayList<>();
        String currentUUID = topLevelClusterUUIDs.get(0);
        while (currentUUID != null && !ClusterState.UNKNOWN_UUID.equals(currentUUID)) {
            validChain.add(currentUUID);
            // Getting the previous cluster UUID of a cluster UUID from the clusterUUID Graph
            currentUUID = clusterUUIDGraph.get(currentUUID);
        }
        logger.info("Known UUIDs found in remote store : [{}]", validChain);
        return validChain;
    }

    /**
     * This method take a map of manifests for different cluster UUIDs and removes the
     * manifest of a cluster UUID if the latest metadata for that cluster UUID is equivalent
     * to the latest metadata of its previous UUID.
     *
     * @return Trimmed map of manifests
     */
    private Map<String, ClusterMetadataManifest> trimClusterUUIDs(
        final Map<String, ClusterMetadataManifest> latestManifestsByClusterUUID,
        final List<String> validClusterUUIDs,
        final String clusterName
    ) {
        final Map<String, ClusterMetadataManifest> trimmedUUIDs = new HashMap<>(latestManifestsByClusterUUID);
        for (String clusterUUID : validClusterUUIDs) {
            ClusterMetadataManifest currentManifest = trimmedUUIDs.get(clusterUUID);
            // Here we compare the manifest of current UUID to that of previous UUID
            // In case currentUUID's latest manifest is same as previous UUIDs latest manifest,
            // that means it was restored from previousUUID and no IndexMetadata update was performed on it.
            if (!ClusterState.UNKNOWN_UUID.equals(currentManifest.getPreviousClusterUUID())) {
                ClusterMetadataManifest previousManifest = trimmedUUIDs.get(currentManifest.getPreviousClusterUUID());
                if (isMetadataEqual(currentManifest, previousManifest, clusterName)
                    && remoteGlobalMetadataManager.isGlobalMetadataEqual(currentManifest, previousManifest, clusterName)) {
                    trimmedUUIDs.remove(clusterUUID);
                }
            }
        }
        return trimmedUUIDs;
    }

    private boolean isMetadataEqual(ClusterMetadataManifest first, ClusterMetadataManifest second, String clusterName) {
        // todo clusterName can be set as final in the constructor
        if (first.getIndices().size() != second.getIndices().size()) {
            return false;
        }
        final Map<String, UploadedIndexMetadata> secondIndices = second.getIndices()
            .stream()
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));
        for (UploadedIndexMetadata uploadedIndexMetadata : first.getIndices()) {
            final IndexMetadata firstIndexMetadata = remoteIndexMetadataManager.getIndexMetadata(
                uploadedIndexMetadata,
                first.getClusterUUID()
            );
            final UploadedIndexMetadata secondUploadedIndexMetadata = secondIndices.get(uploadedIndexMetadata.getIndexName());
            if (secondUploadedIndexMetadata == null) {
                return false;
            }
            final IndexMetadata secondIndexMetadata = remoteIndexMetadataManager.getIndexMetadata(
                secondUploadedIndexMetadata,
                second.getClusterUUID()
            );
            if (firstIndexMetadata.equals(secondIndexMetadata) == false) {
                return false;
            }
        }
        return true;
    }

    private boolean isValidClusterUUID(ClusterMetadataManifest manifest) {
        return manifest.isClusterUUIDCommitted();
    }

    public void writeMetadataFailed() {
        getStats().stateFailed();
    }

    public RemotePersistenceStats getStats() {
        return remoteStateStats;
    }
}
