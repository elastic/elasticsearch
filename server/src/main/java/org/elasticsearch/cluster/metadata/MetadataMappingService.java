/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Service responsible for submitting mapping changes
 */
public class MetadataMappingService {

    // Deliberately not registered so it can only be set in tests/plugins.
    public static final Setting<Priority> PUT_MAPPING_PRIORITY_SETTING = Setting.enumSetting(
        Priority.class,
        "cluster.service.put_mapping.priority",
        Priority.HIGH,
        Setting.Property.NodeScope
    );

    // Deliberately not registered so it can only be set in tests/plugins.
    public static final Setting<TimeValue> PUT_MAPPING_MAX_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.service.put_mapping.max_timeout",
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Logger logger = LogManager.getLogger(MetadataMappingService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    private final MasterServiceTaskQueue<PutMappingClusterStateUpdateTask> taskQueue;
    private volatile TimeValue maxMasterNodeTimeout;

    @Inject
    public MetadataMappingService(
        ClusterService clusterService,
        IndicesService indicesService,
        IndexSettingProviders indexSettingProviders
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.taskQueue = clusterService.createTaskQueue(
            "put-mapping",
            PUT_MAPPING_PRIORITY_SETTING.get(clusterService.getSettings()),
            new PutMappingExecutor(indexSettingProviders)
        );

        // setting only registered in some tests today
        clusterService.getClusterSettings().initializeAndWatchIfRegistered(PUT_MAPPING_MAX_TIMEOUT_SETTING, v -> maxMasterNodeTimeout = v);
    }

    /**
     * Holds the pre-flight merge result for a single index, computed on the MANAGEMENT thread during
     * {@link #isWholeRequestNoop}. Carrying this into {@link PutMappingExecutor} lets the master thread
     * skip the redundant {@link MapperService#merge} work it would otherwise repeat.
     * <p>
     * Both {@code mappingVersion} and {@code settingsVersion} must still match the index metadata at
     * execution time for the cache entry to be usable; a stale entry is discarded and the normal path runs.
     */
    record PreflightCacheEntry(long mappingVersion, long settingsVersion, CompressedXContent preUpdateSource, MapperService mergedService)
        implements
            Closeable {
        @Override
        public void close() throws IOException {
            mergedService.close();
        }
    }

    private sealed interface PreflightResult permits PreflightResult.Noop, PreflightResult.NeedsUpdate {
        record Noop() implements PreflightResult {}

        final class NeedsUpdate implements PreflightResult {
            private Map<Index, PreflightCacheEntry> cache;

            private NeedsUpdate(Map<Index, PreflightCacheEntry> cache) {
                this.cache = cache;
            }

            /**
             * Transfers ownership of the pre-flight cache to the caller. Must be called at most once.
             */
            Map<Index, PreflightCacheEntry> takeCache() {
                if (cache == null) {
                    throw new IllegalStateException("takeCache() called more than once");
                }
                var taken = cache;
                cache = null;
                return taken;
            }
        }
    }

    record PutMappingClusterStateUpdateTask(
        PutMappingClusterStateUpdateRequest request,
        ActionListener<AcknowledgedResponse> listener,
        Map<Index, PreflightCacheEntry> preflightCache
    ) implements ClusterStateTaskListener, ClusterStateAckListener {

        @Override
        public void onFailure(Exception e) {
            IOUtils.closeWhileHandlingException(preflightCache.values());
            preflightCache.clear();
            listener.onFailure(e);
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public void onAllNodesAcked() {
            listener.onResponse(AcknowledgedResponse.of(true));
        }

        @Override
        public void onAckFailure(Exception e) {
            listener.onResponse(AcknowledgedResponse.of(false));
        }

        @Override
        public void onAckTimeout() {
            listener.onResponse(AcknowledgedResponse.FALSE);
        }

        @Override
        public TimeValue ackTimeout() {
            return request.ackTimeout();
        }
    }

    class PutMappingExecutor implements ClusterStateTaskExecutor<PutMappingClusterStateUpdateTask> {
        private final IndexSettingProviders indexSettingProviders;

        PutMappingExecutor() {
            this(IndexSettingProviders.EMPTY);
        }

        PutMappingExecutor(IndexSettingProviders indexSettingProviders) {
            this.indexSettingProviders = indexSettingProviders;
        }

        @Override
        public ClusterState execute(BatchExecutionContext<PutMappingClusterStateUpdateTask> batchExecutionContext) throws Exception {
            // indexMapperServices holds the last successfully committed MapperService state per index.
            // Each task gets its own private taskServices map; on success it is promoted here so that
            // subsequent tasks in the same batch see the accumulated merged state.
            Map<Index, MapperService> indexMapperServices = new HashMap<>();
            try {
                var currentState = batchExecutionContext.initialState();
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    final var task = taskContext.getTask();
                    final PutMappingClusterStateUpdateRequest request = task.request;
                    Map<Index, MapperService> taskServices = new HashMap<>();
                    // activeEntries tracks which indices were served from the pre-flight cache so that
                    // applyRequest can skip the merge call for those indices.
                    Map<Index, PreflightCacheEntry> activeEntries = new HashMap<>();
                    boolean succeeded = false;
                    try (var ignored = taskContext.captureResponseHeaders()) {
                        for (Index index : request.indices()) {
                            currentState.projectState(currentState.metadata().projectFor(index).id()).ensureProjectNotUnderDeletion();
                            final IndexMetadata indexMetadata = currentState.metadata().indexMetadata(index);
                            if (indexMetadata == null) {
                                throw new IllegalStateException("index [" + index.getName() + "] not found in cluster state");
                            }
                            if (indexMapperServices.containsKey(index)) {
                                // Already loaded by a prior task in this batch; reuse from the committed map.
                                // The task's own cache entry for this index (if any) is unconsumed and will be
                                // closed by the success-path finally block below.
                                taskServices.put(index, indexMapperServices.get(index));
                            } else {
                                taskServices.put(index, loadService(task, index, indexMetadata, activeEntries));
                            }
                        }
                        currentState = applyRequest(currentState, request, taskServices, activeEntries);
                        taskContext.success(task);
                        succeeded = true;
                        indexMapperServices.putAll(taskServices);
                    } catch (Exception e) {
                        // Close task-private services that were not promoted to the committed map.
                        for (var entry : taskServices.entrySet()) {
                            if (indexMapperServices.containsKey(entry.getKey()) == false) {
                                IOUtils.closeWhileHandlingException(entry.getValue());
                            }
                        }
                        // task.onFailure closes and clears the remaining preflightCache entries.
                        taskContext.onFailure(e);
                    } finally {
                        if (succeeded) {
                            // Close unconsumed cache entries: those for indices that were already in
                            // indexMapperServices and therefore never loaded into taskServices.
                            IOUtils.closeWhileHandlingException(task.preflightCache.values());
                            task.preflightCache.clear();
                        }
                    }
                }
                return currentState;
            } finally {
                IOUtils.close(indexMapperServices.values());
            }
        }

        /**
         * Returns a {@link MapperService} ready for use in {@link #applyRequest}: either a validated
         * pre-flight service (with both MAPPING_RECOVERY and the update merge already applied) when the
         * cache is fresh, or a newly created service with only MAPPING_RECOVERY applied.
         * <p>
         * On a cache hit the entry is moved into {@code activeEntries} so {@link #applyRequest} knows to
         * skip the merge call. On a stale hit or miss the consumed entry is closed immediately.
         */
        private MapperService loadService(
            PutMappingClusterStateUpdateTask task,
            Index index,
            IndexMetadata indexMetadata,
            Map<Index, PreflightCacheEntry> activeEntries
        ) throws IOException {
            // remove() consumes the entry so task.onFailure cannot double-close it.
            final PreflightCacheEntry cached = task.preflightCache.remove(index);
            if (cached != null
                && cached.mappingVersion() == indexMetadata.getMappingVersion()
                && cached.settingsVersion() == indexMetadata.getSettingsVersion()) {
                // Cache hit: mapping and settings unchanged since pre-flight.
                activeEntries.put(index, cached);
                return cached.mergedService();
            }
            // Cache miss or stale (concurrent mapping/settings update); close the stale entry if present.
            IOUtils.closeWhileHandlingException(cached);
            MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata);
            try {
                // add mappings for all types, we need them for cross-type validation
                mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
            } catch (Exception e) {
                IOUtils.closeWhileHandlingException(mapperService);
                throw e;
            }
            return mapperService;
        }

        private ClusterState applyRequest(
            ClusterState currentState,
            PutMappingClusterStateUpdateRequest request,
            Map<Index, MapperService> taskServices,
            Map<Index, PreflightCacheEntry> activeEntries
        ) {
            MergeReason reason = request.autoUpdate() ? MergeReason.MAPPING_AUTO_UPDATE : MergeReason.MAPPING_UPDATE;
            Metadata.Builder builder = Metadata.builder(currentState.metadata());
            boolean updated = false;
            for (Index index : request.indices()) {
                // IMPORTANT: always get the metadata from the state since it get's batched
                // and if we pull it from the indexService we might miss an update etc.
                final ProjectMetadata projectMetadata = currentState.metadata().projectFor(index);
                final IndexMetadata indexMetadata = projectMetadata.index(index);
                final MapperService mapperService = taskServices.get(index);

                final CompressedXContent existingSource;
                final DocumentMapper mergedMapper;
                final PreflightCacheEntry cached = activeEntries.get(index);
                if (cached != null) {
                    // Cache hit: the merge was already performed on the MANAGEMENT thread; skip it here.
                    existingSource = cached.preUpdateSource();
                    mergedMapper = mapperService.documentMapper();
                } else {
                    existingSource = mapperService.documentMapper() != null ? mapperService.documentMapper().mappingSource() : null;
                    mergedMapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, request.source(), reason);
                }

                CompressedXContent updatedSource = mergedMapper.mappingSource();
                // If the mapping source is the same after merging, then we have no real update, so we skip modifying this index.
                if (updatedSource.equals(existingSource)) {
                    continue;
                }
                logMappingResult(index, existingSource, updatedSource, mergedMapper.type());

                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                // Mapping updates on a single type may have side-effects on other types so we need to
                // update mapping metadata on all types
                indexMetadataBuilder.putMapping(new MappingMetadata(mergedMapper));
                indexMetadataBuilder.putInferenceFields(mergedMapper.mappers().inferenceFields());
                boolean updatedSettings = false;
                final Settings.Builder additionalIndexSettings = Settings.builder();
                indexMetadataBuilder.mappingVersion(1 + indexMetadataBuilder.mappingVersion())
                    .mappingsUpdatedVersion(IndexVersion.current());
                for (IndexSettingProvider provider : indexSettingProviders.getIndexSettingProviders()) {
                    Settings.Builder newAdditionalSettingsBuilder = Settings.builder();
                    provider.onUpdateMappings(indexMetadata, mergedMapper, newAdditionalSettingsBuilder);
                    if (newAdditionalSettingsBuilder.keys().isEmpty() == false) {
                        Settings newAdditionalSettings = newAdditionalSettingsBuilder.build();
                        MetadataCreateIndexService.validateAdditionalSettings(provider, newAdditionalSettings, additionalIndexSettings);
                        additionalIndexSettings.put(newAdditionalSettings);
                        updatedSettings = true;
                    }
                }
                if (updatedSettings) {
                    final Settings.Builder indexSettingsBuilder = Settings.builder();
                    indexSettingsBuilder.put(indexMetadata.getSettings());
                    indexSettingsBuilder.put(additionalIndexSettings.build());
                    indexMetadataBuilder.settings(indexSettingsBuilder.build());
                    indexMetadataBuilder.settingsVersion(1 + indexMetadata.getSettingsVersion());
                }
                /*
                 * This implicitly increments the index metadata version and builds the index metadata. This means that we need to have
                 * already incremented the mapping version if necessary. Therefore, the mapping version increment must remain before this
                 * statement.
                 */
                builder.getProject(projectMetadata.id()).put(indexMetadataBuilder);
                updated = true;
            }
            if (updated) {
                return ClusterState.builder(currentState).metadata(builder).build();
            } else {
                return currentState;
            }
        }

        private void logMappingResult(Index index, CompressedXContent existingSource, CompressedXContent updatedSource, String type) {
            if (existingSource != null) {
                if (existingSource.equals(updatedSource) == false) { // source has changed
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} update_mapping [{}] with source [{}]", index, type, updatedSource);
                    } else if (logger.isInfoEnabled()) {
                        logger.info("{} update_mapping [{}]", index, type);
                    }
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} create_mapping with source [{}]", index, updatedSource);
                } else if (logger.isInfoEnabled()) {
                    logger.info("{} create_mapping", index);
                }
            }
        }

    }

    public void putMapping(final PutMappingClusterStateUpdateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        final PreflightResult preflightResult;
        try {
            preflightResult = isWholeRequestNoop(request);
        } catch (Exception e) {
            // If an exception occurs while checking for no-op, we can return early and avoid submitting a cluster state update task.
            listener.onFailure(e);
            return;
        }

        if (preflightResult instanceof PreflightResult.Noop) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        // TODO: instead of considering the whole request as a no-op, we could filter out indices that don't need an update and only
        // apply the update to the remaining ones.
        final var needsUpdate = (PreflightResult.NeedsUpdate) preflightResult;
        taskQueue.submitTask(
            "put-mapping " + Strings.arrayToCommaDelimitedString(request.indices()),
            new PutMappingClusterStateUpdateTask(request, listener, needsUpdate.takeCache()),
            MasterService.maybeLimitMasterNodeTimeout(request.masterNodeTimeout(), maxMasterNodeTimeout)
        );
    }

    private PreflightResult isWholeRequestNoop(final PutMappingClusterStateUpdateRequest request) throws IOException {
        // To check if the mapping update is a no-op, we will parse and merge the mapping with every index. This can be expensive with
        // large mappings (or many indices), so we need to do this on the management thread pool.
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);
        final ClusterState state = clusterService.state();
        final MergeReason reason = request.autoUpdate() ? MergeReason.MAPPING_AUTO_UPDATE : MergeReason.MAPPING_UPDATE;
        boolean isNoop = true;
        final Map<Index, PreflightCacheEntry> cache = new HashMap<>();
        try {
            for (Index index : request.indices()) {
                var project = state.metadata().lookupProject(index);
                if (project.isEmpty()) {
                    // this is a race condition where the project got deleted from under a mapping update task
                    isNoop = false;
                    continue;
                }
                final IndexMetadata indexMetadata = project.get().index(index);
                if (indexMetadata == null) {
                    // local store recovery sends a mapping update request during application of a cluster state on the data node which we
                    // might receive here before the CS update that created the index has been applied on all nodes and thus the index
                    // isn't found in the state yet, but will be visible to the CS update below
                    isNoop = false;
                    continue;
                }
                final MappingMetadata mappingMetadata = indexMetadata.mapping();
                if (mappingMetadata == null) {
                    isNoop = false;
                    continue;
                }
                // If the mapping sources are already equal, then we already know this index would be a no-op and can skip further checks.
                if (request.source().equals(mappingMetadata.source())) {
                    continue;
                }
                // We check if applying the mapping would result in any changes by merging the mapping update with the existing mapping.
                // If the resulting mapping source is different, then we have a real update. Otherwise, we can skip the cluster state
                // update. Just comparing the mapping update source with the existing mapping isn't sufficient, because the mapper service
                // might add or remove certain default values, which would make the simple comparison fail even though the effective
                // mapping is the same.
                // The pre-update source is captured after MAPPING_RECOVERY (normalized) rather than from mappingMetadata.source()
                // (stored). Comparing against the post-RECOVERY source is consistent with how applyRequest determines existingSource
                // and is more correct: a mapping with an unnormalized stored source that normalizes to the same value after RECOVERY
                // is treated as a real update rather than a silent noop.
                final MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata);
                try {
                    mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
                    final CompressedXContent preUpdateSource = mapperService.documentMapper() != null
                        ? mapperService.documentMapper().mappingSource()
                        : null;
                    final DocumentMapper mergedMapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, request.source(), reason);
                    final CompressedXContent updatedSource = mergedMapper.mappingSource();
                    if (updatedSource.equals(preUpdateSource)) {
                        // Noop for this index; close the service immediately.
                        mapperService.close();
                    } else {
                        isNoop = false;
                        cache.put(
                            index,
                            new PreflightCacheEntry(
                                indexMetadata.getMappingVersion(),
                                indexMetadata.getSettingsVersion(),
                                preUpdateSource,
                                mapperService
                            )
                        );
                    }
                } catch (Exception e) {
                    IOUtils.closeWhileHandlingException(mapperService);
                    throw e;
                }
            }
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(cache.values());
            throw e;
        }

        if (isNoop) {
            assert cache.isEmpty();
            return new PreflightResult.Noop();
        }
        return new PreflightResult.NeedsUpdate(cache);
    }
}
