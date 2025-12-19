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

    private static final Logger logger = LogManager.getLogger(MetadataMappingService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    private final MasterServiceTaskQueue<PutMappingClusterStateUpdateTask> taskQueue;

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
    }

    record PutMappingClusterStateUpdateTask(PutMappingClusterStateUpdateRequest request, ActionListener<AcknowledgedResponse> listener)
        implements
            ClusterStateTaskListener,
            ClusterStateAckListener {

        @Override
        public void onFailure(Exception e) {
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
            Map<Index, MapperService> indexMapperServices = new HashMap<>();
            try {
                var currentState = batchExecutionContext.initialState();
                for (final var taskContext : batchExecutionContext.taskContexts()) {
                    final var task = taskContext.getTask();
                    final PutMappingClusterStateUpdateRequest request = task.request;
                    try (var ignored = taskContext.captureResponseHeaders()) {
                        for (Index index : request.indices()) {
                            final IndexMetadata indexMetadata = currentState.metadata().indexMetadata(index);
                            if (indexMapperServices.containsKey(indexMetadata.getIndex()) == false) {
                                MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata);
                                indexMapperServices.put(index, mapperService);
                                // add mappings for all types, we need them for cross-type validation
                                mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
                            }
                        }
                        currentState = applyRequest(currentState, request, indexMapperServices);
                        taskContext.success(task);
                    } catch (Exception e) {
                        taskContext.onFailure(e);
                    }
                }
                return currentState;
            } finally {
                IOUtils.close(indexMapperServices.values());
            }
        }

        private ClusterState applyRequest(
            ClusterState currentState,
            PutMappingClusterStateUpdateRequest request,
            Map<Index, MapperService> indexMapperServices
        ) {
            MergeReason reason = request.autoUpdate() ? MergeReason.MAPPING_AUTO_UPDATE : MergeReason.MAPPING_UPDATE;
            Metadata.Builder builder = Metadata.builder(currentState.metadata());
            boolean updated = false;
            for (Index index : request.indices()) {
                // IMPORTANT: always get the metadata from the state since it get's batched
                // and if we pull it from the indexService we might miss an update etc.
                final ProjectMetadata projectMetadata = currentState.metadata().projectFor(index);
                final IndexMetadata indexMetadata = projectMetadata.index(index);
                final MapperService mapperService = indexMapperServices.get(index);

                CompressedXContent existingSource = mapperService.documentMapper() != null
                    ? mapperService.documentMapper().mappingSource()
                    : null;
                DocumentMapper mergedMapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, request.source(), reason);
                CompressedXContent updatedSource = mergedMapper.mappingSource();
                // If the mapping source is the same after merging, then we have no real update, so we skip modifying this index.
                if (updatedSource.equals(existingSource)) {
                    continue;
                }
                logMappingResult(index, existingSource, updatedSource, mergedMapper.type());

                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                // Mapping updates on a single type may have side-effects on other types so we need to
                // update mapping metadata on all types
                DocumentMapper docMapper = mapperService.documentMapper();
                if (docMapper != null) {
                    indexMetadataBuilder.putMapping(new MappingMetadata(docMapper));
                    indexMetadataBuilder.putInferenceFields(docMapper.mappers().inferenceFields());
                }
                boolean updatedSettings = false;
                final Settings.Builder additionalIndexSettings = Settings.builder();
                indexMetadataBuilder.mappingVersion(1 + indexMetadataBuilder.mappingVersion())
                    .mappingsUpdatedVersion(IndexVersion.current());
                for (IndexSettingProvider provider : indexSettingProviders.getIndexSettingProviders()) {
                    Settings.Builder newAdditionalSettingsBuilder = Settings.builder();
                    provider.onUpdateMappings(indexMetadata, docMapper, newAdditionalSettingsBuilder);
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
        try {
            // TODO: instead of considering the whole request as a no-op, we could filter out indices that don't need an update and only
            // apply the update to the remaining ones.
            if (isWholeRequestNoop(request)) {
                listener.onResponse(AcknowledgedResponse.TRUE);
                return;
            }
        } catch (Exception e) {
            // If an exception occurs while checking for no-op, we can return early and avoid submitting a cluster state update task.
            listener.onFailure(e);
            return;
        }

        taskQueue.submitTask(
            "put-mapping " + Strings.arrayToCommaDelimitedString(request.indices()),
            new PutMappingClusterStateUpdateTask(request, listener),
            request.masterNodeTimeout()
        );
    }

    private boolean isWholeRequestNoop(final PutMappingClusterStateUpdateRequest request) throws IOException {
        // To check if the mapping update is a no-op, we will parse and merge the mapping with every index. This can be expensive with
        // large mappings (or many indices), so we need to do this on the management thread pool.
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.MANAGEMENT);
        final ClusterState state = clusterService.state();
        final MergeReason reason = request.autoUpdate() ? MergeReason.MAPPING_AUTO_UPDATE : MergeReason.MAPPING_UPDATE;
        for (Index index : request.indices()) {
            var project = state.metadata().lookupProject(index);
            if (project.isEmpty()) {
                // this is a race condition where the project got deleted from under a mapping update task
                return false;
            }
            final IndexMetadata indexMetadata = project.get().index(index);
            if (indexMetadata == null) {
                // local store recovery sends a mapping update request during application of a cluster state on the data node which we might
                // receive here before the CS update that created the index has been applied on all nodes and thus the index isn't found in
                // the state yet, but will be visible to the CS update below
                return false;
            }
            final MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata == null) {
                return false;
            }
            // If the mapping sources are already equal, then we already know this index would be a no-op and can skip further checks.
            if (request.source().equals(mappingMetadata.source())) {
                continue;
            }
            // We check if applying the mapping would result in any changes by merging the mapping update with the existing mapping.
            // If the resulting mapping source is different, then we have a real update. Otherwise, we can skip the cluster state update.
            // Just comparing the mapping update source with the existing mapping isn't sufficient, because the mapper service might add or
            // remove certain default values, which would make the simple comparison fail even though the effective mapping is the same.
            // TODO: it's unfortunate that we throw away the mapping result here and have to re-merge it again during the actual update.
            // We could consider caching the result on the request object to avoid doing the same work twice. This would require some
            // checks to ensure the cached result is only used if the circumstances are the same (e.g., no changes to the index settings).
            try (MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata)) {
                mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
                DocumentMapper mergedMapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, request.source(), reason);
                CompressedXContent updatedSource = mergedMapper.mappingSource();
                if (updatedSource.equals(mappingMetadata.source()) == false) {
                    return false;
                }
            }
        }
        return true;
    }
}
