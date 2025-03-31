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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service responsible for submitting mapping changes
 */
public class MetadataMappingService {

    private static final Logger logger = LogManager.getLogger(MetadataMappingService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    private final MasterServiceTaskQueue<PutMappingClusterStateUpdateTask> taskQueue;

    @Inject
    public MetadataMappingService(ClusterService clusterService, IndicesService indicesService) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.taskQueue = clusterService.createTaskQueue("put-mapping", Priority.HIGH, new PutMappingExecutor());
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

        private static ClusterState applyRequest(
            ClusterState currentState,
            PutMappingClusterStateUpdateRequest request,
            Map<Index, MapperService> indexMapperServices
        ) {

            final CompressedXContent mappingUpdateSource = request.source();
            final Metadata metadata = currentState.metadata();
            final List<IndexMetadata> updateList = new ArrayList<>();
            MergeReason reason = request.autoUpdate() ? MergeReason.MAPPING_AUTO_UPDATE : MergeReason.MAPPING_UPDATE;
            for (Index index : request.indices()) {
                MapperService mapperService = indexMapperServices.get(index);
                // IMPORTANT: always get the metadata from the state since it get's batched
                // and if we pull it from the indexService we might miss an update etc.
                final IndexMetadata indexMetadata = metadata.indexMetadata(index);
                DocumentMapper existingMapper = mapperService.documentMapper();
                if (existingMapper != null && existingMapper.mappingSource().equals(mappingUpdateSource)) {
                    continue;
                }
                // this is paranoia... just to be sure we use the exact same metadata tuple on the update that
                // we used for the validation, it makes this mechanism little less scary (a little)
                updateList.add(indexMetadata);
                // try and parse it (no need to add it here) so we can bail early in case of parsing exception
                // first, simulate: just call merge and ignore the result
                Mapping mapping = mapperService.parseMapping(MapperService.SINGLE_MAPPING_NAME, reason, mappingUpdateSource);
                MapperService.mergeMappings(mapperService.documentMapper(), mapping, reason, mapperService.getIndexSettings());
            }
            Metadata.Builder builder = Metadata.builder(metadata);
            boolean updated = false;
            for (IndexMetadata indexMetadata : updateList) {
                boolean updatedMapping = false;
                // do the actual merge here on the master, and update the mapping source
                // we use the exact same indexService and metadata we used to validate above here to actually apply the update
                final Index index = indexMetadata.getIndex();
                final MapperService mapperService = indexMapperServices.get(index);

                CompressedXContent existingSource = null;
                DocumentMapper existingMapper = mapperService.documentMapper();
                if (existingMapper != null) {
                    existingSource = existingMapper.mappingSource();
                }
                DocumentMapper mergedMapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mappingUpdateSource, reason);
                CompressedXContent updatedSource = mergedMapper.mappingSource();

                if (existingSource != null) {
                    if (existingSource.equals(updatedSource)) {
                        // same source, no changes, ignore it
                    } else {
                        updatedMapping = true;
                        // use the merged mapping source
                        if (logger.isDebugEnabled()) {
                            logger.debug("{} update_mapping [{}] with source [{}]", index, mergedMapper.type(), updatedSource);
                        } else if (logger.isInfoEnabled()) {
                            logger.info("{} update_mapping [{}]", index, mergedMapper.type());
                        }

                    }
                } else {
                    updatedMapping = true;
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} create_mapping with source [{}]", index, updatedSource);
                    } else if (logger.isInfoEnabled()) {
                        logger.info("{} create_mapping", index);
                    }
                }

                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                // Mapping updates on a single type may have side-effects on other types so we need to
                // update mapping metadata on all types
                DocumentMapper docMapper = mapperService.documentMapper();
                if (docMapper != null) {
                    indexMetadataBuilder.putMapping(new MappingMetadata(docMapper));
                    indexMetadataBuilder.putInferenceFields(docMapper.mappers().inferenceFields());
                }
                if (updatedMapping) {
                    indexMetadataBuilder.mappingVersion(1 + indexMetadataBuilder.mappingVersion())
                        .mappingsUpdatedVersion(IndexVersion.current());
                }
                /*
                 * This implicitly increments the index metadata version and builds the index metadata. This means that we need to have
                 * already incremented the mapping version if necessary. Therefore, the mapping version increment must remain before this
                 * statement.
                 */
                builder.getProject(metadata.projectFor(index).id()).put(indexMetadataBuilder);
                updated |= updatedMapping;
            }
            if (updated) {
                return ClusterState.builder(currentState).metadata(builder).build();
            } else {
                return currentState;
            }
        }

    }

    public void putMapping(final PutMappingClusterStateUpdateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        final ClusterState state = clusterService.state();
        boolean noop = true;
        for (Index index : request.indices()) {
            var project = state.metadata().lookupProject(index);
            if (project.isEmpty()) {
                // this is a race condition where the project got deleted from under a mapping update task
                noop = false;
                break;
            }
            final IndexMetadata indexMetadata = project.get().index(index);
            if (indexMetadata == null) {
                // local store recovery sends a mapping update request during application of a cluster state on the data node which we might
                // receive here before the CS update that created the index has been applied on all nodes and thus the index isn't found in
                // the state yet, but will be visible to the CS update below
                noop = false;
                break;
            }
            final MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata == null) {
                noop = false;
                break;
            }
            if (request.source().equals(mappingMetadata.source()) == false) {
                noop = false;
                break;
            }
        }
        if (noop) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        taskQueue.submitTask(
            "put-mapping " + Strings.arrayToCommaDelimitedString(request.indices()),
            new PutMappingClusterStateUpdateTask(request, listener),
            request.masterNodeTimeout()
        );
    }
}
