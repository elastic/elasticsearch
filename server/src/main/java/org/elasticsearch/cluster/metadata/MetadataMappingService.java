/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
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

    final PutMappingExecutor putMappingExecutor = new PutMappingExecutor();


    @Inject
    public MetadataMappingService(ClusterService clusterService, IndicesService indicesService) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    class PutMappingExecutor implements ClusterStateTaskExecutor<PutMappingClusterStateUpdateRequest> {
        @Override
        public ClusterTasksResult<PutMappingClusterStateUpdateRequest>
        execute(ClusterState currentState, List<PutMappingClusterStateUpdateRequest> tasks) throws Exception {
            Map<Index, MapperService> indexMapperServices = new HashMap<>();
            ClusterTasksResult.Builder<PutMappingClusterStateUpdateRequest> builder = ClusterTasksResult.builder();
            try {
                for (PutMappingClusterStateUpdateRequest request : tasks) {
                    try {
                        for (Index index : request.indices()) {
                            final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
                            if (indexMapperServices.containsKey(indexMetadata.getIndex()) == false) {
                                MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);
                                indexMapperServices.put(index, mapperService);
                                // add mappings for all types, we need them for cross-type validation
                                mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
                            }
                        }
                        currentState = applyRequest(currentState, request, indexMapperServices);
                        builder.success(request);
                    } catch (Exception e) {
                        builder.failure(request, e);
                    }
                }
                return builder.build(currentState);
            } finally {
                IOUtils.close(indexMapperServices.values());
            }
        }

        private ClusterState applyRequest(ClusterState currentState, PutMappingClusterStateUpdateRequest request,
                                          Map<Index, MapperService> indexMapperServices) throws IOException {

            CompressedXContent mappingUpdateSource = new CompressedXContent(request.source());
            final Metadata metadata = currentState.metadata();
            final List<IndexMetadata> updateList = new ArrayList<>();
            for (Index index : request.indices()) {
                MapperService mapperService = indexMapperServices.get(index);
                // IMPORTANT: always get the metadata from the state since it get's batched
                // and if we pull it from the indexService we might miss an update etc.
                final IndexMetadata indexMetadata = currentState.getMetadata().getIndexSafe(index);

                // this is paranoia... just to be sure we use the exact same metadata tuple on the update that
                // we used for the validation, it makes this mechanism little less scary (a little)
                updateList.add(indexMetadata);
                // try and parse it (no need to add it here) so we can bail early in case of parsing exception
                // first, simulate: just call merge and ignore the result
                Mapping mapping = mapperService.parseMapping(MapperService.SINGLE_MAPPING_NAME, mappingUpdateSource);
                MapperService.mergeMappings(mapperService.documentMapper(), mapping, MergeReason.MAPPING_UPDATE);
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
                DocumentMapper mergedMapper
                    = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mappingUpdateSource, MergeReason.MAPPING_UPDATE);
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
                DocumentMapper mapper = mapperService.documentMapper();
                if (mapper != null) {
                    indexMetadataBuilder.putMapping(new MappingMetadata(mapper.mappingSource()));
                }
                if (updatedMapping) {
                    indexMetadataBuilder.mappingVersion(1 + indexMetadataBuilder.mappingVersion());
                }
                /*
                 * This implicitly increments the index metadata version and builds the index metadata. This means that we need to have
                 * already incremented the mapping version if necessary. Therefore, the mapping version increment must remain before this
                 * statement.
                 */
                builder.put(indexMetadataBuilder);
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
        clusterService.submitStateUpdateTask("put-mapping " + Strings.arrayToCommaDelimitedString(request.indices()),
                request,
                ClusterStateTaskConfig.build(Priority.HIGH, request.masterNodeTimeout()),
                putMappingExecutor,
                new AckedClusterStateTaskListener() {

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public boolean mustAck(DiscoveryNode discoveryNode) {
                        return true;
                    }

                    @Override
                    public void onAllNodesAcked(@Nullable Exception e) {
                        listener.onResponse(AcknowledgedResponse.of(e == null));
                    }

                    @Override
                    public void onAckTimeout() {
                        listener.onResponse(AcknowledgedResponse.FALSE);
                    }

                    @Override
                    public TimeValue ackTimeout() {
                        return request.ackTimeout();
                    }
                });
    }
}
