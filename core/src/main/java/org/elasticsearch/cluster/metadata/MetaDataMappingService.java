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

package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidTypeNameException;
import org.elasticsearch.percolator.PercolatorService;

import java.io.IOException;
import java.util.*;

/**
 * Service responsible for submitting mapping changes
 */
public class MetaDataMappingService extends AbstractComponent {

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    final ClusterStateTaskExecutor<RefreshTask> refreshExecutor = new RefreshTaskExecutor();
    final ClusterStateTaskExecutor<PutMappingClusterStateUpdateRequest> putMappingExecutor = new PutMappingExecutor();

    @Inject
    public MetaDataMappingService(Settings settings, ClusterService clusterService, IndicesService indicesService) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    static class RefreshTask {
        final String index;
        final String indexUUID;

        RefreshTask(String index, final String indexUUID) {
            this.index = index;
            this.indexUUID = indexUUID;
        }
    }

    class RefreshTaskExecutor extends ClusterStateTaskExecutor<RefreshTask> {
        @Override
        public BatchResult<RefreshTask> execute(ClusterState currentState, List<RefreshTask> tasks) throws Exception {
            ClusterState newClusterState = executeRefresh(currentState, tasks);
            return BatchResult.<RefreshTask>builder().successes(tasks).build(newClusterState);
        }
    }

    /**
     * Batch method to apply all the queued refresh operations. The idea is to try and batch as much
     * as possible so we won't create the same index all the time for example for the updates on the same mapping
     * and generate a single cluster change event out of all of those.
     */
    ClusterState executeRefresh(final ClusterState currentState, final List<RefreshTask> allTasks) throws Exception {
        // break down to tasks per index, so we can optimize the on demand index service creation
        // to only happen for the duration of a single index processing of its respective events
        Map<String, List<RefreshTask>> tasksPerIndex = new HashMap<>();
        for (RefreshTask refreshTask : allTasks) {
            if (refreshTask.index == null) {
                logger.debug("ignoring a mapping task of type [{}] with a null index.", refreshTask);
            }
            List<RefreshTask> indexTasks = tasksPerIndex.get(refreshTask.index);
            if (indexTasks == null) {
                indexTasks = new ArrayList<>();
                tasksPerIndex.put(refreshTask.index, indexTasks);
            }
            indexTasks.add(refreshTask);
        }

        boolean dirty = false;
        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());

        for (Map.Entry<String, List<RefreshTask>> entry : tasksPerIndex.entrySet()) {
            String index = entry.getKey();
            IndexMetaData indexMetaData = mdBuilder.get(index);
            if (indexMetaData == null) {
                // index got deleted on us, ignore...
                logger.debug("[{}] ignoring tasks - index meta data doesn't exist", index);
                continue;
            }
            // the tasks lists to iterate over, filled with the list of mapping tasks, trying to keep
            // the latest (based on order) update mapping one per node
            List<RefreshTask> allIndexTasks = entry.getValue();
            boolean hasTaskWithRightUUID = false;
            for (RefreshTask task : allIndexTasks) {
                if (indexMetaData.isSameUUID(task.indexUUID)) {
                    hasTaskWithRightUUID = true;
                } else {
                    logger.debug("[{}] ignoring task [{}] - index meta data doesn't match task uuid", index, task);
                }
            }
            if (hasTaskWithRightUUID == false) {
                continue;
            }

            // construct the actual index if needed, and make sure the relevant mappings are there
            boolean removeIndex = false;
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                // we need to create the index here, and add the current mapping to it, so we can merge
                indexService = indicesService.createIndex(indexMetaData.getIndex(), indexMetaData.getSettings(), currentState.nodes().localNode().id());
                removeIndex = true;
                for (ObjectCursor<MappingMetaData> metaData : indexMetaData.getMappings().values()) {
                    // don't apply the default mapping, it has been applied when the mapping was created
                    indexService.mapperService().merge(metaData.value.type(), metaData.value.source(), MapperService.MergeReason.MAPPING_RECOVERY, true);
                }
            }

            IndexMetaData.Builder builder = IndexMetaData.builder(indexMetaData);
            try {
                boolean indexDirty = refreshIndexMapping(indexService, builder);
                if (indexDirty) {
                    mdBuilder.put(builder);
                    dirty = true;
                }
            } finally {
                if (removeIndex) {
                    indicesService.removeIndex(index, "created for mapping processing");
                }
            }
        }

        if (!dirty) {
            return currentState;
        }
        return ClusterState.builder(currentState).metaData(mdBuilder).build();
    }

    private boolean refreshIndexMapping(IndexService indexService, IndexMetaData.Builder builder) {
        boolean dirty = false;
        String index = indexService.index().name();
        try {
            List<String> updatedTypes = new ArrayList<>();
            for (DocumentMapper mapper : indexService.mapperService().docMappers(true)) {
                final String type = mapper.type();
                if (!mapper.mappingSource().equals(builder.mapping(type).source())) {
                    updatedTypes.add(type);
                }
            }
            // if a single type is not up-to-date, re-send everything
            if (updatedTypes.isEmpty() == false) {
                logger.warn("[{}] re-syncing mappings with cluster state because of types [{}]", index, updatedTypes);
                dirty = true;
                for (DocumentMapper mapper : indexService.mapperService().docMappers(true)) {
                    builder.putMapping(new MappingMetaData(mapper));
                }
            }
        } catch (Throwable t) {
            logger.warn("[{}] failed to refresh-mapping in cluster state", t, index);
        }
        return dirty;
    }

    /**
     * Refreshes mappings if they are not the same between original and parsed version
     */
    public void refreshMapping(final String index, final String indexUUID) {
        final RefreshTask refreshTask = new RefreshTask(index, indexUUID);
        clusterService.submitStateUpdateTask("refresh-mapping [" + index + "]",
            refreshTask,
            BasicClusterStateTaskConfig.create(Priority.HIGH),
            refreshExecutor,
            new AbstractClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Throwable t) {
                    logger.warn("failure during [{}]", t, source);
                }
            });
    }

    class PutMappingExecutor extends ClusterStateTaskExecutor<PutMappingClusterStateUpdateRequest> {
        @Override
        public BatchResult<PutMappingClusterStateUpdateRequest> execute(ClusterState currentState, List<PutMappingClusterStateUpdateRequest> tasks) throws Exception {
            Set<String> indicesToClose = new HashSet<>();
            BatchResult.Builder<PutMappingClusterStateUpdateRequest> builder = BatchResult.builder();
            try {
                // precreate incoming indices;
                for (PutMappingClusterStateUpdateRequest request : tasks) {
                    // failures here mean something is broken with our cluster state - fail all tasks by letting exceptions bubble up
                    for (String index : request.indices()) {
                        final IndexMetaData indexMetaData = currentState.metaData().index(index);
                        if (indexMetaData != null  && indicesService.hasIndex(index) == false) {
                            // if we don't have the index, we will throw exceptions later;
                            indicesToClose.add(index);
                            IndexService indexService = indicesService.createIndex(indexMetaData.getIndex(), indexMetaData.getSettings(), clusterService.localNode().id());
                            // add mappings for all types, we need them for cross-type validation
                            for (ObjectCursor<MappingMetaData> mapping : indexMetaData.getMappings().values()) {
                                indexService.mapperService().merge(mapping.value.type(), mapping.value.source(), MapperService.MergeReason.MAPPING_RECOVERY, request.updateAllTypes());
                            }
                        }
                    }
                }
                for (PutMappingClusterStateUpdateRequest request : tasks) {
                    try {
                        currentState = applyRequest(currentState, request);
                        builder.success(request);
                    } catch (Throwable t) {
                        builder.failure(request, t);
                    }
                }

                return builder.build(currentState);
            } finally {
                for (String index : indicesToClose) {
                    indicesService.removeIndex(index, "created for mapping processing");
                }
            }
        }

        private ClusterState applyRequest(ClusterState currentState, PutMappingClusterStateUpdateRequest request) throws IOException {
            String mappingType = request.type();
            CompressedXContent mappingUpdateSource = new CompressedXContent(request.source());
            for (String index : request.indices()) {
                IndexService indexService = indicesService.indexServiceSafe(index);
                // try and parse it (no need to add it here) so we can bail early in case of parsing exception
                DocumentMapper newMapper;
                DocumentMapper existingMapper = indexService.mapperService().documentMapper(request.type());
                if (MapperService.DEFAULT_MAPPING.equals(request.type())) {
                    // _default_ types do not go through merging, but we do test the new settings. Also don't apply the old default
                    newMapper = indexService.mapperService().parse(request.type(), mappingUpdateSource, false);
                } else {
                    newMapper = indexService.mapperService().parse(request.type(), mappingUpdateSource, existingMapper == null);
                    if (existingMapper != null) {
                        // first, simulate: just call merge and ignore the result
                        existingMapper.merge(newMapper.mapping(), request.updateAllTypes());
                    } else {
                        // TODO: can we find a better place for this validation?
                        // The reason this validation is here is that the mapper service doesn't learn about
                        // new types all at once , which can create a false error.

                        // For example in MapperService we can't distinguish between a create index api call
                        // and a put mapping api call, so we don't which type did exist before.
                        // Also the order of the mappings may be backwards.
                        if (Version.indexCreated(indexService.indexSettings()).onOrAfter(Version.V_2_0_0_beta1) && newMapper.parentFieldMapper().active()) {
                            IndexMetaData indexMetaData = currentState.metaData().index(index);
                            for (ObjectCursor<MappingMetaData> mapping : indexMetaData.getMappings().values()) {
                                if (newMapper.parentFieldMapper().type().equals(mapping.value.type())) {
                                    throw new IllegalArgumentException("can't add a _parent field that points to an already existing type");
                                }
                            }
                        }
                    }
                }
                if (mappingType == null) {
                    mappingType = newMapper.type();
                } else if (mappingType.equals(newMapper.type()) == false) {
                    throw new InvalidTypeNameException("Type name provided does not match type name within mapping definition");
                }
            }
            assert mappingType != null;

            if (!MapperService.DEFAULT_MAPPING.equals(mappingType) && !PercolatorService.TYPE_NAME.equals(mappingType) && mappingType.charAt(0) == '_') {
                throw new InvalidTypeNameException("Document mapping type name can't start with '_'");
            }
            MetaData.Builder builder = MetaData.builder(currentState.metaData());
            for (String index : request.indices()) {
                // do the actual merge here on the master, and update the mapping source
                IndexService indexService = indicesService.indexService(index);
                if (indexService == null) {
                    continue;
                }

                CompressedXContent existingSource = null;
                DocumentMapper existingMapper = indexService.mapperService().documentMapper(mappingType);
                if (existingMapper != null) {
                    existingSource = existingMapper.mappingSource();
                }
                DocumentMapper mergedMapper = indexService.mapperService().merge(mappingType, mappingUpdateSource, MapperService.MergeReason.MAPPING_UPDATE, request.updateAllTypes());
                CompressedXContent updatedSource = mergedMapper.mappingSource();

                if (existingSource != null) {
                    if (existingSource.equals(updatedSource)) {
                        // same source, no changes, ignore it
                    } else {
                        // use the merged mapping source
                        if (logger.isDebugEnabled()) {
                            logger.debug("[{}] update_mapping [{}] with source [{}]", index, mergedMapper.type(), updatedSource);
                        } else if (logger.isInfoEnabled()) {
                            logger.info("[{}] update_mapping [{}]", index, mergedMapper.type());
                        }
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] create_mapping [{}] with source [{}]", index, mappingType, updatedSource);
                    } else if (logger.isInfoEnabled()) {
                        logger.info("[{}] create_mapping [{}]", index, mappingType);
                    }
                }

                IndexMetaData indexMetaData = currentState.metaData().index(index);
                if (indexMetaData == null) {
                    throw new IndexNotFoundException(index);
                }
                IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexMetaData);
                // Mapping updates on a single type may have side-effects on other types so we need to
                // update mapping metadata on all types
                for (DocumentMapper mapper : indexService.mapperService().docMappers(true)) {
                    indexMetaDataBuilder.putMapping(new MappingMetaData(mapper.mappingSource()));
                }
                builder.put(indexMetaDataBuilder);
            }

            return ClusterState.builder(currentState).metaData(builder).build();
        }
    }

    public void putMapping(final PutMappingClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("put-mapping [" + request.type() + "]",
            request,
            BasicClusterStateTaskConfig.create(Priority.HIGH, request.masterNodeTimeout()),
            putMappingExecutor,
            new AbstractAckedClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Throwable t) {
                    listener.onFailure(t);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return true;
                }

                @Override
                public void onAllNodesAcked(@Nullable Throwable t) {
                    listener.onResponse(new ClusterStateUpdateResponse(true));
                }

                @Override
                public void onAckTimeout() {
                    listener.onResponse(new ClusterStateUpdateResponse(false));
                }

                @Override
                public TimeValue ackTimeout() {
                    return request.ackTimeout();
                }
            });
    }
}
