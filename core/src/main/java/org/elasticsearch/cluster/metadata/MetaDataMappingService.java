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
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
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

    final ClusterStateTaskExecutor<RefreshTask> refreshExectuor = new RefreshTaskExecutor();
    final ClusterStateTaskExecutor<PutMappingClusterStateUpdateRequest> putMappingExecutor = new PutMappingExecutor();
    private final NodeServicesProvider nodeServicesProvider;


    @Inject
    public MetaDataMappingService(Settings settings, ClusterService clusterService, IndicesService indicesService, NodeServicesProvider nodeServicesProvider) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.nodeServicesProvider = nodeServicesProvider;
    }

    static class RefreshTask {
        final String index;
        final String indexUUID;
        final String[] types;

        RefreshTask(String index, final String indexUUID, String[] types) {
            this.index = index;
            this.indexUUID = indexUUID;
            this.types = types;
        }
    }

    class RefreshTaskExecutor implements ClusterStateTaskExecutor<RefreshTask> {
        @Override
        public BatchResult<RefreshTask> execute(ClusterState currentState, List<RefreshTask> tasks) throws Exception {
            ClusterState newClusterState = executeRefresh(currentState, tasks);
            return BatchResult.<RefreshTask>builder().successes(tasks).build(newClusterState);
        }
    }

    /**
     * Batch method to apply all the queued refresh or update operations. The idea is to try and batch as much
     * as possible so we won't create the same index all the time for example for the updates on the same mapping
     * and generate a single cluster change event out of all of those.
     */
    ClusterState executeRefresh(final ClusterState currentState, final List<RefreshTask> allTasks) throws Exception {
        if (allTasks.isEmpty()) {
            return currentState;
        }

        // break down to tasks per index, so we can optimize the on demand index service creation
        // to only happen for the duration of a single index processing of its respective events
        Map<String, List<RefreshTask>> tasksPerIndex = new HashMap<>();
        for (RefreshTask task : allTasks) {
            if (task.index == null) {
                logger.debug("ignoring a mapping task of type [{}] with a null index.", task);
            }
            tasksPerIndex.computeIfAbsent(task.index, k -> new ArrayList<>()).add(task);
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
            List<RefreshTask> tasks = new ArrayList<>();
            for (RefreshTask task : allIndexTasks) {
                if (!indexMetaData.isSameUUID(task.indexUUID)) {
                    logger.debug("[{}] ignoring task [{}] - index meta data doesn't match task uuid", index, task);
                    continue;
                }
                tasks.add(task);
            }

            // construct the actual index if needed, and make sure the relevant mappings are there
            boolean removeIndex = false;
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                // we need to create the index here, and add the current mapping to it, so we can merge
                indexService = indicesService.createIndex(nodeServicesProvider, indexMetaData, Collections.EMPTY_LIST);
                removeIndex = true;
                Set<String> typesToIntroduce = new HashSet<>();
                for (RefreshTask task : tasks) {
                    Collections.addAll(typesToIntroduce, task.types);
                }
                for (String type : typesToIntroduce) {
                    // only add the current relevant mapping (if exists)
                    if (indexMetaData.getMappings().containsKey(type)) {
                        // don't apply the default mapping, it has been applied when the mapping was created
                        indexService.mapperService().merge(type, indexMetaData.getMappings().get(type).source(), false, true);
                    }
                }
            }

            IndexMetaData.Builder builder = IndexMetaData.builder(indexMetaData);
            try {
                boolean indexDirty = processIndexMappingTasks(tasks, indexService, builder);
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

    private boolean processIndexMappingTasks(List<RefreshTask> tasks, IndexService indexService, IndexMetaData.Builder builder) {
        boolean dirty = false;
        String index = indexService.index().name();
        // keep track of what we already refreshed, no need to refresh it again...
        Set<String> processedRefreshes = new HashSet<>();
        for (RefreshTask refreshTask : tasks) {
            try {
                List<String> updatedTypes = new ArrayList<>();
                for (String type : refreshTask.types) {
                    if (processedRefreshes.contains(type)) {
                        continue;
                    }
                    DocumentMapper mapper = indexService.mapperService().documentMapper(type);
                    if (mapper == null) {
                        continue;
                    }
                    if (!mapper.mappingSource().equals(builder.mapping(type).source())) {
                        updatedTypes.add(type);
                        builder.putMapping(new MappingMetaData(mapper));
                    }
                    processedRefreshes.add(type);
                }

                if (updatedTypes.isEmpty()) {
                    continue;
                }

                logger.warn("[{}] re-syncing mappings with cluster state for types [{}]", index, updatedTypes);
                dirty = true;
            } catch (Throwable t) {
                logger.warn("[{}] failed to refresh-mapping in cluster state, types [{}]", index, refreshTask.types);
            }
        }
        return dirty;
    }

    /**
     * Refreshes mappings if they are not the same between original and parsed version
     */
    public void refreshMapping(final String index, final String indexUUID, final String... types) {
        final RefreshTask refreshTask = new RefreshTask(index, indexUUID, types);
        clusterService.submitStateUpdateTask("refresh-mapping [" + index + "][" + Arrays.toString(types) + "]",
                refreshTask,
                ClusterStateTaskConfig.build(Priority.HIGH),
                refreshExectuor,
                (source, t) -> logger.warn("failure during [{}]", t, source)
        );
    }

    class PutMappingExecutor implements ClusterStateTaskExecutor<PutMappingClusterStateUpdateRequest> {
        @Override
        public BatchResult<PutMappingClusterStateUpdateRequest> execute(ClusterState currentState, List<PutMappingClusterStateUpdateRequest> tasks) throws Exception {
            Set<String> indicesToClose = new HashSet<>();
            BatchResult.Builder<PutMappingClusterStateUpdateRequest> builder = BatchResult.builder();
            try {
                // precreate incoming indices;
                for (PutMappingClusterStateUpdateRequest request : tasks) {
                    // failures here mean something is broken with our cluster state - fail all tasks by letting exceptions bubble up
                    for (String index : request.indices()) {
                        if (currentState.metaData().hasIndex(index)) {
                            // if we don't have the index, we will throw exceptions later;
                            if (indicesService.hasIndex(index) == false || indicesToClose.contains(index)) {
                                final IndexMetaData indexMetaData = currentState.metaData().index(index);
                                IndexService indexService;
                                if (indicesService.hasIndex(index) == false) {
                                    indicesToClose.add(index);
                                    indexService = indicesService.createIndex(nodeServicesProvider, indexMetaData, Collections.EMPTY_LIST);
                                    // make sure to add custom default mapping if exists
                                    if (indexMetaData.getMappings().containsKey(MapperService.DEFAULT_MAPPING)) {
                                        indexService.mapperService().merge(MapperService.DEFAULT_MAPPING, indexMetaData.getMappings().get(MapperService.DEFAULT_MAPPING).source(), false, request.updateAllTypes());
                                    }
                                } else {
                                    indexService = indicesService.indexService(index);
                                }
                                // only add the current relevant mapping (if exists)
                                if (indexMetaData.getMappings().containsKey(request.type())) {
                                    indexService.mapperService().merge(request.type(), indexMetaData.getMappings().get(request.type()).source(), false, request.updateAllTypes());
                                }
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
            Map<String, DocumentMapper> newMappers = new HashMap<>();
            Map<String, DocumentMapper> existingMappers = new HashMap<>();
            for (String index : request.indices()) {
                IndexService indexService = indicesService.indexServiceSafe(index);
                // try and parse it (no need to add it here) so we can bail early in case of parsing exception
                DocumentMapper newMapper;
                DocumentMapper existingMapper = indexService.mapperService().documentMapper(request.type());
                if (MapperService.DEFAULT_MAPPING.equals(request.type())) {
                    // _default_ types do not go through merging, but we do test the new settings. Also don't apply the old default
                    newMapper = indexService.mapperService().parse(request.type(), new CompressedXContent(request.source()), false);
                } else {
                    newMapper = indexService.mapperService().parse(request.type(), new CompressedXContent(request.source()), existingMapper == null);
                    if (existingMapper != null) {
                        // first, simulate
                        MergeResult mergeResult = existingMapper.merge(newMapper.mapping(), true, request.updateAllTypes());
                        // if we have conflicts, throw an exception
                        if (mergeResult.hasConflicts()) {
                            throw new MergeMappingException(mergeResult.buildConflicts());
                        }
                    } else {
                        // TODO: can we find a better place for this validation?
                        // The reason this validation is here is that the mapper service doesn't learn about
                        // new types all at once , which can create a false error.

                        // For example in MapperService we can't distinguish between a create index api call
                        // and a put mapping api call, so we don't which type did exist before.
                        // Also the order of the mappings may be backwards.
                        if (newMapper.parentFieldMapper().active()) {
                            IndexMetaData indexMetaData = currentState.metaData().index(index);
                            for (ObjectCursor<MappingMetaData> mapping : indexMetaData.getMappings().values()) {
                                if (newMapper.parentFieldMapper().type().equals(mapping.value.type())) {
                                    throw new IllegalArgumentException("can't add a _parent field that points to an already existing type");
                                }
                            }
                        }
                    }
                }
                newMappers.put(index, newMapper);
                if (existingMapper != null) {
                    existingMappers.put(index, existingMapper);
                }
            }

            String mappingType = request.type();
            if (mappingType == null) {
                mappingType = newMappers.values().iterator().next().type();
            } else if (!mappingType.equals(newMappers.values().iterator().next().type())) {
                throw new InvalidTypeNameException("Type name provided does not match type name within mapping definition");
            }
            if (!MapperService.DEFAULT_MAPPING.equals(mappingType) && !PercolatorService.TYPE_NAME.equals(mappingType) && mappingType.charAt(0) == '_') {
                throw new InvalidTypeNameException("Document mapping type name can't start with '_'");
            }
            final Map<String, MappingMetaData> mappings = new HashMap<>();
            for (Map.Entry<String, DocumentMapper> entry : newMappers.entrySet()) {
                String index = entry.getKey();
                // do the actual merge here on the master, and update the mapping source
                DocumentMapper newMapper = entry.getValue();
                IndexService indexService = indicesService.indexService(index);
                if (indexService == null) {
                    continue;
                }

                CompressedXContent existingSource = null;
                if (existingMappers.containsKey(entry.getKey())) {
                    existingSource = existingMappers.get(entry.getKey()).mappingSource();
                }
                DocumentMapper mergedMapper = indexService.mapperService().merge(newMapper.type(), newMapper.mappingSource(), false, request.updateAllTypes());
                CompressedXContent updatedSource = mergedMapper.mappingSource();

                if (existingSource != null) {
                    if (existingSource.equals(updatedSource)) {
                        // same source, no changes, ignore it
                    } else {
                        // use the merged mapping source
                        mappings.put(index, new MappingMetaData(mergedMapper));
                        if (logger.isDebugEnabled()) {
                            logger.debug("[{}] update_mapping [{}] with source [{}]", index, mergedMapper.type(), updatedSource);
                        } else if (logger.isInfoEnabled()) {
                            logger.info("[{}] update_mapping [{}]", index, mergedMapper.type());
                        }

                    }
                } else {
                    mappings.put(index, new MappingMetaData(mergedMapper));
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] create_mapping [{}] with source [{}]", index, newMapper.type(), updatedSource);
                    } else if (logger.isInfoEnabled()) {
                        logger.info("[{}] create_mapping [{}]", index, newMapper.type());
                    }
                }
            }
            if (mappings.isEmpty()) {
                // no changes, return
                return currentState;
            }
            MetaData.Builder builder = MetaData.builder(currentState.metaData());
            for (String indexName : request.indices()) {
                IndexMetaData indexMetaData = currentState.metaData().index(indexName);
                if (indexMetaData == null) {
                    throw new IndexNotFoundException(indexName);
                }
                MappingMetaData mappingMd = mappings.get(indexName);
                if (mappingMd != null) {
                    builder.put(IndexMetaData.builder(indexMetaData).putMapping(mappingMd));
                }
            }

            return ClusterState.builder(currentState).metaData(builder).build();
        }
    }

    public void putMapping(final PutMappingClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("put-mapping [" + request.type() + "]",
                request,
                ClusterStateTaskConfig.build(Priority.HIGH, request.masterNodeTimeout()),
                putMappingExecutor,
                new AckedClusterStateTaskListener() {

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
