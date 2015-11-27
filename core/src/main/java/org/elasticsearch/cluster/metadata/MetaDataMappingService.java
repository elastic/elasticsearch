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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

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
import org.elasticsearch.indices.IndicesService;

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

        RefreshTask(String index, final String indexUUID) {
            this.index = index;
            this.indexUUID = indexUUID;
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
                indexService = indicesService.createIndex(nodeServicesProvider, indexMetaData, Collections.emptyList());
                removeIndex = true;
                final Map<String, CompressedXContent> mappingSources = new HashMap<>();
                for (ObjectObjectCursor<String, MappingMetaData> e : indexMetaData.getMappings()) {
                    mappingSources.put(e.key, e.value.source());
                }
                // don't apply the default mapping, it has been applied when the mapping was created
                indexService.mapperService().merge(mappingSources, false, true);
            }

            IndexMetaData.Builder builder = IndexMetaData.builder(indexMetaData);
            try {
                boolean indexDirty = processIndexMapping(indexService, builder);
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

    private boolean processIndexMapping(IndexService indexService, IndexMetaData.Builder builder) {
        boolean dirty = false;
        String index = indexService.index().name();
        try {
            List<String> updatedTypes = new ArrayList<>();
            for (DocumentMapper mapper : indexService.mapperService().docMappers(true)) {
                final String type = mapper.type();
                if (builder.mapping(type) == null
                        || !mapper.mappingSource().equals(builder.mapping(type).source())) {
                    updatedTypes.add(type);
                    builder.putMapping(new MappingMetaData(mapper));
                }
            }

            if (updatedTypes.isEmpty() == false) {
                logger.warn("[{}] re-syncing mappings with cluster state for types [{}]", index, updatedTypes);
                dirty = true;
            }
        } catch (Throwable t) {
            logger.warn("[{}] failed to refresh-mapping in cluster state", index);
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
                ClusterStateTaskConfig.build(Priority.HIGH),
                refreshExectuor,
                (source, t) -> logger.warn("failure during [{}]", t, source)
        );
    }

    class PutMappingExecutor implements ClusterStateTaskExecutor<PutMappingClusterStateUpdateRequest> {
        @Override
        public BatchResult<PutMappingClusterStateUpdateRequest> execute(ClusterState currentState, List<PutMappingClusterStateUpdateRequest> tasks) throws Exception {
            List<String> indicesToClose = new ArrayList<>();
            BatchResult.Builder<PutMappingClusterStateUpdateRequest> builder = BatchResult.builder();
            try {
                // precreate incoming indices;
                for (PutMappingClusterStateUpdateRequest request : tasks) {
                    // failures here mean something is broken with our cluster state - fail all tasks by letting exceptions bubble up
                    for (String index : request.indices()) {
                        if (currentState.metaData().hasIndex(index)) {
                            // if we don't have the index, we will throw exceptions later;
                            if (indicesService.hasIndex(index) == false) {
                                final IndexMetaData indexMetaData = currentState.metaData().index(index);
                                IndexService indexService = indicesService.createIndex(nodeServicesProvider, indexMetaData, Collections.EMPTY_LIST);
                                indicesToClose.add(indexMetaData.getIndex());
                                // we need to add all types since we perform cross-type validation
                                final Map<String, CompressedXContent> mappingSources = new HashMap<>();
                                for (ObjectObjectCursor<String, MappingMetaData> entry : indexMetaData.getMappings()) {
                                    mappingSources.put(entry.key, entry.value.source());
                                }
                                indexService.mapperService().merge(mappingSources, false, request.updateAllTypes());
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
            final String mappingType = request.type();

            final Map<String, MappingMetaData> mappings = new HashMap<>();
            for (String index : request.indices()) {
                IndexService indexService = indicesService.indexServiceSafe(index);
                CompressedXContent existingSource = null;
                DocumentMapper oldMapper = indexService.mapperService().documentMapper(mappingType);
                if (oldMapper != null) {
                    existingSource = oldMapper.mappingSource();
                }
                DocumentMapper mergedMapper = indexService.mapperService().merge(Collections.singletonMap(mappingType, new CompressedXContent(request.source())), true, request.updateAllTypes()).get(mappingType);
                CompressedXContent updatedSource = mergedMapper.mappingSource();

                if (existingSource == null || existingSource.equals(updatedSource) == false) {
                    // the mapping was modified
                    final String op = existingSource == null ? "create" : "update";
                    mappings.put(index, new MappingMetaData(mergedMapper));
                    if (logger.isDebugEnabled()) {
                        logger.debug("[{}] {}_mapping [{}] with source [{}]", index, op, mergedMapper.type(), updatedSource);
                    } else if (logger.isInfoEnabled()) {
                        logger.info("[{}] {}_mapping [{}]", index, op, mergedMapper.type());
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
