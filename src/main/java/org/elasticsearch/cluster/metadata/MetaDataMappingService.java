/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingClusterStateUpdateRequest;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.action.index.NodeMappingCreatedAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidTypeNameException;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.percolator.PercolatorService;

import java.util.*;
import java.util.concurrent.BlockingQueue;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.index.mapper.DocumentMapper.MergeFlags.mergeFlags;

/**
 *
 */
public class MetaDataMappingService extends AbstractComponent {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final NodeMappingCreatedAction mappingCreatedAction;

    private final BlockingQueue<MappingTask> refreshOrUpdateQueue = ConcurrentCollections.newBlockingQueue();

    @Inject
    public MetaDataMappingService(Settings settings, ClusterService clusterService, IndicesService indicesService, NodeMappingCreatedAction mappingCreatedAction) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.mappingCreatedAction = mappingCreatedAction;
    }

    static class MappingTask {
        final String index;
        final String indexUUID;

        MappingTask(String index, final String indexUUID) {
            this.index = index;
            this.indexUUID = indexUUID;
        }
    }

    static class RefreshTask extends MappingTask {
        final String[] types;

        RefreshTask(String index, final String indexUUID, String[] types) {
            super(index, indexUUID);
            this.types = types;
        }
    }

    static class UpdateTask extends MappingTask {
        final String type;
        final CompressedString mappingSource;
        final Listener listener;

        UpdateTask(String index, String indexUUID, String type, CompressedString mappingSource, Listener listener) {
            super(index, indexUUID);
            this.type = type;
            this.mappingSource = mappingSource;
            this.listener = listener;
        }
    }

    /**
     * Batch method to apply all the queued refresh or update operations. The idea is to try and batch as much
     * as possible so we won't create the same index all the time for example for the updates on the same mapping
     * and generate a single cluster change event out of all of those.
     */
    ClusterState executeRefreshOrUpdate(final ClusterState currentState) throws Exception {
        List<MappingTask> allTasks = new ArrayList<MappingTask>();
        refreshOrUpdateQueue.drainTo(allTasks);

        if (allTasks.isEmpty()) {
            return currentState;
        }

        // break down to tasks per index, so we can optimize the on demand index service creation
        // to only happen for the duration of a single index processing of its respective events
        Map<String, List<MappingTask>> tasksPerIndex = Maps.newHashMap();
        for (MappingTask task : allTasks) {
            if (task.index == null) {
                logger.debug("ignoring a mapping task of type [{}] with a null index.", task);
            }
            List<MappingTask> indexTasks = tasksPerIndex.get(task.index);
            if (indexTasks == null) {
                indexTasks = new ArrayList<MappingTask>();
                tasksPerIndex.put(task.index, indexTasks);
            }
            indexTasks.add(task);

        }

        boolean dirty = false;
        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
        for (Map.Entry<String, List<MappingTask>> entry : tasksPerIndex.entrySet()) {
            String index = entry.getKey();
            List<MappingTask> tasks = entry.getValue();
            boolean removeIndex = false;
            // keep track of what we already refreshed, no need to refresh it again...
            Set<String> processedRefreshes = Sets.newHashSet();
            try {
                for (MappingTask task : tasks) {
                    final IndexMetaData indexMetaData = mdBuilder.get(index);
                    if (indexMetaData == null) {
                        // index got deleted on us, ignore...
                        logger.debug("[{}] ignoring task [{}] - index meta data doesn't exist", index, task);
                        continue;
                    }

                    if (!indexMetaData.isSameUUID(task.indexUUID)) {
                        // index got deleted on us, ignore...
                        logger.debug("[{}] ignoring task [{}] - index meta data doesn't match task uuid", index, task);
                        continue;
                    }

                    if (task instanceof RefreshTask) {
                        RefreshTask refreshTask = (RefreshTask) task;
                        IndexService indexService = indicesService.indexService(index);
                        if (indexService == null) {
                            // we need to create the index here, and add the current mapping to it, so we can merge
                            indexService = indicesService.createIndex(indexMetaData.index(), indexMetaData.settings(), currentState.nodes().localNode().id());
                            removeIndex = true;
                            for (String type : refreshTask.types) {
                                // only add the current relevant mapping (if exists)
                                if (indexMetaData.mappings().containsKey(type)) {
                                    // don't apply the default mapping, it has been applied when the mapping was created
                                    indexService.mapperService().merge(type, indexMetaData.mappings().get(type).source().string(), false);
                                }
                            }
                        }
                        IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexMetaData);
                        List<String> updatedTypes = Lists.newArrayList();
                        for (String type : refreshTask.types) {
                            if (processedRefreshes.contains(type)) {
                                continue;
                            }
                            DocumentMapper mapper = indexService.mapperService().documentMapper(type);
                            if (!mapper.mappingSource().equals(indexMetaData.mappings().get(type).source())) {
                                updatedTypes.add(type);
                                indexMetaDataBuilder.putMapping(new MappingMetaData(mapper));
                            }
                            processedRefreshes.add(type);
                        }

                        if (updatedTypes.isEmpty()) {
                            continue;
                        }

                        logger.warn("[{}] re-syncing mappings with cluster state for types [{}]", index, updatedTypes);
                        mdBuilder.put(indexMetaDataBuilder);
                        dirty = true;

                    } else if (task instanceof UpdateTask) {
                        UpdateTask updateTask = (UpdateTask) task;
                        String type = updateTask.type;
                        CompressedString mappingSource = updateTask.mappingSource;

                        if (indexMetaData.mappings().containsKey(type) && indexMetaData.mapping(type).source().equals(mappingSource)) {
                            logger.debug("[{}] update_mapping [{}] ignoring mapping update task as it's source is equal to ours", index, updateTask.type);
                            continue;
                        }

                        IndexService indexService = indicesService.indexService(index);
                        if (indexService == null) {
                            // we need to create the index here, and add the current mapping to it, so we can merge
                            indexService = indicesService.createIndex(indexMetaData.index(), indexMetaData.settings(), currentState.nodes().localNode().id());
                            removeIndex = true;
                            // only add the current relevant mapping (if exists)
                            if (indexMetaData.mappings().containsKey(type)) {
                                indexService.mapperService().merge(type, indexMetaData.mappings().get(type).source().string(), false);
                            }
                        }

                        DocumentMapper updatedMapper = indexService.mapperService().merge(type, mappingSource.string(), false);
                        processedRefreshes.add(type);

                        // if we end up with the same mapping as the original once, ignore
                        if (indexMetaData.mappings().containsKey(type) && indexMetaData.mapping(type).source().equals(updatedMapper.mappingSource())) {
                            logger.debug("[{}] update_mapping [{}] ignoring mapping update task as it results in the same source as what we have", index, updateTask.type);
                            continue;
                        }

                        // build the updated mapping source
                        if (logger.isDebugEnabled()) {
                            logger.debug("[{}] update_mapping [{}] (dynamic) with source [{}]", index, type, updatedMapper.mappingSource());
                        } else if (logger.isInfoEnabled()) {
                            logger.info("[{}] update_mapping [{}] (dynamic)", index, type);
                        }

                        mdBuilder.put(IndexMetaData.builder(indexMetaData).putMapping(new MappingMetaData(updatedMapper)));
                        dirty = true;
                    } else {
                        logger.warn("illegal state, got wrong mapping task type [{}]", task);
                    }
                }
            } finally {
                if (removeIndex) {
                    indicesService.removeIndex(index, "created for mapping processing");
                }
                for (Object task : tasks) {
                    if (task instanceof UpdateTask) {
                        ((UpdateTask) task).listener.onResponse(new Response(true));
                    }
                }
            }
        }

        if (!dirty) {
            return currentState;
        }
        return ClusterState.builder(currentState).metaData(mdBuilder).build();
    }

    /**
     * Refreshes mappings if they are not the same between original and parsed version
     */
    public void refreshMapping(final String index, final String indexUUID, final String... types) {
        refreshOrUpdateQueue.add(new RefreshTask(index, indexUUID, types));
        clusterService.submitStateUpdateTask("refresh-mapping [" + index + "][" + Arrays.toString(types) + "]", Priority.HIGH, new ClusterStateUpdateTask() {
            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("failure during [{}]", t, source);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return executeRefreshOrUpdate(currentState);
            }
        });
    }

    public void updateMapping(final String index, final String indexUUID, final String type, final CompressedString mappingSource, final Listener listener) {
        refreshOrUpdateQueue.add(new UpdateTask(index, indexUUID, type, mappingSource, listener));
        clusterService.submitStateUpdateTask("update-mapping [" + index + "][" + type + "]", Priority.HIGH, new ClusterStateUpdateTask() {
            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) throws Exception {
                return executeRefreshOrUpdate(currentState);
            }
        });
    }

    public void removeMapping(final DeleteMappingClusterStateUpdateRequest request, final ClusterStateUpdateListener listener) {
        clusterService.submitStateUpdateTask("remove-mapping [" + request.type() + "]", Priority.HIGH, new AckedClusterStateUpdateTask() {

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

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                if (request.indices().length == 0) {
                    throw new IndexMissingException(new Index("_all"));
                }

                MetaData.Builder builder = MetaData.builder(currentState.metaData());
                boolean changed = false;
                String latestIndexWithout = null;
                for (String indexName : request.indices()) {
                    IndexMetaData indexMetaData = currentState.metaData().index(indexName);
                    if (indexMetaData != null) {
                        if (indexMetaData.mappings().containsKey(request.type())) {
                            builder.put(IndexMetaData.builder(indexMetaData).removeMapping(request.type()));
                            changed = true;
                        } else {
                            latestIndexWithout = indexMetaData.index();
                        }
                    }
                }

                if (!changed) {
                    throw new TypeMissingException(new Index(latestIndexWithout), request.type());
                }

                logger.info("[{}] remove_mapping [{}]", request.indices(), request.type());

                return ClusterState.builder(currentState).metaData(builder).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

            }
        });
    }

    public void putMapping(final PutRequest request, final Listener listener) {

        clusterService.submitStateUpdateTask("put-mapping [" + request.mappingType + "]", Priority.HIGH, new TimeoutClusterStateUpdateTask() {

            CountDownListener countDownListener; // used to count ack responses before confirming operation is complete

            @Override
            public TimeValue timeout() {
                return request.masterTimeout;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) throws Exception {
                List<String> indicesToClose = Lists.newArrayList();
                try {
                    if (request.indices.length == 0) {
                        throw new IndexMissingException(new Index("_all"));
                    }
                    for (String index : request.indices) {
                        if (!currentState.metaData().hasIndex(index)) {
                            throw new IndexMissingException(new Index(index));
                        }
                    }

                    // pre create indices here and add mappings to them so we can merge the mappings here if needed
                    for (String index : request.indices) {
                        if (indicesService.hasIndex(index)) {
                            continue;
                        }
                        final IndexMetaData indexMetaData = currentState.metaData().index(index);
                        IndexService indexService = indicesService.createIndex(indexMetaData.index(), indexMetaData.settings(), clusterService.localNode().id());
                        indicesToClose.add(indexMetaData.index());
                        // only add the current relevant mapping (if exists)
                        if (indexMetaData.mappings().containsKey(request.mappingType)) {
                            indexService.mapperService().merge(request.mappingType, indexMetaData.mappings().get(request.mappingType).source().string(), false);
                        }
                    }

                    Map<String, DocumentMapper> newMappers = newHashMap();
                    Map<String, DocumentMapper> existingMappers = newHashMap();
                    for (String index : request.indices) {
                        IndexService indexService = indicesService.indexService(index);
                        if (indexService != null) {
                            // try and parse it (no need to add it here) so we can bail early in case of parsing exception
                            DocumentMapper newMapper;
                            DocumentMapper existingMapper = indexService.mapperService().documentMapper(request.mappingType);
                            if (MapperService.DEFAULT_MAPPING.equals(request.mappingType)) {
                                // _default_ types do not go through merging, but we do test the new settings. Also don't apply the old default
                                newMapper = indexService.mapperService().parse(request.mappingType, request.mappingSource, false);
                            } else {
                                newMapper = indexService.mapperService().parse(request.mappingType, request.mappingSource);
                                if (existingMapper != null) {
                                    // first, simulate
                                    DocumentMapper.MergeResult mergeResult = existingMapper.merge(newMapper, mergeFlags().simulate(true));
                                    // if we have conflicts, and we are not supposed to ignore them, throw an exception
                                    if (!request.ignoreConflicts && mergeResult.hasConflicts()) {
                                        throw new MergeMappingException(mergeResult.conflicts());
                                    }
                                }
                            }

                            newMappers.put(index, newMapper);
                            if (existingMapper != null) {
                                existingMappers.put(index, existingMapper);
                            }

                        } else {
                            throw new IndexMissingException(new Index(index));
                        }
                    }

                    String mappingType = request.mappingType;
                    if (mappingType == null) {
                        mappingType = newMappers.values().iterator().next().type();
                    } else if (!mappingType.equals(newMappers.values().iterator().next().type())) {
                        throw new InvalidTypeNameException("Type name provided does not match type name within mapping definition");
                    }
                    if (!MapperService.DEFAULT_MAPPING.equals(mappingType) && !PercolatorService.Constants.TYPE_NAME.equals(mappingType) && mappingType.charAt(0) == '_') {
                        throw new InvalidTypeNameException("Document mapping type name can't start with '_'");
                    }

                    final Map<String, MappingMetaData> mappings = newHashMap();
                    for (Map.Entry<String, DocumentMapper> entry : newMappers.entrySet()) {
                        String index = entry.getKey();
                        // do the actual merge here on the master, and update the mapping source
                        DocumentMapper newMapper = entry.getValue();
                        IndexService indexService = indicesService.indexService(index);
                        CompressedString existingSource = null;
                        if (existingMappers.containsKey(entry.getKey())) {
                            existingSource = existingMappers.get(entry.getKey()).mappingSource();
                        }
                        DocumentMapper mergedMapper = indexService.mapperService().merge(newMapper.type(), newMapper.mappingSource().string(), false);
                        CompressedString updatedSource = mergedMapper.mappingSource();

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
                        listener.onResponse(new Response(true));
                        return currentState;
                    }

                    MetaData.Builder builder = MetaData.builder(currentState.metaData());
                    for (String indexName : request.indices) {
                        IndexMetaData indexMetaData = currentState.metaData().index(indexName);
                        if (indexMetaData == null) {
                            throw new IndexMissingException(new Index(indexName));
                        }
                        MappingMetaData mappingMd = mappings.get(indexName);
                        if (mappingMd != null) {
                            builder.put(IndexMetaData.builder(indexMetaData).putMapping(mappingMd));
                        }
                    }

                    ClusterState updatedState = ClusterState.builder(currentState).metaData(builder).build();

                    int counter = 1; // we want to wait on the master node to apply it on its cluster state
                    // also wait for nodes that actually have the index created on them to apply the mappings internally
                    for (String index : request.indices) {
                        IndexRoutingTable indexRoutingTable = updatedState.routingTable().index(index);
                        if (indexRoutingTable != null) {
                            counter += indexRoutingTable.numberOfNodesShardsAreAllocatedOn(updatedState.nodes().masterNodeId());
                        }
                    }

                    logger.debug("Expecting {} mapping created responses for other nodes", counter - 1);

                    // TODO: adding one to the version is based on knowledge on how the parent class will increment the version
                    //       move this to the base class or add another callback before publishing the new cluster state so we
                    //       capture it's version.
                    countDownListener = new CountDownListener(counter, currentState.version() + 1, listener);
                    mappingCreatedAction.add(countDownListener, request.timeout);

                    return updatedState;
                } finally {
                    for (String index : indicesToClose) {
                        indicesService.removeIndex(index, "created for mapping processing");
                    }
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (countDownListener != null) {
                    // the master has applied it on its cluster state
                    countDownListener.decrementCounter();
                }
            }
        });
    }

    public static interface Listener {

        void onResponse(Response response);

        void onFailure(Throwable t);
    }

    public static class PutRequest {

        final String[] indices;

        final String mappingType;

        final String mappingSource;

        boolean ignoreConflicts = false;

        TimeValue timeout = TimeValue.timeValueSeconds(10);
        TimeValue masterTimeout = MasterNodeOperationRequest.DEFAULT_MASTER_NODE_TIMEOUT;

        public PutRequest(String[] indices, String mappingType, String mappingSource) {
            this.indices = indices;
            this.mappingType = mappingType;
            this.mappingSource = mappingSource;
        }

        public PutRequest ignoreConflicts(boolean ignoreConflicts) {
            this.ignoreConflicts = ignoreConflicts;
            return this;
        }

        public PutRequest timeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public PutRequest masterTimeout(TimeValue masterTimeout) {
            this.masterTimeout = masterTimeout;
            return this;
        }
    }

    public static class Response {
        private final boolean acknowledged;

        public Response(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public boolean acknowledged() {
            return acknowledged;
        }
    }

    private class CountDownListener implements NodeMappingCreatedAction.Listener {

        private final CountDown countDown;
        private final Listener listener;
        private final long minClusterStateVersion;

        /**
         * @param countDown              initial counter value
         * @param minClusterStateVersion the minimum cluster state version for which accept responses
         * @param listener               listener to call when counter reaches 0.
         */
        public CountDownListener(int countDown, long minClusterStateVersion, Listener listener) {
            this.countDown = new CountDown(countDown);
            this.listener = listener;
            this.minClusterStateVersion = minClusterStateVersion;
        }

        @Override
        public void onNodeMappingCreated(NodeMappingCreatedAction.NodeMappingCreatedResponse response) {
            if (response.clusterStateVersion() < minClusterStateVersion) {
                return;
            }
            decrementCounter();

        }

        public void decrementCounter() {
            if (countDown.countDown()) {
                mappingCreatedAction.remove(this);
                listener.onResponse(new Response(true));
            }
        }

        @Override
        public void onTimeout() {
            if (countDown.fastForward()) {
                mappingCreatedAction.remove(this);
                listener.onResponse(new Response(false));
            }
        }
    }
}
