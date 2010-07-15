/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.action.index.NodeMappingCreatedAction;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.timer.Timeout;
import org.elasticsearch.common.timer.TimerTask;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.InvalidTypeNameException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.timer.TimerService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.common.collect.Maps.*;
import static org.elasticsearch.common.collect.Sets.*;
import static org.elasticsearch.index.mapper.DocumentMapper.MergeFlags.*;

/**
 * @author kimchy (shay.banon)
 */
public class MetaDataMappingService extends AbstractComponent {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final TimerService timerService;

    private final NodeMappingCreatedAction nodeMappingCreatedAction;

    @Inject public MetaDataMappingService(Settings settings, ClusterService clusterService, IndicesService indicesService,
                                          TimerService timerService, NodeMappingCreatedAction nodeMappingCreatedAction) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.timerService = timerService;
        this.nodeMappingCreatedAction = nodeMappingCreatedAction;
    }

    public void updateMapping(final String index, final String type, final String mappingSource) {
        clusterService.submitStateUpdateTask("update-mapping [" + index + "][" + type + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MapperService mapperService = indicesService.indexServiceSafe(index).mapperService();

                DocumentMapper existingMapper = mapperService.documentMapper(type);
                // parse the updated one
                DocumentMapper updatedMapper = mapperService.parse(type, mappingSource);
                if (existingMapper == null) {
                    existingMapper = updatedMapper;
                } else {
                    // merge from the updated into the existing, ignore conflicts (we know we have them, we just want the new ones)
                    existingMapper.merge(updatedMapper, mergeFlags().simulate(false));
                }
                // build the updated mapping source
                final String updatedMappingSource = existingMapper.buildSource();
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] update_mapping [{}] (dynamic) with source [{}]", index, type, updatedMappingSource);
                } else if (logger.isInfoEnabled()) {
                    logger.info("[{}] update_mapping [{}] (dynamic)", index, type);
                }

                MetaData.Builder builder = newMetaDataBuilder().metaData(currentState.metaData());
                IndexMetaData indexMetaData = currentState.metaData().index(index);
                builder.put(newIndexMetaDataBuilder(indexMetaData).putMapping(type, updatedMappingSource));
                return newClusterStateBuilder().state(currentState).metaData(builder).build();
            }
        });
    }

    public void putMapping(final Request request, final Listener userListener) {
        clusterService.submitStateUpdateTask("put-mapping [" + request.mappingType + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                final PutMappingListener listener = new PutMappingListener(request, userListener);
                try {
                    if (request.indices.length == 0) {
                        throw new IndexMissingException(new Index("_all"));
                    }
                    for (String index : request.indices) {
                        if (!currentState.metaData().hasIndex(index)) {
                            listener.onFailure(new IndexMissingException(new Index(index)));
                        }
                    }

                    Map<String, DocumentMapper> newMappers = newHashMap();
                    Map<String, DocumentMapper> existingMappers = newHashMap();
                    for (String index : request.indices) {
                        IndexService indexService = indicesService.indexService(index);
                        if (indexService != null) {
                            // try and parse it (no need to add it here) so we can bail early in case of parsing exception
                            DocumentMapper newMapper = indexService.mapperService().parse(request.mappingType, request.mappingSource);
                            newMappers.put(index, newMapper);
                            DocumentMapper existingMapper = indexService.mapperService().documentMapper(request.mappingType);
                            if (existingMapper != null) {
                                // first, simulate
                                DocumentMapper.MergeResult mergeResult = existingMapper.merge(newMapper, mergeFlags().simulate(true));
                                // if we have conflicts, and we are not supposed to ignore them, throw an exception
                                if (!request.ignoreConflicts && mergeResult.hasConflicts()) {
                                    throw new MergeMappingException(mergeResult.conflicts());
                                }
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
                    if (mappingType.charAt(0) == '_') {
                        throw new InvalidTypeNameException("Document mapping type name can't start with '_'");
                    }

                    final Map<String, Tuple<String, String>> mappings = newHashMap();
                    int expectedReplies = 0;
                    for (Map.Entry<String, DocumentMapper> entry : newMappers.entrySet()) {
                        String index = entry.getKey();
                        // do the actual merge here on the master, and update the mapping source
                        DocumentMapper newMapper = entry.getValue();
                        if (existingMappers.containsKey(entry.getKey())) {
                            // we have an existing mapping, do the merge here (on the master), it will automatically update the mapping source
                            DocumentMapper existingMapper = existingMappers.get(entry.getKey());
                            String existingSource = existingMapper.mappingSource();
                            existingMapper.merge(newMapper, mergeFlags().simulate(false));
                            String updatedSource = existingMapper.buildSource();
                            if (existingSource.equals(updatedSource)) {
                                // same source, no changes, ignore it
                            } else {
                                expectedReplies += (currentState.nodes().size() - 1); // for this index, on update, don't include the master, since we update it already
                                // use the merged mapping source
                                mappings.put(index, new Tuple<String, String>(existingMapper.type(), updatedSource));
                                if (logger.isDebugEnabled()) {
                                    logger.debug("[{}] update_mapping [{}] with source [{}]", index, existingMapper.type(), updatedSource);
                                } else if (logger.isInfoEnabled()) {
                                    logger.info("[{}] update_mapping [{}]", index, existingMapper.type());
                                }
                            }
                        } else {
                            expectedReplies += currentState.nodes().size();
                            String newSource = newMapper.buildSource();
                            mappings.put(index, new Tuple<String, String>(newMapper.type(), newSource));
                            if (logger.isDebugEnabled()) {
                                logger.debug("[{}] create_mapping [{}] with source [{}]", index, newMapper.type(), newSource);
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

                    MetaData.Builder builder = newMetaDataBuilder().metaData(currentState.metaData());
                    for (String indexName : request.indices) {
                        IndexMetaData indexMetaData = currentState.metaData().index(indexName);
                        if (indexMetaData == null) {
                            throw new IndexMissingException(new Index(indexName));
                        }
                        Tuple<String, String> mapping = mappings.get(indexName);
                        if (mapping != null) {
                            builder.put(newIndexMetaDataBuilder(indexMetaData).putMapping(mapping.v1(), mapping.v2()));
                        }
                    }


                    final AtomicInteger counter = new AtomicInteger(expectedReplies);
                    final Set<String> indicesSet = newHashSet(request.indices);
                    final NodeMappingCreatedAction.Listener nodeMappingListener = new NodeMappingCreatedAction.Listener() {
                        @Override public void onNodeMappingCreated(NodeMappingCreatedAction.NodeMappingCreatedResponse response) {
                            if (indicesSet.contains(response.index()) && response.type().equals(request.mappingType)) {
                                if (counter.decrementAndGet() == 0) {
                                    listener.onResponse(new Response(true));
                                    nodeMappingCreatedAction.remove(this);
                                }
                            }
                        }
                    };
                    nodeMappingCreatedAction.add(nodeMappingListener);

                    Timeout timeoutTask = timerService.newTimeout(new TimerTask() {
                        @Override public void run(Timeout timeout) throws Exception {
                            listener.onResponse(new Response(false));
                            nodeMappingCreatedAction.remove(nodeMappingListener);
                        }
                    }, request.timeout, TimerService.ExecutionType.THREADED);
                    listener.timeout = timeoutTask;


                    return newClusterStateBuilder().state(currentState).metaData(builder).build();
                } catch (Exception e) {
                    listener.onFailure(e);
                    return currentState;
                }
            }
        });
    }

    class PutMappingListener implements Listener {

        private AtomicBoolean notified = new AtomicBoolean();

        private final Request request;

        private final Listener listener;

        volatile Timeout timeout;

        private PutMappingListener(Request request, Listener listener) {
            this.request = request;
            this.listener = listener;
        }

        @Override public void onResponse(final Response response) {
            if (notified.compareAndSet(false, true)) {
                if (timeout != null) {
                    timeout.cancel();
                }
                listener.onResponse(response);
            }
        }

        @Override public void onFailure(Throwable t) {
            if (notified.compareAndSet(false, true)) {
                if (timeout != null) {
                    timeout.cancel();
                }
                listener.onFailure(t);
            }
        }
    }

    public static interface Listener {

        void onResponse(Response response);

        void onFailure(Throwable t);
    }

    public static class Request {

        final String[] indices;

        final String mappingType;

        final String mappingSource;

        boolean ignoreConflicts = false;

        TimeValue timeout = TimeValue.timeValueSeconds(10);

        public Request(String[] indices, String mappingType, String mappingSource) {
            this.indices = indices;
            this.mappingType = mappingType;
            this.mappingSource = mappingSource;
        }

        public Request ignoreConflicts(boolean ignoreConflicts) {
            this.ignoreConflicts = ignoreConflicts;
            return this;
        }

        public Request timeout(TimeValue timeout) {
            this.timeout = timeout;
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
}
