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

package org.elasticsearch.action.admin.indices.mapping.get;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class TransportGetFieldMappingsAction extends TransportAction<GetFieldMappingsRequest, GetFieldMappingsResponse> {


    protected final ClusterService clusterService;
    private final TransportService transportService;
    private final IndicesService indicesService;

    protected AtomicInteger shardPicker = new AtomicInteger();

    @Inject
    public TransportGetFieldMappingsAction(Settings settings, ClusterService clusterService,
                                           TransportService transportService,
                                           IndicesService indicesService,
                                           ThreadPool threadPool) {
        super(settings, threadPool);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.transportService = transportService;
        transportService.registerHandler(GetFieldMappingsAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(final GetFieldMappingsRequest request, ActionListener<GetFieldMappingsResponse> listener) {
        ClusterState state = clusterService.state();
        String[] concreteIndices = state.metaData().concreteIndices(request.indices(), request.indicesOptions());
        request.indices(concreteIndices);
        if (request.local) {
            logger.trace("executing request locally");
            listener.onResponse(new GetFieldMappingsResponse(findMappings(request.indices(), request.types(), request.fields(), request.includeDefaults())));
        } else {
            logger.trace("executing request with remote forwarding");
            new AsyncAction(request, listener).start();
        }
    }

    protected class AsyncAction {

        private final ClusterState state;
        private final GetFieldMappingsRequest origRequest;
        private final ActionListener<GetFieldMappingsResponse> listener;
        private final ObjectIntOpenHashMap<String> mappingsIdPerIndex;
        private final AtomicInteger pendingRequests;
        private final AtomicArray<Throwable> indexErrors;
        private final AtomicArray<ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> indexMappings;
        private final ShardIterator[] shardsPerIndex;

        AsyncAction(GetFieldMappingsRequest origRequest, ActionListener<GetFieldMappingsResponse> listener) {

            this.origRequest = origRequest;
            this.listener = listener;
            this.state = clusterService.state();
            String[] concreteIndices = state.metaData().concreteIndices(origRequest.indices(), origRequest.indicesOptions());
            // normalize, will be used in the response construction.
            origRequest.indices(concreteIndices);
            mappingsIdPerIndex = new ObjectIntOpenHashMap<String>(concreteIndices.length);
            pendingRequests = new AtomicInteger();
            indexErrors = new AtomicArray<Throwable>(concreteIndices.length);
            indexMappings = new AtomicArray<ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>>(concreteIndices.length);

            shardsPerIndex = new ShardIterator[concreteIndices.length];

            // make sure we don't have hot shards
            int shardSeed = shardPicker.getAndIncrement();
            for (int id = 0; id < concreteIndices.length; id++) {
                String index = concreteIndices[id];
                mappingsIdPerIndex.put(index, id);
                int shardNo = state.metaData().getIndices().get(index).getNumberOfShards();
                for (int shard = shardNo - 1; shard >= 0; shard--) {
                    try {
                        shardsPerIndex[id] = clusterService.operationRouting().getShards(state, index, (shard + shardSeed) % shardNo, "_local");
                        break;
                    } catch (IndexShardMissingException e) {
                        if (shard == 0) {
                            // out of shards...
                            throw e;
                        }
                    }
                }

            }
        }

        public void start() {
            sendNodeRequestsForIndices(origRequest.indices());
        }

        private void sendNodeRequestsForIndices(String[] indices) {
            HashMap<String, List<String>> indicesPerNode = new HashMap<String, List<String>>();
            for (int i = 0; i < indices.length; i++) {
                String index = indices[i];
                int id = mappingsIdPerIndex.get(index);
                ShardRouting routing = shardsPerIndex[id].firstOrNull();
                if (routing == null) {
                    assert false : "empty shard iterator for index [" + index + "]";
                    continue; // shouldn't really happen
                }
                List<String> indexList = indicesPerNode.get(routing.currentNodeId());
                if (indexList == null) {
                    indexList = new ArrayList<String>();
                    indicesPerNode.put(routing.currentNodeId(), indexList);
                }
                indexList.add(index);
            }

            logger.trace("forwarding request to [{}] nodes", indicesPerNode.size());
            pendingRequests.addAndGet(indicesPerNode.size());
            DiscoveryNodes nodes = state.nodes();
            for (String nodeId : indicesPerNode.keySet()) {
                final GetFieldMappingsRequest nodeRequest = new GetFieldMappingsRequest(origRequest);
                nodeRequest.local(true);
                nodeRequest.indices(indicesPerNode.get(nodeId).toArray(Strings.EMPTY_ARRAY));
                nodeRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
                if (nodes.localNodeId().equals(nodeId)) {
                    try {
                        handleNodeResponse(findMappings(nodeRequest.indices(), nodeRequest.types(), nodeRequest.fields(),
                                nodeRequest.includeDefaults()),
                                nodeRequest);
                    } catch (Throwable t) {
                        handleNodeException(t, nodeRequest);
                    } finally {

                    }
                } else {
                    transportService.sendRequest(nodes.get(nodeId), GetFieldMappingsAction.NAME,
                            nodeRequest, new BaseTransportResponseHandler<GetFieldMappingsResponse>() {
                        @Override
                        public GetFieldMappingsResponse newInstance() {
                            return new GetFieldMappingsResponse();
                        }

                        @Override
                        public void handleResponse(GetFieldMappingsResponse nodeResponse) {
                            handleNodeResponse(nodeResponse.mappings(), nodeRequest);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            handleNodeException(exp, nodeRequest);
                        }


                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    }
                    );
                }
            }

        }

        protected void handleNodeException(Throwable exp, GetFieldMappingsRequest nodeRequest) {
            try {
                ArrayList<String> retryIndices = new ArrayList<String>();
                for (String index : nodeRequest.indices()) {
                    int id = mappingsIdPerIndex.get(index);
                    if (shardsPerIndex[id].nextOrNull() == null) {
                        // nope.
                        indexErrors.set(id, exp);
                        // no point in trying, we will return an error
                        retryIndices.clear();
                        break;
                    } else {
                        retryIndices.add(index);
                    }

                }
                if (retryIndices.size() != 0) {
                    // resend requests for failed indices
                    sendNodeRequestsForIndices(retryIndices.toArray(Strings.EMPTY_ARRAY));
                }

            } finally {
                if (pendingRequests.decrementAndGet() == 0) {
                    finnishHim();
                }
            }
        }

        private void handleNodeResponse(ImmutableMap<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> mappings,
                                        GetFieldMappingsRequest nodeRequest) {
            try {
                ArrayList<String> retryIndices = new ArrayList<String>();
                for (String index : nodeRequest.indices()) {
                    ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>> indexMapping = mappings.get(index);
                    int id = mappingsIdPerIndex.get(index);
                    if (indexMapping == null) {
                        // advance the relevant iter, hopefully we have more.
                        if (shardsPerIndex[id].nextOrNull() == null) {
                            // nope.
                            indexErrors.set(id, new IndexShardMissingException(shardsPerIndex[id].shardId()));
                            // no point in trying, we will return an error
                            retryIndices.clear();
                            break;
                        } else {
                            retryIndices.add(index);
                        }
                    } else {
                        indexMappings.set(id, indexMapping);
                    }
                }
                if (retryIndices.size() != 0) {
                    // resend requests for failed indices
                    sendNodeRequestsForIndices(retryIndices.toArray(Strings.EMPTY_ARRAY));
                }

            } finally {
                if (pendingRequests.decrementAndGet() == 0) {
                    finnishHim();
                }
            }

        }

        private void finnishHim() {
            // for simplicity, just return an error if we had any
            for (int i = 0; i < indexErrors.length(); i++) {
                if (indexErrors.get(i) != null) {
                    listener.onFailure(indexErrors.get(i));
                    return;
                }
            }

            ImmutableMap.Builder<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> indexMapBuilder = ImmutableMap.builder();
            for (int id = 0; id < origRequest.indices().length; id++) {
                indexMapBuilder.put(origRequest.indices()[id], indexMappings.get(id));
            }
            GetFieldMappingsResponse response = new GetFieldMappingsResponse(indexMapBuilder.build());
            listener.onResponse(response);
        }

    }


    private ImmutableMap<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> findMappings(String[] concreteIndices,
                                                                                                                final String[] types,
                                                                                                                final String[] fields,
                                                                                                                boolean includeDefaults) {
        assert types != null;
        assert concreteIndices != null;
        if (concreteIndices.length == 0) {
            return ImmutableMap.of();
        }

        boolean isProbablySingleFieldRequest = concreteIndices.length == 1 && types.length == 1 && fields.length == 1;
        ImmutableMap.Builder<String, ImmutableMap<String, ImmutableMap<String, FieldMappingMetaData>>> indexMapBuilder = ImmutableMap.builder();
        for (String index : concreteIndices) {
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                continue;
            }
            Collection<String> typeIntersection;
            if (types.length == 0) {
                typeIntersection = indexService.mapperService().types();

            } else {
                typeIntersection = Collections2.filter(indexService.mapperService().types(), new Predicate<String>() {

                    @Override
                    public boolean apply(String type) {
                        return Regex.simpleMatch(types, type);
                    }

                });
            }

            MapBuilder<String, ImmutableMap<String, FieldMappingMetaData>> typeMappings = new MapBuilder<String, ImmutableMap<String, FieldMappingMetaData>>();
            for (String type : typeIntersection) {
                DocumentMapper documentMapper = indexService.mapperService().documentMapper(type);
                ImmutableMap<String, FieldMappingMetaData> fieldMapping = findFieldMappingsByType(documentMapper, fields, includeDefaults, isProbablySingleFieldRequest);
                if (!fieldMapping.isEmpty()) {
                    typeMappings.put(type, fieldMapping);
                }
            }

            indexMapBuilder.put(index, typeMappings.immutableMap());
        }

        return indexMapBuilder.build();
    }

    private static final ToXContent.Params includeDefaultsParams = new ToXContent.Params() {

        final static String INCLUDE_DEFAULTS = "include_defaults";

        @Override
        public String param(String key) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return "true";
            }
            return defaultValue;
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }

        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            if (INCLUDE_DEFAULTS.equals(key)) {
                return true;
            }
            return defaultValue;
        }

        @Override
        @Deprecated
        public Boolean paramAsBooleanOptional(String key, Boolean defaultValue) {
            return paramAsBoolean(key, defaultValue);
        }
    };

    private ImmutableMap<String, FieldMappingMetaData> findFieldMappingsByType(DocumentMapper documentMapper, String[] fields,
                                                                               boolean includeDefaults, boolean isProbablySingleFieldRequest) throws ElasticsearchException {
        MapBuilder<String, FieldMappingMetaData> fieldMappings = new MapBuilder<String, FieldMappingMetaData>();
        ImmutableList<FieldMapper> allFieldMappers = documentMapper.mappers().mappers();
        for (String field : fields) {
            if (Regex.isMatchAllPattern(field)) {
                for (FieldMapper fieldMapper : allFieldMappers) {
                    addFieldMapper(fieldMapper.names().fullName(), fieldMapper, fieldMappings, includeDefaults);
                }
            } else if (Regex.isSimpleMatchPattern(field)) {
                // go through the field mappers 3 times, to make sure we give preference to the resolve order: full name, index name, name.
                // also make sure we only store each mapper once.
                boolean[] resolved = new boolean[allFieldMappers.size()];
                for (int i = 0; i < allFieldMappers.size(); i++) {
                    FieldMapper fieldMapper = allFieldMappers.get(i);
                    if (Regex.simpleMatch(field, fieldMapper.names().fullName())) {
                        addFieldMapper(fieldMapper.names().fullName(), fieldMapper, fieldMappings, includeDefaults);
                        resolved[i] = true;
                    }
                }
                for (int i = 0; i < allFieldMappers.size(); i++) {
                    if (resolved[i]) {
                        continue;
                    }
                    FieldMapper fieldMapper = allFieldMappers.get(i);
                    if (Regex.simpleMatch(field, fieldMapper.names().indexName())) {
                        addFieldMapper(fieldMapper.names().indexName(), fieldMapper, fieldMappings, includeDefaults);
                        resolved[i] = true;
                    }
                }
                for (int i = 0; i < allFieldMappers.size(); i++) {
                    if (resolved[i]) {
                        continue;
                    }
                    FieldMapper fieldMapper = allFieldMappers.get(i);
                    if (Regex.simpleMatch(field, fieldMapper.names().name())) {
                        addFieldMapper(fieldMapper.names().name(), fieldMapper, fieldMappings, includeDefaults);
                        resolved[i] = true;
                    }
                }

            } else {
                // not a pattern
                FieldMapper fieldMapper = documentMapper.mappers().smartNameFieldMapper(field);
                if (fieldMapper != null) {
                    addFieldMapper(field, fieldMapper, fieldMappings, includeDefaults);
                } else if (isProbablySingleFieldRequest) {
                    fieldMappings.put(field, FieldMappingMetaData.NULL);
                }
            }
        }
        return fieldMappings.immutableMap();
    }

    private void addFieldMapper(String field, FieldMapper fieldMapper, MapBuilder<String, FieldMappingMetaData> fieldMappings, boolean includeDefaults) {
        if (fieldMappings.containsKey(field)) {
            return;
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            fieldMapper.toXContent(builder, includeDefaults ? includeDefaultsParams : ToXContent.EMPTY_PARAMS);
            builder.endObject();
            fieldMappings.put(field, new FieldMappingMetaData(fieldMapper.names().fullName(), builder.bytes()));
        } catch (IOException e) {
            throw new ElasticsearchException("failed to serialize XContent of field [" + field + "]", e);
        }
    }


    private class TransportHandler extends BaseTransportRequestHandler<GetFieldMappingsRequest> {

        @Override
        public GetFieldMappingsRequest newInstance() {
            return new GetFieldMappingsRequest();
        }

        @Override
        public void messageReceived(GetFieldMappingsRequest request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);

            execute(request, new ActionListener<GetFieldMappingsResponse>() {
                @Override
                public void onResponse(GetFieldMappingsResponse result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for get field mapping", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

}