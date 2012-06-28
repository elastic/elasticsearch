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
import com.google.common.io.Closeables;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.action.index.NodeIndexCreatedAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.percolator.PercolatorService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.metadata.MetaData.newMetaDataBuilder;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 *
 */
public class MetaDataCreateIndexService extends AbstractComponent {

    private final Environment environment;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AllocationService allocationService;

    private final NodeIndexCreatedAction nodeIndexCreatedAction;

    private final MetaDataService metaDataService;

    private final String riverIndexName;

    @Inject
    public MetaDataCreateIndexService(Settings settings, Environment environment, ThreadPool threadPool, ClusterService clusterService, IndicesService indicesService,
                                      AllocationService allocationService, NodeIndexCreatedAction nodeIndexCreatedAction, MetaDataService metaDataService, @RiverIndexName String riverIndexName) {
        super(settings);
        this.environment = environment;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.allocationService = allocationService;
        this.nodeIndexCreatedAction = nodeIndexCreatedAction;
        this.metaDataService = metaDataService;
        this.riverIndexName = riverIndexName;
    }

    public void createIndex(final Request request, final Listener userListener) {
        ImmutableSettings.Builder updatedSettingsBuilder = ImmutableSettings.settingsBuilder();
        for (Map.Entry<String, String> entry : request.settings.getAsMap().entrySet()) {
            if (!entry.getKey().startsWith("index.")) {
                updatedSettingsBuilder.put("index." + entry.getKey(), entry.getValue());
            } else {
                updatedSettingsBuilder.put(entry.getKey(), entry.getValue());
            }
        }
        request.settings(updatedSettingsBuilder.build());

        // we lock here, and not within the cluster service callback since we don't want to
        // block the whole cluster state handling
        MetaDataService.MdLock mdLock = metaDataService.indexMetaDataLock(request.index);
        try {
            mdLock.lock();
        } catch (InterruptedException e) {
            userListener.onFailure(e);
            return;
        }

        final CreateIndexListener listener = new CreateIndexListener(mdLock, request, userListener);

        clusterService.submitStateUpdateTask("create-index [" + request.index + "], cause [" + request.cause + "]", new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                try {
                    try {
                        validate(request, currentState);
                    } catch (Exception e) {
                        listener.onFailure(e);
                        return currentState;
                    }

                    // we only find a template when its an API call (a new index)
                    // find templates, highest order are better matching
                    List<IndexTemplateMetaData> templates = findTemplates(request, currentState);

                    Map<String, Custom> customs = Maps.newHashMap();

                    // add the request mapping
                    Map<String, Map<String, Object>> mappings = Maps.newHashMap();

                    // if its a _percolator index, don't index the query object
                    if (request.index.equals(PercolatorService.INDEX_NAME)) {
                        mappings.put(MapperService.DEFAULT_MAPPING, parseMapping("{\n" +
                                "    \"_default_\":{\n" +
                                "        \"properties\" : {\n" +
                                "            \"query\" : {\n" +
                                "                \"type\" : \"object\",\n" +
                                "                \"enabled\" : false\n" +
                                "            }\n" +
                                "        }\n" +
                                "    }\n" +
                                "}"));
                    }

                    for (Map.Entry<String, String> entry : request.mappings.entrySet()) {
                        mappings.put(entry.getKey(), parseMapping(entry.getValue()));
                    }

                    for (Map.Entry<String, Custom> entry : request.customs.entrySet()) {
                        customs.put(entry.getKey(), entry.getValue());
                    }

                    // apply templates, merging the mappings into the request mapping if exists
                    for (IndexTemplateMetaData template : templates) {
                        for (Map.Entry<String, CompressedString> entry : template.mappings().entrySet()) {
                            if (mappings.containsKey(entry.getKey())) {
                                XContentHelper.mergeDefaults(mappings.get(entry.getKey()), parseMapping(entry.getValue().string()));
                            } else {
                                mappings.put(entry.getKey(), parseMapping(entry.getValue().string()));
                            }
                        }
                        // handle custom
                        for (Map.Entry<String, Custom> customEntry : template.customs().entrySet()) {
                            String type = customEntry.getKey();
                            IndexMetaData.Custom custom = customEntry.getValue();
                            IndexMetaData.Custom existing = customs.get(type);
                            if (existing == null) {
                                customs.put(type, custom);
                            } else {
                                IndexMetaData.Custom merged = IndexMetaData.lookupFactorySafe(type).merge(existing, custom);
                                customs.put(type, merged);
                            }
                        }
                    }

                    // now add config level mappings
                    File mappingsDir = new File(environment.configFile(), "mappings");
                    if (mappingsDir.exists() && mappingsDir.isDirectory()) {
                        // first index level
                        File indexMappingsDir = new File(mappingsDir, request.index);
                        if (indexMappingsDir.exists() && indexMappingsDir.isDirectory()) {
                            addMappings(mappings, indexMappingsDir);
                        }

                        // second is the _default mapping
                        File defaultMappingsDir = new File(mappingsDir, "_default");
                        if (defaultMappingsDir.exists() && defaultMappingsDir.isDirectory()) {
                            addMappings(mappings, defaultMappingsDir);
                        }
                    }

                    ImmutableSettings.Builder indexSettingsBuilder = settingsBuilder();
                    // apply templates, here, in reverse order, since first ones are better matching
                    for (int i = templates.size() - 1; i >= 0; i--) {
                        indexSettingsBuilder.put(templates.get(i).settings());
                    }
                    // now, put the request settings, so they override templates
                    indexSettingsBuilder.put(request.settings);

                    if (request.index.equals(PercolatorService.INDEX_NAME)) {
                        // if its percolator, always 1 shard
                        indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, 1);
                    } else {
                        if (indexSettingsBuilder.get(SETTING_NUMBER_OF_SHARDS) == null) {
                            if (request.index.equals(riverIndexName)) {
                                indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 1));
                            } else {
                                indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
                            }
                        }
                    }
                    if (request.index.equals(PercolatorService.INDEX_NAME)) {
                        // if its percolator, always set number of replicas to 0, and expand to 0-all
                        indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, 0);
                        indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, "0-all");
                    } else {
                        if (indexSettingsBuilder.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                            if (request.index.equals(riverIndexName)) {
                                indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                            } else {
                                indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                            }
                        }
                    }

                    if (settings.get(SETTING_AUTO_EXPAND_REPLICAS) != null && indexSettingsBuilder.get(SETTING_AUTO_EXPAND_REPLICAS) == null) {
                        indexSettingsBuilder.put(SETTING_AUTO_EXPAND_REPLICAS, settings.get(SETTING_AUTO_EXPAND_REPLICAS));
                    }

                    indexSettingsBuilder.put(SETTING_VERSION_CREATED, Version.CURRENT);

                    Settings actualIndexSettings = indexSettingsBuilder.build();

                    // Set up everything, now locally create the index to see that things are ok, and apply

                    // create the index here (on the master) to validate it can be created, as well as adding the mapping
                    indicesService.createIndex(request.index, actualIndexSettings, clusterService.state().nodes().localNode().id());
                    // now add the mappings
                    IndexService indexService = indicesService.indexServiceSafe(request.index);
                    MapperService mapperService = indexService.mapperService();
                    // first, add the default mapping
                    if (mappings.containsKey(MapperService.DEFAULT_MAPPING)) {
                        try {
                            mapperService.add(MapperService.DEFAULT_MAPPING, XContentFactory.jsonBuilder().map(mappings.get(MapperService.DEFAULT_MAPPING)).string());
                        } catch (Exception e) {
                            indicesService.deleteIndex(request.index, "failed on parsing default mapping on index creation");
                            throw new MapperParsingException("mapping [" + MapperService.DEFAULT_MAPPING + "]", e);
                        }
                    }
                    for (Map.Entry<String, Map<String, Object>> entry : mappings.entrySet()) {
                        if (entry.getKey().equals(MapperService.DEFAULT_MAPPING)) {
                            continue;
                        }
                        try {
                            mapperService.add(entry.getKey(), XContentFactory.jsonBuilder().map(entry.getValue()).string());
                        } catch (Exception e) {
                            indicesService.deleteIndex(request.index, "failed on parsing mappings on index creation");
                            throw new MapperParsingException("mapping [" + entry.getKey() + "]", e);
                        }
                    }
                    // now, update the mappings with the actual source
                    Map<String, MappingMetaData> mappingsMetaData = Maps.newHashMap();
                    for (DocumentMapper mapper : mapperService) {
                        MappingMetaData mappingMd = new MappingMetaData(mapper);
                        mappingsMetaData.put(mapper.type(), mappingMd);
                    }

                    final IndexMetaData.Builder indexMetaDataBuilder = newIndexMetaDataBuilder(request.index).settings(actualIndexSettings);
                    for (MappingMetaData mappingMd : mappingsMetaData.values()) {
                        indexMetaDataBuilder.putMapping(mappingMd);
                    }
                    for (Map.Entry<String, Custom> customEntry : customs.entrySet()) {
                        indexMetaDataBuilder.putCustom(customEntry.getKey(), customEntry.getValue());
                    }
                    indexMetaDataBuilder.state(request.state);
                    final IndexMetaData indexMetaData = indexMetaDataBuilder.build();

                    MetaData newMetaData = newMetaDataBuilder()
                            .metaData(currentState.metaData())
                            .put(indexMetaData, false)
                            .build();

                    logger.info("[{}] creating index, cause [{}], shards [{}]/[{}], mappings {}", request.index, request.cause, indexMetaData.numberOfShards(), indexMetaData.numberOfReplicas(), mappings.keySet());

                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    if (!request.blocks.isEmpty()) {
                        for (ClusterBlock block : request.blocks) {
                            blocks.addIndexBlock(request.index, block);
                        }
                    }
                    if (request.state == State.CLOSE) {
                        blocks.addIndexBlock(request.index, MetaDataStateIndexService.INDEX_CLOSED_BLOCK);
                    }

                    ClusterState updatedState = newClusterStateBuilder().state(currentState).blocks(blocks).metaData(newMetaData).build();

                    if (request.state == State.OPEN) {
                        RoutingTable.Builder routingTableBuilder = RoutingTable.builder().routingTable(updatedState.routingTable())
                                .add(updatedState.metaData().index(request.index), true);
                        RoutingAllocation.Result routingResult = allocationService.reroute(newClusterStateBuilder().state(updatedState).routingTable(routingTableBuilder).build());
                        updatedState = newClusterStateBuilder().state(updatedState).routingResult(routingResult).build();
                    }

                    // we wait for events from all nodes that the index has been added to the metadata
                    final AtomicInteger counter = new AtomicInteger(currentState.nodes().size());

                    final NodeIndexCreatedAction.Listener nodeIndexCreatedListener = new NodeIndexCreatedAction.Listener() {
                        @Override
                        public void onNodeIndexCreated(String index, String nodeId) {
                            if (index.equals(request.index)) {
                                if (counter.decrementAndGet() == 0) {
                                    listener.onResponse(new Response(true, indexMetaData));
                                    nodeIndexCreatedAction.remove(this);
                                }
                            }
                        }
                    };

                    nodeIndexCreatedAction.add(nodeIndexCreatedListener);

                    listener.future = threadPool.schedule(request.timeout, ThreadPool.Names.SAME, new Runnable() {
                        @Override
                        public void run() {
                            listener.onResponse(new Response(false, indexMetaData));
                            nodeIndexCreatedAction.remove(nodeIndexCreatedListener);
                        }
                    });

                    return updatedState;
                } catch (Exception e) {
                    logger.warn("[{}] failed to create", e, request.index);
                    listener.onFailure(e);
                    return currentState;
                }
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
            }
        });
    }

    class CreateIndexListener implements Listener {

        private final AtomicBoolean notified = new AtomicBoolean();

        private final MetaDataService.MdLock mdLock;

        private final Request request;

        private final Listener listener;

        volatile ScheduledFuture future;

        private CreateIndexListener(MetaDataService.MdLock mdLock, Request request, Listener listener) {
            this.mdLock = mdLock;
            this.request = request;
            this.listener = listener;
        }

        @Override
        public void onResponse(final Response response) {
            if (notified.compareAndSet(false, true)) {
                mdLock.unlock();
                if (future != null) {
                    future.cancel(false);
                }
                listener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (notified.compareAndSet(false, true)) {
                mdLock.unlock();
                if (future != null) {
                    future.cancel(false);
                }
                listener.onFailure(t);
            }
        }
    }


    private Map<String, Object> parseMapping(String mappingSource) throws Exception {
        return XContentFactory.xContent(mappingSource).createParser(mappingSource).mapAndClose();
    }

    private void addMappings(Map<String, Map<String, Object>> mappings, File mappingsDir) {
        File[] mappingsFiles = mappingsDir.listFiles();
        for (File mappingFile : mappingsFiles) {
            if (mappingFile.isHidden()) {
                continue;
            }
            String mappingType = mappingFile.getName().substring(0, mappingFile.getName().lastIndexOf('.'));
            try {
                String mappingSource = Streams.copyToString(new FileReader(mappingFile));
                if (mappings.containsKey(mappingType)) {
                    XContentHelper.mergeDefaults(mappings.get(mappingType), parseMapping(mappingSource));
                } else {
                    mappings.put(mappingType, parseMapping(mappingSource));
                }
            } catch (Exception e) {
                logger.warn("failed to read / parse mapping [" + mappingType + "] from location [" + mappingFile + "], ignoring...", e);
            }
        }
    }

    private List<IndexTemplateMetaData> findTemplates(Request request, ClusterState state) {
        List<IndexTemplateMetaData> templates = Lists.newArrayList();
        for (IndexTemplateMetaData template : state.metaData().templates().values()) {
            if (Regex.simpleMatch(template.template(), request.index)) {
                templates.add(template);
            }
        }

        // see if we have templates defined under config
        File templatesDir = new File(environment.configFile(), "templates");
        if (templatesDir.exists() && templatesDir.isDirectory()) {
            File[] templatesFiles = templatesDir.listFiles();
            if (templatesFiles != null) {
                for (File templatesFile : templatesFiles) {
                    XContentParser parser = null;
                    try {
                        byte[] templatesData = Streams.copyToByteArray(templatesFile);
                        parser = XContentHelper.createParser(templatesData, 0, templatesData.length);
                        IndexTemplateMetaData template = IndexTemplateMetaData.Builder.fromXContentStandalone(parser);
                        if (Regex.simpleMatch(template.template(), request.index)) {
                            templates.add(template);
                        }
                    } catch (Exception e) {
                        logger.warn("[{}] failed to read template [{}] from config", e, request.index, templatesFile.getAbsolutePath());
                    } finally {
                        Closeables.closeQuietly(parser);
                    }
                }
            }
        }

        Collections.sort(templates, new Comparator<IndexTemplateMetaData>() {
            @Override
            public int compare(IndexTemplateMetaData o1, IndexTemplateMetaData o2) {
                return o2.order() - o1.order();
            }
        });
        return templates;
    }

    private void validate(Request request, ClusterState state) throws ElasticSearchException {
        if (state.routingTable().hasIndex(request.index)) {
            throw new IndexAlreadyExistsException(new Index(request.index));
        }
        if (state.metaData().hasIndex(request.index)) {
            throw new IndexAlreadyExistsException(new Index(request.index));
        }
        if (request.index.contains(" ")) {
            throw new InvalidIndexNameException(new Index(request.index), request.index, "must not contain whitespace");
        }
        if (request.index.contains(",")) {
            throw new InvalidIndexNameException(new Index(request.index), request.index, "must not contain ',");
        }
        if (request.index.contains("#")) {
            throw new InvalidIndexNameException(new Index(request.index), request.index, "must not contain '#");
        }
        if (!request.index.equals(riverIndexName) && !request.index.equals(PercolatorService.INDEX_NAME) && request.index.charAt(0) == '_') {
            throw new InvalidIndexNameException(new Index(request.index), request.index, "must not start with '_'");
        }
        if (!request.index.toLowerCase().equals(request.index)) {
            throw new InvalidIndexNameException(new Index(request.index), request.index, "must be lowercase");
        }
        if (!Strings.validFileName(request.index)) {
            throw new InvalidIndexNameException(new Index(request.index), request.index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (state.metaData().aliases().containsKey(request.index)) {
            throw new InvalidIndexNameException(new Index(request.index), request.index, "an alias with the same name already exists");
        }
    }

    public static interface Listener {

        void onResponse(Response response);

        void onFailure(Throwable t);
    }

    public static class Request {

        final String cause;

        final String index;

        State state = State.OPEN;

        Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

        Map<String, String> mappings = Maps.newHashMap();

        Map<String, IndexMetaData.Custom> customs = newHashMap();


        TimeValue timeout = TimeValue.timeValueSeconds(5);

        Set<ClusterBlock> blocks = Sets.newHashSet();

        public Request(String cause, String index) {
            this.cause = cause;
            this.index = index;
        }

        public Request settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Request mappings(Map<String, String> mappings) {
            this.mappings.putAll(mappings);
            return this;
        }

        public Request mappingsMetaData(Map<String, MappingMetaData> mappings) throws IOException {
            for (Map.Entry<String, MappingMetaData> entry : mappings.entrySet()) {
                this.mappings.put(entry.getKey(), entry.getValue().source().string());
            }
            return this;
        }

        public Request mappingsCompressed(Map<String, CompressedString> mappings) throws IOException {
            for (Map.Entry<String, CompressedString> entry : mappings.entrySet()) {
                this.mappings.put(entry.getKey(), entry.getValue().string());
            }
            return this;
        }

        public Request customs(Map<String, Custom> customs) {
            this.customs.putAll(customs);
            return this;
        }

        public Request blocks(Set<ClusterBlock> blocks) {
            this.blocks.addAll(blocks);
            return this;
        }

        public Request state(State state) {
            this.state = state;
            return this;
        }

        public Request timeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }
    }

    public static class Response {
        private final boolean acknowledged;
        private final IndexMetaData indexMetaData;

        public Response(boolean acknowledged, IndexMetaData indexMetaData) {
            this.acknowledged = acknowledged;
            this.indexMetaData = indexMetaData;
        }

        public boolean acknowledged() {
            return acknowledged;
        }

        public IndexMetaData indexMetaData() {
            return indexMetaData;
        }
    }
}
