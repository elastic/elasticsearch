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
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.action.index.NodeIndexCreatedAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardsAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.timer.Timeout;
import org.elasticsearch.common.timer.TimerTask;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.timer.TimerService;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;

/**
 * @author kimchy (shay.banon)
 */
public class MetaDataCreateIndexService extends AbstractComponent {

    private final Environment environment;

    private final TimerService timerService;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final ShardsAllocation shardsAllocation;

    private final NodeIndexCreatedAction nodeIndexCreatedAction;

    private final String riverIndexName;

    @Inject public MetaDataCreateIndexService(Settings settings, Environment environment, TimerService timerService, ClusterService clusterService, IndicesService indicesService,
                                              ShardsAllocation shardsAllocation, NodeIndexCreatedAction nodeIndexCreatedAction, @RiverIndexName String riverIndexName) {
        super(settings);
        this.environment = environment;
        this.timerService = timerService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardsAllocation = shardsAllocation;
        this.nodeIndexCreatedAction = nodeIndexCreatedAction;
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

        clusterService.submitStateUpdateTask("create-index [" + request.index + "], cause [" + request.cause + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                final CreateIndexListener listener = new CreateIndexListener(request, userListener);
                try {
                    if (currentState.routingTable().hasIndex(request.index)) {
                        listener.onFailure(new IndexAlreadyExistsException(new Index(request.index)));
                        return currentState;
                    }
                    if (currentState.metaData().hasIndex(request.index)) {
                        listener.onFailure(new IndexAlreadyExistsException(new Index(request.index)));
                        return currentState;
                    }
                    if (request.index.contains(" ")) {
                        listener.onFailure(new InvalidIndexNameException(new Index(request.index), request.index, "must not contain whitespace"));
                        return currentState;
                    }
                    if (request.index.contains(",")) {
                        listener.onFailure(new InvalidIndexNameException(new Index(request.index), request.index, "must not contain ',"));
                        return currentState;
                    }
                    if (request.index.contains("#")) {
                        listener.onFailure(new InvalidIndexNameException(new Index(request.index), request.index, "must not contain '#"));
                        return currentState;
                    }
                    if (!request.index.equals(riverIndexName) && request.index.charAt(0) == '_') {
                        listener.onFailure(new InvalidIndexNameException(new Index(request.index), request.index, "must not start with '_'"));
                        return currentState;
                    }
                    if (!request.index.toLowerCase().equals(request.index)) {
                        listener.onFailure(new InvalidIndexNameException(new Index(request.index), request.index, "must be lowercase"));
                        return currentState;
                    }
                    if (!Strings.validFileName(request.index)) {
                        listener.onFailure(new InvalidIndexNameException(new Index(request.index), request.index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS));
                        return currentState;
                    }
                    if (currentState.metaData().aliases().contains(request.index)) {
                        listener.onFailure(new InvalidIndexNameException(new Index(request.index), request.index, "an alias with the same name already exists"));
                        return currentState;
                    }

                    // add to the mappings files that exists within the config/mappings location
                    Map<String, CompressedString> mappings = Maps.newHashMap();
                    File mappingsDir = new File(environment.configFile(), "mappings");
                    if (mappingsDir.exists() && mappingsDir.isDirectory()) {
                        File defaultMappingsDir = new File(mappingsDir, "_default");
                        if (defaultMappingsDir.exists() && defaultMappingsDir.isDirectory()) {
                            addMappings(mappings, defaultMappingsDir);
                        }
                        File indexMappingsDir = new File(mappingsDir, request.index);
                        if (indexMappingsDir.exists() && indexMappingsDir.isDirectory()) {
                            addMappings(mappings, indexMappingsDir);
                        }
                    }

                    // put this last so index level mappings can override default mappings
                    for (Map.Entry<String, String> entry : request.mappings.entrySet()) {
                        mappings.put(entry.getKey(), new CompressedString(entry.getValue()));
                    }

                    ImmutableSettings.Builder indexSettingsBuilder = settingsBuilder().put(request.settings);
                    if (request.settings.get(SETTING_NUMBER_OF_SHARDS) == null) {
                        if (request.index.equals(riverIndexName)) {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 1));
                        } else {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
                        }
                    }
                    if (request.settings.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                        if (request.index.equals(riverIndexName)) {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 2));
                        } else {
                            indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                        }
                    }
                    Settings actualIndexSettings = indexSettingsBuilder.build();

                    // create the index here (on the master) to validate it can be created, as well as adding the mapping
                    indicesService.createIndex(request.index, actualIndexSettings, clusterService.state().nodes().localNode().id());
                    // now add the mappings
                    IndexService indexService = indicesService.indexServiceSafe(request.index);
                    MapperService mapperService = indexService.mapperService();
                    for (Map.Entry<String, CompressedString> entry : mappings.entrySet()) {
                        try {
                            mapperService.add(entry.getKey(), entry.getValue().string());
                        } catch (Exception e) {
                            indicesService.deleteIndex(request.index);
                            throw new MapperParsingException("mapping [" + entry.getKey() + "]", e);
                        }
                    }
                    // now, update the mappings with the actual source
                    mappings.clear();
                    for (DocumentMapper mapper : mapperService) {
                        mappings.put(mapper.type(), mapper.mappingSource());
                    }

                    final IndexMetaData.Builder indexMetaDataBuilder = newIndexMetaDataBuilder(request.index).settings(actualIndexSettings);
                    for (Map.Entry<String, CompressedString> entry : mappings.entrySet()) {
                        indexMetaDataBuilder.putMapping(entry.getKey(), entry.getValue());
                    }
                    final IndexMetaData indexMetaData = indexMetaDataBuilder.build();

                    MetaData newMetaData = newMetaDataBuilder()
                            .metaData(currentState.metaData())
                            .put(indexMetaData)
                            .build();

                    logger.info("[{}] creating index, cause [{}], shards [{}]/[{}], mappings {}", request.index, request.cause, indexMetaData.numberOfShards(), indexMetaData.numberOfReplicas(), mappings.keySet());

                    final AtomicInteger counter = new AtomicInteger(currentState.nodes().size() - 1); // -1 since we added it on the master already
                    if (counter.get() == 0) {
                        // no nodes to add to
                        listener.onResponse(new Response(true, indexMetaData));
                    } else {

                        final NodeIndexCreatedAction.Listener nodeIndexCreateListener = new NodeIndexCreatedAction.Listener() {
                            @Override public void onNodeIndexCreated(String index, String nodeId) {
                                if (index.equals(request.index)) {
                                    if (counter.decrementAndGet() == 0) {
                                        listener.onResponse(new Response(true, indexMetaData));
                                        nodeIndexCreatedAction.remove(this);
                                    }
                                }
                            }
                        };
                        nodeIndexCreatedAction.add(nodeIndexCreateListener);

                        Timeout timeoutTask = timerService.newTimeout(new TimerTask() {
                            @Override public void run(Timeout timeout) throws Exception {
                                listener.onResponse(new Response(false, indexMetaData));
                                nodeIndexCreatedAction.remove(nodeIndexCreateListener);
                            }
                        }, request.timeout, TimerService.ExecutionType.THREADED);
                        listener.timeout = timeoutTask;
                    }

                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    if (!request.blocks.isEmpty()) {
                        for (ClusterBlock block : request.blocks) {
                            blocks.addIndexBlock(request.index, block);
                        }
                    }

                    return newClusterStateBuilder().state(currentState).blocks(blocks).metaData(newMetaData).build();
                } catch (Exception e) {
                    listener.onFailure(e);
                    return currentState;
                }
            }
        });
    }

    private void addMappings(Map<String, CompressedString> mappings, File mappingsDir) {
        File[] mappingsFiles = mappingsDir.listFiles();
        for (File mappingFile : mappingsFiles) {
            String fileNameNoSuffix = mappingFile.getName().substring(0, mappingFile.getName().lastIndexOf('.'));
            if (mappings.containsKey(fileNameNoSuffix)) {
                // if we have the mapping defined, ignore it
                continue;
            }
            try {
                mappings.put(fileNameNoSuffix, new CompressedString(Streams.copyToString(new FileReader(mappingFile))));
            } catch (IOException e) {
                logger.warn("failed to read mapping [" + fileNameNoSuffix + "] from location [" + mappingFile + "], ignoring...", e);
            }
        }
    }

    class CreateIndexListener implements Listener {

        private AtomicBoolean notified = new AtomicBoolean();

        private final Request request;

        private final Listener listener;

        volatile Timeout timeout;

        private CreateIndexListener(Request request, Listener listener) {
            this.request = request;
            this.listener = listener;
        }

        @Override public void onResponse(final Response response) {
            if (notified.compareAndSet(false, true)) {
                if (timeout != null) {
                    timeout.cancel();
                }
                // do the reroute after indices have been created on all the other nodes so we can query them for some info (like shard allocation)
                clusterService.submitStateUpdateTask("reroute after index [" + request.index + "] creation", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        RoutingTable.Builder routingTableBuilder = new RoutingTable.Builder();
                        for (IndexRoutingTable indexRoutingTable : currentState.routingTable().indicesRouting().values()) {
                            routingTableBuilder.add(indexRoutingTable);
                        }
                        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(request.index)
                                .initializeEmpty(currentState.metaData().index(request.index));
                        routingTableBuilder.add(indexRoutingBuilder);
                        RoutingAllocation.Result routingResult = shardsAllocation.reroute(newClusterStateBuilder().state(currentState).routingTable(routingTableBuilder).build());
                        return newClusterStateBuilder().state(currentState).routingResult(routingResult).build();
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        logger.info("[{}] created and added to cluster_state", request.index);
                        listener.onResponse(response);
                    }
                });
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

        final String cause;

        final String index;

        Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

        Map<String, String> mappings = Maps.newHashMap();

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

        public Request mappingsCompressed(Map<String, CompressedString> mappings) throws IOException {
            for (Map.Entry<String, CompressedString> entry : mappings.entrySet()) {
                this.mappings.put(entry.getKey(), entry.getValue().string());
            }
            return this;
        }

        public Request blocks(Set<ClusterBlock> blocks) {
            this.blocks.addAll(blocks);
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
