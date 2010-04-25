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

import org.elasticsearch.util.gcommon.collect.Maps;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.action.index.NodeIndexCreatedAction;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.action.index.NodeMappingCreatedAction;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.strategy.ShardsRoutingStrategy;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.InvalidTypeNameException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.Tuple;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.io.Streams;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.util.gcommon.collect.Maps.*;
import static org.elasticsearch.util.gcommon.collect.Sets.*;
import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.index.mapper.DocumentMapper.MergeFlags.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;

/**
 * @author kimchy (Shay Banon)
 */
public class MetaDataService extends AbstractComponent {

    private final Environment environment;

    private final ClusterService clusterService;

    private final ShardsRoutingStrategy shardsRoutingStrategy;

    private final IndicesService indicesService;

    private final NodeIndexCreatedAction nodeIndexCreatedAction;

    private final NodeIndexDeletedAction nodeIndexDeletedAction;

    private final NodeMappingCreatedAction nodeMappingCreatedAction;

    @Inject public MetaDataService(Settings settings, Environment environment, ClusterService clusterService, IndicesService indicesService, ShardsRoutingStrategy shardsRoutingStrategy,
                                   NodeIndexCreatedAction nodeIndexCreatedAction, NodeIndexDeletedAction nodeIndexDeletedAction,
                                   NodeMappingCreatedAction nodeMappingCreatedAction) {
        super(settings);
        this.environment = environment;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardsRoutingStrategy = shardsRoutingStrategy;
        this.nodeIndexCreatedAction = nodeIndexCreatedAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
        this.nodeMappingCreatedAction = nodeMappingCreatedAction;
    }

    // TODO should find nicer solution than sync here, since we block for timeout (same for other ops)

    public synchronized IndicesAliasesResult indicesAliases(final List<AliasAction> aliasActions) {
        ClusterState clusterState = clusterService.state();

        for (AliasAction aliasAction : aliasActions) {
            if (!clusterState.metaData().hasIndex(aliasAction.index())) {
                throw new IndexMissingException(new Index(aliasAction.index()));
            }
        }

        clusterService.submitStateUpdateTask("index-aliases", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData.Builder builder = newMetaDataBuilder().metaData(currentState.metaData());
                for (AliasAction aliasAction : aliasActions) {
                    IndexMetaData indexMetaData = builder.get(aliasAction.index());
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(aliasAction.index()));
                    }
                    Set<String> indexAliases = newHashSet(indexMetaData.settings().getAsArray("index.aliases"));
                    if (aliasAction.actionType() == AliasAction.Type.ADD) {
                        indexAliases.add(aliasAction.alias());
                    } else if (aliasAction.actionType() == AliasAction.Type.REMOVE) {
                        indexAliases.remove(aliasAction.alias());
                    }

                    Settings settings = settingsBuilder().put(indexMetaData.settings())
                            .putArray("index.aliases", indexAliases.toArray(new String[indexAliases.size()]))
                            .build();

                    builder.put(newIndexMetaDataBuilder(indexMetaData).settings(settings));
                }
                return newClusterStateBuilder().state(currentState).metaData(builder).build();
            }
        });

        return new IndicesAliasesResult();
    }

    public synchronized CreateIndexResult createIndex(final String cause, final String index, final Settings indexSettings, Map<String, String> mappings, TimeValue timeout) throws IndexAlreadyExistsException {
        ClusterState clusterState = clusterService.state();

        if (clusterState.routingTable().hasIndex(index)) {
            throw new IndexAlreadyExistsException(new Index(index));
        }
        if (clusterState.metaData().hasIndex(index)) {
            throw new IndexAlreadyExistsException(new Index(index));
        }
        if (index.contains(" ")) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain whitespace");
        }
        if (index.contains(",")) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain ',");
        }
        if (index.contains("#")) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain '#");
        }
        if (index.charAt(0) == '_') {
            throw new InvalidIndexNameException(new Index(index), index, "must not start with '_'");
        }
        if (!index.toLowerCase().equals(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "must be lowercase");
        }
        if (!Strings.validFileName(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (clusterState.metaData().aliases().contains(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "an alias with the same name already exists");
        }

        // add to the mappings files that exists within the config/mappings location
        if (mappings == null) {
            mappings = Maps.newHashMap();
        } else {
            mappings = Maps.newHashMap(mappings);
        }
        File mappingsDir = new File(environment.configFile(), "mappings");
        if (mappingsDir.exists() && mappingsDir.isDirectory()) {
            File defaultMappingsDir = new File(mappingsDir, "_default");
            if (mappingsDir.exists() && mappingsDir.isDirectory()) {
                addMappings(mappings, defaultMappingsDir);
            }
            File indexMappingsDir = new File(mappingsDir, index);
            if (mappingsDir.exists() && mappingsDir.isDirectory()) {
                addMappings(mappings, indexMappingsDir);
            }
        }

        final Map<String, String> fMappings = mappings;

        final CountDownLatch latch = new CountDownLatch(clusterService.state().nodes().size());
        NodeIndexCreatedAction.Listener nodeCreatedListener = new NodeIndexCreatedAction.Listener() {
            @Override public void onNodeIndexCreated(String mIndex, String nodeId) {
                if (index.equals(mIndex)) {
                    latch.countDown();
                }
            }
        };
        nodeIndexCreatedAction.add(nodeCreatedListener);
        clusterService.submitStateUpdateTask("create-index [" + index + "], cause [" + cause + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                RoutingTable.Builder routingTableBuilder = new RoutingTable.Builder();
                for (IndexRoutingTable indexRoutingTable : currentState.routingTable().indicesRouting().values()) {
                    routingTableBuilder.add(indexRoutingTable);
                }
                ImmutableSettings.Builder indexSettingsBuilder = settingsBuilder().put(indexSettings);
                if (indexSettings.get(SETTING_NUMBER_OF_SHARDS) == null) {
                    indexSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
                }
                if (indexSettings.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                    indexSettingsBuilder.put(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                }
                Settings actualIndexSettings = indexSettingsBuilder.build();

                IndexMetaData.Builder indexMetaData = newIndexMetaDataBuilder(index).settings(actualIndexSettings);
                for (Map.Entry<String, String> entry : fMappings.entrySet()) {
                    indexMetaData.putMapping(entry.getKey(), entry.getValue());
                }
                MetaData newMetaData = newMetaDataBuilder()
                        .metaData(currentState.metaData())
                        .put(indexMetaData)
                        .build();

                IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index)
                        .initializeEmpty(newMetaData.index(index));
                routingTableBuilder.add(indexRoutingBuilder);

                logger.info("Creating Index [{}], cause [{}], shards [{}]/[{}], mappings {}", new Object[]{index, cause, indexMetaData.numberOfShards(), indexMetaData.numberOfReplicas(), fMappings.keySet()});
                RoutingTable newRoutingTable = shardsRoutingStrategy.reroute(newClusterStateBuilder().state(currentState).routingTable(routingTableBuilder).metaData(newMetaData).build());
                return newClusterStateBuilder().state(currentState).routingTable(newRoutingTable).metaData(newMetaData).build();
            }
        });

        boolean acknowledged;
        try {
            acknowledged = latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            acknowledged = false;
        } finally {
            nodeIndexCreatedAction.remove(nodeCreatedListener);
        }
        return new CreateIndexResult(acknowledged);
    }

    private void addMappings(Map<String, String> mappings, File mappingsDir) {
        File[] mappingsFiles = mappingsDir.listFiles();
        for (File mappingFile : mappingsFiles) {
            String fileNameNoSuffix = mappingFile.getName().substring(0, mappingFile.getName().lastIndexOf('.'));
            if (mappings.containsKey(fileNameNoSuffix)) {
                // if we have the mapping defined, ignore it
                continue;
            }
            try {
                mappings.put(fileNameNoSuffix, Streams.copyToString(new FileReader(mappingFile)));
            } catch (IOException e) {
                logger.warn("Failed to read mapping [" + fileNameNoSuffix + "] from location [" + mappingFile + "], ignoring...", e);
            }
        }
    }

    public synchronized DeleteIndexResult deleteIndex(final String index, TimeValue timeout) throws IndexMissingException {
        RoutingTable routingTable = clusterService.state().routingTable();
        if (!routingTable.hasIndex(index)) {
            throw new IndexMissingException(new Index(index));
        }

        logger.info("Deleting index [{}]", index);

        final CountDownLatch latch = new CountDownLatch(clusterService.state().nodes().size());
        NodeIndexDeletedAction.Listener listener = new NodeIndexDeletedAction.Listener() {
            @Override public void onNodeIndexDeleted(String fIndex, String nodeId) {
                if (fIndex.equals(index)) {
                    latch.countDown();
                }
            }
        };
        nodeIndexDeletedAction.add(listener);
        clusterService.submitStateUpdateTask("delete-index [" + index + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                RoutingTable.Builder routingTableBuilder = new RoutingTable.Builder();
                for (IndexRoutingTable indexRoutingTable : currentState.routingTable().indicesRouting().values()) {
                    if (!indexRoutingTable.index().equals(index)) {
                        routingTableBuilder.add(indexRoutingTable);
                    }
                }
                MetaData newMetaData = newMetaDataBuilder()
                        .metaData(currentState.metaData())
                        .remove(index)
                        .build();

                RoutingTable newRoutingTable = shardsRoutingStrategy.reroute(
                        newClusterStateBuilder().state(currentState).routingTable(routingTableBuilder).metaData(newMetaData).build());
                return newClusterStateBuilder().state(currentState).routingTable(newRoutingTable).metaData(newMetaData).build();
            }
        });
        boolean acknowledged;
        try {
            acknowledged = latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            acknowledged = false;
        } finally {
            nodeIndexDeletedAction.remove(listener);
        }
        return new DeleteIndexResult(acknowledged);
    }

    public synchronized void updateMapping(final String index, final String type, final String mappingSource) {
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
            logger.debug("Index [" + index + "]: Update mapping [" + type + "] (dynamic) with source [" + updatedMappingSource + "]");
        } else if (logger.isInfoEnabled()) {
            logger.info("Index [" + index + "]: Update mapping [" + type + "] (dynamic)");
        }
        // publish the new mapping
        clusterService.submitStateUpdateTask("update-mapping [" + index + "][" + type + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData.Builder builder = newMetaDataBuilder().metaData(currentState.metaData());
                IndexMetaData indexMetaData = currentState.metaData().index(index);
                builder.put(newIndexMetaDataBuilder(indexMetaData).putMapping(type, updatedMappingSource));
                return newClusterStateBuilder().state(currentState).metaData(builder).build();
            }
        });
    }

    public synchronized PutMappingResult putMapping(final String[] indices, String mappingType, final String mappingSource, boolean ignoreConflicts, TimeValue timeout) throws ElasticSearchException {
        ClusterState clusterState = clusterService.state();
        if (indices.length == 0) {
            throw new IndexMissingException(new Index("_all"));
        }
        for (String index : indices) {
            IndexRoutingTable indexTable = clusterState.routingTable().indicesRouting().get(index);
            if (indexTable == null) {
                throw new IndexMissingException(new Index(index));
            }
        }

        Map<String, DocumentMapper> newMappers = newHashMap();
        Map<String, DocumentMapper> existingMappers = newHashMap();
        for (String index : indices) {
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                // try and parse it (no need to add it here) so we can bail early in case of parsing exception
                DocumentMapper newMapper = indexService.mapperService().parse(mappingType, mappingSource);
                newMappers.put(index, newMapper);
                DocumentMapper existingMapper = indexService.mapperService().documentMapper(mappingType);
                if (existingMapper != null) {
                    // first, simulate
                    DocumentMapper.MergeResult mergeResult = existingMapper.merge(newMapper, mergeFlags().simulate(true));
                    // if we have conflicts, and we are not supposed to ignore them, throw an exception
                    if (!ignoreConflicts && mergeResult.hasConflicts()) {
                        throw new MergeMappingException(mergeResult.conflicts());
                    }
                    existingMappers.put(index, existingMapper);
                }
            } else {
                throw new IndexMissingException(new Index(index));
            }
        }

        if (mappingType == null) {
            mappingType = newMappers.values().iterator().next().type();
        } else if (!mappingType.equals(newMappers.values().iterator().next().type())) {
            throw new InvalidTypeNameException("Type name provided does not match type name within mapping definition");
        }
        if (mappingType.charAt(0) == '_') {
            throw new InvalidTypeNameException("Document mapping type name can't start with '_'");
        }

        final Map<String, Tuple<String, String>> mappings = newHashMap();
        for (Map.Entry<String, DocumentMapper> entry : newMappers.entrySet()) {
            Tuple<String, String> mapping;
            String index = entry.getKey();
            // do the actual merge here on the master, and update the mapping source
            DocumentMapper newMapper = entry.getValue();
            if (existingMappers.containsKey(entry.getKey())) {
                // we have an existing mapping, do the merge here (on the master), it will automatically update the mapping source
                DocumentMapper existingMapper = existingMappers.get(entry.getKey());
                existingMapper.merge(newMapper, mergeFlags().simulate(false));
                // use the merged mapping source
                mapping = new Tuple<String, String>(existingMapper.type(), existingMapper.buildSource());
            } else {
                mapping = new Tuple<String, String>(newMapper.type(), newMapper.buildSource());
            }
            mappings.put(index, mapping);
            if (logger.isDebugEnabled()) {
                logger.debug("Index [" + index + "]: Put mapping [" + mapping.v1() + "] with source [" + mapping.v2() + "]");
            } else if (logger.isInfoEnabled()) {
                logger.info("Index [" + index + "]: Put mapping [" + mapping.v1() + "]");
            }
        }

        final CountDownLatch latch = new CountDownLatch(clusterService.state().nodes().size() * indices.length);
        final Set<String> indicesSet = newHashSet(indices);
        final String fMappingType = mappingType;
        NodeMappingCreatedAction.Listener listener = new NodeMappingCreatedAction.Listener() {
            @Override public void onNodeMappingCreated(NodeMappingCreatedAction.NodeMappingCreatedResponse response) {
                if (indicesSet.contains(response.index()) && response.type().equals(fMappingType)) {
                    latch.countDown();
                }
            }
        };
        nodeMappingCreatedAction.add(listener);

        clusterService.submitStateUpdateTask("put-mapping [" + mappingType + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData.Builder builder = newMetaDataBuilder().metaData(currentState.metaData());
                for (String indexName : indices) {
                    IndexMetaData indexMetaData = currentState.metaData().index(indexName);
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(indexName));
                    }
                    Tuple<String, String> mapping = mappings.get(indexName);
                    builder.put(newIndexMetaDataBuilder(indexMetaData).putMapping(mapping.v1(), mapping.v2()));
                }
                return newClusterStateBuilder().state(currentState).metaData(builder).build();
            }
        });

        boolean acknowledged;
        try {
            acknowledged = latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            acknowledged = false;
        } finally {
            nodeMappingCreatedAction.remove(listener);
        }

        return new PutMappingResult(acknowledged);
    }

    /**
     * The result of a putting mapping.
     */
    public static class PutMappingResult {

        private final boolean acknowledged;

        public PutMappingResult(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public boolean acknowledged() {
            return acknowledged;
        }
    }

    public static class CreateIndexResult {

        private final boolean acknowledged;

        public CreateIndexResult(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public boolean acknowledged() {
            return acknowledged;
        }
    }

    public static class DeleteIndexResult {

        private final boolean acknowledged;

        public DeleteIndexResult(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public boolean acknowledged() {
            return acknowledged;
        }
    }

    public static class IndicesAliasesResult {

        public IndicesAliasesResult() {
        }
    }
}
