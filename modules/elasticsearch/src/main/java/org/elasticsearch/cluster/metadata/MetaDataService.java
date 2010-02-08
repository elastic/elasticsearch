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

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.action.index.NodeIndexCreatedAction;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.strategy.ShardsRoutingStrategy;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.InvalidTypeNameException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;

/**
 * @author kimchy (Shay Banon)
 */
public class MetaDataService extends AbstractComponent {

    private final ClusterService clusterService;

    private final ShardsRoutingStrategy shardsRoutingStrategy;

    private final IndicesService indicesService;

    private final NodeIndexCreatedAction nodeIndexCreatedAction;

    private final NodeIndexDeletedAction nodeIndexDeletedAction;

    @Inject public MetaDataService(Settings settings, ClusterService clusterService, IndicesService indicesService, ShardsRoutingStrategy shardsRoutingStrategy,
                                   NodeIndexCreatedAction nodeIndexCreatedAction, NodeIndexDeletedAction nodeIndexDeletedAction) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardsRoutingStrategy = shardsRoutingStrategy;
        this.nodeIndexCreatedAction = nodeIndexCreatedAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
    }

    public synchronized boolean createIndex(final String index, final Settings indexSettings, TimeValue timeout) throws IndexAlreadyExistsException {
        if (clusterService.state().routingTable().hasIndex(index)) {
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

        final CountDownLatch latch = new CountDownLatch(clusterService.state().nodes().size());
        NodeIndexCreatedAction.Listener nodeCreatedListener = new NodeIndexCreatedAction.Listener() {
            @Override public void onNodeIndexCreated(String mIndex, String nodeId) {
                if (index.equals(mIndex)) {
                    latch.countDown();
                }
            }
        };
        nodeIndexCreatedAction.add(nodeCreatedListener);
        clusterService.submitStateUpdateTask("create-index [" + index + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                RoutingTable.Builder routingTableBuilder = new RoutingTable.Builder();
                for (IndexRoutingTable indexRoutingTable : currentState.routingTable().indicesRouting().values()) {
                    routingTableBuilder.add(indexRoutingTable);
                }
                ImmutableSettings.Builder indexSettingsBuilder = new ImmutableSettings.Builder().putAll(indexSettings);
                if (indexSettings.get(SETTING_NUMBER_OF_SHARDS) == null) {
                    indexSettingsBuilder.putInt(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
                }
                if (indexSettings.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                    indexSettingsBuilder.putInt(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                }
                Settings actualIndexSettings = indexSettingsBuilder.build();

                IndexMetaData indexMetaData = newIndexMetaDataBuilder(index).settings(actualIndexSettings).build();
                MetaData newMetaData = newMetaDataBuilder()
                        .metaData(currentState.metaData())
                        .put(indexMetaData)
                        .build();

                IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index)
                        .initializeEmpty(newMetaData.index(index));
                routingTableBuilder.add(indexRoutingBuilder);

                logger.info("Creating Index [{}], shards [{}]/[{}]", new Object[]{index, indexMetaData.numberOfShards(), indexMetaData.numberOfReplicas()});
                RoutingTable newRoutingTable = shardsRoutingStrategy.reroute(newClusterStateBuilder().state(currentState).routingTable(routingTableBuilder).metaData(newMetaData).build());
                return newClusterStateBuilder().state(currentState).routingTable(newRoutingTable).metaData(newMetaData).build();
            }
        });

        try {
            return latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        } finally {
            nodeIndexCreatedAction.remove(nodeCreatedListener);
        }
    }

    public synchronized boolean deleteIndex(final String index, TimeValue timeout) throws IndexMissingException {
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
        try {
            return latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        } finally {
            nodeIndexDeletedAction.remove(listener);
        }
    }

    public void addMapping(final String[] indices, String mappingType, final String mappingSource) throws ElasticSearchException {
        ClusterState clusterState = clusterService.state();
        for (String index : indices) {
            IndexRoutingTable indexTable = clusterState.routingTable().indicesRouting().get(index);
            if (indexTable == null) {
                throw new IndexMissingException(new Index(index));
            }
        }

        DocumentMapper documentMapper = null;
        for (String index : indices) {
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                // try and parse it (no need to add it here) so we can bail early in case of parsing exception
                documentMapper = indexService.mapperService().parse(mappingType, mappingSource);
            } else {
                throw new IndexMissingException(new Index(index));
            }
        }

        if (mappingType == null) {
            mappingType = documentMapper.type();
        } else if (!mappingType.equals(documentMapper.type())) {
            throw new InvalidTypeNameException("Type name provided does not match type name within mapping definition");
        }
        if (mappingType.charAt(0) == '_') {
            throw new InvalidTypeNameException("Document mapping type name can't start with '_'");
        }

        logger.info("Indices [" + Arrays.toString(indices) + "]: Creating mapping [" + mappingType + "] with source [" + mappingSource + "]");

        final String mappingTypeP = mappingType;
        clusterService.submitStateUpdateTask("create-mapping [" + mappingTypeP + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData.Builder builder = newMetaDataBuilder().metaData(currentState.metaData());
                for (String indexName : indices) {
                    IndexMetaData indexMetaData = currentState.metaData().index(indexName);
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(indexName));
                    }
                    builder.put(newIndexMetaDataBuilder(indexMetaData).addMapping(mappingTypeP, mappingSource));
                }
                return newClusterStateBuilder().state(currentState).metaData(builder).build();
            }
        });
    }

}
