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

package org.elasticsearch.indexer.routing;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indexer.IndexerName;
import org.elasticsearch.indexer.cluster.IndexerClusterService;
import org.elasticsearch.indexer.cluster.IndexerClusterState;
import org.elasticsearch.indexer.cluster.IndexerClusterStateUpdateTask;

import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class IndexersRouter extends AbstractLifecycleComponent<IndexersRouter> implements ClusterStateListener {

    private final String indexerIndexName;

    private final Client client;

    private final IndexerClusterService indexerClusterService;

    @Inject public IndexersRouter(Settings settings, Client client, ClusterService clusterService, IndexerClusterService indexerClusterService) {
        super(settings);
        this.indexerIndexName = settings.get("indexer.index_name", "indexer");
        this.indexerClusterService = indexerClusterService;
        this.client = client;
        clusterService.add(this);
    }

    @Override protected void doStart() throws ElasticSearchException {
    }

    @Override protected void doStop() throws ElasticSearchException {
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    @Override public void clusterChanged(final ClusterChangedEvent event) {
        if (!event.localNodeMaster()) {
            return;
        }
        if (event.nodesChanged() || event.metaDataChanged()) {
            indexerClusterService.submitStateUpdateTask("reroute_indexers_node_changed", new IndexerClusterStateUpdateTask() {
                @Override public IndexerClusterState execute(IndexerClusterState currentState) {
                    if (!event.state().metaData().hasIndex(indexerIndexName)) {
                        // if there are routings, publish an empty one (so it will be deleted on nodes), otherwise, return the same state
                        if (!currentState.routing().isEmpty()) {
                            return IndexerClusterState.builder().state(currentState).routing(IndexersRouting.builder()).build();
                        }
                        return currentState;
                    }

                    IndexersRouting.Builder routingBuilder = IndexersRouting.builder().routing(currentState.routing());
                    boolean dirty = false;

                    IndexMetaData indexMetaData = event.state().metaData().index(indexerIndexName);
                    // go over and create new indexer routing (with no node) for new types (indexers names)
                    for (Map.Entry<String, CompressedString> entry : indexMetaData.mappings().entrySet()) {
                        String mappingType = entry.getKey(); // mapping type is the name of the indexer
                        if (!currentState.routing().hasIndexerByName(mappingType)) {
                            // no indexer, we need to add it to the routing with no node allocation
                            try {
                                GetResponse getResponse = client.prepareGet(indexerIndexName, mappingType, "_meta").execute().actionGet();
                                if (getResponse.exists()) {
                                    String indexerType = XContentMapValues.nodeStringValue(getResponse.sourceAsMap().get("type"), null);
                                    if (indexerType == null) {
                                        logger.warn("no indexer type provided for [{}], ignoring...", indexerIndexName);
                                    } else {
                                        routingBuilder.put(new IndexerRouting(new IndexerName(mappingType, indexerType), IndexerRoutingState.UNASSIGNED, null));
                                        dirty = true;
                                    }
                                }
                            } catch (Exception e) {
                                logger.warn("failed to get/parse _meta for [{}]", mappingType);
                            }
                        }
                    }
                    // now, remove routings that were deleted
                    for (IndexerRouting routing : currentState.routing()) {
                        if (!indexMetaData.mappings().containsKey(routing.indexerName().name())) {
                            routingBuilder.remove(routing);
                            dirty = true;
                        }
                    }

                    // now, allocate indexers

                    // see if we can relocate indexers (we can simply first unassign then, then publish) and then, next round, they will be assigned
                    // but, we need to make sure that there will *be* next round of this is the logic


                    if (dirty) {
                        return IndexerClusterState.builder().state(currentState).routing(routingBuilder).build();
                    }
                    return currentState;
                }
            });
        }
    }
}
