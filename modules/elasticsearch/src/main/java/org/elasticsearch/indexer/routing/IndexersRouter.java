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
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indexer.IndexerIndexName;
import org.elasticsearch.indexer.IndexerName;
import org.elasticsearch.indexer.cluster.IndexerClusterService;
import org.elasticsearch.indexer.cluster.IndexerClusterState;
import org.elasticsearch.indexer.cluster.IndexerClusterStateUpdateTask;
import org.elasticsearch.indexer.cluster.IndexerNodeHelper;
import org.elasticsearch.indices.IndexMissingException;

import java.util.Iterator;
import java.util.List;
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
        this.indexerIndexName = IndexerIndexName.Conf.indexName(settings);
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
        if (event.nodesChanged() || event.metaDataChanged() || event.blocksChanged()) {
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
                                client.admin().indices().prepareRefresh(indexerIndexName).execute().actionGet();
                                GetResponse getResponse = client.prepareGet(indexerIndexName, mappingType, "_meta").execute().actionGet();
                                if (getResponse.exists()) {
                                    String indexerType = XContentMapValues.nodeStringValue(getResponse.sourceAsMap().get("type"), null);
                                    if (indexerType == null) {
                                        logger.warn("no indexer type provided for [{}], ignoring...", indexerIndexName);
                                    } else {
                                        routingBuilder.put(new IndexerRouting(new IndexerName(indexerType, mappingType), null));
                                        dirty = true;
                                    }
                                }
                            } catch (ClusterBlockException e) {
                                // ignore, we will get it next time
                            } catch (IndexMissingException e) {
                                // ignore, we will get it next time
                            } catch (Exception e) {
                                logger.warn("failed to get/parse _meta for [{}]", e, mappingType);
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

                    // build a list from nodes to indexers
                    Map<DiscoveryNode, List<IndexerRouting>> nodesToIndexers = Maps.newHashMap();

                    for (DiscoveryNode node : event.state().nodes()) {
                        if (IndexerNodeHelper.isIndexerNode(node)) {
                            nodesToIndexers.put(node, Lists.<IndexerRouting>newArrayList());
                        }
                    }

                    List<IndexerRouting> unassigned = Lists.newArrayList();
                    for (IndexerRouting routing : routingBuilder.build()) {
                        if (routing.node() == null) {
                            unassigned.add(routing);
                        } else {
                            List<IndexerRouting> l = nodesToIndexers.get(routing.node());
                            if (l == null) {
                                l = Lists.newArrayList();
                                nodesToIndexers.put(routing.node(), l);
                            }
                            l.add(routing);
                        }
                    }
                    for (Iterator<IndexerRouting> it = unassigned.iterator(); it.hasNext();) {
                        IndexerRouting routing = it.next();
                        DiscoveryNode smallest = null;
                        int smallestSize = Integer.MAX_VALUE;
                        for (Map.Entry<DiscoveryNode, List<IndexerRouting>> entry : nodesToIndexers.entrySet()) {
                            if (IndexerNodeHelper.isIndexerNode(entry.getKey(), routing.indexerName())) {
                                if (entry.getValue().size() < smallestSize) {
                                    smallestSize = entry.getValue().size();
                                    smallest = entry.getKey();
                                }
                            }
                        }
                        if (smallest != null) {
                            dirty = true;
                            it.remove();
                            routing.node(smallest);
                            nodesToIndexers.get(smallest).add(routing);
                        }
                    }


                    // add relocation logic...

                    if (dirty) {
                        return IndexerClusterState.builder().state(currentState).routing(routingBuilder).build();
                    }
                    return currentState;
                }
            });
        }
    }
}
