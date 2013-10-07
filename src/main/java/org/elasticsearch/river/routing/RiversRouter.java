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

package org.elasticsearch.river.routing;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.cluster.RiverClusterService;
import org.elasticsearch.river.cluster.RiverClusterState;
import org.elasticsearch.river.cluster.RiverClusterStateUpdateTask;
import org.elasticsearch.river.cluster.RiverNodeHelper;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RiversRouter extends AbstractLifecycleComponent<RiversRouter> implements ClusterStateListener {

    private final String riverIndexName;

    private final Client client;

    private final RiverClusterService riverClusterService;

    @Inject
    public RiversRouter(Settings settings, Client client, ClusterService clusterService, RiverClusterService riverClusterService) {
        super(settings);
        this.riverIndexName = RiverIndexName.Conf.indexName(settings);
        this.riverClusterService = riverClusterService;
        this.client = client;
        clusterService.add(this);
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (!event.localNodeMaster()) {
            return;
        }
        riverClusterService.submitStateUpdateTask("reroute_rivers_node_changed", new RiverClusterStateUpdateTask() {
            @Override
            public RiverClusterState execute(RiverClusterState currentState) {
                if (!event.state().metaData().hasIndex(riverIndexName)) {
                    // if there are routings, publish an empty one (so it will be deleted on nodes), otherwise, return the same state
                    if (!currentState.routing().isEmpty()) {
                        return RiverClusterState.builder().state(currentState).routing(RiversRouting.builder()).build();
                    }
                    return currentState;
                }

                RiversRouting.Builder routingBuilder = RiversRouting.builder().routing(currentState.routing());
                boolean dirty = false;

                IndexMetaData indexMetaData = event.state().metaData().index(riverIndexName);
                // go over and create new river routing (with no node) for new types (rivers names)
                for (MappingMetaData mappingMd : indexMetaData.mappings().values()) {
                    String mappingType = mappingMd.type(); // mapping type is the name of the river
                    if (!currentState.routing().hasRiverByName(mappingType)) {
                        // no river, we need to add it to the routing with no node allocation
                        try {
                            GetResponse getResponse = client.prepareGet(riverIndexName, mappingType, "_meta").execute().actionGet();
                            if (getResponse.isExists()) {
                                String riverType = XContentMapValues.nodeStringValue(getResponse.getSourceAsMap().get("type"), null);
                                if (riverType == null) {
                                    logger.warn("no river type provided for [{}], ignoring...", riverIndexName);
                                } else {
                                    routingBuilder.put(new RiverRouting(new RiverName(riverType, mappingType), null));
                                    dirty = true;
                                }
                            }
                        } catch (NoShardAvailableActionException e) {
                            // ignore, we will get it next time...
                        } catch (ClusterBlockException e) {
                            // ignore, we will get it next time
                        } catch (IndexMissingException e) {
                            // ignore, we will get it next time
                        } catch (IllegalIndexShardStateException e) {
                            // ignore, we will get it next time
                        } catch (Exception e) {
                            logger.warn("failed to get/parse _meta for [{}]", e, mappingType);
                        }
                    }
                }
                // now, remove routings that were deleted
                // also, apply nodes that were removed and rivers were running on
                for (RiverRouting routing : currentState.routing()) {
                    if (!indexMetaData.mappings().containsKey(routing.riverName().name())) {
                        routingBuilder.remove(routing);
                        dirty = true;
                    } else if (routing.node() != null && !event.state().nodes().nodeExists(routing.node().id())) {
                        routingBuilder.remove(routing);
                        routingBuilder.put(new RiverRouting(routing.riverName(), null));
                        dirty = true;
                    }
                }

                // build a list from nodes to rivers
                Map<DiscoveryNode, List<RiverRouting>> nodesToRivers = Maps.newHashMap();

                for (DiscoveryNode node : event.state().nodes()) {
                    if (RiverNodeHelper.isRiverNode(node)) {
                        nodesToRivers.put(node, Lists.<RiverRouting>newArrayList());
                    }
                }

                List<RiverRouting> unassigned = Lists.newArrayList();
                for (RiverRouting routing : routingBuilder.build()) {
                    if (routing.node() == null) {
                        unassigned.add(routing);
                    } else {
                        List<RiverRouting> l = nodesToRivers.get(routing.node());
                        if (l == null) {
                            l = Lists.newArrayList();
                            nodesToRivers.put(routing.node(), l);
                        }
                        l.add(routing);
                    }
                }
                for (Iterator<RiverRouting> it = unassigned.iterator(); it.hasNext(); ) {
                    RiverRouting routing = it.next();
                    DiscoveryNode smallest = null;
                    int smallestSize = Integer.MAX_VALUE;
                    for (Map.Entry<DiscoveryNode, List<RiverRouting>> entry : nodesToRivers.entrySet()) {
                        if (RiverNodeHelper.isRiverNode(entry.getKey(), routing.riverName())) {
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
                        nodesToRivers.get(smallest).add(routing);
                    }
                }


                // add relocation logic...

                if (dirty) {
                    return RiverClusterState.builder().state(currentState).routing(routingBuilder).build();
                }
                return currentState;
            }
        });
    }
}
