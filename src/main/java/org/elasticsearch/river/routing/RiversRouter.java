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

package org.elasticsearch.river.routing;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.cluster.RiverClusterService;
import org.elasticsearch.river.cluster.RiverClusterState;
import org.elasticsearch.river.cluster.RiverClusterStateUpdateTask;
import org.elasticsearch.river.cluster.RiverNodeHelper;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RiversRouter extends AbstractLifecycleComponent<RiversRouter> implements ClusterStateListener {

    private static final TimeValue RIVER_START_RETRY_INTERVAL = TimeValue.timeValueMillis(1000);
    private static final int RIVER_START_MAX_RETRIES = 5;

    private final String riverIndexName;

    private final Client client;

    private final RiverClusterService riverClusterService;

    private final ThreadPool threadPool;

    @Inject
    public RiversRouter(Settings settings, Client client, ClusterService clusterService, RiverClusterService riverClusterService, ThreadPool threadPool) {
        super(settings);
        this.riverIndexName = RiverIndexName.Conf.indexName(settings);
        this.riverClusterService = riverClusterService;
        this.client = client;
        this.threadPool = threadPool;
        clusterService.add(this);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (!event.localNodeMaster()) {
            return;
        }
        final String source = "reroute_rivers_node_changed";
        //we'll try again a few times if we don't find the river _meta document while the type is there
        final CountDown countDown = new CountDown(RIVER_START_MAX_RETRIES);
        riverClusterService.submitStateUpdateTask(source, new RiverClusterStateUpdateTask() {
            @Override
            public RiverClusterState execute(RiverClusterState currentState) {
                return updateRiverClusterState(source, currentState, event.state(), countDown);
            }
        });
    }

    protected RiverClusterState updateRiverClusterState(final String source, final RiverClusterState currentState,
                                                                     ClusterState newClusterState, final CountDown countDown) {
        if (!newClusterState.metaData().hasIndex(riverIndexName)) {
            // if there are routings, publish an empty one (so it will be deleted on nodes), otherwise, return the same state
            if (!currentState.routing().isEmpty()) {
                return RiverClusterState.builder().state(currentState).routing(RiversRouting.builder()).build();
            }
            return currentState;
        }

        RiversRouting.Builder routingBuilder = RiversRouting.builder().routing(currentState.routing());
        boolean dirty = false;
        IndexMetaData indexMetaData = newClusterState.metaData().index(riverIndexName);

        boolean metaFound = true;
        // go over and create new river routing (with no node) for new types (rivers names)
        for (ObjectCursor<MappingMetaData> cursor : indexMetaData.mappings().values()) {
            String mappingType = cursor.value.type(); // mapping type is the name of the river
            if (MapperService.DEFAULT_MAPPING.equals(mappingType)) {
                continue;
            }
            if (!currentState.routing().hasRiverByName(mappingType)) {
                // no river, we need to add it to the routing with no node allocation
                try {
                    GetResponse getResponse = client.prepareGet(riverIndexName, mappingType, "_meta").setPreference("_primary").get();
                    if (getResponse.isExists()) {

                        logger.debug("{}/{}/_meta document found.", riverIndexName, mappingType);

                        String riverType = XContentMapValues.nodeStringValue(getResponse.getSourceAsMap().get("type"), null);
                        if (riverType == null) {
                            logger.warn("no river type provided for [{}], ignoring...", riverIndexName);
                        } else {
                            routingBuilder.put(new RiverRouting(new RiverName(riverType, mappingType), null));
                            dirty = true;
                        }
                    } else {
                        // At least one type does not have _meta
                        metaFound = false;
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

        // At least one type does not have _meta, so we are
        // going to reschedule some checks
        if (!metaFound) {
            if (countDown.countDown()) {
                logger.warn("no river _meta document found after {} attempts", RIVER_START_MAX_RETRIES);
            } else {
                logger.debug("no river _meta document found retrying in {} ms", RIVER_START_RETRY_INTERVAL.millis());
                try {
                    threadPool.schedule(RIVER_START_RETRY_INTERVAL, ThreadPool.Names.GENERIC, new Runnable() {
                        @Override
                        public void run() {
                            riverClusterService.submitStateUpdateTask(source, new RiverClusterStateUpdateTask() {
                                @Override
                                public RiverClusterState execute(RiverClusterState currentState) {
                                    return updateRiverClusterState(source, currentState, riverClusterService.state(), countDown);
                                }
                            });
                        }
                    });
                } catch (EsRejectedExecutionException ex) {
                    logger.debug("Couldn't schedule river start retry, node might be shutting down", ex);
                }
            }
        }

        // now, remove routings that were deleted
        // also, apply nodes that were removed and rivers were running on
        for (RiverRouting routing : currentState.routing()) {
            if (!indexMetaData.mappings().containsKey(routing.riverName().name())) {
                routingBuilder.remove(routing);
                dirty = true;
            } else if (routing.node() != null && !newClusterState.nodes().nodeExists(routing.node().id())) {
                routingBuilder.remove(routing);
                routingBuilder.put(new RiverRouting(routing.riverName(), null));
                dirty = true;
            }
        }

        // build a list from nodes to rivers
        Map<DiscoveryNode, List<RiverRouting>> nodesToRivers = Maps.newHashMap();

        for (DiscoveryNode node : newClusterState.nodes()) {
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
                logger.debug("going to allocate river [{}] on node {}", routing.riverName().getName(), smallest);
            }
        }


        // add relocation logic...

        if (dirty) {
            return RiverClusterState.builder().state(currentState).routing(routingBuilder).build();
        }
        return currentState;
    }
}
