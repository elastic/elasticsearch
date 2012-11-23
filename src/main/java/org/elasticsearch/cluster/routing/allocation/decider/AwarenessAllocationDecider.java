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

package org.elasticsearch.cluster.routing.allocation.decider;

import com.google.common.collect.Maps;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.HashMap;
import java.util.Map;

/**
 * This {@link AllocationDecider} controls shard allocation based on
 * <tt>awareness</tt> key-value pairs defined in the node configuration.
 * Awareness explicitly controls where replicas should be allocated based on
 * attributes like node or physical rack locations. Awareness attributes accept
 * arbitrary configuration keys like a rack data-center identifier. For example
 * the setting:
 * 
 * <pre>
 * cluster.routing.allocation.awareness.attributes: rack_id
 * </pre>
 * 
 * will cause allocations to be distributed over different racks such that
 * ideally at least one replicas of the all shard is available on the same rack.
 * To enable allocation awareness in this example nodes should contain a value
 * for the <tt>rack_id</tt> key like:
 * 
 * <pre>
 * node.rack_id:1
 * </pre>
 * 
 * Awareness can also be used to prevent over-allocation in the case of node or
 * even "zone" failure. For example in cloud-computing infrastructures like
 * Amazone AWS a cluster might span over multiple "zones". Awareness can be used
 * to distribute replicas to individual zones by setting:
 * 
 * <pre>
 * cluster.routing.allocation.awareness.attributes: zone
 * </pre>
 * 
 * and forcing allocation to be aware of the following zone the data resides in:
 * 
 * <pre>
 * cluster.routing.allocation.awareness.force.zone.values: zone1,zone2
 * </pre>
 * 
 * In contrast to regular awareness this setting will prevent over-allocation on
 * <tt>zone1</tt> even if <tt>zone2</tt> fails partially or becomes entirely
 * unavailable. Nodes that belong to a certain zone / group should be started
 * with the zone id configured on the node-level settings like:
 * 
 * <pre>
 * node.zone: zone1
 * </pre>
 */
public class AwarenessAllocationDecider extends AllocationDecider {

    static {
        MetaData.addDynamicSettings(
                "cluster.routing.allocation.awareness.attributes",
                "cluster.routing.allocation.awareness.force.*"
        );
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            String[] awarenessAttributes = settings.getAsArray("cluster.routing.allocation.awareness.attributes", null);
            if (awarenessAttributes != null) {
                logger.info("updating [cluster.routing.allocation.awareness.attributes] from [{}] to [{}]", AwarenessAllocationDecider.this.awarenessAttributes, awarenessAttributes);
                AwarenessAllocationDecider.this.awarenessAttributes = awarenessAttributes;
            }
            Map<String, String[]> forcedAwarenessAttributes = new HashMap<String, String[]>(AwarenessAllocationDecider.this.forcedAwarenessAttributes);
            Map<String, Settings> forceGroups = settings.getGroups("cluster.routing.allocation.awareness.force.");
            if (!forceGroups.isEmpty()) {
                for (Map.Entry<String, Settings> entry : forceGroups.entrySet()) {
                    String[] aValues = entry.getValue().getAsArray("values");
                    if (aValues.length > 0) {
                        forcedAwarenessAttributes.put(entry.getKey(), aValues);
                    }
                }
            }
            AwarenessAllocationDecider.this.forcedAwarenessAttributes = forcedAwarenessAttributes;
        }
    }

    private String[] awarenessAttributes;

    private Map<String, String[]> forcedAwarenessAttributes;

    /**
     * Creates a new {@link AwarenessAllocationDecider} instance
     */
    public AwarenessAllocationDecider() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    /**
     * Creates a new {@link AwarenessAllocationDecider} instance from given settings
     * @param settings {@link Settings} to use
     */
    public AwarenessAllocationDecider(Settings settings) {
        this(settings, new NodeSettingsService(settings));
    }

    @Inject
    public AwarenessAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.awarenessAttributes = settings.getAsArray("cluster.routing.allocation.awareness.attributes");

        forcedAwarenessAttributes = Maps.newHashMap();
        Map<String, Settings> forceGroups = settings.getGroups("cluster.routing.allocation.awareness.force.");
        for (Map.Entry<String, Settings> entry : forceGroups.entrySet()) {
            String[] aValues = entry.getValue().getAsArray("values");
            if (aValues.length > 0) {
                forcedAwarenessAttributes.put(entry.getKey(), aValues);
            }
        }

        nodeSettingsService.addListener(new ApplySettings());
    }

    /**
     * Get the attributes defined by this instance 
     * @return attributes defined by this instance
     */
    public String[] awarenessAttributes() {
        return this.awarenessAttributes;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return underCapacity(shardRouting, node, allocation, true) ? Decision.YES : Decision.NO;
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return underCapacity(shardRouting, node, allocation, false) ? Decision.YES : Decision.NO;
    }

    private boolean underCapacity(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation, boolean moveToNode) {
        if (awarenessAttributes.length == 0) {
            return true;
        }

        IndexMetaData indexMetaData = allocation.metaData().index(shardRouting.index());
        int shardCount = indexMetaData.numberOfReplicas() + 1; // 1 for primary
        for (String awarenessAttribute : awarenessAttributes) {
            // the node the shard exists on must be associated with an awareness attribute
            if (!node.node().attributes().containsKey(awarenessAttribute)) {
                return false;
            }

            // build attr_value -> nodes map
            TObjectIntHashMap<String> nodesPerAttribute = allocation.routingNodes().nodesPerAttributesCounts(awarenessAttribute);

            // build the count of shards per attribute value
            TObjectIntHashMap<String> shardPerAttribute = new TObjectIntHashMap<String>();
            for (RoutingNode routingNode : allocation.routingNodes()) {
                for (int i = 0; i < routingNode.shards().size(); i++) {
                    MutableShardRouting nodeShardRouting = routingNode.shards().get(i);
                    if (nodeShardRouting.shardId().equals(shardRouting.shardId())) {
                        // if the shard is relocating, then make sure we count it as part of the node it is relocating to
                        if (nodeShardRouting.relocating()) {
                            RoutingNode relocationNode = allocation.routingNodes().node(nodeShardRouting.relocatingNodeId());
                            shardPerAttribute.adjustOrPutValue(relocationNode.node().attributes().get(awarenessAttribute), 1, 1);
                        } else if (nodeShardRouting.started()) {
                            shardPerAttribute.adjustOrPutValue(routingNode.node().attributes().get(awarenessAttribute), 1, 1);
                        }
                    }
                }
            }
            if (moveToNode) {
                if (shardRouting.assignedToNode()) {
                    String nodeId = shardRouting.relocating() ? shardRouting.relocatingNodeId() : shardRouting.currentNodeId();
                    if (!node.nodeId().equals(nodeId)) {
                        // we work on different nodes, move counts around
                        shardPerAttribute.adjustOrPutValue(allocation.routingNodes().node(nodeId).node().attributes().get(awarenessAttribute), -1, 0);
                        shardPerAttribute.adjustOrPutValue(node.node().attributes().get(awarenessAttribute), 1, 1);
                    }
                } else {
                    shardPerAttribute.adjustOrPutValue(node.node().attributes().get(awarenessAttribute), 1, 1);
                }
            }

            int numberOfAttributes = nodesPerAttribute.size();
            String[] fullValues = forcedAwarenessAttributes.get(awarenessAttribute);
            if (fullValues != null) {
                for (String fullValue : fullValues) {
                    if (!shardPerAttribute.contains(fullValue)) {
                        numberOfAttributes++;
                    }
                }
            }
            // TODO should we remove ones that are not part of full list?

            int averagePerAttribute = shardCount / numberOfAttributes;
            int totalLeftover = shardCount % numberOfAttributes;
            int requiredCountPerAttribute;
            if (averagePerAttribute == 0) {
                // if we have more attributes values than shard count, no leftover
                totalLeftover = 0;
                requiredCountPerAttribute = 1;
            } else {
                requiredCountPerAttribute = averagePerAttribute;
            }
            int leftoverPerAttribute = totalLeftover == 0 ? 0 : 1;

            int currentNodeCount = shardPerAttribute.get(node.node().attributes().get(awarenessAttribute));
            // if we are above with leftover, then we know we are not good, even with mod
            if (currentNodeCount > (requiredCountPerAttribute + leftoverPerAttribute)) {
                return false;
            }
            // all is well, we are below or same as average
            if (currentNodeCount <= requiredCountPerAttribute) {
                continue;
            }
        }

        return true;
    }
}
