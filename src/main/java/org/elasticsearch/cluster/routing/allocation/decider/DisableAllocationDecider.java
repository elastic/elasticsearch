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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Map;

/**
 * This {@link AllocationDecider} prevents cluster-wide shard allocations. The
 * behavior of this {@link AllocationDecider} can be changed in real-time via
 * the cluster settings API. It respects the following settings:
 * <ul>
 * <li><tt>cluster.routing.allocation.disable_new_allocation</tt> - if set to
 * <code>true</code> no new shard-allocation are allowed. Note: this setting is
 * only applied if the allocated shard is a primary and it has not been
 * allocated before the this setting was applied.</li>
 * <p/>
 * <li><tt>cluster.routing.allocation.disable_allocation</tt> - if set to
 * <code>true</code> cluster wide allocations are disabled</li>
 * <p/>
 * <li><tt>cluster.routing.allocation.disable_replica_allocation</tt> - if set
 * to <code>true</code> cluster wide replica allocations are disabled while
 * primary shards can still be allocated</li>
 * </ul>
 * <p/>
 * <p>
 * Note: all of the above settings might be ignored if the allocation happens on
 * a shard that explicitly ignores disabled allocations via
 * {@link RoutingAllocation#ignoreDisable()}. Which is set if allocation are
 * explicit.
 * </p>
 */
public class DisableAllocationDecider extends AllocationDecider {

    public static final String CLUSTER_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION = "cluster.routing.allocation.disable_new_allocation";
    public static final String CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION = "cluster.routing.allocation.disable_allocation";
    public static final String CLUSTER_ROUTING_ALLOCATION_DISABLE_REPLICA_ALLOCATION = "cluster.routing.allocation.disable_replica_allocation";
    public static final String CLUSTER_ROUTING_ALLOCATION_DISABLE_PRIMARY_ATTRIBUTES = "cluster.routing.allocation.disable.primary.attributes";
    public static final String CLUSTER_ROUTING_ALLOCATION_DISABLE_PRIMARY_GROUPS = "cluster.routing.allocation.disable.primary.groups.";
    
    public static final String INDEX_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION = "index.routing.allocation.disable_new_allocation";
    public static final String INDEX_ROUTING_ALLOCATION_DISABLE_ALLOCATION = "index.routing.allocation.disable_allocation";
    public static final String INDEX_ROUTING_ALLOCATION_DISABLE_REPLICA_ALLOCATION = "index.routing.allocation.disable_replica_allocation";
    
    
    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            boolean disableNewAllocation = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION, DisableAllocationDecider.this.disableNewAllocation);
            if (disableNewAllocation != DisableAllocationDecider.this.disableNewAllocation) {
                logger.info("updating [cluster.routing.allocation.disable_new_allocation] from [{}] to [{}]", DisableAllocationDecider.this.disableNewAllocation, disableNewAllocation);
                DisableAllocationDecider.this.disableNewAllocation = disableNewAllocation;
            }

            boolean disableAllocation = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, DisableAllocationDecider.this.disableAllocation);
            if (disableAllocation != DisableAllocationDecider.this.disableAllocation) {
                logger.info("updating [cluster.routing.allocation.disable_allocation] from [{}] to [{}]", DisableAllocationDecider.this.disableAllocation, disableAllocation);
                DisableAllocationDecider.this.disableAllocation = disableAllocation;
            }

            boolean disableReplicaAllocation = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_DISABLE_REPLICA_ALLOCATION, DisableAllocationDecider.this.disableReplicaAllocation);
            if (disableReplicaAllocation != DisableAllocationDecider.this.disableReplicaAllocation) {
                logger.info("updating [cluster.routing.allocation.disable_replica_allocation] from [{}] to [{}]", DisableAllocationDecider.this.disableReplicaAllocation, disableReplicaAllocation);
                DisableAllocationDecider.this.disableReplicaAllocation = disableReplicaAllocation;
            }
            Map<String, Settings> disablePrimaryGroups = settings.getGroups(CLUSTER_ROUTING_ALLOCATION_DISABLE_PRIMARY_GROUPS);
            if(!disablePrimaryGroups.isEmpty()){
                initDisablePrimaryAllocation(settings);
            }
           
        }
    }

    private volatile boolean disableNewAllocation;
    private volatile boolean disableAllocation;
    private volatile boolean disableReplicaAllocation;
    public static Map<String, String[]> disablePrimaryGroupAttributes;
    public static volatile boolean disablePrimaryEnable;
    private static String[] disablePrimaryAttributes ;
    
    @Inject
    public DisableAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.disableNewAllocation = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION, false);
        this.disableAllocation = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, false);
        this.disableReplicaAllocation = settings.getAsBoolean(CLUSTER_ROUTING_ALLOCATION_DISABLE_REPLICA_ALLOCATION, false);
        
        nodeSettingsService.addListener(new ApplySettings());
        initDisablePrimaryAllocation(settings);
    }

    /**
     * init disable primary allocation
     * 
     * @param settings
     */
    private void initDisablePrimaryAllocation(Settings settings) {
        this.disablePrimaryAttributes=settings.getAsArray(CLUSTER_ROUTING_ALLOCATION_DISABLE_PRIMARY_ATTRIBUTES);
        Map<String, String[]> disablePrimaryGroupAttributes = Maps.newHashMap();
        Map<String, Settings> disablePrimaryGroups = settings.getGroups(CLUSTER_ROUTING_ALLOCATION_DISABLE_PRIMARY_GROUPS);
        if (!disablePrimaryGroups.isEmpty()) {
            for (Map.Entry<String, Settings> entry : disablePrimaryGroups.entrySet()) {
                String[] aValues = entry.getValue().getAsArray("values");
                if (aValues.length > 0) {
                    disablePrimaryGroupAttributes.put(entry.getKey(), aValues);
                }
            }
        }
        if (disablePrimaryGroupAttributes.isEmpty()) {
            disablePrimaryEnable = false;
        } else {
            disablePrimaryEnable = true;
        }
        this.disablePrimaryGroupAttributes = disablePrimaryGroupAttributes;
    }
    
    /**
     * disable primary shard allocate
     * 
     * @param node
     * @return
     */
    public static boolean isDisablePrimary(RoutingNode node) {
        for (String disablePrimaryAttribute : disablePrimaryAttributes) {
            if (!node.node().attributes().containsKey(disablePrimaryAttribute)) {
                continue;
            } else {
                String[] disablePrimaryValues = disablePrimaryGroupAttributes.get(disablePrimaryAttribute);
                if (null != disablePrimaryValues) {
                    for (String disablePrimaryValue : disablePrimaryValues) {
                        if (node.node().attributes().containsValue(disablePrimaryValue)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
    
    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            if (isDisablePrimary(node)) {
                return Decision.NO;
            }
        }
        
        if (allocation.ignoreDisable()) {
            return Decision.YES;
        }
        Settings indexSettings = allocation.routingNodes().metaData().index(shardRouting.index()).settings();
        if (shardRouting.primary() && !allocation.routingNodes().routingTable().index(shardRouting.index()).shard(shardRouting.id()).primaryAllocatedPostApi()) {
            // if its primary, and it hasn't been allocated post API (meaning its a "fresh newly created shard"), only disable allocation
            // on a special disable allocation flag
            return indexSettings.getAsBoolean(INDEX_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION, disableNewAllocation) ? Decision.NO : Decision.YES;
        }
        if (indexSettings.getAsBoolean(INDEX_ROUTING_ALLOCATION_DISABLE_ALLOCATION, disableAllocation)) {
            return Decision.NO;
        }
        if (indexSettings.getAsBoolean(INDEX_ROUTING_ALLOCATION_DISABLE_REPLICA_ALLOCATION, disableReplicaAllocation)) {
            return shardRouting.primary() ? Decision.YES : Decision.NO;
        }
        return Decision.YES;
    }
}
