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

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;

/**
 * This {@link AllocationDecider} control shard allocation by include and
 * exclude filters via dynamic cluster and index routing settings.
 * <p>
 * This filter is used to make explicit decision on which nodes certain shard
 * can / should be allocated. The decision if a shard can be allocated, must not
 * be allocated or should be allocated is based on either cluster wide dynamic
 * settings (<tt>cluster.routing.allocation.*</tt>) or index specific dynamic
 * settings (<tt>index.routing.allocation.*</tt>). All of those settings can be
 * changed at runtime via the cluster or the index update settings API.
 * </p>
 * Note: Cluster settings are applied first and will override index specific
 * settings such that if a shard can be allocated according to the index routing
 * settings it wont be allocated on a node if the cluster specific settings
 * would disallow the allocation. Filters are applied in the following order:
 * <ol>
 * <li><tt>required</tt> - filters required allocations.
 * If any <tt>required</tt> filters are set the allocation is denied if the index is <b>not</b> in the set of <tt>required</tt> to allocate
 * on the filtered node</li>
 * <li><tt>include</tt> - filters "allowed" allocations.
 * If any <tt>include</tt> filters are set the allocation is denied if the index is <b>not</b> in the set of <tt>include</tt> filters for
 * the filtered node</li>
 * <li><tt>exclude</tt> - filters "prohibited" allocations.
 * If any <tt>exclude</tt> filters are set the allocation is denied if the index is in the set of <tt>exclude</tt> filters for the
 * filtered node</li>
 * </ol>
 */
public class FilterAllocationDecider extends AllocationDecider {

    public static final String NAME = "filter";

    public static final Setting<Settings> CLUSTER_ROUTING_REQUIRE_GROUP_SETTING =
        Setting.groupSetting("cluster.routing.allocation.require.", Property.Dynamic, Property.NodeScope);
    public static final Setting<Settings> CLUSTER_ROUTING_INCLUDE_GROUP_SETTING =
        Setting.groupSetting("cluster.routing.allocation.include.", Property.Dynamic, Property.NodeScope);
    public static final Setting<Settings> CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING =
        Setting.groupSetting("cluster.routing.allocation.exclude.", Property.Dynamic, Property.NodeScope);

    private volatile DiscoveryNodeFilters clusterRequireFilters;
    private volatile DiscoveryNodeFilters clusterIncludeFilters;
    private volatile DiscoveryNodeFilters clusterExcludeFilters;

    @Inject
    public FilterAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        setClusterRequireFilters(CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.get(settings));
        setClusterExcludeFilters(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.get(settings));
        setClusterIncludeFilters(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_REQUIRE_GROUP_SETTING, this::setClusterRequireFilters);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING, this::setClusterExcludeFilters);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING, this::setClusterIncludeFilters);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.unassigned()) {
            // only for unassigned - we filter allocation right after the index creation ie. for shard shrinking etc. to ensure
            // that once it has been allocated post API the replicas can be allocated elsewhere without user interaction
            // this is a setting that can only be set within the system!
            IndexMetaData indexMd = allocation.metaData().getIndexSafe(shardRouting.index());
            DiscoveryNodeFilters initialRecoveryFilters = indexMd.getInitialRecoveryFilters();
            if (shardRouting.allocatedPostIndexCreate(indexMd) == false &&
                initialRecoveryFilters != null &&
                initialRecoveryFilters.match(node.node()) == false) {
                return allocation.decision(Decision.NO, NAME, "node does not match index initial recovery filters [%s]",
                    indexMd.includeFilters());
            }
        }
        return shouldFilter(shardRouting, node, allocation);
    }

    @Override
    public Decision canAllocate(IndexMetaData indexMetaData, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(indexMetaData, node, allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(shardRouting, node, allocation);
    }

    private Decision shouldFilter(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node, allocation);
        if (decision != null) return decision;

        decision = shouldIndexFilter(allocation.metaData().getIndexSafe(shardRouting.index()), node, allocation);
        if (decision != null) return decision;

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require filters");
    }

    private Decision shouldFilter(IndexMetaData indexMd, RoutingNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node, allocation);
        if (decision != null) return decision;

        decision = shouldIndexFilter(indexMd, node, allocation);
        if (decision != null) return decision;

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require filters");
    }

    private Decision shouldIndexFilter(IndexMetaData indexMd, RoutingNode node, RoutingAllocation allocation) {
        if (indexMd.requireFilters() != null) {
            if (!indexMd.requireFilters().match(node.node())) {
                return allocation.decision(Decision.NO, NAME, "node does not match index required filters [%s]", indexMd.requireFilters());
            }
        }
        if (indexMd.includeFilters() != null) {
            if (!indexMd.includeFilters().match(node.node())) {
                return allocation.decision(Decision.NO, NAME, "node does not match index include filters [%s]", indexMd.includeFilters());
            }
        }
        if (indexMd.excludeFilters() != null) {
            if (indexMd.excludeFilters().match(node.node())) {
                return allocation.decision(Decision.NO, NAME, "node matches index exclude filters [%s]", indexMd.excludeFilters());
            }
        }
        return null;
    }

    private Decision shouldClusterFilter(RoutingNode node, RoutingAllocation allocation) {
        if (clusterRequireFilters != null) {
            if (!clusterRequireFilters.match(node.node())) {
                return allocation.decision(Decision.NO, NAME, "node does not match global required filters [%s]", clusterRequireFilters);
            }
        }
        if (clusterIncludeFilters != null) {
            if (!clusterIncludeFilters.match(node.node())) {
                return allocation.decision(Decision.NO, NAME, "node does not match global include filters [%s]", clusterIncludeFilters);
            }
        }
        if (clusterExcludeFilters != null) {
            if (clusterExcludeFilters.match(node.node())) {
                return allocation.decision(Decision.NO, NAME, "node matches global exclude filters [%s]", clusterExcludeFilters);
            }
        }
        return null;
    }

    private void setClusterRequireFilters(Settings settings) {
        clusterRequireFilters = DiscoveryNodeFilters.buildFromKeyValue(AND, settings.getAsMap());
    }
    private void setClusterIncludeFilters(Settings settings) {
        clusterIncludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, settings.getAsMap());
    }
    private void setClusterExcludeFilters(Settings settings) {
        clusterExcludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, settings.getAsMap());
    }
}
