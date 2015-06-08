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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

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
 * If any <tt>required</tt> filters are set the allocation is denied if the index is <b>not</b> in the set of <tt>required</tt> to allocate on the filtered node</li>
 * <p/>
 * <li><tt>include</tt> - filters "allowed" allocations.
 * If any <tt>include</tt> filters are set the allocation is denied if the index is <b>not</b> in the set of <tt>include</tt> filters for the filtered node</li>
 * <p/>
 * <li><tt>exclude</tt> - filters "prohibited" allocations.
 * If any <tt>exclude</tt> filters are set the allocation is denied if the index is in the set of <tt>exclude</tt> filters for the filtered node</li>
 * </ol>
 */
public class FilterAllocationDecider extends AllocationDecider {

    public static final String NAME = "filter";

    public static final String INDEX_ROUTING_REQUIRE_GROUP = "index.routing.allocation.require.";
    public static final String INDEX_ROUTING_INCLUDE_GROUP = "index.routing.allocation.include.";
    public static final String INDEX_ROUTING_EXCLUDE_GROUP = "index.routing.allocation.exclude.";

    public static final String CLUSTER_ROUTING_REQUIRE_GROUP = "cluster.routing.allocation.require.";
    public static final String CLUSTER_ROUTING_INCLUDE_GROUP = "cluster.routing.allocation.include.";
    public static final String CLUSTER_ROUTING_EXCLUDE_GROUP = "cluster.routing.allocation.exclude.";

    private volatile DiscoveryNodeFilters clusterRequireFilters;
    private volatile DiscoveryNodeFilters clusterIncludeFilters;
    private volatile DiscoveryNodeFilters clusterExcludeFilters;

    @Inject
    public FilterAllocationDecider(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        ImmutableMap<String, String> requireMap = settings.getByPrefix(CLUSTER_ROUTING_REQUIRE_GROUP).getAsMap();
        if (requireMap.isEmpty()) {
            clusterRequireFilters = null;
        } else {
            clusterRequireFilters = DiscoveryNodeFilters.buildFromKeyValue(AND, requireMap);
        }
        ImmutableMap<String, String> includeMap = settings.getByPrefix(CLUSTER_ROUTING_INCLUDE_GROUP).getAsMap();
        if (includeMap.isEmpty()) {
            clusterIncludeFilters = null;
        } else {
            clusterIncludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, includeMap);
        }
        ImmutableMap<String, String> excludeMap = settings.getByPrefix(CLUSTER_ROUTING_EXCLUDE_GROUP).getAsMap();
        if (excludeMap.isEmpty()) {
            clusterExcludeFilters = null;
        } else {
            clusterExcludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, excludeMap);
        }
        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(shardRouting, node, allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(shardRouting, node, allocation);
    }

    private Decision shouldFilter(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
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

        IndexMetaData indexMd = allocation.routingNodes().metaData().index(shardRouting.index());
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

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require filters");
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            ImmutableMap<String, String> requireMap = settings.getByPrefix(CLUSTER_ROUTING_REQUIRE_GROUP).getAsMap();
            if (!requireMap.isEmpty()) {
                clusterRequireFilters = DiscoveryNodeFilters.buildFromKeyValue(AND, requireMap);
            }
            ImmutableMap<String, String> includeMap = settings.getByPrefix(CLUSTER_ROUTING_INCLUDE_GROUP).getAsMap();
            if (!includeMap.isEmpty()) {
                clusterIncludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, includeMap);
            }
            ImmutableMap<String, String> excludeMap = settings.getByPrefix(CLUSTER_ROUTING_EXCLUDE_GROUP).getAsMap();
            if (!excludeMap.isEmpty()) {
                clusterExcludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, excludeMap);
            }
        }
    }
}
