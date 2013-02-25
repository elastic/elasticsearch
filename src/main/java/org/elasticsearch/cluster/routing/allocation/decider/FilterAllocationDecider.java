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
 */
public class FilterAllocationDecider extends AllocationDecider {

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
        return shouldFilter(shardRouting, node, allocation) ? Decision.NO : Decision.YES;
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(shardRouting, node, allocation) ? Decision.NO : Decision.YES;
    }

    private boolean shouldFilter(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (clusterRequireFilters != null) {
            if (!clusterRequireFilters.match(node.node())) {
                return true;
            }
        }
        if (clusterIncludeFilters != null) {
            if (!clusterIncludeFilters.match(node.node())) {
                return true;
            }
        }
        if (clusterExcludeFilters != null) {
            if (clusterExcludeFilters.match(node.node())) {
                return true;
            }
        }

        IndexMetaData indexMd = allocation.routingNodes().metaData().index(shardRouting.index());
        if (indexMd.requireFilters() != null) {
            if (!indexMd.requireFilters().match(node.node())) {
                return true;
            }
        }
        if (indexMd.includeFilters() != null) {
            if (!indexMd.includeFilters().match(node.node())) {
                return true;
            }
        }
        if (indexMd.excludeFilters() != null) {
            if (indexMd.excludeFilters().match(node.node())) {
                return true;
            }
        }

        return false;
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
