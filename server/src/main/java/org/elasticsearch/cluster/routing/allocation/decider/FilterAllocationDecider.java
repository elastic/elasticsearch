/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.validateIpValue;

/**
 * This {@link AllocationDecider} control shard allocation by include and
 * exclude filters via dynamic cluster and index routing settings.
 * <p>
 * This filter is used to make explicit decision on which nodes certain shard
 * can / should be allocated. The decision if a shard can be allocated, must not
 * be allocated or should be allocated is based on either cluster wide dynamic
 * settings ({@code cluster.routing.allocation.*}) or index specific dynamic
 * settings ({@code index.routing.allocation.*}). All of those settings can be
 * changed at runtime via the cluster or the index update settings API.
 * </p>
 * Note: Cluster settings are applied first and will override index specific
 * settings such that if a shard can be allocated according to the index routing
 * settings it wont be allocated on a node if the cluster specific settings
 * would disallow the allocation. Filters are applied in the following order:
 * <ol>
 * <li>{@code required} - filters required allocations.
 * If any {@code required} filters are set the allocation is denied if the index is <b>not</b> in the set of {@code required} to allocate
 * on the filtered node</li>
 * <li>{@code include} - filters "allowed" allocations.
 * If any {@code include} filters are set the allocation is denied if the index is <b>not</b> in the set of {@code include} filters for
 * the filtered node</li>
 * <li>{@code exclude} - filters "prohibited" allocations.
 * If any {@code exclude} filters are set the allocation is denied if the index is in the set of {@code exclude} filters for the
 * filtered node</li>
 * </ol>
 */
public class FilterAllocationDecider extends AllocationDecider {

    public static final String NAME = "filter";

    private static final String CLUSTER_ROUTING_REQUIRE_GROUP_PREFIX = "cluster.routing.allocation.require";
    private static final String CLUSTER_ROUTING_INCLUDE_GROUP_PREFIX = "cluster.routing.allocation.include";
    private static final String CLUSTER_ROUTING_EXCLUDE_GROUP_PREFIX = "cluster.routing.allocation.exclude";

    public static final Setting.AffixSetting<List<String>> CLUSTER_ROUTING_REQUIRE_GROUP_SETTING = Setting.prefixKeySetting(
        CLUSTER_ROUTING_REQUIRE_GROUP_PREFIX + ".",
        key -> Setting.stringListSetting(key, value -> validateIpValue(key, value), Property.Dynamic, Property.NodeScope)
    );
    public static final Setting.AffixSetting<List<String>> CLUSTER_ROUTING_INCLUDE_GROUP_SETTING = Setting.prefixKeySetting(
        CLUSTER_ROUTING_INCLUDE_GROUP_PREFIX + ".",
        key -> Setting.stringListSetting(key, value -> validateIpValue(key, value), Property.Dynamic, Property.NodeScope)
    );
    public static final Setting.AffixSetting<List<String>> CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING = Setting.prefixKeySetting(
        CLUSTER_ROUTING_EXCLUDE_GROUP_PREFIX + ".",
        key -> Setting.stringListSetting(key, value -> validateIpValue(key, value), Property.Dynamic, Property.NodeScope)
    );

    private volatile DiscoveryNodeFilters clusterRequireFilters;
    private volatile DiscoveryNodeFilters clusterIncludeFilters;
    private volatile DiscoveryNodeFilters clusterExcludeFilters;

    public FilterAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        setClusterRequireFilters(CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(settings));
        setClusterExcludeFilters(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getAsMap(settings));
        setClusterIncludeFilters(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getAsMap(settings));
        clusterSettings.addAffixMapUpdateConsumer(CLUSTER_ROUTING_REQUIRE_GROUP_SETTING, this::setClusterRequireFilters, (a, b) -> {});
        clusterSettings.addAffixMapUpdateConsumer(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING, this::setClusterExcludeFilters, (a, b) -> {});
        clusterSettings.addAffixMapUpdateConsumer(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING, this::setClusterIncludeFilters, (a, b) -> {});
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        if (shardRouting.unassigned() && shardRouting.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            // only for unassigned - we filter allocation right after the index creation (for shard shrinking) to ensure
            // that once it has been allocated post API the replicas can be allocated elsewhere without user interaction
            // this is a setting that can only be set within the system!
            DiscoveryNodeFilters initialRecoveryFilters = DiscoveryNodeFilters.trimTier(indexMetadata.getInitialRecoveryFilters());
            if (initialRecoveryFilters != null && initialRecoveryFilters.match(node.node()) == false) {
                String explanation =
                    "initial allocation of the shrunken index is only allowed on nodes [%s] that hold a copy of every shard in the index";
                return allocation.decision(Decision.NO, NAME, explanation, initialRecoveryFilters);
            }
        }
        return shouldFilter(indexMetadata, node.node(), allocation);
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(indexMetadata, node.node(), allocation);
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(indexMetadata, node.node(), allocation);
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node, allocation);
        if (decision != null) return decision;

        decision = shouldIndexFilter(indexMetadata, node, allocation);
        if (decision != null) return decision;

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require filters");
    }

    private Decision shouldFilter(IndexMetadata indexMd, DiscoveryNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node, allocation);
        if (decision != null) return decision;

        decision = shouldIndexFilter(indexMd, node, allocation);
        if (decision != null) return decision;

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require filters");
    }

    private static Decision shouldIndexFilter(IndexMetadata indexMd, DiscoveryNode node, RoutingAllocation allocation) {
        DiscoveryNodeFilters indexRequireFilters = DiscoveryNodeFilters.trimTier(indexMd.requireFilters());
        DiscoveryNodeFilters indexIncludeFilters = DiscoveryNodeFilters.trimTier(indexMd.includeFilters());
        DiscoveryNodeFilters indexExcludeFilters = DiscoveryNodeFilters.trimTier(indexMd.excludeFilters());

        if (indexRequireFilters != null) {
            if (indexRequireFilters.match(node) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match index setting [%s] filters [%s]",
                    IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX,
                    indexRequireFilters
                );
            }
        }
        if (indexIncludeFilters != null) {
            if (indexIncludeFilters.match(node) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match index setting [%s] filters [%s]",
                    IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX,
                    indexIncludeFilters
                );
            }
        }
        if (indexExcludeFilters != null) {
            if (indexExcludeFilters.match(node)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node matches index setting [%s] filters [%s]",
                    IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey(),
                    indexExcludeFilters
                );
            }
        }
        return null;
    }

    private Decision shouldClusterFilter(DiscoveryNode node, RoutingAllocation allocation) {
        if (clusterRequireFilters != null) {
            if (clusterRequireFilters.match(node) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match cluster setting [%s] filters [%s]",
                    CLUSTER_ROUTING_REQUIRE_GROUP_PREFIX,
                    clusterRequireFilters
                );
            }
        }
        if (clusterIncludeFilters != null) {
            if (clusterIncludeFilters.match(node) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match cluster setting [%s] filters [%s]",
                    CLUSTER_ROUTING_INCLUDE_GROUP_PREFIX,
                    clusterIncludeFilters
                );
            }
        }
        if (clusterExcludeFilters != null) {
            if (clusterExcludeFilters.match(node)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node matches cluster setting [%s] filters [%s]",
                    CLUSTER_ROUTING_EXCLUDE_GROUP_PREFIX,
                    clusterExcludeFilters
                );
            }
        }
        return null;
    }

    private void setClusterRequireFilters(Map<String, List<String>> filters) {
        clusterRequireFilters = DiscoveryNodeFilters.trimTier(DiscoveryNodeFilters.buildFromKeyValues(AND, filters));
    }

    private void setClusterIncludeFilters(Map<String, List<String>> filters) {
        clusterIncludeFilters = DiscoveryNodeFilters.trimTier(DiscoveryNodeFilters.buildFromKeyValues(OR, filters));
    }

    private void setClusterExcludeFilters(Map<String, List<String>> filters) {
        clusterExcludeFilters = DiscoveryNodeFilters.trimTier(DiscoveryNodeFilters.buildFromKeyValues(OR, filters));
    }

    @Override
    public Optional<Set<String>> getForcedInitialShardAllocationToNodes(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (shardRouting.unassigned() && shardRouting.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            var indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
            var initialRecoveryFilters = DiscoveryNodeFilters.trimTier(indexMetadata.getInitialRecoveryFilters());

            if (initialRecoveryFilters != null) {
                return Optional.of(
                    allocation.nodes().stream().filter(initialRecoveryFilters::match).map(DiscoveryNode::getId).collect(toUnmodifiableSet())
                );
            }
        }
        return super.getForcedInitialShardAllocationToNodes(shardRouting, allocation);
    }
}
