/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * This {@link AllocationDecider} controls shard allocation based on
 * {@code awareness} key-value pairs defined in the node configuration.
 * Awareness explicitly controls where replicas should be allocated based on
 * attributes like node or physical rack locations. Awareness attributes accept
 * arbitrary configuration keys like a rack data-center identifier. For example
 * the setting:
 * <pre>
 * cluster.routing.allocation.awareness.attributes: rack_id
 * </pre>
 * <p>
 * will cause allocations to be distributed over different racks such that
 * ideally at least one replicas of the all shard is available on the same rack.
 * To enable allocation awareness in this example nodes should contain a value
 * for the {@code rack_id} key like:
 * <pre>
 * node.attr.rack_id:1
 * </pre>
 * <p>
 * Awareness can also be used to prevent over-allocation in the case of node or
 * even "zone" failure. For example in cloud-computing infrastructures like
 * Amazon AWS a cluster might span over multiple "zones". Awareness can be used
 * to distribute replicas to individual zones by setting:
 * <pre>
 * cluster.routing.allocation.awareness.attributes: zone
 * </pre>
 * <p>
 * and forcing allocation to be aware of the following zone the data resides in:
 * <pre>
 * cluster.routing.allocation.awareness.force.zone.values: zone1,zone2
 * </pre>
 * <p>
 * In contrast to regular awareness this setting will prevent over-allocation on
 * {@code zone1} even if {@code zone2} fails partially or becomes entirely
 * unavailable. Nodes that belong to a certain zone / group should be started
 * with the zone id configured on the node-level settings like:
 * <pre>
 * node.zone: zone1
 * </pre>
 */
public class AwarenessAllocationDecider extends AllocationDecider {

    public static final String NAME = "awareness";

    public static final Setting<List<String>> CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING = Setting.stringListSetting(
        "cluster.routing.allocation.awareness.attributes",
        Property.Dynamic,
        Property.NodeScope
    );

    private static final String FORCE_GROUP_SETTING_PREFIX = "cluster.routing.allocation.awareness.force.";

    public static final Setting<Settings> CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING = Setting.groupSetting(
        FORCE_GROUP_SETTING_PREFIX,
        AwarenessAllocationDecider::validateForceAwarenessSettings,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile List<String> awarenessAttributes;

    private volatile Map<String, List<String>> forcedAwarenessAttributes;

    public AwarenessAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.awarenessAttributes = CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING, this::setAwarenessAttributes);
        setForcedAwarenessAttributes(CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING,
            this::setForcedAwarenessAttributes
        );
    }

    private void setForcedAwarenessAttributes(Settings forceSettings) {
        Map<String, List<String>> forcedAwarenessAttributes = new HashMap<>();
        Map<String, Settings> forceGroups = forceSettings.getAsGroups();
        for (Map.Entry<String, Settings> entry : forceGroups.entrySet()) {
            List<String> aValues = entry.getValue().getAsList("values");
            if (aValues.size() > 0) {
                forcedAwarenessAttributes.put(entry.getKey(), aValues);
            }
        }
        this.forcedAwarenessAttributes = forcedAwarenessAttributes;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return underCapacity(allocation.metadata().getIndexSafe(shardRouting.index()), shardRouting, node, allocation, true);
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        // We need to meet the criteria for shard awareness even during a replacement so that all
        // copies of a shard do not get allocated to the same host/rack/AZ, so this explicitly
        // checks the awareness 'canAllocate' to ensure we don't violate that constraint.
        return canAllocate(shardRouting, node, allocation);
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return underCapacity(indexMetadata, shardRouting, node, allocation, false);
    }

    private static final Decision YES_NOT_ENABLED = Decision.single(
        Decision.Type.YES,
        NAME,
        "allocation awareness is not enabled, set cluster setting ["
            + CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey()
            + "] to enable it"
    );

    private static final Decision YES_AUTO_EXPAND_ALL = Decision.single(
        Decision.Type.YES,
        NAME,
        "allocation awareness is ignored, this index is set to auto-expand to all nodes"
    );

    private static final Decision YES_ALL_MET = Decision.single(Decision.Type.YES, NAME, "node meets all awareness attribute requirements");

    private Decision underCapacity(
        IndexMetadata indexMetadata,
        ShardRouting shardRouting,
        RoutingNode node,
        RoutingAllocation allocation,
        boolean moveToNode
    ) {
        if (awarenessAttributes.isEmpty()) {
            return YES_NOT_ENABLED;
        }

        final boolean debug = allocation.debugDecision();

        if (indexMetadata.getAutoExpandReplicas().expandToAllNodes()) {
            return YES_AUTO_EXPAND_ALL;
        }

        final int shardCount = indexMetadata.getNumberOfReplicas() + 1; // 1 for primary
        for (String awarenessAttribute : awarenessAttributes) {
            // the node the shard exists on must be associated with an awareness attribute
            if (node.node().getAttributes().containsKey(awarenessAttribute) == false) {
                return debug ? debugNoMissingAttribute(awarenessAttribute, awarenessAttributes) : Decision.NO;
            }

            final Set<String> actualAttributeValues = allocation.routingNodes().getAttributeValues(awarenessAttribute);
            final String targetAttributeValue = node.node().getAttributes().get(awarenessAttribute);
            assert targetAttributeValue != null : "attribute [" + awarenessAttribute + "] missing on " + node.node();
            assert actualAttributeValues.contains(targetAttributeValue)
                : "attribute [" + awarenessAttribute + "] on " + node.node() + " is not in " + actualAttributeValues;

            int shardsForTargetAttributeValue = 0;
            // Will be the count of shards on nodes with attribute `awarenessAttribute` matching the one on `node`.

            for (ShardRouting assignedShard : allocation.routingNodes().assignedShards(shardRouting.shardId())) {
                if (assignedShard.started() || assignedShard.initializing()) {
                    // Note: this also counts relocation targets as that will be the new location of the shard.
                    // Relocation sources should not be counted as the shard is moving away
                    final RoutingNode assignedNode = allocation.routingNodes().node(assignedShard.currentNodeId());
                    if (targetAttributeValue.equals(assignedNode.node().getAttributes().get(awarenessAttribute))) {
                        shardsForTargetAttributeValue += 1;
                    }
                }
            }

            if (moveToNode) {
                if (shardRouting.assignedToNode()) {
                    final RoutingNode currentNode = allocation.routingNodes()
                        .node(shardRouting.relocating() ? shardRouting.relocatingNodeId() : shardRouting.currentNodeId());
                    if (targetAttributeValue.equals(currentNode.node().getAttributes().get(awarenessAttribute)) == false) {
                        shardsForTargetAttributeValue += 1;
                    } // else this shard is already on a node in the same zone as the target node, so moving it doesn't change the count
                } else {
                    shardsForTargetAttributeValue += 1;
                }
            }

            final List<String> forcedValues = forcedAwarenessAttributes.get(awarenessAttribute);
            final int valueCount = forcedValues == null
                ? actualAttributeValues.size()
                : Math.toIntExact(Stream.concat(actualAttributeValues.stream(), forcedValues.stream()).distinct().count());

            final int maximumShardsPerAttributeValue = (shardCount + valueCount - 1) / valueCount; // ceil(shardCount/valueCount)
            if (shardsForTargetAttributeValue > maximumShardsPerAttributeValue) {
                return debug
                    ? debugNoTooManyCopies(
                        shardCount,
                        awarenessAttribute,
                        node.node().getAttributes().get(awarenessAttribute),
                        valueCount,
                        actualAttributeValues.stream().sorted().collect(toList()),
                        forcedValues == null ? null : forcedValues.stream().sorted().collect(toList()),
                        shardsForTargetAttributeValue,
                        maximumShardsPerAttributeValue
                    )
                    : Decision.NO;
            }
        }

        return YES_ALL_MET;
    }

    private static Decision debugNoTooManyCopies(
        int shardCount,
        String attributeName,
        String attributeValue,
        int numberOfAttributes,
        List<String> realAttributes,
        List<String> forcedAttributes,
        int actualShardCount,
        int maximumShardCount
    ) {
        return Decision.single(
            Decision.Type.NO,
            NAME,
            "there are [%d] copies of this shard and [%d] values for attribute [%s] (%s from nodes in the cluster and %s) so there "
                + "may be at most [%d] copies of this shard allocated to nodes with each value, but (including this copy) there "
                + "would be [%d] copies allocated to nodes with [node.attr.%s: %s]",
            shardCount,
            numberOfAttributes,
            attributeName,
            realAttributes,
            forcedAttributes == null ? "no forced awareness" : forcedAttributes + " from forced awareness",
            maximumShardCount,
            actualShardCount,
            attributeName,
            attributeValue
        );
    }

    private static Decision debugNoMissingAttribute(String awarenessAttribute, List<String> awarenessAttributes) {
        return Decision.single(
            Decision.Type.NO,
            NAME,
            "node does not contain the awareness attribute [%s]; required attributes cluster setting [%s=%s]",
            awarenessAttribute,
            CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(),
            Strings.collectionToCommaDelimitedString(awarenessAttributes)
        );
    }

    private static void validateForceAwarenessSettings(Settings forceSettings) {
        final Map<String, Settings> settingGroups;
        try {
            settingGroups = forceSettings.getAsGroups();
        } catch (SettingsException e) {
            throw new IllegalArgumentException("invalid forced awareness settings with prefix [" + FORCE_GROUP_SETTING_PREFIX + "]", e);
        }
        for (Map.Entry<String, Settings> entry : settingGroups.entrySet()) {
            final Optional<String> notValues = entry.getValue().keySet().stream().filter(s -> s.equals("values") == false).findFirst();
            if (notValues.isPresent()) {
                throw new IllegalArgumentException(
                    "invalid forced awareness setting [" + FORCE_GROUP_SETTING_PREFIX + entry.getKey() + "." + notValues.get() + "]"
                );
            }
        }
    }
}
