/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.Booleans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.isIndexVerifiedBeforeClosed;

/**
 * This class acts as a functional wrapper around the {@code index.auto_expand_replicas} setting.
 * This setting or rather it's value is expanded into a min and max value which requires special handling
 * based on the number of datanodes in the cluster. This class handles all the parsing and streamlines the access to these values.
 */
public record AutoExpandReplicas(int minReplicas, int maxReplicas, boolean enabled) {

    // the value we recognize in the "max" position to mean all the nodes
    private static final String ALL_NODES_VALUE = "all";

    private static final AutoExpandReplicas FALSE_INSTANCE = new AutoExpandReplicas(0, 0, false);

    public static final Setting<AutoExpandReplicas> SETTING = new Setting<>(
        IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS,
        "false",
        AutoExpandReplicas::parse,
        Property.Dynamic,
        Property.IndexScope
    );

    private static AutoExpandReplicas parse(String value) {
        final int min;
        final int max;
        if (Booleans.isFalse(value)) {
            return FALSE_INSTANCE;
        }
        final int dash = value.indexOf('-');
        if (-1 == dash) {
            throw new IllegalArgumentException(
                "failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS + "] from value: [" + value + "] at index " + dash
            );
        }
        final String sMin = value.substring(0, dash);
        try {
            min = Integer.parseInt(sMin);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS + "] from value: [" + value + "] at index " + dash,
                e
            );
        }
        String sMax = value.substring(dash + 1);
        if (sMax.equals(ALL_NODES_VALUE)) {
            max = Integer.MAX_VALUE;
        } else {
            try {
                max = Integer.parseInt(sMax);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS + "] from value: [" + value + "] at index " + dash,
                    e
                );
            }
        }
        return new AutoExpandReplicas(min, max, true);
    }

    public AutoExpandReplicas {
        if (minReplicas > maxReplicas) {
            throw new IllegalArgumentException(
                "["
                    + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS
                    + "] minReplicas must be =< maxReplicas but wasn't "
                    + minReplicas
                    + " > "
                    + maxReplicas
            );
        }
    }

    public boolean expandToAllNodes() {
        return maxReplicas == Integer.MAX_VALUE;
    }

    public int getDesiredNumberOfReplicas(IndexMetadata indexMetadata, RoutingAllocation allocation) {
        assert enabled : "should only be called when enabled";
        int numMatchingDataNodes = 0;
        for (DiscoveryNode discoveryNode : allocation.nodes().getDataNodes().values()) {
            Decision decision = allocation.deciders().shouldAutoExpandToNode(indexMetadata, discoveryNode, allocation);
            if (decision.type() != Decision.Type.NO) {
                numMatchingDataNodes++;
            }
        }
        return calculateDesiredNumberOfReplicas(numMatchingDataNodes);
    }

    // package private for testing
    int calculateDesiredNumberOfReplicas(int numMatchingDataNodes) {
        int numberOfReplicas = numMatchingDataNodes - 1;
        // Make sure number of replicas is always between min and max
        if (numberOfReplicas < minReplicas) {
            numberOfReplicas = minReplicas;
        } else if (numberOfReplicas > maxReplicas) {
            numberOfReplicas = maxReplicas;
        }
        return numberOfReplicas;
    }

    @Override
    public String toString() {
        return enabled ? minReplicas + "-" + maxReplicas : "false";
    }

    /**
     * Checks if there are replicas with the auto-expand feature that need to be adapted.
     * Returns a map of updates, which maps the indices to be updated to the desired number of replicas.
     * The map has the desired number of replicas as key and the indices to update as value, as this allows the result
     * of this method to be directly applied to RoutingTable.Builder#updateNumberOfReplicas.
     */
    public static Map<Integer, List<String>> getAutoExpandReplicaChanges(
        Metadata metadata,
        Supplier<RoutingAllocation> allocationSupplier
    ) {
        Map<Integer, List<String>> nrReplicasChanged = new HashMap<>();
        // RoutingAllocation is fairly expensive to compute, only lazy create it via the supplier if we actually need it
        RoutingAllocation allocation = null;
        for (final IndexMetadata indexMetadata : metadata) {
            if (indexMetadata.getState() == IndexMetadata.State.OPEN || isIndexVerifiedBeforeClosed(indexMetadata)) {
                AutoExpandReplicas autoExpandReplicas = indexMetadata.getAutoExpandReplicas();
                if (autoExpandReplicas.enabled() == false) {
                    continue;
                }
                if (allocation == null) {
                    allocation = allocationSupplier.get();
                }
                int numberOfReplicas = autoExpandReplicas.getDesiredNumberOfReplicas(indexMetadata, allocation);
                if (numberOfReplicas != indexMetadata.getNumberOfReplicas()) {
                    nrReplicasChanged.computeIfAbsent(numberOfReplicas, ArrayList::new).add(indexMetadata.getIndex().getName());
                }
            }
        }
        return nrReplicasChanged;
    }
}
