/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.isIndexVerifiedBeforeClosed;

/**
 * This class acts as a functional wrapper around the {@code index.auto_expand_replicas} setting.
 * This setting or rather it's value is expanded into a min and max value which requires special handling
 * based on the number of datanodes in the cluster. This class handles all the parsing and streamlines the access to these values.
 */
public final class AutoExpandReplicas {
    // the value we recognize in the "max" position to mean all the nodes
    private static final String ALL_NODES_VALUE = "all";

    private static final AutoExpandReplicas FALSE_INSTANCE = new AutoExpandReplicas(0, 0, false);

    public static final Setting<AutoExpandReplicas> SETTING = new Setting<>(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "false",
        AutoExpandReplicas::parse, Property.Dynamic, Property.IndexScope);

    private static AutoExpandReplicas parse(String value) {
        final int min;
        final int max;
        if (Booleans.isFalse(value)) {
            return FALSE_INSTANCE;
        }
        final int dash = value.indexOf('-');
        if (-1 == dash) {
            throw new IllegalArgumentException("failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS +
                "] from value: [" + value + "] at index " + dash);
        }
        final String sMin = value.substring(0, dash);
        try {
            min = Integer.parseInt(sMin);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS +
                "] from value: [" + value + "] at index "  + dash, e);
        }
        String sMax = value.substring(dash + 1);
        if (sMax.equals(ALL_NODES_VALUE)) {
            max = Integer.MAX_VALUE;
        } else {
            try {
                max = Integer.parseInt(sMax);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("failed to parse [" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS +
                    "] from value: [" + value + "] at index "  + dash, e);
            }
        }
        return new AutoExpandReplicas(min, max, true);
    }

    private final int minReplicas;
    private final int maxReplicas;
    private final boolean enabled;

    private AutoExpandReplicas(int minReplicas, int maxReplicas, boolean enabled) {
        if (minReplicas > maxReplicas) {
            throw new IllegalArgumentException("[" + IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS +
                "] minReplicas must be =< maxReplicas but wasn't " + minReplicas + " > "  + maxReplicas);
        }
        this.minReplicas = minReplicas;
        this.maxReplicas = maxReplicas;
        this.enabled = enabled;
    }

    int getMinReplicas() {
        return minReplicas;
    }

    int getMaxReplicas(int numDataNodes) {
        return Math.min(maxReplicas, numDataNodes-1);
    }

    public boolean expandToAllNodes() {
        return maxReplicas == Integer.MAX_VALUE;
    }

    private OptionalInt getDesiredNumberOfReplicas(IndexMetadata indexMetadata, RoutingAllocation allocation) {
        if (enabled) {
            int numMatchingDataNodes = 0;
            for (ObjectCursor<DiscoveryNode> cursor : allocation.nodes().getDataNodes().values()) {
                Decision decision = allocation.deciders().shouldAutoExpandToNode(indexMetadata, cursor.value, allocation);
                if (decision.type() != Decision.Type.NO) {
                    numMatchingDataNodes++;
                }
            }

            final int min = getMinReplicas();
            final int max = getMaxReplicas(numMatchingDataNodes);
            int numberOfReplicas = numMatchingDataNodes - 1;
            if (numberOfReplicas < min) {
                numberOfReplicas = min;
            } else if (numberOfReplicas > max) {
                numberOfReplicas = max;
            }

            if (numberOfReplicas >= min && numberOfReplicas <= max) {
                return OptionalInt.of(numberOfReplicas);
            }
        }
        return OptionalInt.empty();
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
    public static Map<Integer, List<String>> getAutoExpandReplicaChanges(Metadata metadata, RoutingAllocation allocation) {
        Map<Integer, List<String>> nrReplicasChanged = new HashMap<>();

        for (final IndexMetadata indexMetadata : metadata) {
            if (indexMetadata.getState() == IndexMetadata.State.OPEN || isIndexVerifiedBeforeClosed(indexMetadata)) {
                AutoExpandReplicas autoExpandReplicas = SETTING.get(indexMetadata.getSettings());
                autoExpandReplicas.getDesiredNumberOfReplicas(indexMetadata, allocation).ifPresent(numberOfReplicas -> {
                    if (numberOfReplicas != indexMetadata.getNumberOfReplicas()) {
                        nrReplicasChanged.computeIfAbsent(numberOfReplicas, ArrayList::new).add(indexMetadata.getIndex().getName());
                    }
                });
            }
        }
        return nrReplicasChanged;
    }
}


