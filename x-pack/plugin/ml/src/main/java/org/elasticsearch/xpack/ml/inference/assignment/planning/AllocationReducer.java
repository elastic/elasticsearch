/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * Reduces the number of allocations of a {@link TrainedModelAssignment}.
 */
public class AllocationReducer {

    private static final Logger logger = LogManager.getLogger(AllocationReducer.class);

    private final TrainedModelAssignment assignment;
    private final Map<List<String>, Set<String>> nodeIdsByZone;

    public AllocationReducer(TrainedModelAssignment assignment, Map<List<String>, Collection<DiscoveryNode>> nodesByZone) {
        this.assignment = Objects.requireNonNull(assignment);
        this.nodeIdsByZone = nodesByZone.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().map(DiscoveryNode::getId).collect(Collectors.toSet())));
    }

    public TrainedModelAssignment.Builder reduceTo(int numberOfAllocations) {
        final Map<String, Integer> allocationsByNode = assignment.getNodeRoutingTable()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getTargetAllocations()));

        final Map<List<String>, Integer> allocationsByZone = nodeIdsByZone.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream().mapToInt(nodeId -> allocationsByNode.getOrDefault(nodeId, 0)).sum()
                )
            );

        int totalRemainingAllocations = allocationsByZone.values().stream().mapToInt(Integer::intValue).sum();

        if (totalRemainingAllocations <= numberOfAllocations) {
            String msg = "request to reduce allocations is greater than or equal to the existing target number of allocations";
            throw new IllegalArgumentException(msg);
        }

        while (totalRemainingAllocations > numberOfAllocations) {
            // While we reduce allocations there are 2 concerns:
            // 1. Remove entire assignments when possible to free the used memory
            // 2. Preserve balance across zones
            // The way we achieve this here is simple. Each iteration, we pick the zone with the most allocations
            // and find its smallest assignment. We then check if we can remove that assignment entirely, or we
            // reduce its allocations by 1.

            final int allocationsToRemove = totalRemainingAllocations - numberOfAllocations;
            List<Map.Entry<List<String>, Integer>> allocationsPerZoneInAscendingOrder = allocationsByZone.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .toList();
            if (allocationsPerZoneInAscendingOrder.isEmpty()) {
                logger.warn("no allocations remain in any zone");
                throw new IllegalStateException("no allocations remain in any zone");
            }
            final List<String> largestZone = allocationsPerZoneInAscendingOrder.get(allocationsPerZoneInAscendingOrder.size() - 1).getKey();
            final int largestZoneAllocations = allocationsPerZoneInAscendingOrder.get(allocationsPerZoneInAscendingOrder.size() - 1)
                .getValue();
            final int minAllocationsInOtherZones = allocationsPerZoneInAscendingOrder.size() <= 1
                ? 0
                : allocationsPerZoneInAscendingOrder.get(0).getValue();

            List<Map.Entry<String, Integer>> largestZoneAssignmentsInAscendingOrder = allocationsByNode.entrySet()
                .stream()
                .filter(e -> nodeIdsByZone.get(largestZone).contains(e.getKey()))
                .sorted(Map.Entry.comparingByValue())
                .toList();

            if (largestZoneAssignmentsInAscendingOrder.isEmpty()) {
                logger.warn("no assignments remain in the largest zone");
                throw new IllegalStateException("no assignments remain in the largest zone");
            }

            Map.Entry<String, Integer> smallestAssignmentInLargestZone = largestZoneAssignmentsInAscendingOrder.get(0);
            if (canAssignmentBeRemovedEntirely(
                smallestAssignmentInLargestZone,
                minAllocationsInOtherZones,
                largestZoneAllocations,
                allocationsToRemove
            )) {
                logger.debug(
                    () -> format(
                        "[%s] removing assignment with [%s] allocations on node [%s]",
                        assignment.getDeploymentId(),
                        smallestAssignmentInLargestZone.getValue(),
                        smallestAssignmentInLargestZone.getKey()
                    )
                );
                allocationsByNode.remove(smallestAssignmentInLargestZone.getKey());
                allocationsByZone.computeIfPresent(largestZone, (k, v) -> v - smallestAssignmentInLargestZone.getValue());
                totalRemainingAllocations -= smallestAssignmentInLargestZone.getValue();
            } else {
                logger.debug(
                    () -> format(
                        "[%s] removing 1 allocation from assignment with [%s] allocations on node [%s]",
                        assignment.getDeploymentId(),
                        smallestAssignmentInLargestZone.getValue(),
                        smallestAssignmentInLargestZone.getKey()
                    )
                );
                allocationsByNode.computeIfPresent(smallestAssignmentInLargestZone.getKey(), (k, v) -> v - 1);
                allocationsByZone.computeIfPresent(largestZone, (k, v) -> v - 1);
                totalRemainingAllocations -= 1;
            }
        }

        return buildUpdatedAssignment(numberOfAllocations, allocationsByNode);
    }

    private boolean canAssignmentBeRemovedEntirely(
        Map.Entry<String, Integer> assignment,
        int minAllocationsInOtherZones,
        int zoneAllocations,
        int allocationsToRemove
    ) {
        // Assignment has a single allocations so we should be able to remove it entirely
        if (assignment.getValue() == 1) {
            return true;
        }

        // Assignment has more allocations so we cannot remove it entirely.
        if (assignment.getValue() > allocationsToRemove) {
            return false;
        }

        // No allocations in other zones means we do not have to consider preserving balance of allocations across zones
        if (minAllocationsInOtherZones == 0) {
            return true;
        }
        // If we remove the allocations of the assignment from the zone and we still have as many allocations
        // as the smallest of the other zones we're still fairly balanced.
        return zoneAllocations - assignment.getValue() >= minAllocationsInOtherZones;
    }

    private TrainedModelAssignment.Builder buildUpdatedAssignment(int numberOfAllocations, Map<String, Integer> allocationsByNode) {
        TrainedModelAssignment.Builder reducedAssignmentBuilder = TrainedModelAssignment.Builder.fromAssignment(assignment);
        reducedAssignmentBuilder.setNumberOfAllocations(numberOfAllocations);
        for (Map.Entry<String, RoutingInfo> routingEntries : assignment.getNodeRoutingTable().entrySet()) {
            final String nodeId = routingEntries.getKey();
            if (allocationsByNode.containsKey(nodeId)) {
                final RoutingInfo existingRoutingInfo = routingEntries.getValue();
                reducedAssignmentBuilder.updateExistingRoutingEntry(
                    nodeId,
                    new RoutingInfo(
                        existingRoutingInfo.getCurrentAllocations(),
                        allocationsByNode.get(nodeId),
                        existingRoutingInfo.getState(),
                        existingRoutingInfo.getReason()
                    )
                );
            } else {
                reducedAssignmentBuilder.removeRoutingEntry(nodeId);
            }
        }
        return reducedAssignmentBuilder;
    }
}
