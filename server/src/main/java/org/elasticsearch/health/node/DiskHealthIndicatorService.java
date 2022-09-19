/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DiskHealthIndicatorService implements HealthIndicatorService {
    public static final String NAME = "disk";

    private static final Logger logger = LogManager.getLogger(DiskHealthIndicatorService.class);

    private final ClusterService clusterService;

    public DiskHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain, HealthInfo healthInfo) {
        Map<String, DiskHealthInfo> diskHealthInfoMap = healthInfo.diskInfoByNode();
        if (diskHealthInfoMap == null || diskHealthInfoMap.isEmpty()) {
            return createIndicator(
                HealthStatus.UNKNOWN,
                "No disk usage data.",
                HealthIndicatorDetails.EMPTY,
                Collections.emptyList(),
                Collections.emptyList()
            );
        }
        Set<String> indicesWithBlock = clusterService.state()
            .blocks()
            .indices()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().contains(IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        boolean hasAtLeastOneIndexReadOnlyAllowDeleteBlock = indicesWithBlock.isEmpty() == false;
        logMissingHealthInfoData(diskHealthInfoMap);
        HealthIndicatorDetails details = getDetails(explain, diskHealthInfoMap);
        final HealthStatus healthStatusFromNodes = HealthStatus.merge(
            diskHealthInfoMap.values().stream().map(DiskHealthInfo::healthStatus)
        );
        final HealthStatus healthStatus = hasAtLeastOneIndexReadOnlyAllowDeleteBlock ? HealthStatus.RED : healthStatusFromNodes;
        final HealthIndicatorResult healthIndicatorResult;
        if (HealthStatus.GREEN.equals(healthStatus)) {
            healthIndicatorResult = createIndicator(
                healthStatus,
                "The cluster has enough available disk space.",
                details,
                List.of(),
                List.of()
            );
        } else {
            if (HealthStatus.RED.equals(healthStatusFromNodes)) {
                final Set<String> nodesReportingRed = diskHealthInfoMap.entrySet()
                    .stream()
                    .filter(entry -> HealthStatus.RED.equals(entry.getValue().healthStatus()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
                Set<DiscoveryNodeRole> rolesOnRedNodes = getRolesOnNodes(nodesReportingRed);
                if (hasAtLeastOneIndexReadOnlyAllowDeleteBlock || rolesOnRedNodes.stream().anyMatch(DiscoveryNodeRole::canContainData)) {
                    healthIndicatorResult = getResultForRedIndicesOrDataNodes(
                        nodesReportingRed,
                        Stream.concat(indicesWithBlock.stream(), getIndicesForNodes(nodesReportingRed).stream())
                            .collect(Collectors.toSet()),
                        true,
                        details,
                        healthStatus
                    );
                } else {
                    healthIndicatorResult = getResultForNonDataNodeProblem(rolesOnRedNodes, nodesReportingRed, details, healthStatus);
                }
            } else {
                final Set<String> nodesReportingYellow = diskHealthInfoMap.entrySet()
                    .stream()
                    .filter(entry -> HealthStatus.YELLOW.equals(entry.getValue().healthStatus()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
                Set<DiscoveryNodeRole> rolesOnYellowNodes = getRolesOnNodes(nodesReportingYellow);
                if (hasAtLeastOneIndexReadOnlyAllowDeleteBlock) {
                    healthIndicatorResult = getResultForRedIndicesOrDataNodes(
                        nodesReportingYellow,
                        indicesWithBlock,
                        false,
                        details,
                        healthStatus
                    );
                } else if (rolesOnYellowNodes.stream().anyMatch(DiscoveryNodeRole::canContainData)) {
                    healthIndicatorResult = getResultForYellowDataNodes(nodesReportingYellow, details, healthStatus);
                } else {
                    healthIndicatorResult = getResultForNonDataNodeProblem(rolesOnYellowNodes, nodesReportingYellow, details, healthStatus);
                }
            }
        }
        return healthIndicatorResult;
    }

    private HealthIndicatorResult getResultForNonDataNodeProblem(
        Set<DiscoveryNodeRole> roles,
        Set<String> problemNodes,
        HealthIndicatorDetails details,
        HealthStatus status
    ) {
        String symptom;
        final List<HealthIndicatorImpact> impacts;
        final List<Diagnosis> diagnosisList;
        if (roles.contains(DiscoveryNodeRole.MASTER_ROLE)) {
            Set<DiscoveryNode> problemMasterNodes = clusterService.state()
                .nodes()
                .getNodes()
                .values()
                .stream()
                .filter(node -> problemNodes.contains(node.getId()))
                .filter(node -> node.getRoles().contains(DiscoveryNodeRole.MASTER_ROLE))
                .collect(Collectors.toSet());
            symptom = String.format(
                Locale.ROOT,
                "%d node%s with role master %s out of disk space. As a result %s functions might be impaired.",
                problemMasterNodes.size(),
                problemMasterNodes.size() == 1 ? "" : "s",
                problemMasterNodes.size() == 1 ? "is" : "are",
                problemMasterNodes.size() == 1 ? "its" : "their"
            );
            impacts = List.of(
                new HealthIndicatorImpact(2, "Cluster stability might be impaired.", List.of(ImpactArea.DEPLOYMENT_MANAGEMENT))
            );
            diagnosisList = List.of(
                new Diagnosis(
                    new Diagnosis.Definition(
                        "free-disk-space-or-add-capacity-master-nodes",
                        "Disk is almost full.",
                        "Please add capacity to the current nodes, or replace them with ones with higher capacity.",
                        "https://ela.st/free-disk-space-or-add-capacity-master-nodes"
                    ),
                    problemMasterNodes.stream().map(DiscoveryNode::getId).toList()
                )
            );
        } else {
            symptom = String.format(
                Locale.ROOT,
                "%d node%s with roles [%s] %s out of disk space. As a result %s functions might be impaired.",
                problemNodes.size(),
                problemNodes.size() == 1 ? "" : "s",
                roles.stream().map(DiscoveryNodeRole::roleName).sorted().collect(Collectors.joining(", ")),
                problemNodes.size() == 1 ? "is" : "are",
                problemNodes.size() == 1 ? "its" : "their"
            );
            impacts = List.of(
                new HealthIndicatorImpact(2, "Some cluster functionality might be unavailable.", List.of(ImpactArea.DEPLOYMENT_MANAGEMENT))
            );
            diagnosisList = List.of(
                new Diagnosis(
                    new Diagnosis.Definition(
                        "free-disk-space-or-add-capacity-other-nodes",
                        "Disk is almost full.",
                        "Please add capacity to the current nodes, or replace them with ones with higher capacity.",
                        "https://ela.st/free-disk-space-or-add-capacity-other-nodes"
                    ),
                    problemNodes.stream().toList()
                )
            );
        }
        return createIndicator(status, symptom, details, impacts, diagnosisList);
    }

    public HealthIndicatorResult getResultForRedIndicesOrDataNodes(
        Set<String> nodesReportingProblems,
        Set<String> impactedIndices,
        boolean statusFromNodesWasRed,
        HealthIndicatorDetails details,
        HealthStatus status
    ) {
        final String symptom;
        if (impactedIndices.isEmpty()) {
            symptom = String.format(
                Locale.ROOT,
                "%d data node%s %s disk space.",
                nodesReportingProblems.size(),
                nodesReportingProblems.size() == 1 ? " is" : "s are",
                statusFromNodesWasRed ? "out of" : "running low on"
            );
        } else {
            symptom = String.format(
                Locale.ROOT,
                "%d %s blocked and cannot be updated %s.",
                impactedIndices.size(),
                impactedIndices.size() == 1 ? "index is" : "indices are",
                nodesReportingProblems.isEmpty()
                    ? "but 0 nodes are currently out of space"
                    : String.format(
                        Locale.ROOT,
                        "because %d node%s %s disk space",
                        nodesReportingProblems.size(),
                        nodesReportingProblems.size() == 1 ? " is" : "s are",
                        statusFromNodesWasRed ? "out of" : "running low on"
                    )
            );
        }
        List<HealthIndicatorImpact> impacts = List.of(
            new HealthIndicatorImpact(1, "Cannot insert or update documents in the affected indices.", List.of(ImpactArea.INGEST))
        );
        List<Diagnosis> diagnosisList = List.of(
            new Diagnosis(
                new Diagnosis.Definition(
                    "free-disk-space-or-add-capacity-data-nodes",
                    String.format(
                        Locale.ROOT,
                        "%d %s reside%s on nodes that have run out of space and writing has been blocked by the system.",
                        impactedIndices.size(),
                        impactedIndices.size() == 1 ? "index" : "indices",
                        impactedIndices.size() == 1 ? "s" : ""
                    ),
                    "Enable autoscaling (if applicable), add disk capacity or free up disk space to resolve "
                        + "this. If you have already taken action please wait for the rebalancing to complete.",
                    "https://ela.st/free-disk-space-or-add-capacity-data-nodes"
                ),
                nodesReportingProblems.stream().toList()
            )
        );
        return createIndicator(status, symptom, details, impacts, diagnosisList);
    }

    public HealthIndicatorResult getResultForYellowDataNodes(
        Set<String> problemNodes,
        HealthIndicatorDetails details,
        HealthStatus status
    ) {
        final Set<String> problemIndices = getIndicesForNodes(problemNodes);
        final String symptom = String.format(
            Locale.ROOT,
            "%d data node%s increased disk usage. As a result %d %s at risk of not being able to process any more " + "updates.",
            problemNodes.size(),
            problemNodes.size() == 1 ? " has" : "s have",
            problemIndices.size(),
            problemIndices.size() == 1 ? "index is" : "indices are"
        );
        final List<HealthIndicatorImpact> impacts = List.of(
            new HealthIndicatorImpact(
                1,
                "At risk of not being able to insert or update documents in the affected indices.",
                List.of(ImpactArea.INGEST)
            )
        );
        final List<Diagnosis> diagnosisList = List.of(
            new Diagnosis(
                new Diagnosis.Definition(
                    "free-disk-space-or-add-capacity-data-nodes",
                    String.format(
                        Locale.ROOT,
                        "%d %s reside%s on nodes that have run out of space and writing has been blocked by the system.",
                        problemIndices.size(),
                        problemIndices.size() == 1 ? "index" : "indices",
                        problemIndices.size() == 1 ? "s" : ""
                    ),
                    "Enable autoscaling (if applicable), add disk capacity or free up disk space to resolve "
                        + "this. If you have already taken action please wait for the rebalancing to complete.",
                    "https://ela.st/free-disk-space-or-add-capacity-data-nodes"
                ),
                problemNodes.stream().toList()
            )
        );
        return createIndicator(status, symptom, details, impacts, diagnosisList);
    }

    private Set<DiscoveryNodeRole> getRolesOnNodes(Set<String> nodeIds) {
        return clusterService.state()
            .nodes()
            .getNodes()
            .values()
            .stream()
            .filter(node -> nodeIds.contains(node.getId()))
            .map(DiscoveryNode::getRoles)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }

    private Set<String> getIndicesForNodes(Set<String> nodes) {
        return clusterService.state()
            .routingTable()
            .allShards()
            .stream()
            .filter(routing -> nodes.contains(routing.currentNodeId()))
            .map(routing -> routing.index().getName())
            .collect(Collectors.toSet());
    }

    /**
     * This method logs if any nodes in the cluster state do not have health info results reported. This is logged at debug level and is
     * not ordinarly important, but could be useful in tracking down problems where nodes have stopped reporting health node information.
     * @param diskHealthInfoMap A map of nodeId to DiskHealthInfo
     */
    private void logMissingHealthInfoData(Map<String, DiskHealthInfo> diskHealthInfoMap) {
        if (logger.isDebugEnabled()) {
            Set<DiscoveryNode> nodesInClusterState = new HashSet<>(clusterService.state().nodes());
            Set<String> nodeIdsInClusterState = nodesInClusterState.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
            Set<String> nodeIdsInHealthInfo = diskHealthInfoMap.keySet();
            if (nodeIdsInHealthInfo.containsAll(nodeIdsInClusterState) == false) {
                String problemNodes = nodesInClusterState.stream()
                    .filter(node -> nodeIdsInHealthInfo.contains(node.getId()) == false)
                    .map(node -> String.format(Locale.ROOT, "{%s / %s}", node.getId(), node.getName()))
                    .collect(Collectors.joining(", "));
                logger.debug("The following nodes are in the cluster state but not reporting health data: [{}}]", problemNodes);
            }
        }
    }

    private HealthIndicatorDetails getDetails(boolean explain, Map<String, DiskHealthInfo> diskHealthInfoMap) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        return (builder, params) -> {
            builder.startObject();
            builder.array("nodes", arrayXContentBuilder -> {
                for (Map.Entry<String, DiskHealthInfo> entry : diskHealthInfoMap.entrySet()) {
                    builder.startObject();
                    String nodeId = entry.getKey();
                    builder.field("node_id", nodeId);
                    String nodeName = getNameForNodeId(nodeId);
                    if (nodeName != null) {
                        builder.field("name", nodeName);
                    }
                    builder.field("status", entry.getValue().healthStatus());
                    DiskHealthInfo.Cause cause = entry.getValue().cause();
                    if (cause != null) {
                        builder.field("cause", entry.getValue().cause());
                    }
                    builder.endObject();
                }
            });
            return builder.endObject();
        };
    }

    /**
     * Returns the name of the node with the given nodeId, as seen in the cluster state at this moment. The name of a node is optional,
     * so if the node does not have a name (or the node with the given nodeId is no longer in the cluster state), null is returned.
     * @param nodeId The id of the node whose name is to be returned
     * @return The current name of the node, or null if the node is not in the cluster state or does not have a name
     */
    @Nullable
    private String getNameForNodeId(String nodeId) {
        DiscoveryNode node = clusterService.state().nodes().get(nodeId);
        if (node == null) {
            return null;
        } else {
            String nodeName = node.getName();
            return Objects.requireNonNullElse(nodeName, null);
        }
    }
}
