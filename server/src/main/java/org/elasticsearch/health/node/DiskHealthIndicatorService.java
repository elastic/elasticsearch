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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DiskHealthIndicatorService implements HealthIndicatorService {
    public static final String NAME = "disk";

    private static final Logger logger = LogManager.getLogger(DiskHealthIndicatorService.class);

    private static final String IMPACT_INGEST_UNAVAILABLE_ID = "ingest_capability_unavailable";
    private static final String IMPACT_INGEST_AT_RISK_ID = "ingest_capability_at_risk";
    private static final String IMPACT_CLUSTER_STABILITY_AT_RISK_ID = "cluster_stability_at_risk";
    private static final String IMPACT_CLUSTER_FUNCTIONALITY_UNAVAILABLE_ID = "cluster_functionality_unavailable";

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
            /*
             * If there is no disk health info, that either means that a new health node was just elected, or something is seriously
             * wrong with health data collection on the health node. Either way, we immediately return UNKNOWN. If there are at least
             * some health info results then we work with what we have (and log any missing ones at debug level immediately below this).
             */
            return createIndicator(
                HealthStatus.UNKNOWN,
                "No disk usage data.",
                HealthIndicatorDetails.EMPTY,
                Collections.emptyList(),
                Collections.emptyList()
            );
        }
        ClusterState clusterState = clusterService.state();
        logMissingHealthInfoData(diskHealthInfoMap, clusterState);

        /*
         * If there are any index blocks in the cluster state, that makes the overall status automatically red, regardless of the statuses
         * returned by the nodes. If there is no cluster block, we just use the merged statuses of the nodes.
         */
        Set<String> indicesWithBlock = clusterState.blocks()
            .indices()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().contains(IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        boolean clusterHasBlockedIndex = indicesWithBlock.isEmpty() == false;
        HealthIndicatorDetails details = getDetails(explain, diskHealthInfoMap, clusterState);
        final HealthStatus healthStatusFromNodes = HealthStatus.merge(
            diskHealthInfoMap.values().stream().map(DiskHealthInfo::healthStatus)
        );
        final HealthStatus healthStatus = clusterHasBlockedIndex ? HealthStatus.RED : healthStatusFromNodes;

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
            /*
             * In this case the status is either RED or YELLOW. So we collect information about red and yellow indices (including indices
             *  with blocks placed on them), and red and yellow nodes (including those with a blocked index). We then use that
             * information to get the sympotom, impacts, and diagnoses for the result.
             */
            Set<String> nodesWithBlockedIndices = getNodeIdsForIndices(indicesWithBlock, clusterState);
            Set<String> nodesReportingRed = getNodeIdsReportingStatus(diskHealthInfoMap, HealthStatus.RED);
            Set<String> indicesOnRedNodes = getIndicesForNodes(nodesReportingRed, clusterState);
            Set<String> nodesReportingYellow = getNodeIdsReportingStatus(diskHealthInfoMap, HealthStatus.YELLOW);
            Set<String> indicesOnYellowNodes = getIndicesForNodes(nodesReportingYellow, clusterState);
            Set<String> redDataNodes = getNodesWithDataRole(nodesReportingRed, clusterState);
            Set<String> yellowDataNodes = getNodesWithDataRole(nodesReportingYellow, clusterState);
            Set<String> redMasterNodes = getNodesWithMasterRole(nodesReportingRed, clusterState);
            Set<String> yellowMasterNodes = getNodesWithMasterRole(nodesReportingYellow, clusterState);
            Set<String> redNonDataNonMasterNodes = getNodesWithNonDataNonMasterRoles(nodesReportingRed, clusterState);
            Set<String> yellowNonDataNonMasterNodes = getNodesWithNonDataNonMasterRoles(nodesReportingYellow, clusterState);

            String symptom = getSymptom(
                clusterHasBlockedIndex,
                indicesWithBlock,
                nodesWithBlockedIndices,
                nodesReportingRed,
                nodesReportingYellow,
                clusterState
            );
            List<HealthIndicatorImpact> impacts = getImpacts(
                indicesWithBlock,
                indicesOnRedNodes,
                indicesOnYellowNodes,
                nodesWithBlockedIndices,
                redDataNodes,
                yellowDataNodes,
                redMasterNodes,
                yellowMasterNodes,
                redNonDataNonMasterNodes,
                yellowNonDataNonMasterNodes
            );
            List<Diagnosis> diagnosisList = getDiagnoses(
                indicesWithBlock,
                indicesOnRedNodes,
                indicesOnYellowNodes,
                nodesWithBlockedIndices,
                redDataNodes,
                yellowDataNodes,
                redMasterNodes,
                yellowMasterNodes,
                redNonDataNonMasterNodes,
                yellowNonDataNonMasterNodes
            );
            healthIndicatorResult = createIndicator(healthStatus, symptom, details, impacts, diagnosisList);
        }
        return healthIndicatorResult;
    }

    private String getSymptom(
        boolean clusterHasBlockedIndex,
        Set<String> blockedIndices,
        Set<String> nodesWithBlockedIndices,
        Set<String> nodesReportingRed,
        Set<String> nodesReportingYellow,
        ClusterState clusterState
    ) {
        Set<String> allUnhealthyNodes = (Stream.concat(
            Stream.concat(nodesWithBlockedIndices.stream(), nodesReportingRed.stream()),
            nodesReportingYellow.stream()
        )).collect(Collectors.toSet());
        Set<String> allRolesOnUnhealthyNodes = getRolesOnNodes(allUnhealthyNodes, clusterState).stream()
            .map(DiscoveryNodeRole::roleName)
            .collect(Collectors.toSet());
        final String symptom;
        if (clusterHasBlockedIndex && allUnhealthyNodes.isEmpty()) {
            // In this case the disk issue has been resolved but the index block has not been automatically removed yet:
            symptom = String.format(
                Locale.ROOT,
                "%d %s blocked and cannot be updated but 0 nodes are currently out of space.",
                blockedIndices.size(),
                blockedIndices.size() == 1 ? "index is" : "indices are"
            );
        } else {
            symptom = String.format(
                Locale.ROOT,
                "%d node%s with roles: [%s] %s out of disk or running low on disk space.",
                allUnhealthyNodes.size(),
                allUnhealthyNodes.size() == 1 ? "" : "s",
                allRolesOnUnhealthyNodes.stream().sorted().collect(Collectors.joining(", ")),
                allUnhealthyNodes.size() == 1 ? "is" : "are"
            );
        }
        return symptom;
    }

    private List<HealthIndicatorImpact> getImpacts(
        Set<String> indicesWithBlock,
        Set<String> indicesOnRedNodes,
        Set<String> indicesOnYellowNodes,
        Set<String> nodesWithBlockedIndices,
        Set<String> redDataNodes,
        Set<String> yellowDataNodes,
        Set<String> redMasterNodes,
        Set<String> yellowMasterNodes,
        Set<String> redNonDataNonMasterNodes,
        Set<String> yellowNonDataNonMasterNodes
    ) {
        List<HealthIndicatorImpact> impacts = new ArrayList<>();
        if (indicesWithBlock.isEmpty() == false
            || indicesOnRedNodes.isEmpty() == false
            || nodesWithBlockedIndices.isEmpty() == false
            || redDataNodes.isEmpty() == false) {
            impacts.add(
                new HealthIndicatorImpact(
                    NAME,
                    IMPACT_INGEST_UNAVAILABLE_ID,
                    1,
                    "Cannot insert or update documents in the affected indices.",
                    List.of(ImpactArea.INGEST)
                )
            );
        } else if (indicesOnYellowNodes.isEmpty() == false || yellowDataNodes.isEmpty() == false) {
            impacts.add(
                new HealthIndicatorImpact(
                    NAME,
                    IMPACT_INGEST_AT_RISK_ID,
                    1,
                    "At risk of not being able to insert or update documents in the affected indices.",
                    List.of(ImpactArea.INGEST)
                )
            );
        }
        if (redMasterNodes.isEmpty() == false || yellowMasterNodes.isEmpty() == false) {
            impacts.add(
                new HealthIndicatorImpact(
                    NAME,
                    IMPACT_CLUSTER_STABILITY_AT_RISK_ID,
                    2,
                    "Cluster stability might be impaired.",
                    List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                )
            );
        }
        if (redNonDataNonMasterNodes.isEmpty() == false || yellowNonDataNonMasterNodes.isEmpty() == false) {
            impacts.add(
                new HealthIndicatorImpact(
                    NAME,
                    IMPACT_CLUSTER_FUNCTIONALITY_UNAVAILABLE_ID,
                    2,
                    "Some cluster functionality might be unavailable.",
                    List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                )
            );
        }
        return impacts;
    }

    private List<Diagnosis> getDiagnoses(
        Set<String> indicesWithBlock,
        Set<String> indicesOnRedNodes,
        Set<String> indicesOnYellowNodes,
        Set<String> nodesWithBlockedIndices,
        Set<String> redDataNodes,
        Set<String> yellowDataNodes,
        Set<String> redMasterNodes,
        Set<String> yellowMasterNodes,
        Set<String> redNonDataNonMasterNodes,
        Set<String> yellowNonDataNonMasterNodes
    ) {
        List<Diagnosis> diagnosisList = new ArrayList<>();
        if (indicesWithBlock.isEmpty() == false
            || nodesWithBlockedIndices.isEmpty() == false
            || indicesOnRedNodes.isEmpty() == false
            || redDataNodes.isEmpty() == false
            || indicesOnYellowNodes.isEmpty() == false
            || yellowDataNodes.isEmpty() == false) {
            Set<String> impactedIndices = Stream.concat(
                Stream.concat(indicesWithBlock.stream(), indicesOnRedNodes.stream()),
                indicesOnYellowNodes.stream()
            ).collect(Collectors.toSet());
            Set<String> unhealthyNodes = Stream.concat(
                Stream.concat(nodesWithBlockedIndices.stream(), redDataNodes.stream()),
                yellowDataNodes.stream()
            ).collect(Collectors.toSet());
            diagnosisList.add(
                new Diagnosis(
                    new Diagnosis.Definition(
                        NAME,
                        "add_disk_capacity_data_nodes",
                        String.format(
                            Locale.ROOT,
                            "%d %s reside%s on nodes that have run out of space and writing has been blocked by the system.",
                            impactedIndices.size(),
                            impactedIndices.size() == 1 ? "index" : "indices",
                            impactedIndices.size() == 1 ? "s" : ""
                        ),
                        "Enable autoscaling (if applicable), add disk capacity or free up disk space to resolve "
                            + "this. If you have already taken action please wait for the rebalancing to complete.",
                        "https://ela.st/fix-data-disk"
                    ),
                    unhealthyNodes.stream().sorted().toList()
                )
            );
        }
        if (redMasterNodes.isEmpty() == false || yellowMasterNodes.isEmpty() == false) {
            diagnosisList.add(
                new Diagnosis(
                    new Diagnosis.Definition(
                        NAME,
                        "add_disk_capacity_master_nodes",
                        "Disk is almost full.",
                        "Please add capacity to the current nodes, or replace them with ones with higher capacity.",
                        "https://ela.st/fix-master-disk"
                    ),
                    Stream.concat(redMasterNodes.stream(), yellowMasterNodes.stream()).sorted().toList()
                )
            );
        }
        if (redNonDataNonMasterNodes.isEmpty() == false || yellowNonDataNonMasterNodes.isEmpty() == false) {
            diagnosisList.add(
                new Diagnosis(
                    new Diagnosis.Definition(
                        NAME,
                        "add_disk_capacity",
                        "Disk is almost full.",
                        "Please add capacity to the current nodes, or replace them with ones with higher capacity.",
                        "https://ela.st/fix-disk-space"
                    ),
                    Stream.concat(redNonDataNonMasterNodes.stream(), yellowNonDataNonMasterNodes.stream()).sorted().toList()
                )
            );
        }
        return diagnosisList;
    }

    // Non-private for unit testing
    static Set<String> getNodeIdsReportingStatus(Map<String, DiskHealthInfo> diskHealthInfoMap, HealthStatus status) {
        return diskHealthInfoMap.entrySet()
            .stream()
            .filter(entry -> status.equals(entry.getValue().healthStatus()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    // Non-private for unit testing
    static Set<DiscoveryNodeRole> getRolesOnNodes(Set<String> nodeIds, ClusterState clusterState) {
        return clusterState.nodes()
            .getNodes()
            .values()
            .stream()
            .filter(node -> nodeIds.contains(node.getId()))
            .map(DiscoveryNode::getRoles)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }

    // Non-private for unit testing
    static Set<String> getNodesWithDataRole(Set<String> nodeIds, ClusterState clusterState) {
        return clusterState.nodes()
            .getNodes()
            .values()
            .stream()
            .filter(node -> nodeIds.contains(node.getId()))
            .filter(node -> node.getRoles().stream().anyMatch(DiscoveryNodeRole::canContainData))
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());
    }

    // Non-private for unit testing
    static Set<String> getNodesWithMasterRole(Set<String> nodeIds, ClusterState clusterState) {
        return clusterState.nodes()
            .getNodes()
            .values()
            .stream()
            .filter(node -> nodeIds.contains(node.getId()))
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.MASTER_ROLE))
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());
    }

    // Non-private for unit testing
    static Set<String> getNodesWithNonDataNonMasterRoles(Set<String> nodeIds, ClusterState clusterState) {
        return clusterState.nodes()
            .getNodes()
            .values()
            .stream()
            .filter(node -> nodeIds.contains(node.getId()))
            .filter(
                node -> node.getRoles()
                    .stream()
                    .anyMatch(role -> (role.equals(DiscoveryNodeRole.MASTER_ROLE) || role.canContainData()) == false)
            )
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());
    }

    // Non-private for unit testing
    static Set<String> getIndicesForNodes(Set<String> nodes, ClusterState clusterState) {
        return clusterState.routingTable()
            .allShards()
            .stream()
            .filter(routing -> nodes.contains(routing.currentNodeId()))
            .map(routing -> routing.index().getName())
            .collect(Collectors.toSet());
    }

    // Non-private for unit testing
    static Set<String> getNodeIdsForIndices(Set<String> indices, ClusterState clusterState) {
        return clusterState.routingTable()
            .allShards()
            .stream()
            .filter(routing -> indices.contains(routing.index().getName()))
            .map(ShardRouting::currentNodeId)
            .collect(Collectors.toSet());
    }

    /**
     * This method logs if any nodes in the cluster state do not have health info results reported. This is logged at debug level and is
     * not ordinarly important, but could be useful in tracking down problems where nodes have stopped reporting health node information.
     * @param diskHealthInfoMap A map of nodeId to DiskHealthInfo
     */
    private void logMissingHealthInfoData(Map<String, DiskHealthInfo> diskHealthInfoMap, ClusterState clusterState) {
        if (logger.isDebugEnabled()) {
            Set<DiscoveryNode> nodesInClusterState = new HashSet<>(clusterState.nodes());
            Set<String> nodeIdsInClusterState = nodesInClusterState.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
            Set<String> nodeIdsInHealthInfo = diskHealthInfoMap.keySet();
            if (nodeIdsInHealthInfo.containsAll(nodeIdsInClusterState) == false) {
                String nodesWithMissingData = nodesInClusterState.stream()
                    .filter(node -> nodeIdsInHealthInfo.contains(node.getId()) == false)
                    .map(node -> String.format(Locale.ROOT, "{%s / %s}", node.getId(), node.getName()))
                    .collect(Collectors.joining(", "));
                logger.debug("The following nodes are in the cluster state but not reporting health data: [{}}]", nodesWithMissingData);
            }
        }
    }

    private HealthIndicatorDetails getDetails(boolean explain, Map<String, DiskHealthInfo> diskHealthInfoMap, ClusterState clusterState) {
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
                    String nodeName = getNameForNodeId(nodeId, clusterState);
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
    private String getNameForNodeId(String nodeId, ClusterState clusterState) {
        DiscoveryNode node = clusterState.nodes().get(nodeId);
        return node == null ? null : node.getName();
    }
}
