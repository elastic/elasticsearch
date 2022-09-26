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
import org.elasticsearch.common.util.set.Sets;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.getTruncatedIndices;

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
        HealthIndicatorDetails details = getDetails(explain, diskHealthInfoMap, indicesWithBlock);
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
                indicesWithBlock,
                redDataNodes,
                yellowDataNodes,
                nodesReportingRed,
                nodesReportingYellow,
                clusterState
            );
            List<HealthIndicatorImpact> impacts = getImpacts(
                indicesWithBlock,
                Sets.union(indicesOnYellowNodes, indicesOnRedNodes),
                nodesReportingRed,
                nodesReportingYellow,
                clusterState
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
        Set<String> blockedIndices,
        Set<String> redDataNodes,
        Set<String> yellowDataNodes,
        Set<String> nodesReportingRed,
        Set<String> nodesReportingYellow,
        ClusterState clusterState
    ) {
        Set<String> unhealthyDataNodes = Sets.union(redDataNodes, yellowDataNodes);
        Set<String> allUnhealthyNodes = Sets.union(nodesReportingRed, nodesReportingYellow);

        String symptom;
        if (blockedIndices.isEmpty() == false) {
            symptom = String.format(
                Locale.ROOT,
                "%d %s not allowed to be updated because ",
                blockedIndices.size(),
                blockedIndices.size() == 1 ? "index is" : "indices are"
            );
            if (unhealthyDataNodes.isEmpty()) {
                // In this case the disk issue has been resolved but the index block has not been removed yet or the
                // cluster is still moving shards away from data nodes that are over the high watermark.
                symptom +=
                    ("the cluster was running out of disk space. The cluster is recovering and ingest capabilities should be restored "
                        + "within a few minutes.");
            } else {
                symptom += String.format(
                    Locale.ROOT,
                    "%d %s out of disk or running low on disk space.",
                    unhealthyDataNodes.size(),
                    unhealthyDataNodes.size() == 1 ? "node is" : "nodes are"
                );
            }
            if (unhealthyDataNodes.size() < allUnhealthyNodes.size()) {
                Set<String> unhealthyNonDataNodes = Sets.difference(allUnhealthyNodes, unhealthyDataNodes);
                String roles = getRolesOnNodes(unhealthyNonDataNodes, clusterState).stream()
                    .map(DiscoveryNodeRole::roleName)
                    .distinct()
                    .sorted()
                    .collect(Collectors.joining(", "));
                symptom += String.format(
                    Locale.ROOT,
                    " Furthermore %d node%s with roles: [%s] %s out of disk or running low on disk space.",
                    unhealthyNonDataNodes.size(),
                    unhealthyNonDataNodes.size() == 1 ? "" : "s",
                    roles,
                    unhealthyNonDataNodes.size() == 1 ? "is" : "are"
                );
            }
        } else {
            String roles = getRolesOnNodes(allUnhealthyNodes, clusterState).stream()
                .map(DiscoveryNodeRole::roleName)
                .distinct()
                .sorted()
                .collect(Collectors.joining(", "));
            symptom = String.format(
                Locale.ROOT,
                "%d node%s with roles: [%s] %s out of disk or running low on disk space.",
                allUnhealthyNodes.size(),
                allUnhealthyNodes.size() == 1 ? "" : "s",
                roles,
                allUnhealthyNodes.size() == 1 ? "is" : "are"
            );
        }
        return symptom;
    }

    private List<HealthIndicatorImpact> getImpacts(
        Set<String> indicesWithBlock,
        Set<String> indicesOnUnhealthyNodes,
        Set<String> nodesReportingRed,
        Set<String> nodesReportingYellow,
        ClusterState clusterState
    ) {
        List<HealthIndicatorImpact> impacts = new ArrayList<>();
        if (indicesWithBlock.isEmpty() == false) {
            impacts.add(
                new HealthIndicatorImpact(
                    NAME,
                    IMPACT_INGEST_UNAVAILABLE_ID,
                    1,
                    String.format(
                        Locale.ROOT,
                        "Cannot insert or update documents in the affected indices [%s].",
                        getTruncatedIndices(indicesWithBlock, clusterState.getMetadata())
                    ),
                    List.of(ImpactArea.INGEST)
                )
            );
        } else if (indicesOnUnhealthyNodes.isEmpty() == false) {
            impacts.add(
                new HealthIndicatorImpact(
                    NAME,
                    IMPACT_INGEST_AT_RISK_ID,
                    1,
                    String.format(
                        Locale.ROOT,
                        "The cluster is at risk of not being able to insert or update documents in the affected indices [%s].",
                        getTruncatedIndices(indicesOnUnhealthyNodes, clusterState.metadata())
                    ),
                    List.of(ImpactArea.INGEST)
                )
            );
        }
        Set<String> unhealthyNodes = Stream.concat(nodesReportingRed.stream(), nodesReportingYellow.stream()).collect(Collectors.toSet());
        Set<DiscoveryNodeRole> unhealthyRoles = getRolesOnNodes(unhealthyNodes, clusterState);
        if (unhealthyRoles.contains(DiscoveryNodeRole.MASTER_ROLE)) {
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
        String unhealthyNonMasterNonDataRoles = unhealthyRoles.stream()
            .filter(role -> role.canContainData() == false && role.equals(DiscoveryNodeRole.MASTER_ROLE) == false)
            .map(DiscoveryNodeRole::roleName)
            .distinct()
            .sorted()
            .collect(Collectors.joining(", "));
        if (unhealthyNonMasterNonDataRoles.isBlank() == false) {
            impacts.add(
                new HealthIndicatorImpact(
                    NAME,
                    IMPACT_CLUSTER_FUNCTIONALITY_UNAVAILABLE_ID,
                    2,
                    String.format(Locale.ROOT, "The [%s] functionality might be impaired.", unhealthyNonMasterNonDataRoles),
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
            .filter(node -> node.canContainData() == false && node.isMasterNode())
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
            .filter(node -> node.canContainData() == false && node.isMasterNode() == false)
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
     * not ordinary important, but could be useful in tracking down problems where nodes have stopped reporting health node information.
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
                    .map(HealthIndicatorDisplayValues::getNodeName)
                    .collect(Collectors.joining(", "));
                logger.debug("The following nodes are in the cluster state but not reporting health data: [{}]", nodesWithMissingData);
            }
        }
    }

    private HealthIndicatorDetails getDetails(boolean explain, Map<String, DiskHealthInfo> diskHealthInfoMap, Set<String> blockedIndices) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        Map<HealthStatus, Integer> healthNodesCount = new HashMap<>();
        for (HealthStatus healthStatus : HealthStatus.values()) {
            healthNodesCount.put(healthStatus, 0);
        }
        for (DiskHealthInfo diskHealthInfo : diskHealthInfoMap.values()) {
            healthNodesCount.computeIfPresent(diskHealthInfo.healthStatus(), (key, oldCount) -> oldCount + 1);
        }
        return ((builder, params) -> {
            builder.startObject();
            builder.field("blocked_indices", blockedIndices.size());
            for (HealthStatus healthStatus : HealthStatus.values()) {
                builder.field(healthStatus.name().toLowerCase(Locale.ROOT) + "_nodes", healthNodesCount.get(healthStatus));
            }
            return builder.endObject();
        });
    }
}
