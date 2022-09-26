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
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.index.Index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.getSortedUniqueValuesString;
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
        logNodesMissingHealthInfo(diskHealthInfoMap, clusterState);

        /*
         * If there are any index blocks in the cluster state, that makes the overall status automatically red, regardless of the statuses
         * returned by the nodes. If there is no cluster block, we just use the merged statuses of the nodes.
         */
        Set<String> blockedIndices = clusterState.blocks()
            .indices()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().contains(IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        HealthIndicatorDetails details = getDetails(explain, diskHealthInfoMap, blockedIndices);

        DiskHealthAnalyzer diskHealthAnalyzer = new DiskHealthAnalyzer(diskHealthInfoMap, blockedIndices, clusterState);
        return createIndicator(
            diskHealthAnalyzer.getHealthStatus(),
            diskHealthAnalyzer.getSymptom(),
            details,
            diskHealthAnalyzer.getImpacts(),
            diskHealthAnalyzer.getDiagnoses()
        );
    }

    /**
     * This method logs if any nodes in the cluster state do not have health info results reported. This is logged at debug level and is
     * not ordinary important, but could be useful in tracking down problems where nodes have stopped reporting health node information.
     * @param diskHealthInfoMap A map of nodeId to DiskHealthInfo
     */
    private void logNodesMissingHealthInfo(Map<String, DiskHealthInfo> diskHealthInfoMap, ClusterState clusterState) {
        if (logger.isDebugEnabled()) {
            String nodesMissingHealthInfo = getSortedUniqueValuesString(
                clusterState.getNodes(),
                node -> diskHealthInfoMap.containsKey(node.getId()) == false,
                HealthIndicatorDisplayValues::getNodeName
            );
            if (nodesMissingHealthInfo.isBlank() == false) {
                logger.debug("The following nodes are in the cluster state but not reporting health data: [{}]", nodesMissingHealthInfo);
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

    static class DiskHealthAnalyzer {

        private final ClusterState clusterState;
        private final Set<String> blockedIndices;
        private final Set<DiscoveryNode> dataNodes = new HashSet<>();
        // In this context a dedicated master node, is a master node that cannot contain data.
        private final Set<DiscoveryNode> dedicatedMasterNodes = new HashSet<>();
        // In this context "other" nodes are nodes that cannot contain data and are not masters.
        private final Set<DiscoveryNode> otherNodes = new HashSet<>();
        private final Set<DiscoveryNodeRole> impactedRoles = new HashSet<>();
        private final Set<String> indicesAtRisk;
        private final HealthStatus healthStatus;

        DiskHealthAnalyzer(Map<String, DiskHealthInfo> diskHealthByNode, Set<String> blockedIndices, ClusterState clusterState) {
            this.clusterState = clusterState;
            this.blockedIndices = blockedIndices;
            HealthStatus mostSevereNodeHealthStatus = HealthStatus.GREEN;
            for (String nodeId : diskHealthByNode.keySet()) {
                DiscoveryNode node = clusterState.getNodes().get(nodeId);
                HealthStatus healthStatus = diskHealthByNode.get(nodeId).healthStatus();
                // TODO #90213 update this only after we check that this health status indicates a problem.
                if (mostSevereNodeHealthStatus.value() < healthStatus.value()) {
                    mostSevereNodeHealthStatus = healthStatus;
                }
                if (node == null || healthStatus.indicatesHealthProblem() == false) {
                    continue;
                }
                impactedRoles.addAll(node.getRoles());
                if (node.canContainData()) {
                    dataNodes.add(node);
                } else if (node.isMasterNode()) {
                    dedicatedMasterNodes.add(node);
                } else {
                    otherNodes.add(node);
                }
            }
            indicesAtRisk = getIndicesForNodes(dataNodes, clusterState);
            healthStatus = blockedIndices.isEmpty() == false ? HealthStatus.RED : mostSevereNodeHealthStatus;
        }

        public HealthStatus getHealthStatus() {
            return healthStatus;
        }

        String getSymptom() {
            if (healthStatus == HealthStatus.GREEN) {
                return "The cluster has enough available disk space.";
            }
            String symptom;
            if (blockedIndices.isEmpty() == false) {
                symptom = String.format(
                    Locale.ROOT,
                    "%d %s not allowed to be updated because ",
                    blockedIndices.size(),
                    blockedIndices.size() == 1 ? "index is" : "indices are"
                );
                if (hasUnhealthyDataNodes()) {
                    symptom += String.format(
                        Locale.ROOT,
                        "%d %s out of disk or running low on disk space.",
                        dataNodes.size(),
                        dataNodes.size() == 1 ? "node is" : "nodes are"
                    );
                } else {
                    // In this case the disk issue has been resolved but the index block has not been removed yet or the
                    // cluster is still moving shards away from data nodes that are over the high watermark.
                    symptom +=
                        ("the cluster was running out of disk space. The cluster is recovering and ingest capabilities should be restored "
                            + "within a few minutes.");
                }
                if (hasUnhealthyMasterNodes() || hasUnhealthyOtherNodes()) {
                    String roles = Stream.concat(dedicatedMasterNodes.stream(), otherNodes.stream())
                        .flatMap(node -> node.getRoles().stream())
                        .map(DiscoveryNodeRole::roleName)
                        .distinct()
                        .sorted()
                        .collect(Collectors.joining(", "));

                    int unhealthyNodesCount = dedicatedMasterNodes.size() + otherNodes.size();
                    symptom += String.format(
                        Locale.ROOT,
                        " Furthermore %d node%s with roles: [%s] %s out of disk or running low on disk space.",
                        unhealthyNodesCount,
                        unhealthyNodesCount == 1 ? "" : "s",
                        roles,
                        unhealthyNodesCount == 1 ? "is" : "are"
                    );
                }
            } else {
                String roles = getSortedUniqueValuesString(impactedRoles, DiscoveryNodeRole::roleName);
                int unhealthyNodesCount = dedicatedMasterNodes.size() + otherNodes.size() + dataNodes.size();
                symptom = String.format(
                    Locale.ROOT,
                    "%d node%s with roles: [%s] %s out of disk or running low on disk space.",
                    unhealthyNodesCount,
                    unhealthyNodesCount == 1 ? "" : "s",
                    roles,
                    unhealthyNodesCount == 1 ? "is" : "are"
                );
            }
            return symptom;
        }

        List<HealthIndicatorImpact> getImpacts() {
            if (healthStatus == HealthStatus.GREEN) {
                return List.of();
            }
            List<HealthIndicatorImpact> impacts = new ArrayList<>();
            if (hasBlockedIndices()) {
                impacts.add(
                    new HealthIndicatorImpact(
                        NAME,
                        IMPACT_INGEST_UNAVAILABLE_ID,
                        1,
                        String.format(
                            Locale.ROOT,
                            "Cannot insert or update documents in the affected indices [%s].",
                            getTruncatedIndices(blockedIndices, clusterState.getMetadata())
                        ),
                        List.of(ImpactArea.INGEST)
                    )
                );
            } else {
                if (indicesAtRisk.isEmpty() == false) {
                    impacts.add(
                        new HealthIndicatorImpact(
                            NAME,
                            IMPACT_INGEST_AT_RISK_ID,
                            1,
                            String.format(
                                Locale.ROOT,
                                "The cluster is at risk of not being able to insert or update documents in the affected indices [%s].",
                                getTruncatedIndices(indicesAtRisk, clusterState.metadata())
                            ),
                            List.of(ImpactArea.INGEST)
                        )
                    );
                }
            }
            if (impactedRoles.contains(DiscoveryNodeRole.MASTER_ROLE)) {
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
            String impactedOtherRoles = getSortedUniqueValuesString(
                impactedRoles,
                role -> role.canContainData() == false && role.equals(DiscoveryNodeRole.MASTER_ROLE) == false,
                DiscoveryNodeRole::roleName
            );
            if (impactedOtherRoles.isBlank() == false) {
                impacts.add(
                    new HealthIndicatorImpact(
                        NAME,
                        IMPACT_CLUSTER_FUNCTIONALITY_UNAVAILABLE_ID,
                        2,
                        String.format(Locale.ROOT, "The [%s] functionality might be impaired.", impactedOtherRoles),
                        List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                    )
                );
            }
            return impacts;
        }

        private List<Diagnosis> getDiagnoses() {
            if (healthStatus == HealthStatus.GREEN) {
                return List.of();
            }
            List<Diagnosis> diagnosisList = new ArrayList<>();
            if (hasBlockedIndices() || hasUnhealthyDataNodes()) {
                Set<String> impactedIndices = Sets.union(blockedIndices, indicesAtRisk);
                diagnosisList.add(
                    new Diagnosis(
                        new Diagnosis.Definition(
                            NAME,
                            "add_disk_capacity_data_nodes",
                            String.format(
                                Locale.ROOT,
                                "%d %s reside%s on nodes that have run or are likely to run out of space, this can temporarily "
                                    + "disable writing on this indices.",
                                impactedIndices.size(),
                                impactedIndices.size() == 1 ? "index" : "indices",
                                impactedIndices.size() == 1 ? "s" : ""
                            ),
                            "Enable autoscaling (if applicable), add disk capacity or free up disk space to resolve "
                                + "this. If you have already taken action please wait for the rebalancing to complete.",
                            "https://ela.st/fix-data-disk"
                        ),
                        dataNodes.stream().map(DiscoveryNode::getId).sorted().toList()
                    )
                );
            }
            if (hasUnhealthyMasterNodes()) {
                diagnosisList.add(
                    new Diagnosis(
                        new Diagnosis.Definition(
                            NAME,
                            "add_disk_capacity_master_nodes",
                            "Disk is almost full.",
                            "Please add capacity to the current nodes, or replace them with ones with higher capacity.",
                            "https://ela.st/fix-master-disk"
                        ),
                        dedicatedMasterNodes.stream().map(DiscoveryNode::getId).sorted().toList()
                    )
                );
            }
            if (hasUnhealthyOtherNodes()) {
                diagnosisList.add(
                    new Diagnosis(
                        new Diagnosis.Definition(
                            NAME,
                            "add_disk_capacity",
                            "Disk is almost full.",
                            "Please add capacity to the current nodes, or replace them with ones with higher capacity.",
                            "https://ela.st/fix-disk-space"
                        ),
                        otherNodes.stream().map(DiscoveryNode::getId).sorted().toList()
                    )
                );
            }
            return diagnosisList;
        }

        private boolean hasUnhealthyDataNodes() {
            return dataNodes.isEmpty() == false;
        }

        private boolean hasUnhealthyMasterNodes() {
            return dedicatedMasterNodes.isEmpty() == false;
        }

        private boolean hasUnhealthyOtherNodes() {
            return otherNodes.isEmpty() == false;
        }

        private boolean hasBlockedIndices() {
            return blockedIndices.isEmpty() == false;
        }

        // Non-private for unit testing
        static Set<String> getIndicesForNodes(Set<DiscoveryNode> nodes, ClusterState clusterState) {
            RoutingNodes routingNodes = clusterState.getRoutingNodes();
            return nodes.stream()
                .map(node -> routingNodes.node(node.getId()))
                .filter(Objects::nonNull)
                .flatMap(routingNode -> Arrays.stream(routingNode.copyIndices()))
                .map(Index::getName)
                .collect(Collectors.toSet());
        }
    }
}
