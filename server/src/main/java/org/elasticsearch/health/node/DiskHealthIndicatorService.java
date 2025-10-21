/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.core.FixForMultiProject;
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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.node.DiscoveryNode.DISCOVERY_NODE_COMPARATOR;
import static org.elasticsearch.common.util.CollectionUtils.limitSize;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.are;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.getSortedUniqueValuesString;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.getTruncatedIndices;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.indices;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.indicesComparatorByPriorityAndName;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.regularNoun;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.regularVerb;
import static org.elasticsearch.health.node.HealthIndicatorDisplayValues.these;

/**
 * This indicator reports the clusters' disk health aka if the cluster has enough available space to function.
 * Indicator will report YELLOW status when:
 * - a data node's disk usage is above the high watermark and it's not relocating any of its shards.
 * - a non data node's disk usage is above the high watermark.
 * Indicator will report RED status when:
 * - an index has the INDEX_READ_ONLY_ALLOW_DELETE_BLOCK which indicates that an index has been blocked because a node was out of space.
 * - any node's disk usage is above the flood stage watermark.
 */
public class DiskHealthIndicatorService implements HealthIndicatorService {
    public static final String NAME = "disk";

    private static final Logger logger = LogManager.getLogger(DiskHealthIndicatorService.class);

    // VisibleForTesting
    public static final String IMPACT_INGEST_UNAVAILABLE_ID = "ingest_capability_unavailable";
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
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        ClusterState clusterState = clusterService.state();
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
        logNodesMissingHealthInfo(diskHealthInfoMap, clusterState);

        DiskHealthAnalyzer diskHealthAnalyzer = new DiskHealthAnalyzer(diskHealthInfoMap, clusterState);
        return createIndicator(
            diskHealthAnalyzer.getHealthStatus(),
            diskHealthAnalyzer.getSymptom(),
            diskHealthAnalyzer.getDetails(verbose),
            diskHealthAnalyzer.getImpacts(),
            diskHealthAnalyzer.getDiagnoses(verbose, maxAffectedResourcesCount)
        );
    }

    /**
     * This method logs if any nodes in the cluster state do not have health info results reported. This is logged at debug level and is
     * not ordinary important, but could be useful in tracking down problems where nodes have stopped reporting health node information.
     * @param diskHealthInfoMap A map of nodeId to DiskHealthInfo
     */
    private static void logNodesMissingHealthInfo(Map<String, DiskHealthInfo> diskHealthInfoMap, ClusterState clusterState) {
        if (logger.isDebugEnabled()) {
            String nodesMissingHealthInfo = getSortedUniqueValuesString(
                clusterState.getNodes().getAllNodes(),
                node -> diskHealthInfoMap.containsKey(node.getId()) == false,
                HealthIndicatorDisplayValues::getNodeName
            );
            if (nodesMissingHealthInfo.isBlank() == false) {
                logger.debug("The following nodes are in the cluster state but not reporting health data: [{}]", nodesMissingHealthInfo);
            }
        }
    }

    /**
     * The disk health analyzer takes into consideration the blocked indices and the health status of the all the nodes and calculates
     * the different aspects of the disk indicator such as the overall status, the symptom, the impacts and the diagnoses.
     */
    static class DiskHealthAnalyzer {

        public static final String INDICES_WITH_READONLY_BLOCK = "indices_with_readonly_block";
        public static final String NODES_WITH_ENOUGH_DISK_SPACE = "nodes_with_enough_disk_space";
        public static final String NODES_OVER_FLOOD_STAGE_WATERMARK = "nodes_over_flood_stage_watermark";
        public static final String NODES_OVER_HIGH_WATERMARK = "nodes_over_high_watermark";
        public static final String NODES_WITH_UNKNOWN_DISK_STATUS = "nodes_with_unknown_disk_status";

        private final ClusterState clusterState;
        private final Set<String> blockedIndices;
        private final List<DiscoveryNode> dataNodes = new ArrayList<>();
        // In this context a master node, is a master node that cannot contain data.
        private final Map<HealthStatus, List<DiscoveryNode>> masterNodes = new EnumMap<>(HealthStatus.class);
        // In this context "other" nodes are nodes that cannot contain data and are not masters.
        private final Map<HealthStatus, List<DiscoveryNode>> otherNodes = new EnumMap<>(HealthStatus.class);
        private final Set<DiscoveryNodeRole> affectedRoles = new HashSet<>();
        private final Set<String> indicesAtRisk;
        private final HealthStatus healthStatus;
        private final Map<HealthStatus, Integer> healthStatusNodeCount;

        @FixForMultiProject(description = "blockedIndices and indicesAtRisk should work correctly for indices across projects")
        DiskHealthAnalyzer(Map<String, DiskHealthInfo> diskHealthByNode, ClusterState clusterState) {
            this.clusterState = clusterState;
            blockedIndices = clusterState.metadata()
                .projects()
                .keySet()
                .stream()
                .flatMap(
                    projectId -> clusterState.blocks()
                        .indices(projectId)
                        .entrySet()
                        .stream()
                        .filter(entry -> entry.getValue().contains(IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
                        .map(Map.Entry::getKey)
                )
                .collect(Collectors.toSet());
            HealthStatus mostSevereStatusSoFar = blockedIndices.isEmpty() ? HealthStatus.GREEN : HealthStatus.RED;
            for (String nodeId : diskHealthByNode.keySet()) {
                DiscoveryNode node = clusterState.getNodes().get(nodeId);
                HealthStatus healthStatus = diskHealthByNode.get(nodeId).healthStatus();
                if (node == null || healthStatus.indicatesHealthProblem() == false) {
                    continue;
                }

                if (mostSevereStatusSoFar.value() < healthStatus.value()) {
                    mostSevereStatusSoFar = healthStatus;
                }
                affectedRoles.addAll(node.getRoles());
                if (node.canContainData()) {
                    dataNodes.add(node);
                } else if (node.isMasterNode()) {
                    masterNodes.computeIfAbsent(healthStatus, ignored -> new ArrayList<>()).add(node);
                } else {
                    otherNodes.computeIfAbsent(healthStatus, ignored -> new ArrayList<>()).add(node);
                }
            }
            dataNodes.sort(DISCOVERY_NODE_COMPARATOR);
            for (List<DiscoveryNode> masterNodes : masterNodes.values()) {
                masterNodes.sort(DISCOVERY_NODE_COMPARATOR);
            }
            for (List<DiscoveryNode> nodes : otherNodes.values()) {
                nodes.sort(DISCOVERY_NODE_COMPARATOR);
            }
            indicesAtRisk = getIndicesForNodes(dataNodes, clusterState);
            healthStatus = mostSevereStatusSoFar;
            healthStatusNodeCount = countNodesByHealthStatus(diskHealthByNode, clusterState);
        }

        public HealthStatus getHealthStatus() {
            return healthStatus;
        }

        String getSymptom() {
            if (healthStatus == HealthStatus.GREEN) {
                return "The cluster has enough available disk space.";
            }
            String symptom;
            if (hasBlockedIndices()) {
                symptom = String.format(
                    Locale.ROOT,
                    "%d %s %s not allowed to be updated.",
                    blockedIndices.size(),
                    indices(blockedIndices.size()),
                    are(blockedIndices.size())
                );
                if (hasUnhealthyDataNodes()) {
                    symptom += String.format(
                        Locale.ROOT,
                        " %d %s %s out of disk or running low on disk space.",
                        dataNodes.size(),
                        regularNoun("node", dataNodes.size()),
                        are(dataNodes.size())
                    );
                } else {
                    // In this case the disk issue has been resolved but the index block has not been removed yet or the
                    // cluster is still moving shards away from data nodes that are over the high watermark.
                    symptom += " The cluster is recovering and ingest capabilities should be restored within a few minutes.";
                }
                if (hasUnhealthyMasterNodes() || hasUnhealthyOtherNodes()) {
                    String roles = Stream.concat(masterNodes.values().stream(), otherNodes.values().stream())
                        .flatMap(Collection::stream)
                        .flatMap(node -> node.getRoles().stream())
                        .map(DiscoveryNodeRole::roleName)
                        .distinct()
                        .sorted()
                        .collect(Collectors.joining(", "));

                    int unhealthyNodesCount = getUnhealthyNodeSize(masterNodes) + getUnhealthyNodeSize(otherNodes);
                    symptom += String.format(
                        Locale.ROOT,
                        " %d %s with roles: [%s] %s out of disk or running low on disk space.",
                        unhealthyNodesCount,
                        regularNoun("node", unhealthyNodesCount),
                        roles,
                        are(unhealthyNodesCount)
                    );
                }
            } else {
                String roles = getSortedUniqueValuesString(affectedRoles, DiscoveryNodeRole::roleName);
                int unhealthyNodesCount = dataNodes.size() + getUnhealthyNodeSize(masterNodes) + getUnhealthyNodeSize(otherNodes);
                symptom = String.format(
                    Locale.ROOT,
                    "%d %s with roles: [%s] %s out of disk or running low on disk space.",
                    unhealthyNodesCount,
                    regularNoun("node", unhealthyNodesCount),
                    roles,
                    are(unhealthyNodesCount)
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
                // data nodes don't have space, but no indices had the write block in the cluster
                if (hasUnhealthyDataNodes()) {
                    impacts.add(
                        new HealthIndicatorImpact(
                            NAME,
                            IMPACT_INGEST_AT_RISK_ID,
                            2,
                            String.format(
                                Locale.ROOT,
                                "%d %s %s out of disk or running low on disk space. %s %s cannot be used to store data anymore.",
                                dataNodes.size(),
                                regularNoun("node", dataNodes.size()),
                                are(dataNodes.size()),
                                these(dataNodes.size()),
                                regularNoun("node", dataNodes.size())
                            ),
                            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                        )
                    );
                }
            }
            if (affectedRoles.contains(DiscoveryNodeRole.MASTER_ROLE)) {
                impacts.add(
                    new HealthIndicatorImpact(
                        NAME,
                        IMPACT_CLUSTER_STABILITY_AT_RISK_ID,
                        1,
                        "Cluster stability might be impaired.",
                        List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                    )
                );
            }
            String impactedOtherRoles = getSortedUniqueValuesString(
                affectedRoles,
                role -> role.canContainData() == false && role.equals(DiscoveryNodeRole.MASTER_ROLE) == false,
                DiscoveryNodeRole::roleName
            );
            if (impactedOtherRoles.isBlank() == false) {
                impacts.add(
                    new HealthIndicatorImpact(
                        NAME,
                        IMPACT_CLUSTER_FUNCTIONALITY_UNAVAILABLE_ID,
                        3,
                        String.format(Locale.ROOT, "The [%s] functionality might be impaired.", impactedOtherRoles),
                        List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
                    )
                );
            }
            return impacts;
        }

        private List<Diagnosis> getDiagnoses(boolean verbose, int size) {
            if (verbose == false || healthStatus == HealthStatus.GREEN) {
                return List.of();
            }
            List<Diagnosis> diagnosisList = new ArrayList<>();
            if (hasBlockedIndices() || hasUnhealthyDataNodes()) {
                Set<String> affectedIndices = Sets.union(blockedIndices, indicesAtRisk);
                List<Diagnosis.Resource> affectedResources = new ArrayList<>();
                if (dataNodes.size() > 0) {
                    Diagnosis.Resource nodeResources = new Diagnosis.Resource(limitSize(dataNodes, size));
                    affectedResources.add(nodeResources);
                }
                if (affectedIndices.size() > 0) {
                    Diagnosis.Resource indexResources = new Diagnosis.Resource(
                        Diagnosis.Resource.Type.INDEX,
                        affectedIndices.stream()
                            .sorted(indicesComparatorByPriorityAndName(clusterState.metadata()))
                            .limit(Math.min(affectedIndices.size(), size))
                            .collect(Collectors.toList())
                    );
                    affectedResources.add(indexResources);
                }
                diagnosisList.add(createDataNodeDiagnosis(affectedIndices.size(), affectedResources));
            }
            if (masterNodes.containsKey(HealthStatus.RED)) {
                diagnosisList.add(createNonDataNodeDiagnosis(HealthStatus.RED, masterNodes.get(HealthStatus.RED), size, true));
            }
            if (masterNodes.containsKey(HealthStatus.YELLOW)) {
                diagnosisList.add(createNonDataNodeDiagnosis(HealthStatus.YELLOW, masterNodes.get(HealthStatus.YELLOW), size, true));
            }
            if (otherNodes.containsKey(HealthStatus.RED)) {
                diagnosisList.add(createNonDataNodeDiagnosis(HealthStatus.RED, otherNodes.get(HealthStatus.RED), size, false));
            }
            if (otherNodes.containsKey(HealthStatus.YELLOW)) {
                diagnosisList.add(createNonDataNodeDiagnosis(HealthStatus.YELLOW, otherNodes.get(HealthStatus.YELLOW), size, false));
            }
            return diagnosisList;
        }

        HealthIndicatorDetails getDetails(boolean verbose) {
            if (verbose == false) {
                return HealthIndicatorDetails.EMPTY;
            }
            return ((builder, params) -> {
                builder.startObject();
                builder.field(INDICES_WITH_READONLY_BLOCK, blockedIndices.size());
                for (HealthStatus healthStatus : HealthStatus.values()) {
                    builder.field(getDetailsDisplayKey(healthStatus), healthStatusNodeCount.get(healthStatus));
                }
                return builder.endObject();
            });
        }

        // Visible for testing
        static Map<HealthStatus, Integer> countNodesByHealthStatus(
            Map<String, DiskHealthInfo> diskHealthInfoMap,
            ClusterState clusterState
        ) {
            Map<HealthStatus, Integer> counts = new EnumMap<>(HealthStatus.class);
            for (HealthStatus healthStatus : HealthStatus.values()) {
                counts.put(healthStatus, 0);
            }
            for (DiscoveryNode node : clusterState.getNodes()) {
                HealthStatus healthStatus = diskHealthInfoMap.containsKey(node.getId())
                    ? diskHealthInfoMap.get(node.getId()).healthStatus()
                    : HealthStatus.UNKNOWN;
                counts.computeIfPresent(healthStatus, (ignored, count) -> count + 1);
            }
            return counts;
        }

        private static String getDetailsDisplayKey(HealthStatus status) {
            return switch (status) {
                case GREEN -> NODES_WITH_ENOUGH_DISK_SPACE;
                case UNKNOWN -> NODES_WITH_UNKNOWN_DISK_STATUS;
                case YELLOW -> NODES_OVER_HIGH_WATERMARK;
                case RED -> NODES_OVER_FLOOD_STAGE_WATERMARK;
            };
        }

        private boolean hasUnhealthyDataNodes() {
            return dataNodes.isEmpty() == false;
        }

        private boolean hasUnhealthyMasterNodes() {
            return masterNodes.isEmpty() == false;
        }

        private boolean hasUnhealthyOtherNodes() {
            return otherNodes.isEmpty() == false;
        }

        private boolean hasBlockedIndices() {
            return blockedIndices.isEmpty() == false;
        }

        // Non-private for unit testing
        static Set<String> getIndicesForNodes(List<DiscoveryNode> nodes, ClusterState clusterState) {
            RoutingNodes routingNodes = clusterState.getRoutingNodes();
            return nodes.stream()
                .map(node -> routingNodes.node(node.getId()))
                .filter(Objects::nonNull)
                .flatMap(routingNode -> Arrays.stream(routingNode.copyIndices()))
                .map(Index::getName)
                .collect(Collectors.toSet());
        }

        // Visible for testing
        static Diagnosis createDataNodeDiagnosis(int numberOfAffectedIndices, List<Diagnosis.Resource> affectedResources) {
            String message = numberOfAffectedIndices == 0
                ? "Disk is almost full."
                : String.format(
                    Locale.ROOT,
                    "%d %s %s on nodes that have run or are likely to run out of disk space, "
                        + "this can temporarily disable writing on %s %s.",
                    numberOfAffectedIndices,
                    indices(numberOfAffectedIndices),
                    regularVerb("reside", numberOfAffectedIndices),
                    these(numberOfAffectedIndices),
                    indices(numberOfAffectedIndices)
                );
            return new Diagnosis(
                new Diagnosis.Definition(
                    NAME,
                    "add_disk_capacity_data_nodes",
                    message,
                    "Enable autoscaling (if applicable), add disk capacity or free up disk space to resolve "
                        + "this. If you have already taken action please wait for the rebalancing to complete.",
                    "https://ela.st/fix-data-disk"
                ),
                affectedResources
            );
        }

        // Visible for testing
        static Diagnosis createNonDataNodeDiagnosis(HealthStatus healthStatus, List<DiscoveryNode> nodes, int size, boolean isMaster) {
            return new Diagnosis(
                new Diagnosis.Definition(
                    NAME,
                    isMaster ? "add_disk_capacity_master_nodes" : "add_disk_capacity",
                    healthStatus == HealthStatus.RED ? "Disk is full." : "The cluster is running low on disk space.",
                    "Please add capacity to the current nodes, or replace them with ones with higher capacity.",
                    isMaster ? "https://ela.st/fix-master-disk" : "https://ela.st/fix-disk-space"
                ),
                List.of(new Diagnosis.Resource(limitSize(nodes, size)))
            );
        }

        private static int getUnhealthyNodeSize(Map<HealthStatus, List<DiscoveryNode>> nodes) {
            return (nodes.containsKey(HealthStatus.RED) ? nodes.get(HealthStatus.RED).size() : 0) + (nodes.containsKey(HealthStatus.YELLOW)
                ? nodes.get(HealthStatus.YELLOW).size()
                : 0);
        }
    }
}
