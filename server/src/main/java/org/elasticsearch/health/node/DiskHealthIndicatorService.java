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
import org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    private static final HealthIndicatorImpact RED_HEALTH_AFFECTED_INDICES_INGEST_IMPACT = new HealthIndicatorImpact(
        1,
        "Cannot insert or update documents in the affected indices.",
        List.of(ImpactArea.INGEST)
    );

    private static final HealthIndicatorImpact YELLOW_HEALTH_AFFECTED_INDICES_INGEST_IMPACT = new HealthIndicatorImpact(
        1,
        "May not be able to insert or update documents in the affected indices.",
        List.of(ImpactArea.INGEST)
    );

    private static final HealthIndicatorImpact HEALTH_INGEST_NODES_PIPELINES_IMPACT = new HealthIndicatorImpact(
        2,
        "Cannot run ingest pipelines",
        List.of(ImpactArea.INGEST)
    );

    private static final HealthIndicatorImpact HEALTH_INGEST_NODES_MONITORING_IMPACT = new HealthIndicatorImpact(
        2,
        "Stack monitoring will not run",
        List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
    );

    private static final HealthIndicatorImpact HEALTH_ML_NODES_IMPACT = new HealthIndicatorImpact(
        2,
        "Cannot run machine learning jobs",
        List.of(ImpactArea.ANALYTICS)
    );

    private static final HealthIndicatorImpact HEALTH_REMOTE_CLUSTER_CLIENT_NODES_IMPACT = new HealthIndicatorImpact(
        2,
        "Cannot run cross cluster search",
        List.of(ImpactArea.SEARCH)
    );

    private static final HealthIndicatorImpact HEALTH_TRANSFORM_NODES_IMPACT = new HealthIndicatorImpact(
        2,
        "Cannot run Fleet, Elasticsearch Security, or transforms",
        List.of(ImpactArea.ANALYTICS)
    );

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
                "No disk usage data",
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
        final String symptom;
        HealthIndicatorDetails details = getDetails(explain, diskHealthInfoMap);
        final List<HealthIndicatorImpact> impacts = new ArrayList<>();
        final List<Diagnosis> diagnosisList;
        final HealthStatus healthStatus = hasAtLeastOneIndexReadOnlyAllowDeleteBlock
            ? HealthStatus.RED
            : HealthStatus.merge(diskHealthInfoMap.values().stream().map(DiskHealthInfo::healthStatus));
        if (HealthStatus.GREEN.equals(healthStatus)) {
            symptom = "Disk usage is within configured thresholds";
            diagnosisList = List.of();
        } else {
            final Set<String> problemNodes;
            final Set<String> problemIndices;
            if (HealthStatus.RED.equals(healthStatus)) {
                problemNodes = diskHealthInfoMap.entrySet()
                    .stream()
                    .filter(entry -> HealthStatus.RED.equals(entry.getValue().healthStatus()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
                problemIndices = Stream.concat(getIndicesForNodes(problemNodes).stream(), indicesWithBlock.stream())
                    .collect(Collectors.toSet());
                symptom = String.format(
                    Locale.ROOT,
                    "%d node%s out of disk space.%s",
                    problemNodes.size(),
                    problemNodes.size() > 1 ? "s are" : " is",
                    problemIndices.size() > 0
                        ? String.format(
                            Locale.ROOT,
                            " As a result %d ind%s cannot process any more updates.",
                            problemIndices.size(),
                            problemIndices.size() > 1 ? "ices" : "ex"
                        )
                        : ""
                );
                if (problemIndices.size() > 1) {
                    impacts.add(RED_HEALTH_AFFECTED_INDICES_INGEST_IMPACT);
                }
            } else {
                problemNodes = diskHealthInfoMap.entrySet()
                    .stream()
                    .filter(entry -> HealthStatus.YELLOW.equals(entry.getValue().healthStatus()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
                problemIndices = getIndicesForNodes(problemNodes);
                symptom = String.format(
                    Locale.ROOT,
                    "%d node%s increased disk space.%s",
                    problemNodes.size(),
                    problemNodes.size() > 1 ? "s have" : " has",
                    problemIndices.size() > 0
                        ? String.format(
                            Locale.ROOT,
                            " As a result %d ind%s at risk of not being able to process any more updates.",
                            problemIndices.size(),
                            problemIndices.size() > 1 ? "ices are" : "ex is"
                        )
                        : ""
                );
                if (problemIndices.size() > 1) {
                    impacts.add(YELLOW_HEALTH_AFFECTED_INDICES_INGEST_IMPACT);
                }
            }
            Set<DiscoveryNodeRole> roles = clusterService.state()
                .nodes()
                .getNodes()
                .values()
                .stream()
                .filter(node -> problemNodes.contains(node.getId()))
                .map(DiscoveryNode::getRoles)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
            if (roles.contains(DiscoveryNodeRole.MASTER_ROLE)) {
                impacts.addAll(StableMasterHealthIndicatorService.UNSTABLE_MASTER_IMPACTS);
            }
            if (roles.contains(DiscoveryNodeRole.INGEST_ROLE)) {
                impacts.add(HEALTH_INGEST_NODES_PIPELINES_IMPACT);
                impacts.add(HEALTH_INGEST_NODES_MONITORING_IMPACT);
            }
            if (roles.contains(DiscoveryNodeRole.ML_ROLE)) {
                impacts.add(HEALTH_ML_NODES_IMPACT);
            }
            if (roles.contains(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)) {
                impacts.add(HEALTH_REMOTE_CLUSTER_CLIENT_NODES_IMPACT);
            }
            if (roles.contains(DiscoveryNodeRole.TRANSFORM_ROLE)) {
                impacts.add(HEALTH_TRANSFORM_NODES_IMPACT);
            }
            diagnosisList = getDiskProblemsDiagnosis(explain, problemNodes, problemIndices);
        }
        return createIndicator(healthStatus, symptom, details, impacts, diagnosisList);
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
            Set<String> nodeIdsInClusterState = clusterService.state()
                .nodes()
                .stream()
                .map(DiscoveryNode::getId)
                .collect(Collectors.toSet());
            Set<String> nodeIdsInHealthInfo = diskHealthInfoMap.keySet();
            if (nodeIdsInHealthInfo.containsAll(nodeIdsInClusterState) == false) {
                String problemNodes = nodeIdsInClusterState.stream()
                    .filter(node -> nodeIdsInHealthInfo.contains(node) == false)
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

    private List<Diagnosis> getDiskProblemsDiagnosis(boolean explain, Set<String> nodeIds, Set<String> indices) {
        if (explain == false) {
            return List.of();
        }
        return List.of(
            new Diagnosis(
                new Diagnosis.Definition(
                    "free-disk-space-or-add-capacity",
                    "Disk thresholds have been exceeded",
                    "Free up disk space or add disk capacity",
                    "https://ela.st/free-disk-space-or-add-capacity"
                ),
                Stream.concat(nodeIds.stream(), indices.stream()).toList()
            )
        );
    }
}
