/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DiskHealthIndicatorService implements HealthIndicatorService {
    public static final String NAME = "disk_health";

    private final ClusterService clusterService;

    private static final String DISK_PROBLEMS_INGEST_IMPACT = "The cluster cannot create, delete, or rebalance indices, and cannot "
        + "insert or update documents.";

    private static final List<HealthIndicatorImpact> DISK_PROBLEMS_IMPACTS = List.of(
        new HealthIndicatorImpact(1, DISK_PROBLEMS_INGEST_IMPACT, List.of(ImpactArea.INGEST))
    );

    public DiskHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain) {
        return calculate(explain, null); // TODO delete this method
    }

    // @Override //TODO: put this back
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
        boolean hasIndexReadOnlyAllowDeleteBlock = indicesWithBlock.isEmpty() == false;
        final HealthStatus healthStatus;
        final String symptom;
        HealthIndicatorDetails details = getDetails(explain, diskHealthInfoMap);
        final List<HealthIndicatorImpact> impacts;
        final List<Diagnosis> diagnosisList;
        if (hasIndexReadOnlyAllowDeleteBlock) {
            healthStatus = HealthStatus.RED;
            symptom = String.format(
                Locale.ROOT,
                "%d %s a read only / allow deletes block",
                indicesWithBlock.size(),
                indicesWithBlock.size() > 1 ? "indices have" : "index has"
            );
            impacts = DISK_PROBLEMS_IMPACTS;
            diagnosisList = getDiskProblemsDiagnosis(explain, Set.of(), indicesWithBlock);
        } else {
            Set<String> nodeIdsInClusterState = clusterService.state()
                .nodes()
                .stream()
                .map(DiscoveryNode::getId)
                .collect(Collectors.toSet());
            Set<String> nodeIdsInHealthInfo = diskHealthInfoMap.keySet();
            if (nodeIdsInHealthInfo.containsAll(nodeIdsInClusterState) == false) {
                /*
                 * There are some nodes that we don't have information about, so return UNKNOWN
                 */
                healthStatus = HealthStatus.UNKNOWN;
                impacts = DISK_PROBLEMS_IMPACTS;
                symptom = "Some nodes are not reporting disk usage data";
                Set<String> problemNodes = nodeIdsInClusterState.stream()
                    .filter(node -> nodeIdsInHealthInfo.contains(node) == false)
                    .collect(Collectors.toSet());
                diagnosisList = getDiskProblemsDiagnosis(explain, problemNodes, getIndicesForNodes(problemNodes));
            } else {
                healthStatus = HealthStatus.merge(diskHealthInfoMap.values().stream().map(DiskHealthInfo::healthStatus));
                if (HealthStatus.GREEN.equals(healthStatus)) {
                    symptom = "Disk usage is within configured thresholds";
                    impacts = List.of();
                    diagnosisList = List.of();
                } else {
                    Set<String> problemNodes;
                    Set<String> problemIndices;
                    if (HealthStatus.RED.equals(healthStatus)) {
                        problemNodes = diskHealthInfoMap.entrySet()
                            .stream()
                            .filter(entry -> HealthStatus.RED.equals(entry.getValue().healthStatus()))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
                        problemIndices = getIndicesForNodes(problemNodes);
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
                    } else {
                        problemNodes = diskHealthInfoMap.entrySet()
                            .stream()
                            .filter(entry -> HealthStatus.YELLOW.equals(entry.getValue().healthStatus()))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
                        problemIndices = getIndicesForNodes(problemNodes);
                        symptom = String.format(
                            Locale.ROOT,
                            "%d node%s increased disk space. As a result %d ind%s at risk of not being able to process any more updates.",
                            problemNodes.size(),
                            problemNodes.size() > 1 ? "s have" : " has",
                            problemIndices.size(),
                            problemIndices.size() > 1 ? "ices are" : "ex is"
                        );
                    }
                    impacts = DISK_PROBLEMS_IMPACTS;
                    diagnosisList = getDiskProblemsDiagnosis(explain, problemNodes, problemIndices);
                }
            }
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

    private HealthIndicatorDetails getDetails(boolean explain, Map<String, DiskHealthInfo> diskHealthInfoMap) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        return (builder, params) -> {
            builder.startObject();
            builder.array("disk_health_by_node", arrayXContentBuilder -> {
                for (Map.Entry<String, DiskHealthInfo> entry : diskHealthInfoMap.entrySet()) {
                    builder.startObject();
                    builder.field("node_id", entry.getKey());
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
