/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.autoscaling.AutoscalingResources;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

public final class MlAutoscalingResourceTracker {

    private static final Logger logger = LogManager.getLogger(MlAutoscalingResourceTracker.class);

    private MlAutoscalingResourceTracker() {}

    public static void getMlAutoscalingResources(
        ClusterState clusterState,
        Client client,
        TimeValue timeout,
        MlMemoryTracker mlMemoryTracker,
        ActionListener<AutoscalingResources> listener
    ) {
        String[] mlNodes = clusterState.nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.ML_ROLE))
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);

        getMlNodeStats(
            mlNodes,
            client,
            timeout,
            ActionListener.wrap(
                osStatsPerNode -> getMemoryAndCpu(clusterState, mlMemoryTracker, osStatsPerNode, listener),
                listener::onFailure
            )
        );

    }

    static void getMlNodeStats(String[] mlNodes, Client client, TimeValue timeout, ActionListener<Map<String, OsStats>> listener) {
        client.admin()
            .cluster()
            .prepareNodesStats(mlNodes)
            .clear()
            .setOs(true)
            .setTimeout(timeout)
            .execute(
                ActionListener.wrap(
                    nodesStatsResponse -> listener.onResponse(
                        nodesStatsResponse.getNodes()
                            .stream()
                            .collect(Collectors.toMap(nodeStats -> nodeStats.getNode().getId(), NodeStats::getOs))
                    ),
                    listener::onFailure
                )
            );
    }

    static void getMemoryAndCpu(
        ClusterState clusterState,
        MlMemoryTracker mlMemoryTracker,
        Map<String, OsStats> osStatsPerNode,
        ActionListener<AutoscalingResources> listener
    ) {
        Set<String> nodesWithRunningJobs = new HashSet<>();
        long memoryBytesSum = osStatsPerNode.values()
            .stream()
            .map(s -> s.getMem().getAdjustedTotal().getBytes())
            .collect(Collectors.summingLong(Long::longValue));

        long modelMemoryBytesSum = 0;
        long extraSingleNodeModelMemoryInBytes = 0;
        int extraSingleNodeProcessors = 0;
        int minNodes = 0;
        int extraProcessors = 0;

        final MlAutoscalingContext autoscalingContext = new MlAutoscalingContext(clusterState);

        for (var task : autoscalingContext.anomalyDetectionTasks) {
            String jobId = ((OpenJobAction.JobParams) task.getParams()).getJobId();
            if (AWAITING_LAZY_ASSIGNMENT.equals(task.getAssignment())) {
                extraSingleNodeModelMemoryInBytes = Math.max(
                    extraSingleNodeModelMemoryInBytes,
                    mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(jobId)
                );
                extraSingleNodeProcessors = Math.max(extraSingleNodeProcessors, 1);
                ++extraProcessors;
            } else {
                modelMemoryBytesSum += mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(jobId);
                nodesWithRunningJobs.add(task.getExecutorNode());
                minNodes = 1;
            }
        }
        for (var modelAssignment : autoscalingContext.modelAssignments.entrySet()) {
            if (AssignmentState.STARTING.equals(modelAssignment.getValue().getAssignmentState())
                && modelAssignment.getValue().getNodeRoutingTable().isEmpty()) {
                extraSingleNodeModelMemoryInBytes = Math.max(
                    extraSingleNodeModelMemoryInBytes,
                    modelAssignment.getValue().getTaskParams().estimateMemoryUsageBytes()
                );
                final int unassignedProcessors = modelAssignment.getValue().getTaskParams().getNumberOfAllocations() * modelAssignment
                    .getValue()
                    .getTaskParams()
                    .getThreadsPerAllocation();

                extraSingleNodeProcessors = Math.max(extraSingleNodeProcessors, unassignedProcessors);

                extraProcessors += unassignedProcessors;
            } else {
                modelMemoryBytesSum += modelAssignment.getValue().getTaskParams().estimateMemoryUsageBytes();

                final int assignedProcessors = modelAssignment.getValue().getTaskParams().getNumberOfAllocations() * modelAssignment
                    .getValue()
                    .getTaskParams()
                    .getThreadsPerAllocation();

                // min(3, max(number of allocations over all deployed models), as this is always 3
                minNodes = Math.min(3, Math.max(minNodes, assignedProcessors));

                nodesWithRunningJobs.addAll(modelAssignment.getValue().getNodeRoutingTable().keySet());
            }
        }

        listener.onResponse(
            new AutoscalingResources(
                osStatsPerNode.size(),
                memoryBytesSum,
                modelMemoryBytesSum,
                minNodes,
                extraSingleNodeModelMemoryInBytes,
                extraSingleNodeProcessors,
                extraSingleNodeModelMemoryInBytes,
                extraProcessors,
                0
            )
        );
    }
}
