/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeLoadDetectorTests extends ESTestCase {

    // To simplify the logic in this class all jobs have the same memory requirement
    private static final ByteSizeValue JOB_MEMORY_REQUIREMENT = ByteSizeValue.ofMb(10);

    private static final long MODEL_MEMORY_REQUIREMENT = ByteSizeValue.ofMb(50).getBytes();

    private NodeLoadDetector nodeLoadDetector;

    @Before
    public void setup() {
        MlMemoryTracker memoryTracker = mock(MlMemoryTracker.class);
        when(memoryTracker.isRecentlyRefreshed()).thenReturn(true);
        when(memoryTracker.getAnomalyDetectorJobMemoryRequirement(anyString())).thenReturn(JOB_MEMORY_REQUIREMENT.getBytes());
        when(memoryTracker.getDataFrameAnalyticsJobMemoryRequirement(anyString())).thenReturn(JOB_MEMORY_REQUIREMENT.getBytes());
        when(memoryTracker.getJobMemoryRequirement(anyString(), anyString())).thenReturn(JOB_MEMORY_REQUIREMENT.getBytes());
        nodeLoadDetector = new NodeLoadDetector(memoryTracker);
    }

    public void testNodeLoadDetection() {
        // MachineLearning.MACHINE_MEMORY_NODE_ATTR negative, so this won't allocate any jobs that aren't already allocated
        // (in the past it would have fallen back to allocating by count, but we don't do that any more)
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "-1",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "10000000"
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    nodeAttr,
                    Set.of(DiscoveryNodeRole.ML_ROLE)
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    nodeAttr,
                    Set.of(DiscoveryNodeRole.ML_ROLE)
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name3",
                    "_node_id3",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                    nodeAttr,
                    Set.of(DiscoveryNodeRole.ML_ROLE)
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "_node_name4",
                    "_node_id4",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9303),
                    nodeAttr,
                    Set.of(DiscoveryNodeRole.ML_ROLE)
                )
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id1", "_node_id1", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id2", "_node_id1", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id3", "_node_id2", null, tasksBuilder);
        OpenJobPersistentTasksExecutorTests.addJobTask("job_id4", "_node_id4", JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        final ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .nodes(nodes)
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, tasks)
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                "model1",
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        "model1",
                                        "deployment1",
                                        MODEL_MEMORY_REQUIREMENT,
                                        1,
                                        1,
                                        1024,
                                        ByteSizeValue.ofBytes(MODEL_MEMORY_REQUIREMENT),
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    ),
                                    null
                                )
                                    .addRoutingEntry("_node_id4", new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                    .addRoutingEntry("_node_id2", new RoutingInfo(1, 1, RoutingState.FAILED, "test"))
                                    .addRoutingEntry("_node_id1", new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                    .updateExistingRoutingEntry(
                                        "_node_id1",
                                        new RoutingInfo(1, 1, randomFrom(RoutingState.STOPPED, RoutingState.FAILED), "test")
                                    )
                            )
                            .build()
                    )
            )
            .build();

        NodeLoad load = nodeLoadDetector.detectNodeLoad(cs, nodes.get("_node_id1"), 10, 30, false);
        assertThat(load.getAssignedJobMemory(), equalTo(52428800L));
        assertThat(load.getNumAllocatingJobs(), equalTo(2));
        assertThat(load.getNumAssignedJobsAndModels(), equalTo(2));
        assertThat(load.getMaxJobs(), equalTo(10));
        assertThat(load.getMaxMlMemory(), equalTo(0L));

        load = nodeLoadDetector.detectNodeLoad(cs, nodes.get("_node_id2"), 5, 30, false);
        assertThat(load.getAssignedJobMemory(), equalTo(41943040L));
        assertThat(load.getNumAllocatingJobs(), equalTo(1));
        assertThat(load.getNumAssignedJobsAndModels(), equalTo(1));
        assertThat(load.getMaxJobs(), equalTo(5));
        assertThat(load.getMaxMlMemory(), equalTo(0L));

        load = nodeLoadDetector.detectNodeLoad(cs, nodes.get("_node_id3"), 5, 30, false);
        assertThat(load.getAssignedJobMemory(), equalTo(0L));
        assertThat(load.getNumAllocatingJobs(), equalTo(0));
        assertThat(load.getNumAssignedJobsAndModels(), equalTo(0));
        assertThat(load.getMaxJobs(), equalTo(5));
        assertThat(load.getMaxMlMemory(), equalTo(0L));

        load = nodeLoadDetector.detectNodeLoad(cs, nodes.get("_node_id4"), 5, 30, false);
        assertThat(load.getAssignedJobMemory(), equalTo(398458880L));
        assertThat(load.getNumAllocatingJobs(), equalTo(0));
        assertThat(load.getNumAssignedJobsAndModels(), equalTo(2));
        assertThat(load.getMaxJobs(), equalTo(5));
        assertThat(load.getMaxMlMemory(), equalTo(0L));
    }

    /**
     * Regression test for https://github.com/elastic/elasticsearch/issues/160923 (fix #1). When a deployment has an
     * observed per-allocation memory the node load must account for that observed figure (the same value the assignment
     * planner and the autoscaling resource tracker use) rather than the a priori task-parameter estimate. The assertion
     * compares the load with and without the observed value so it is independent of any fixed node overhead: the
     * difference must equal the difference between the observed-aware and a priori memory estimates, and when no observed
     * value is present the load must be unchanged from the a priori estimate.
     */
    public void testTrainedModelAssignmentUsesObservedPerAllocationMemory() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "4294967296",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "1717567488"
        );
        String nodeId = "ml-node-1";
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "ml-node-name-1",
                    nodeId,
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    nodeAttr,
                    Set.of(DiscoveryNodeRole.ML_ROLE)
                )
            )
            .build();

        int allocations = 2;
        long observedPerAllocationMemoryBytes = ByteSizeValue.ofMb(900).getBytes();
        var taskParams = new StartTrainedModelDeploymentAction.TaskParams(
            "model-observed",
            "deployment-observed",
            MODEL_MEMORY_REQUIREMENT,
            allocations,
            1,
            1024,
            ByteSizeValue.ZERO,
            Priority.NORMAL,
            0L,
            0L
        );

        TrainedModelAssignment observedAssignment = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                "deployment-observed",
                TrainedModelAssignment.Builder.empty(taskParams, null)
                    .setObservedPerAllocationMemoryBytes(observedPerAllocationMemoryBytes)
                    .addRoutingEntry(nodeId, new RoutingInfo(allocations, allocations, RoutingState.STARTED, ""))
            )
            .build()
            .getDeploymentAssignment("deployment-observed");
        TrainedModelAssignment aPrioriAssignment = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                "deployment-observed",
                TrainedModelAssignment.Builder.empty(taskParams, null)
                    .addRoutingEntry(nodeId, new RoutingInfo(allocations, allocations, RoutingState.STARTED, ""))
            )
            .build()
            .getDeploymentAssignment("deployment-observed");

        long observedLoad = loadFor(nodes, nodeId, observedAssignment);
        long aPrioriLoad = loadFor(nodes, nodeId, aPrioriAssignment);

        // The observed path reserves the (larger) observed memory. Comparing the two loads cancels any fixed node
        // overhead: the delta must equal the difference between the observed-aware and a priori estimates, which also
        // confirms the a priori path (observed absent) is unchanged by the fix.
        assertThat(observedLoad, greaterThan(aPrioriLoad));
        assertThat(
            observedLoad - aPrioriLoad,
            equalTo(observedAssignment.estimateMemoryUsageBytes(allocations) - taskParams.estimateMemoryUsageBytes())
        );
    }

    private long loadFor(DiscoveryNodes nodes, String nodeId, TrainedModelAssignment assignment) {
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .nodes(nodes)
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(assignment.getDeploymentId(), TrainedModelAssignment.Builder.fromAssignment(assignment))
                            .build()
                    )
            )
            .build();
        return nodeLoadDetector.detectNodeLoad(cs, nodes.get(nodeId), 10, 30, true).getAssignedJobMemory();
    }
}
