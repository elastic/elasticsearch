/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
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
                new DiscoveryNode(
                    "_node_name1",
                    "_node_id1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    nodeAttr,
                    Set.of(DiscoveryNodeRole.ML_ROLE),
                    Version.CURRENT
                )
            )
            .add(
                new DiscoveryNode(
                    "_node_name2",
                    "_node_id2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    nodeAttr,
                    Set.of(DiscoveryNodeRole.ML_ROLE),
                    Version.CURRENT
                )
            )
            .add(
                new DiscoveryNode(
                    "_node_name3",
                    "_node_id3",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                    nodeAttr,
                    Set.of(DiscoveryNodeRole.ML_ROLE),
                    Version.CURRENT
                )
            )
            .add(
                new DiscoveryNode(
                    "_node_name4",
                    "_node_id4",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9303),
                    nodeAttr,
                    Set.of(DiscoveryNodeRole.ML_ROLE),
                    Version.CURRENT
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
                                        MODEL_MEMORY_REQUIREMENT,
                                        1,
                                        1,
                                        1024,
                                        ByteSizeValue.ofBytes(MODEL_MEMORY_REQUIREMENT)
                                    )
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
}
