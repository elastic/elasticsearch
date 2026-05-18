/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.autoscaling.MlAutoscalingStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentTests;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingResourceTracker.MlDummyAutoscalingEntity;
import static org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingResourceTracker.MlJobRequirements;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlAutoscalingResourceTrackerTests extends ESTestCase {

    public void testGetMemoryAndProcessors() throws InterruptedException {
        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext();
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        long memory = randomLongBetween(100, 1_000_000);
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-1", memory, "ml-2", memory),
                memory / 2,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(2, stats.currentTotalNodes());
                assertEquals(0, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );

        // simulate 1 small, 1 bigger node
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-1", randomLongBetween(0, memory), "ml-2", randomLongBetween(0, memory)),
                memory / 2,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(0, stats.currentPerNodeMemoryBytes());
                assertEquals(2, stats.currentTotalNodes());
                assertEquals(0, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );

        // Simulate 1 node & 1 "dummy" task requiring 1 processor and the same memory as the other node
        // We don't expect any extra memory or processor usage in this situation.
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-1", randomLongBetween(0, memory), "ml-2", randomLongBetween(0, memory)),
                memory / 2,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(memory / 2, 1),
                1,
                listener
            ),
            stats -> {
                assertEquals(0, stats.currentPerNodeMemoryBytes());
                assertEquals(2, stats.currentTotalNodes());
                assertEquals(0, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.wantedExtraModelMemoryBytes());
                assertEquals(0, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    public void testScaleUpByProcessorsWhenAlreadyStarted() throws InterruptedException, IOException {
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        long memory = randomLongBetween(100, 1_000_000);
        var taskParams1 = new StartTrainedModelDeploymentAction.TaskParams(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            memory,
            2,
            1,
            randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomLongBetween(0, memory)),
            Priority.NORMAL,
            memory,
            memory
        );

        var taskParams2 = new StartTrainedModelDeploymentAction.TaskParams(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            memory,
            randomIntBetween(3, 80),
            1,
            randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomLongBetween(0, memory)),
            Priority.NORMAL,
            memory,
            memory
        );

        var randomAssignment1 = TrainedModelAssignmentTests.randomInstanceBuilder(taskParams1, AssignmentState.STARTED)
            .clearNodeRoutingTable()
            .addRoutingEntry("ml-1", new RoutingInfo(2, 2, RoutingState.STARTED, ""))
            .build();

        var randomAssignment2 = TrainedModelAssignmentTests.randomInstanceBuilder(taskParams2, AssignmentState.STARTED)
            .clearNodeRoutingTable()
            .addRoutingEntry("ml-2", new RoutingInfo(2, 2, RoutingState.STARTED, ""))
            .build();

        List<DiscoveryNode> nodes = new java.util.ArrayList<>(
            Stream.of(randomAssignment1.getNodeRoutingTable().values())
                .map(r -> mock(DiscoveryNode.class))
                .peek(n -> when(n.getRoles()).thenReturn(Set.of(DiscoveryNodeRole.ML_ROLE)))
                .peek(n -> when(n.getAttributes()).thenReturn(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "2.0")))
                .toList()
        );
        nodes.addAll(
            Stream.of(randomAssignment2.getNodeRoutingTable().values())
                .map(r -> mock(DiscoveryNode.class))
                .peek(n -> when(n.getRoles()).thenReturn(Set.of(DiscoveryNodeRole.ML_ROLE)))
                .peek(n -> when(n.getAttributes()).thenReturn(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "2.0")))
                .toList()
        );
        MlAutoscalingContext scaleUpContext = new MlAutoscalingContext(
            List.of(),
            List.of(),
            List.of(),
            Map.of("deployment-1", randomAssignment1, "deployment-2", randomAssignment2),
            nodes,
            null
        );

        int expectedProcessorsPerNode = scaleUpContext.modelAssignments.values()
            .stream()
            .map(TrainedModelAssignment::getTaskParams)
            .mapToInt(StartTrainedModelDeploymentAction.TaskParams::getThreadsPerAllocation)
            .max()
            .orElse(0);
        int expectedTotalProccessors = scaleUpContext.modelAssignments.values()
            .stream()
            .map(TrainedModelAssignment::getTaskParams)
            .mapToInt(tp -> tp.getNumberOfAllocations() * tp.getThreadsPerAllocation())
            .sum();
        int existantProccessors = scaleUpContext.modelAssignments.values()
            .stream()
            .map(TrainedModelAssignment::getNodeRoutingTable)
            .mapToInt(m -> m.values().stream().mapToInt(RoutingInfo::getTargetAllocations).sum()) // threads == 1
            .sum();
        int extraProcessors = expectedTotalProccessors - existantProccessors;

        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                scaleUpContext,
                mockTracker,
                Map.of("ml-1", memory, "ml-2", memory),
                memory,
                2,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(2, stats.currentTotalNodes());
                assertEquals(extraProcessors, stats.wantedExtraProcessors());
                assertEquals(expectedProcessorsPerNode, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.wantedExtraModelMemoryBytes());
                assertEquals(0, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    public void testScaleUpByProcessorsWhenStarting() throws InterruptedException {
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);
        long memory = randomLongBetween(100, 1_000_000);
        long model_size = randomLongBetween(10, 10_000_000);
        var taskParams = new StartTrainedModelDeploymentAction.TaskParams(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            model_size,
            randomIntBetween(5, 10),
            1,
            randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomNonNegativeLong()),
            Priority.NORMAL,
            model_size,
            model_size
        );

        var randomAssignment = TrainedModelAssignmentTests.randomInstanceBuilder(taskParams, AssignmentState.STARTING)
            .setAssignmentState(AssignmentState.STARTING)
            .setNumberOfAllocations(0)
            .clearNodeRoutingTable()
            .build();

        List<DiscoveryNode> nodes = Stream.of("ml-1", "ml-2")
            .map(n -> mock(DiscoveryNode.class))
            .peek(n -> when(n.getRoles()).thenReturn(Set.of(DiscoveryNodeRole.ML_ROLE)))
            .peek(n -> when(n.getAttributes()).thenReturn(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "2.0")))
            .toList();

        MlAutoscalingContext scaleUpContext = new MlAutoscalingContext(List.of(), List.of(), List.of(), Map.of(), nodes, null);

        int expectedTotalProccessors = scaleUpContext.modelAssignments.values()
            .stream()
            .map(TrainedModelAssignment::getTaskParams)
            .mapToInt(tp -> tp.getNumberOfAllocations() * tp.getThreadsPerAllocation())
            .sum();
        int existantProccessors = scaleUpContext.modelAssignments.values()
            .stream()
            .map(TrainedModelAssignment::getNodeRoutingTable)
            .mapToInt(m -> m.values().stream().mapToInt(RoutingInfo::getTargetAllocations).sum())
            .sum();
        int extraProcessors = expectedTotalProccessors - existantProccessors;

        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                scaleUpContext,
                mockTracker,
                Map.of("ml-1", memory, "ml-2", memory),
                memory,
                2,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(2, stats.currentTotalNodes());
                assertEquals(Math.max(extraProcessors, 0), stats.wantedExtraProcessors());
                assertEquals(extraProcessors > 0 ? 1 : 0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    public void testGetMemoryAndProcessorsScaleUpGivenAwaitingLazyAssignment() throws InterruptedException {
        long memory = 1000000000;
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(memory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000",
            MachineLearning.ML_CONFIG_VERSION_NODE_ATTR,
            "7.2.0"
        );
        String jobId = "lazy-job";
        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext(
            List.of(
                new PersistentTasksCustomMetadata.PersistentTask<>(
                    MlTasks.jobTaskId(jobId),
                    MlTasks.JOB_TASK_NAME,
                    new OpenJobAction.JobParams(jobId),
                    1,
                    AWAITING_LAZY_ASSIGNMENT
                )
            ),
            List.of(),
            List.of(),
            Map.of(),
            List.of(
                DiscoveryNodeUtils.builder("ml-1")
                    .name("ml-1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build(),
                DiscoveryNodeUtils.builder("ml-2")
                    .name("ml-2")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build()
            ),
            PersistentTasksCustomMetadata.builder().build()
        );
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);
        when(mockTracker.getAnomalyDetectorJobMemoryRequirement(jobId)).thenReturn(memory / 4);
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-1", memory, "ml-2", memory),
                memory / 2,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(2, stats.currentTotalNodes());
                assertEquals(1, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraProcessors());
                assertEquals(0, stats.currentTotalModelMemoryBytes());
                assertEquals(0, stats.currentTotalProcessorsInUse());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(memory / 4, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(memory / 4, stats.wantedExtraModelMemoryBytes());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );

        // As above but allocate an equal amount of memory to a dummy task
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-1", memory, "ml-2", memory),
                memory / 2,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(memory / 4, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(2, stats.currentTotalNodes());
                assertEquals(1, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraProcessors());
                assertEquals(memory / 4, stats.currentTotalModelMemoryBytes());
                assertEquals(0, stats.currentTotalProcessorsInUse());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(memory / 4, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(memory / 4, stats.wantedExtraModelMemoryBytes());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );

        // As above but also allocate a processor to the dummy task
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-1", memory, "ml-2", memory),
                memory / 2,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(memory / 4, 1),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(2, stats.currentTotalNodes());
                assertEquals(1, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraProcessors());
                assertEquals(memory / 4, stats.currentTotalModelMemoryBytes());
                assertEquals(1, stats.currentTotalProcessorsInUse());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(memory / 4, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(memory / 4, stats.wantedExtraModelMemoryBytes());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    public void testGetMemoryAndProcessorsScaleUpGivenAwaitingLazyAssignmentButFailed() throws InterruptedException {
        long memory = 1000000000;
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(memory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000",
            MachineLearning.ML_CONFIG_VERSION_NODE_ATTR,
            "7.2.0"
        );
        String jobId = "lazy-job";
        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext(
            List.of(
                new PersistentTasksCustomMetadata.PersistentTask<>(
                    new PersistentTasksCustomMetadata.PersistentTask<>(
                        MlTasks.jobTaskId(jobId),
                        MlTasks.JOB_TASK_NAME,
                        new OpenJobAction.JobParams(jobId),
                        1,
                        AWAITING_LAZY_ASSIGNMENT
                    ),
                    new JobTaskState(JobState.FAILED, 1, "a nasty bug", Instant.now())
                )
            ),
            List.of(),
            List.of(),
            Map.of(),
            List.of(
                DiscoveryNodeUtils.builder("ml-1")
                    .name("ml-1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build(),
                DiscoveryNodeUtils.builder("ml-2")
                    .name("ml-2")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build()
            ),
            PersistentTasksCustomMetadata.builder().build()
        );
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);
        when(mockTracker.getAnomalyDetectorJobMemoryRequirement(jobId)).thenReturn(memory / 4);
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-1", memory, "ml-2", memory),
                memory / 2,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(memory, stats.unwantedNodeMemoryBytesToRemove());
                assertEquals(2, stats.currentTotalNodes());
                assertEquals(0, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(0, stats.wantedExtraModelMemoryBytes());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    public void testCheckIfJobsCanBeMovedInLeastEfficientWayMemoryOnly() {
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(10L, 0)),
                Map.of("node_a", MlJobRequirements.of(100L, 0)),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(10L, 0)),
                Map.of("node_a", MlJobRequirements.of(995L, 0)),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // equal sizes fit on all nodes
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(976L, 0),
                    "node_b",
                    MlJobRequirements.of(986L, 0),
                    "node_c",
                    MlJobRequirements.of(967L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // run into max open job limit
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(976L, 0, 3),
                    "node_b",
                    MlJobRequirements.of(986L, 0, 3),
                    "node_c",
                    MlJobRequirements.of(967L, 0, 2)
                ),
                1000L,
                10,
                4
            )
        );

        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(980L, 0),
                    "node_b",
                    MlJobRequirements.of(990L, 0),
                    "node_c",
                    MlJobRequirements.of(970L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // doesn't fit
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(976L, 0),
                    "node_b",
                    MlJobRequirements.of(986L, 0),
                    "node_c",
                    MlJobRequirements.of(967L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
        assertEquals(
            40L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(40L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(976L, 0),
                    "node_b",
                    MlJobRequirements.of(946L, 0),
                    "node_c",
                    MlJobRequirements.of(967L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            130L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(20L, 0),
                    MlJobRequirements.of(30L, 0),
                    MlJobRequirements.of(40L, 0),
                    MlJobRequirements.of(50L, 0),
                    MlJobRequirements.of(60L, 0),
                    MlJobRequirements.of(70L, 0)
                ), // 280, with better packing this could return 20 + 50
                Map.of(
                    "node_a",
                    MlJobRequirements.of(886L, 0),
                    "node_b",
                    MlJobRequirements.of(926L, 0),
                    "node_c",
                    MlJobRequirements.of(967L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            70L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(20L, 0),
                    MlJobRequirements.of(30L, 0),
                    MlJobRequirements.of(40L, 0),
                    MlJobRequirements.of(50L, 0),
                    MlJobRequirements.of(60L, 0),
                    MlJobRequirements.of(70L, 0)
                ), // 280, solvable with optimal packing
                Map.of(
                    "node_a",
                    MlJobRequirements.of(886L, 0),
                    "node_b",
                    MlJobRequirements.of(906L, 0),
                    "node_c",
                    MlJobRequirements.of(917L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            70L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 0),
                    MlJobRequirements.of(20L, 0),
                    MlJobRequirements.of(30L, 0),
                    MlJobRequirements.of(40L, 0),
                    MlJobRequirements.of(50L, 0),
                    MlJobRequirements.of(60L, 0),
                    MlJobRequirements.of(70L, 0)
                ), // 280, solvable with optimal packing
                Map.of(
                    "node_a",
                    MlJobRequirements.of(866L, 0),
                    "node_b",
                    MlJobRequirements.of(886L, 0),
                    "node_c",
                    MlJobRequirements.of(917L, 0)
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            500L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(500L, 0), MlJobRequirements.of(200L, 0)),
                Map.of("node_a", MlJobRequirements.of(1400L, 0), "node_b", MlJobRequirements.of(1700L, 0)),
                2000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            700L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(500L, 0), MlJobRequirements.of(200L, 0)),
                Collections.emptyMap(),
                2000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                Collections.emptyList(),
                Collections.emptyMap(),
                2000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                Collections.emptyList(),
                Map.of("node_a", MlJobRequirements.of(1400L, 0), "node_b", MlJobRequirements.of(1700L, 0)),
                2000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
    }

    public void testCheckIfJobsCanBeMovedInLeastEfficientWayProcessorsAndMemory() {
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(10L, 2)),
                Map.of("node_a", MlJobRequirements.of(100L, 2)),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // fits memory-wise, but not processors
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(10L, 2)),
                Map.of("node_a", MlJobRequirements.of(100L, 2)),
                1000L,
                3,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
        // fits processors, but not memory
        assertEquals(
            10L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(MlJobRequirements.of(10L, 1)),
                Map.of("node_a", MlJobRequirements.of(995L, 2)),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // fit, but requires some shuffling
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(980L, 3),
                    "node_b",
                    MlJobRequirements.of(980L, 1),
                    "node_c",
                    MlJobRequirements.of(970L, 0)
                ),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // special processor placement
        assertEquals(
            0L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 4),
                    MlJobRequirements.of(10L, 3),
                    MlJobRequirements.of(10L, 2),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(900L, 3),
                    "node_b",
                    MlJobRequirements.of(920L, 1),
                    "node_c",
                    MlJobRequirements.of(940L, 0),
                    "node_d",
                    MlJobRequirements.of(960L, 0)
                ),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // special processor placement, but doesn't fit due to open job limit
        assertEquals(
            30L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 4),
                    MlJobRequirements.of(10L, 3),
                    MlJobRequirements.of(10L, 2),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 0)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(900L, 3),
                    "node_b",
                    MlJobRequirements.of(920L, 1),
                    "node_c",
                    MlJobRequirements.of(940L, 0, 5),
                    "node_d",
                    MlJobRequirements.of(960L, 0, 4)
                ),
                1000L,
                4,
                5
            )
        );

        // plenty of space, but no processor
        assertEquals(
            40L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(980L, 3),
                    "node_b",
                    MlJobRequirements.of(980L, 4),
                    "node_c",
                    MlJobRequirements.of(970L, 4)
                ),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );

        // processor available, but not in combination with memory
        assertEquals(
            30L,
            MlAutoscalingResourceTracker.checkIfJobsCanBeMovedInLeastEfficientWay(
                List.of(
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1),
                    MlJobRequirements.of(10L, 1)
                ),
                Map.of(
                    "node_a",
                    MlJobRequirements.of(980L, 1),
                    "node_b",
                    MlJobRequirements.of(980L, 4),
                    "node_c",
                    MlJobRequirements.of(970L, 4)
                ),
                1000L,
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE
            )
        );
    }

    public void testCheckIfOneNodeCouldBeRemovedMemoryOnly() {
        assertEquals(
            true,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                600L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );

        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(280L, 0), MlJobRequirements.of(300L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                600L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );

        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                600L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );

        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Collections.emptyMap(),
                999L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );

        // solvable case with optimal packing, but not possible if badly packed
        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(280L, 0), MlJobRequirements.of(300L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(500L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );

        // same with smaller jobs, that can be re-arranged
        assertEquals(
            true,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(280L, 0), MlJobRequirements.of(300L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );

        assertEquals(
            true,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(280L, 0), MlJobRequirements.of(300L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(50L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );

        assertEquals(
            true,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 0), MlJobRequirements.of(200L, 0), MlJobRequirements.of(300L, 0)),
                    "node_b",
                    List.of(MlJobRequirements.of(280L, 0), MlJobRequirements.of(325L, 0)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(100L, 0),
                        MlJobRequirements.of(50L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0),
                        MlJobRequirements.of(10L, 0)
                    )
                ),
                1000L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );
    }

    public void testCheckIfOneNodeCouldBeRemovedProcessorAndMemory() {
        // plenty of processors and memory
        assertEquals(
            true,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 1), MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1)),
                    "node_b",
                    List.of(MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1)
                    )
                ),
                600L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );

        // processors limit
        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 1), MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1)),
                    "node_b",
                    List.of(MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1)
                    )
                ),
                600L,
                2,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );

        // job limit
        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(MlJobRequirements.of(100L, 1), MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1)),
                    "node_b",
                    List.of(MlJobRequirements.of(200L, 1), MlJobRequirements.of(300L, 1), MlJobRequirements.of(10L, 1)),
                    "node_c",
                    List.of(
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1)
                    )
                ),
                600L,
                10,
                5,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );

        // 1 node with some jobs that require processors
        assertEquals(
            false,
            MlAutoscalingResourceTracker.checkIfOneNodeCouldBeRemoved(
                Map.of(
                    "node_a",
                    List.of(
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 1),
                        MlJobRequirements.of(10L, 3)
                    )
                ),
                600L,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0)
            )
        );
    }

    public void testGetMemoryAndProcessorsScaleDownToZero() throws InterruptedException {
        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext();
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        long memory = randomLongBetween(100, 1_000_000);
        long perNodeAvailableModelMemoryInBytes = memory / 2;

        // scale to zero
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-1", memory),
                perNodeAvailableModelMemoryInBytes,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(1, stats.currentTotalNodes());
                assertEquals(0, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(memory, stats.unwantedNodeMemoryBytesToRemove());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );

        // Dummy task should not affect results
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-1", memory),
                perNodeAvailableModelMemoryInBytes,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 1),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(1, stats.currentTotalNodes());
                assertEquals(0, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.wantedExtraProcessors());
                assertEquals(memory, stats.unwantedNodeMemoryBytesToRemove());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );

        // 3 nodes with no jobs
        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-1", memory, "ml-2", memory, "ml-3", memory),
                perNodeAvailableModelMemoryInBytes,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(3, stats.currentTotalNodes());
                assertEquals(0, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(memory, stats.unwantedNodeMemoryBytesToRemove());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    // scenario: 3 ml nodes, but only 2 have assigned models
    public void testGetMemoryAndProcessorsScaleDown() throws InterruptedException {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000",
            MachineLearning.ML_CONFIG_VERSION_NODE_ATTR,
            "7.2.0",
            MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
            "2.0"
        );

        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext(
            List.of(),
            List.of(),
            List.of(),
            Map.of(
                "model-1",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-1",
                        "model-1-deployment",
                        400,
                        1,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                ).addRoutingEntry("ml-node-1", new RoutingInfo(1, 1, RoutingState.STARTED, "")).build(),
                "model-2",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-2",
                        "model-2-deployment",
                        400,
                        1,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                ).addRoutingEntry("ml-node-3", new RoutingInfo(1, 1, RoutingState.STARTED, "")).build()
            ),
            List.of(
                DiscoveryNodeUtils.builder("ml-node-1")
                    .name("ml-node-name-1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build(),
                DiscoveryNodeUtils.builder("ml-node-3")
                    .name("ml-node-name-3")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build()
            ),
            PersistentTasksCustomMetadata.builder().build()
        );
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        long memory = 1000000000;
        long perNodeAvailableModelMemoryInBytes = 600000000;

        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-node-1", memory, "ml-node-2", memory, "ml-node-3", memory),
                perNodeAvailableModelMemoryInBytes,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(3, stats.currentTotalNodes());
                assertEquals(1, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(memory, stats.unwantedNodeMemoryBytesToRemove());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    // scenario: 3 ml nodes, could scale down purely considering memory but high availability needs prevent it
    public void testGetMemoryAndProcessorsScaleDownPreventedByMinNodes() throws InterruptedException {
        long memory = 8589934592L;
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(memory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "3435134976",
            MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
            "4.0",
            MachineLearning.ML_CONFIG_VERSION_NODE_ATTR,
            "11.0.0"
        );
        Settings settings = Settings.builder().put(MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT.getKey(), true).build();
        DiscoveryNode firstNode = DiscoveryNodeUtils.builder("ml-node-1")
            .name("ml-node-name-1")
            .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
            .attributes(nodeAttr)
            .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
            .build();
        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext(
            List.of(),
            List.of(),
            List.of(),
            Map.of(
                ".elser_model_2_linux-x86_64",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        ".elser_model_2_linux-x86_64",
                        ".elser_model_2_linux-x86_64",
                        274756282,
                        4,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                )
                    .addRoutingEntry("ml-node-1", new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                    .addRoutingEntry("ml-node-2", new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                    .build(),
                "intfloat__multilingual-e5-base",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "intfloat__multilingual-e5-base",
                        "intfloat__multilingual-e5-base",
                        1109885608,
                        1,
                        1,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                ).addRoutingEntry("ml-node-3", new RoutingInfo(1, 1, RoutingState.STARTED, "")).build()
            ),
            List.of(
                firstNode,
                DiscoveryNodeUtils.builder("ml-node-2")
                    .name("ml-node-name-2")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build(),
                DiscoveryNodeUtils.builder("ml-node-3")
                    .name("ml-node-name-3")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build()
            ),
            PersistentTasksCustomMetadata.builder().build()
        );
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-node-1", memory, "ml-node-2", memory, "ml-node-3", memory),
                NativeMemoryCalculator.allowedBytesForMl(firstNode, settings).getAsLong(),
                4,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(3, stats.currentTotalNodes());
                assertEquals(3, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.unwantedNodeMemoryBytesToRemove());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    // scenario: 3 ml nodes, but only 2 have assigned models. This situation would normally result in a scale down but that is prevented
    // by a "dummy" entity having sufficient memory to do so.
    public void testGetMemoryAndProcessorsScaleDownPreventedByDummyEntityMemory() throws InterruptedException {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000",
            MachineLearning.ML_CONFIG_VERSION_NODE_ATTR,
            "7.2.0",
            MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
            "2.0"
        );

        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext(
            List.of(),
            List.of(),
            List.of(),
            Map.of(
                "model-1",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-1",
                        "model-1-deployment",
                        400,
                        1,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                ).addRoutingEntry("ml-node-1", new RoutingInfo(1, 1, RoutingState.STARTED, "")).build(),
                "model-2",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-2",
                        "model-2-deployment",
                        400,
                        1,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                ).addRoutingEntry("ml-node-3", new RoutingInfo(1, 1, RoutingState.STARTED, "")).build()
            ),
            List.of(
                DiscoveryNodeUtils.builder("ml-node-1")
                    .name("ml-node-name-1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build(),
                DiscoveryNodeUtils.builder("ml-node-3")
                    .name("ml-node-name-3")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build()
            ),
            PersistentTasksCustomMetadata.builder().build()
        );
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        long memory = 1000000000;
        long perNodeAvailableModelMemoryInBytes = 600000000;

        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-node-1", memory, "ml-node-2", memory, "ml-node-3", memory),
                perNodeAvailableModelMemoryInBytes,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(perNodeAvailableModelMemoryInBytes, 1),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(perNodeAvailableModelMemoryInBytes + 503318080, stats.currentTotalModelMemoryBytes()); // total model memory
                                                                                                                    // is that
                // configured in the dummy
                // entity plus that used by the
                // trained models.
                assertEquals(5, stats.currentTotalProcessorsInUse()); // account for the extra processor from the dummy entity
                assertEquals(3, stats.currentTotalNodes());
                assertEquals(1, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.wantedExtraProcessors());
                assertEquals(0, stats.wantedExtraModelMemoryBytes());
                assertEquals(0, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    // scenario: 3 ml nodes, but only 2 have assigned models. This situation does result in a scale down since dummy
    // processors alone are not sufficient to prevent it.
    public void testGetMemoryAndProcessorsScaleDownNotPreventedByDummyEntityProcessors() throws InterruptedException {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000",
            MachineLearning.ML_CONFIG_VERSION_NODE_ATTR,
            "7.2.0",
            MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
            "2.0"
        );

        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext(
            List.of(),
            List.of(),
            List.of(),
            Map.of(
                "model-1",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-1",
                        "model-1-deployment",
                        400,
                        1,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                ).addRoutingEntry("ml-node-1", new RoutingInfo(1, 1, RoutingState.STARTED, "")).build(),
                "model-2",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-2",
                        "model-2-deployment",
                        400,
                        1,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                ).addRoutingEntry("ml-node-3", new RoutingInfo(1, 1, RoutingState.STARTED, "")).build()
            ),
            List.of(
                DiscoveryNodeUtils.builder("ml-node-1")
                    .name("ml-node-name-1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build(),
                DiscoveryNodeUtils.builder("ml-node-3")
                    .name("ml-node-name-3")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build()
            ),
            PersistentTasksCustomMetadata.builder().build()
        );
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        long memory = 1000000000;
        long perNodeAvailableModelMemoryInBytes = 600000000;

        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-node-1", memory, "ml-node-2", memory, "ml-node-3", memory),
                perNodeAvailableModelMemoryInBytes,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0L, 9),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(503318080, stats.currentTotalModelMemoryBytes());
                assertEquals(13, stats.currentTotalProcessorsInUse()); // account for the extra processors from the dummy entity
                assertEquals(3, stats.currentTotalNodes());
                assertEquals(1, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.wantedExtraProcessors());
                assertEquals(0, stats.wantedExtraModelMemoryBytes());
                assertEquals(0, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    public void testGetMemoryAndProcessorsScaleDownNotPreventedByDummyEntityAsMemoryTooLow() throws InterruptedException {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            "1000000000",
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000",
            MachineLearning.ML_CONFIG_VERSION_NODE_ATTR,
            "7.2.0",
            MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
            "2.0"
        );

        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext(
            List.of(),
            List.of(),
            List.of(),
            Map.of(
                "model-1",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-1",
                        "model-1-deployment",
                        400,
                        1,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                ).addRoutingEntry("ml-node-1", new RoutingInfo(1, 1, RoutingState.STARTED, "")).build(),
                "model-2",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-2",
                        "model-2-deployment",
                        400,
                        1,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                ).addRoutingEntry("ml-node-3", new RoutingInfo(1, 1, RoutingState.STARTED, "")).build()
            ),
            List.of(
                DiscoveryNodeUtils.builder("ml-node-1")
                    .name("ml-node-name-1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build(),
                DiscoveryNodeUtils.builder("ml-node-3")
                    .name("ml-node-name-3")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build()
            ),
            PersistentTasksCustomMetadata.builder().build()
        );
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        long memory = 1000000000;
        long perNodeAvailableModelMemoryInBytes = 600000000;

        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-node-1", memory, "ml-node-2", memory, "ml-node-3", memory),
                perNodeAvailableModelMemoryInBytes,
                10,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(1024, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(503318080, stats.currentTotalModelMemoryBytes());
                assertEquals(4, stats.currentTotalProcessorsInUse());
                assertEquals(3, stats.currentTotalNodes());
                assertEquals(1, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.wantedExtraProcessors());
                assertEquals(0, stats.wantedExtraModelMemoryBytes());
                assertEquals(0, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    public void testGetMemoryAndProcessorsScaleDownForModelWithZeroAllocations() throws InterruptedException {
        long memory = 1000000000;
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(memory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000",
            MachineLearning.ML_CONFIG_VERSION_NODE_ATTR,
            "7.2.0",
            MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
            "2.0"
        );

        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext(
            List.of(),
            List.of(),
            List.of(),
            Map.of(
                "model-with-zero-allocations",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-with-zero-allocations",
                        "model-with-zero-allocations-deployment",
                        400,
                        0,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    new AdaptiveAllocationsSettings(true, 0, 4)
                ).build()
            ),
            List.of(
                DiscoveryNodeUtils.builder("ml-node-1")
                    .name("ml-node-name-1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build()
            ),
            PersistentTasksCustomMetadata.builder().build()
        );
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-node-1", memory),
                600000000,
                2,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(0, stats.currentTotalModelMemoryBytes());
                assertEquals(0, stats.currentTotalProcessorsInUse());
                assertEquals(1, stats.currentTotalNodes());
                assertEquals(0, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.wantedExtraProcessors());
                assertEquals(0, stats.wantedExtraModelMemoryBytes());
                assertEquals(0, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(memory, stats.unwantedNodeMemoryBytesToRemove());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    public void testGetMemoryAndProcessorsIgnoreThreadsOfModelWithZeroAllocations() throws InterruptedException {
        long memory = 1000000000;
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(memory),
            MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
            "400000000",
            MachineLearning.ML_CONFIG_VERSION_NODE_ATTR,
            "7.2.0",
            MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
            "2.0"
        );

        MlAutoscalingContext mlAutoscalingContext = new MlAutoscalingContext(
            List.of(),
            List.of(),
            List.of(),
            Map.of(
                "model-with-one-allocation",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-with-one-allocation",
                        "model-with-one-allocation-deployment",
                        400,
                        1,
                        2,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    null
                ).addRoutingEntry("ml-node-1", new RoutingInfo(1, 1, RoutingState.STARTED, "")).build(),
                "model-with-zero-allocations",
                TrainedModelAssignment.Builder.empty(
                    new StartTrainedModelDeploymentAction.TaskParams(
                        "model-with-zero-allocations",
                        "model-with-zero-allocations-deployment",
                        400,
                        0,
                        4,
                        100,
                        null,
                        Priority.NORMAL,
                        0L,
                        0L
                    ),
                    new AdaptiveAllocationsSettings(true, 0, 4)
                ).build()
            ),
            List.of(
                DiscoveryNodeUtils.builder("ml-node-1")
                    .name("ml-node-name-1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr)
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .build()
            ),
            PersistentTasksCustomMetadata.builder().build()
        );
        MlMemoryTracker mockTracker = mock(MlMemoryTracker.class);

        this.<MlAutoscalingStats>assertAsync(
            listener -> MlAutoscalingResourceTracker.getMemoryAndProcessors(
                mlAutoscalingContext,
                mockTracker,
                Map.of("ml-node-1", memory),
                600000000,
                2,
                MachineLearning.DEFAULT_MAX_OPEN_JOBS_PER_NODE,
                MlDummyAutoscalingEntity.of(0, 0),
                1,
                listener
            ),
            stats -> {
                assertEquals(memory, stats.currentPerNodeMemoryBytes());
                assertEquals(251659040, stats.currentTotalModelMemoryBytes());
                assertEquals(2, stats.currentTotalProcessorsInUse());
                assertEquals(1, stats.currentTotalNodes());
                assertEquals(1, stats.wantedMinNodes());
                assertEquals(0, stats.wantedExtraPerNodeNodeProcessors());
                assertEquals(0, stats.wantedExtraProcessors());
                assertEquals(0, stats.wantedExtraModelMemoryBytes());
                assertEquals(0, stats.wantedExtraPerNodeMemoryBytes());
                assertEquals(0, stats.unwantedNodeMemoryBytesToRemove());
                assertEquals(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), stats.currentPerNodeMemoryOverheadBytes());
            }
        );
    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, Consumer<T> furtherTests) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            furtherTests.accept(r);
        }, e -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            fail("got unexpected exception: " + e);
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
    }
}
