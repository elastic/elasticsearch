/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.autoscaling.MlAutoscalingStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.core.ml.MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.MACHINE_MEMORY_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_JVM_SIZE_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class MlAutoscalingResourceTrackerParameterizedTests extends ESTestCase {
    private static final long MODEL_BYTES = ByteSizeValue.ofGb(2).getBytes();
    private static final String NODE_NAME_PREFIX = "ML-Node-";
    private final TestCase testCase;

    @ParametersFactory(shuffle = true)
    public static Iterable<Object[]> parameterizedTestCases() {
        List<TestCase> testCases = List.of(
            WhenStartTrainedModelDeployment_ThenScaleUp_GivenNoExistingDeployments(1),
            WhenStartTrainedModelDeployment_ThenScaleUp_GivenExistingDeployments(2),
            WhenStartTrainedModelDeployment_ThenNoScale_GivenExistingDeployments(3),

            WhenUpdateTrainedModelDeployment_ThenScaleUp_GivenDeploymentGetsLarger(4),
            WhenUpdateTrainedModelDeployment_ThenNoScale_GivenDeploymentGetsLargerAndNodesAreSufficient(5),
            WhenUpdateTrainedModelDeployment_ThenNoScale_GivenDeploymentGetsSmallerButAllNodesAreStillRequired(6),
            // WhenUpdateTrainedModelDeployment_ThenScaleDown_GivenDeploymentGetsSmaller(7) TODO we don't currently support shrinking nodes

            // Some of the below test cases test for states in between the Stop request being made and being completed
            WhenStoppingTrainedModelDeployment_ThenNoScale_GivenAllNodesAreStillRequired(8),
            WhenStopTrainedModelDeployment_ThenNoScale_GivenAllNodesAreStillRequired(9),
            WhenStoppedTrainedModelDeployment_ThenNoScale_GivenAllNodesAreStillRequired(10),
            WhenStopTrainedModelDeployment_ThenScaledown_GivenDeploymentRequiredAWholeNode(11)
        );

        return testCases.stream().map(MlAutoscalingResourceTrackerParameterizedTests.TestCase::toArray).collect(toList());
    }

    private record TestCase(
        String testDescription,
        ClusterState clusterState,
        ClusterSettings clusterSettings,
        MlMemoryTracker mlMemoryTracker,
        Settings settings,
        ActionListener<MlAutoscalingStats> verificationListener
    ) {
        Object[] toArray() {
            return new Object[] { this };
        }
    }

    public MlAutoscalingResourceTrackerParameterizedTests(MlAutoscalingResourceTrackerParameterizedTests.TestCase testCase) {
        this.testCase = testCase;
    }

    static ActionListener<MlAutoscalingStats> createVerificationListener(String message, MlAutoscalingStats expectedStats) {
        return new ActionListener<>() {
            @Override
            public void onResponse(MlAutoscalingStats actualMlAutoscalingStats) {
                assertEquals(message, expectedStats, actualMlAutoscalingStats);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Unexpected failure" + e);
            }
        };
    }

    static ClusterState createClusterStateWithoutNodes(TrainedModelAssignmentMetadata trainedModelAssignmentMetadata) {
        return createClusterState(trainedModelAssignmentMetadata, null);
    }

    static ClusterState createClusterState(TrainedModelAssignmentMetadata trainedModelAssignmentMetadata, DiscoveryNodes nodes) {
        ClusterState.Builder csBuilder = new ClusterState.Builder(ClusterState.EMPTY_STATE);

        Metadata.Builder metadataBuilder = Metadata.builder();
        // TODO PersistentTasksCustomMetadata is required for jobs other than TrainedModels
        // .customs(Map.of(PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata.builder().build()))
        if (trainedModelAssignmentMetadata != null) {
            metadataBuilder.putCustom(TrainedModelAssignmentMetadata.NAME, trainedModelAssignmentMetadata);
        }

        Metadata metadata = metadataBuilder.build();
        csBuilder.metadata(metadata);

        if (nodes != null) {
            csBuilder.nodes(nodes);
        }
        return csBuilder.build();
    }

    /**
     * Create nodes of the same size which together have enough resources to satisfy the requirements. The smallest nodes which satisfy the
     * requirements are used.
     * <p>
     * Using the smallest nodes is a business requirement to minimize costs and simplify the logic.
     *
     * @param minProcessorsPerNode
     * @param minMemoryPerNode
     * @param totalProcessors
     * @param totalMemory
     * @return an iterable of the smallest nodes which satisfy the requirements
     */
    static DiscoveryNodes createMlNodesOfUniformSize(
        int minProcessorsPerNode,
        ByteSizeValue minMemoryPerNode,
        int totalProcessors,
        ByteSizeValue totalMemory
    ) {

        List<ByteSizeValue> nodeMemorySizes = List.of(
            ByteSizeValue.ofGb(4),
            ByteSizeValue.ofGb(8),
            ByteSizeValue.ofGb(16),
            ByteSizeValue.ofGb(32),
            ByteSizeValue.ofGb(64)
        );
        List<Integer> nodeProcessorSizes = List.of(2, 4, 8, 16, 32);
        assertEquals(
            "Test misconfigured: nodeMemorySizes and nodeProcessorSizes must have the same size",
            nodeMemorySizes.size(),
            nodeProcessorSizes.size()
        );

        int smallestSufficientNodeIndex = nodeMemorySizes.size();

        for (int i = 0; i < nodeMemorySizes.size(); i++) {
            if (nodeMemorySizes.get(i).getBytes() >= minMemoryPerNode.getBytes() && nodeProcessorSizes.get(i) >= minProcessorsPerNode) {
                smallestSufficientNodeIndex = i;
                break;
            }
        }

        double numProcessorsPerNode = nodeProcessorSizes.get(smallestSufficientNodeIndex);
        ByteSizeValue memoryPerNode = nodeMemorySizes.get(smallestSufficientNodeIndex);

        int assignedProcessors = 0;
        ByteSizeValue assignedMemory = ByteSizeValue.ZERO;
        DiscoveryNodes.Builder dnBuilder = DiscoveryNodes.builder();
        int nodeCount = 0;

        while (assignedProcessors < totalProcessors || assignedMemory.getBytes() < totalMemory.getBytes()) {
            dnBuilder.add(buildDiscoveryNode(numProcessorsPerNode, String.valueOf(memoryPerNode.getBytes()), nodeCount));
            assignedProcessors += (int) numProcessorsPerNode;
            assignedMemory = ByteSizeValue.add(memoryPerNode, assignedMemory);
            nodeCount += 1;
        }

        return dnBuilder.build();
    }

    private static DiscoveryNode buildDiscoveryNode(double numProcessorsPerNode, String memoryPerNode, int nodeNumber) {
        Map<String, String> attributes = Map.of(
            MACHINE_MEMORY_NODE_ATTR,
            memoryPerNode,
            MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
            String.valueOf(numProcessorsPerNode),
            MAX_JVM_SIZE_NODE_ATTR,
            Long.toString(Runtime.getRuntime().maxMemory())
        );

        return DiscoveryNodeUtils.builder(NODE_NAME_PREFIX + nodeNumber)
            .attributes(attributes)
            .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
            .build();
    }

    private static ClusterSettings createClusterSettings() {
        return new ClusterSettings(Settings.EMPTY, Set.of(MachineLearning.ALLOCATED_PROCESSORS_SCALE));
    }

    private static MlMemoryTracker createMlMemoryTracker() {
        return mock(MlMemoryTracker.class);
    }

    private static Settings createSettings() {
        return Settings.builder()
            .put(MAX_MACHINE_MEMORY_PERCENT.getKey(), MAX_MACHINE_MEMORY_PERCENT.get(Settings.EMPTY))
            .put(USE_AUTO_MACHINE_MEMORY_PERCENT.getKey(), USE_AUTO_MACHINE_MEMORY_PERCENT.get(Settings.EMPTY))
            .build();
    }

    private static StartTrainedModelDeploymentAction.TaskParams createTaskParams(int numAllocations, int seed) {

        String modelId = "modelId" + seed;
        String deploymentId = "deploymentId" + seed;
        long modelBytes = MODEL_BYTES;
        int threadsPerAllocation = 1; // TODO expand to multiple threads per allocation
        int queueCapacity = 1024;
        ByteSizeValue cacheSize = null; // TODO expand to include cachesizes
        Priority priority = Priority.NORMAL;
        long perDeploymentMemoryBytes = modelBytes;
        long perAllocationMemoryBytes = 0;

        return new StartTrainedModelDeploymentAction.TaskParams(
            modelId,
            deploymentId,
            modelBytes,
            numAllocations,
            threadsPerAllocation,
            queueCapacity,
            cacheSize,
            priority,
            perDeploymentMemoryBytes,
            perAllocationMemoryBytes
        );
    }

    private static Map<String, TrainedModelAssignment> createModelAssignments(
        int numAssignments,
        int[] numAllocationsPerAssignment,
        Map<String, RoutingInfo> routingInfo,
        int seed
    ) {
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);

        for (int i = 0; i < numAssignments; i++) {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(numAllocationsPerAssignment[i], seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTING);
            for (var entry : routingInfo.entrySet()) {
                tmaBuilder.addRoutingEntry(entry.getKey(), entry.getValue());
            }
            assignments.put("TrainedModelAssignment-" + seed + "-" + i, tmaBuilder.build());
        }

        return assignments;
    }

    private static int calculateThreadsPerAllocation(Map<String, TrainedModelAssignment> assignments) {
        return assignments.values().stream().findFirst().get().getTaskParams().getThreadsPerAllocation();
    }

    private static long calculateExistingPerNodeMemoryBytes(ClusterState clusterState) {
        return Long.parseLong(clusterState.nodes().getAllNodes().stream().findFirst().get().getAttributes().get(MACHINE_MEMORY_NODE_ATTR));
    }

    private static DiscoveryNodes createDiscoveryNode(int memoryGb, int processors) {
        ByteSizeValue minMemoryPerNode = ByteSizeValue.ofGb(memoryGb);
        int totalProcessors = processors;
        ByteSizeValue totalMemory = ByteSizeValue.ofBytes(minMemoryPerNode.getBytes());
        DiscoveryNodes nodes = createMlNodesOfUniformSize(processors, minMemoryPerNode, totalProcessors, totalMemory);
        return nodes;
    }

    private static DiscoveryNodes createSmallDiscoveryNodes(int memoryGb, int totalProcessors) {
        ByteSizeValue minMemoryPerNode = ByteSizeValue.ofGb(memoryGb);
        ByteSizeValue totalMemory = ByteSizeValue.ofBytes(minMemoryPerNode.getBytes());
        DiscoveryNodes nodes = createMlNodesOfUniformSize(2, minMemoryPerNode, totalProcessors, totalMemory);
        return nodes;
    }

    private static long calculateExtraPerNodeModelMemoryBytes(Map<String, TrainedModelAssignment> assignments) {
        return assignments.values().stream().findFirst().get().getTaskParams().estimateMemoryUsageBytes();
    }

    private static int calculateTotalExistingAndUsedProcessors(Map<String, TrainedModelAssignment> assignments) {
        return assignments.values().stream().mapToInt(TrainedModelAssignment::totalTargetAllocations).sum();
    }

    private static long calculateExistingTotalModelMemoryBytes(Map<String, TrainedModelAssignment> assignments) {
        return assignments.values().stream().filter(tma -> {
            if (tma.getAssignmentState() == AssignmentState.STARTED) {
                return true;
            } else if (tma.getAssignmentState() == AssignmentState.STARTING && tma.getNodeRoutingTable().isEmpty() == false) {
                return true;
            } else if (tma.getAssignmentState() == AssignmentState.STOPPING && tma.getNodeRoutingTable().isEmpty() == false) {
                return true;
            } else {
                return false;
            }
        }).mapToLong(tma -> tma.getTaskParams().estimateMemoryUsageBytes()).sum();
    }

    /**
     * This test is run for each of the supplied {@link TestCase} configurations.
     * @throws IOException _
     */
    public void test() throws IOException {
        SetOnce<Boolean> executeCalled = new SetOnce<>();

        var executionVerificationListener = new ActionListener<MlAutoscalingStats>() {
            @Override
            public void onResponse(MlAutoscalingStats mlAutoscalingStats) {
                executeCalled.set(true);
                testCase.verificationListener.onResponse(mlAutoscalingStats);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Unexpected failure" + e);
            }
        };

        MlAutoscalingResourceTracker.getMlAutoscalingStats(
            testCase.clusterState,
            testCase.clusterSettings,
            testCase.mlMemoryTracker,
            testCase.settings,
            executionVerificationListener
        );

        assertThat(testCase.testDescription, executeCalled.get(), equalTo(true));
        // other assertions are run in testCase.verificationListener
    }

    private static TestCase WhenStartTrainedModelDeployment_ThenScaleUp_GivenNoExistingDeployments(int seed) {
        String testDescription = "test scaling from zero";

        // generic parameters
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments
        int numAllocationsRequested = 1;
        Map<String, TrainedModelAssignment> assignments = createModelAssignments(1, new int[] { numAllocationsRequested }, Map.of(), seed);
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state starts with zero nodes
        ClusterState clusterState = createClusterStateWithoutNodes(trainedModelAssignmentMetadata);

        // expected stats:
        int existingNodes = 0;
        long existingPerNodeMemoryBytes = 0;
        long existingTotalModelMemoryBytes = 0;
        int totalExistingProcessors = 0;
        int minNodes = 1;
        long extraSingleNodeModelMemoryInBytes = assignments.values()
            .stream()
            .mapToLong(tma -> tma.getTaskParams().estimateMemoryUsageBytes())
            .sum();
        int extraSingleNodeProcessors = 1;
        long extraModelMemoryInBytes = extraSingleNodeModelMemoryInBytes;
        int extraProcessors = 1;
        long removeNodeMemoryInBytes = 0;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraSingleNodeModelMemoryInBytes,
            extraSingleNodeProcessors,
            extraModelMemoryInBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

    private static TestCase WhenStartTrainedModelDeployment_ThenScaleUp_GivenExistingDeployments(int seed) {
        String testDescription = "test scaling up with existing deployment";
        // Generic settings
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments
        int numAllocationsRequestedPerviously = seed;
        DiscoveryNodes nodes = createDiscoveryNode(seed, numAllocationsRequestedPerviously);
        int numAssignments = 2;
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);
        // assignment 1 - already deployed
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(numAllocationsRequestedPerviously, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTED);
            tmaBuilder.addRoutingEntry(
                NODE_NAME_PREFIX + 0,
                new RoutingInfo(numAllocationsRequestedPerviously, numAllocationsRequestedPerviously, RoutingState.STARTED, null)
            );
            assignments.put("TrainedModelAssignment-" + seed + "-" + 0, tmaBuilder.build());
        }
        // asssignment 2 - not deployed yet
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(numAllocationsRequestedPerviously, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTING);
            tmaBuilder.clearNodeRoutingTable();
            assignments.put("TrainedModelAssignment-" + seed + "-" + 1, tmaBuilder.build());
        }
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state
        ClusterState clusterState = createClusterState(trainedModelAssignmentMetadata, nodes);

        // expected stats:
        int existingNodes = clusterState.nodes().getSize();
        long existingPerNodeMemoryBytes = calculateExistingPerNodeMemoryBytes(clusterState);
        long existingTotalModelMemoryBytes = calculateExistingTotalModelMemoryBytes(assignments);
        int totalExistingProcessors = calculateTotalExistingAndUsedProcessors(assignments);
        int minNodes = 2; // TODO understand why this value
        long extraPerNodeModelMemoryBytes = calculateExtraPerNodeModelMemoryBytes(assignments);
        int extraPerNodeProcessors = calculateThreadsPerAllocation(assignments);
        long extraModelMemoryBytes = extraPerNodeModelMemoryBytes;
        int extraProcessors = seed;
        long removeNodeMemoryInBytes = 0;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraPerNodeModelMemoryBytes,
            extraPerNodeProcessors,
            extraModelMemoryBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

    private static TestCase WhenStartTrainedModelDeployment_ThenNoScale_GivenExistingDeployments(int seed) {
        String testDescription = "test scaling when existing nodes have room for the new deployment";

        // Generic settings
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments
        int numAllocationsRequestedPerviously = seed;
        DiscoveryNodes nodes = createDiscoveryNode(16, numAllocationsRequestedPerviously);
        int numAssignments = 2;
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);
        // assignment 1 - already deployed
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(numAllocationsRequestedPerviously, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTED);
            tmaBuilder.addRoutingEntry(
                NODE_NAME_PREFIX + 0,
                new RoutingInfo(numAllocationsRequestedPerviously, numAllocationsRequestedPerviously, RoutingState.STARTED, null)
            );
            assignments.put("TrainedModelAssignment-" + seed + "-" + 0, tmaBuilder.build());
        }
        // asssignment 2 - not deployed yet
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(numAllocationsRequestedPerviously, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTING);
            tmaBuilder.addRoutingEntry(
                NODE_NAME_PREFIX + 0,
                new RoutingInfo(0, numAllocationsRequestedPerviously, RoutingState.STARTING, null)
            );
            assignments.put("TrainedModelAssignment-" + seed + "-" + 1, tmaBuilder.build());
        }
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state
        ClusterState clusterState = createClusterState(trainedModelAssignmentMetadata, nodes);

        // expected stats:
        int existingNodes = clusterState.nodes().getSize();
        long existingPerNodeMemoryBytes = calculateExistingPerNodeMemoryBytes(clusterState);
        long existingTotalModelMemoryBytes = calculateExistingTotalModelMemoryBytes(assignments);
        int totalExistingProcessors = calculateTotalExistingAndUsedProcessors(assignments);
        int minNodes = 3; // TODO understand why this value
        long extraPerNodeModelMemoryBytes = 0;
        int extraPerNodeProcessors = 0;
        long extraModelMemoryBytes = extraPerNodeModelMemoryBytes;
        int extraProcessors = 0;
        long removeNodeMemoryInBytes = 0;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraPerNodeModelMemoryBytes,
            extraPerNodeProcessors,
            extraModelMemoryBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

    private static TestCase WhenUpdateTrainedModelDeployment_ThenScaleUp_GivenDeploymentGetsLarger(int seed) {
        String testDescription = "test scaling up when updating existing deployment to be larger";

        // Generic settings
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments
        int numAllocationsRequestedPerviously = 4;
        int updatedNumAllocations = 8;
        DiscoveryNodes nodes = createDiscoveryNode(8, numAllocationsRequestedPerviously);
        int numAssignments = 1;
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);
        // assignment 1 - already deployed - just updated to be larger
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(updatedNumAllocations, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTED);
            tmaBuilder.addRoutingEntry(
                NODE_NAME_PREFIX + 0,
                new RoutingInfo(numAllocationsRequestedPerviously, numAllocationsRequestedPerviously, RoutingState.STARTED, null)
            );
            assignments.put("TrainedModelAssignment-" + seed + "-" + 0, tmaBuilder.build());
        }
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state
        ClusterState clusterState = createClusterState(trainedModelAssignmentMetadata, nodes);

        // expected stats:
        int existingNodes = clusterState.nodes().getSize();
        long existingPerNodeMemoryBytes = calculateExistingPerNodeMemoryBytes(clusterState);
        long existingTotalModelMemoryBytes = calculateExistingTotalModelMemoryBytes(assignments);
        int totalExistingProcessors = calculateTotalExistingAndUsedProcessors(assignments);
        int minNodes = 3; // TODO understand why this value
        long extraPerNodeModelMemoryBytes = assignments.values().stream().findFirst().get().getTaskParams().estimateMemoryUsageBytes();
        int extraPerNodeProcessors = 1;
        long extraModelMemoryBytes = extraPerNodeModelMemoryBytes;
        int extraProcessors = updatedNumAllocations - numAllocationsRequestedPerviously;
        long removeNodeMemoryInBytes = 0;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraPerNodeModelMemoryBytes,
            extraPerNodeProcessors,
            extraModelMemoryBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

    private static TestCase WhenUpdateTrainedModelDeployment_ThenNoScale_GivenDeploymentGetsLargerAndNodesAreSufficient(int seed) {
        String testDescription = "test scaling when updating existing deployment to be larger but still fits in existing nodes";

        // Generic settings
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments
        int numAllocationsRequestedPreviously = 4;
        int updatedNumAllocations = 8;
        DiscoveryNodes nodes = createDiscoveryNode(16, numAllocationsRequestedPreviously);
        int numAssignments = 1;
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);
        // assignment 1 - already deployed - just updated to be larger
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(updatedNumAllocations, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTED);
            tmaBuilder.addRoutingEntry(
                NODE_NAME_PREFIX + 0,
                new RoutingInfo(numAllocationsRequestedPreviously, numAllocationsRequestedPreviously, RoutingState.STARTED, null)
            );
            assignments.put("TrainedModelAssignment-" + seed + "-" + 0, tmaBuilder.build());
        }
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state
        ClusterState clusterState = createClusterState(trainedModelAssignmentMetadata, nodes);

        // expected stats:
        int existingNodes = clusterState.nodes().getSize();
        long existingPerNodeMemoryBytes = calculateExistingPerNodeMemoryBytes(clusterState);
        long existingTotalModelMemoryBytes = calculateExistingTotalModelMemoryBytes(assignments);
        int totalExistingProcessors = calculateTotalExistingAndUsedProcessors(assignments);
        int minNodes = 3; // TODO understand why this value
        long extraPerNodeModelMemoryBytes = 0;
        int extraPerNodeProcessors = 0;
        long extraModelMemoryBytes = 0;
        int extraProcessors = 0;
        long removeNodeMemoryInBytes = 0;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraPerNodeModelMemoryBytes,
            extraPerNodeProcessors,
            extraModelMemoryBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

    private static TestCase WhenUpdateTrainedModelDeployment_ThenNoScale_GivenDeploymentGetsSmallerButAllNodesAreStillRequired(int seed) {
        String testDescription = "test scaling up when updating existing deployment to be smaller but all nodes are still required";

        // Generic settings
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments
        int numAllocationsRequestedPreviously = 4;
        int updatedNumAllocations = 3;
        DiscoveryNodes nodes = createDiscoveryNode(8, numAllocationsRequestedPreviously);
        int numAssignments = 1;
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);
        // assignment 1 - already deployed - just updated to be smaller
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(updatedNumAllocations, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTED);
            tmaBuilder.addRoutingEntry(
                NODE_NAME_PREFIX + 0,
                new RoutingInfo(numAllocationsRequestedPreviously, numAllocationsRequestedPreviously, RoutingState.STARTED, null)
            );
            assignments.put("TrainedModelAssignment-" + seed + "-" + 0, tmaBuilder.build());
        }
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state
        ClusterState clusterState = createClusterState(trainedModelAssignmentMetadata, nodes);

        // expected stats:
        int existingNodes = clusterState.nodes().getSize();
        long existingPerNodeMemoryBytes = calculateExistingPerNodeMemoryBytes(clusterState);
        long existingTotalModelMemoryBytes = calculateExistingTotalModelMemoryBytes(assignments);
        int totalExistingProcessors = calculateTotalExistingAndUsedProcessors(assignments);
        int minNodes = 3; // TODO understand why this value
        long extraPerNodeModelMemoryBytes = 0;
        int extraPerNodeProcessors = 0;
        long extraModelMemoryBytes = 0;
        int extraProcessors = 0;
        long removeNodeMemoryInBytes = 0;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraPerNodeModelMemoryBytes,
            extraPerNodeProcessors,
            extraModelMemoryBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

    private static TestCase WhenUpdateTrainedModelDeployment_ThenScaleDown_GivenDeploymentGetsSmaller(int seed) {
        String testDescription = "test scaling down when updating existing deployment to be smaller";
        // TODO change this test to use threadsPerAllocation to accurately require a larger node size which then needs to be scaled down
        // when
        // threadsPerAllocation gets updated to a smaller number

        // Generic settings
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments
        int numAllocationsRequestedPreviously = 8;
        int updatedNumAllocations = 2;
        DiscoveryNodes nodes = createDiscoveryNode(16, numAllocationsRequestedPreviously);
        int numAssignments = 1;
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);
        // assignment 1
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(updatedNumAllocations, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTED);
            tmaBuilder.addRoutingEntry(
                NODE_NAME_PREFIX + 0,
                new RoutingInfo(updatedNumAllocations, updatedNumAllocations, RoutingState.STARTED, null)
            );
            assignments.put("TrainedModelAssignment-" + seed + "-" + 0, tmaBuilder.build());
        }
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state
        ClusterState clusterState = createClusterState(trainedModelAssignmentMetadata, nodes);

        // expected stats:
        int existingNodes = clusterState.nodes().getSize();
        long existingPerNodeMemoryBytes = calculateExistingPerNodeMemoryBytes(clusterState);
        long existingTotalModelMemoryBytes = calculateExistingTotalModelMemoryBytes(assignments);
        int totalExistingProcessors = calculateTotalExistingAndUsedProcessors(assignments);
        int minNodes = 3; // TODO understand why this value
        long extraPerNodeModelMemoryBytes = 0;
        int extraPerNodeProcessors = 0;
        long extraModelMemoryBytes = 0;
        int extraProcessors = 0;
        long removeNodeMemoryInBytes = 1;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraPerNodeModelMemoryBytes,
            extraPerNodeProcessors,
            extraModelMemoryBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

    private static TestCase WhenStoppingTrainedModelDeployment_ThenNoScale_GivenAllNodesAreStillRequired(int seed) {
        String testDescription = "test scaling when the existing deployments require the same nodes when as small deployment is stopping";

        // Generic settings
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments;
        DiscoveryNodes nodes = createDiscoveryNode(8, 4);
        int numAssignments = 2;
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);
        // assignment 1 - has 3 allocations
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(3, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTED);
            tmaBuilder.addRoutingEntry(NODE_NAME_PREFIX + 0, new RoutingInfo(3, 3, RoutingState.STARTED, null));
            assignments.put("TrainedModelAssignment-" + seed + "-" + 0, tmaBuilder.build());
        }
        // assignment 2 - is stopping, has 1 allocation
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(1, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STOPPING);
            tmaBuilder.addRoutingEntry(NODE_NAME_PREFIX + 0, new RoutingInfo(1, 1, RoutingState.STOPPING, "stopping deployment"));
            assignments.put("TrainedModelAssignment-" + seed + "-" + 2, tmaBuilder.build());
        }
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state
        ClusterState clusterState = createClusterState(trainedModelAssignmentMetadata, nodes);

        // expected stats:
        int existingNodes = clusterState.nodes().getSize();
        long existingPerNodeMemoryBytes = calculateExistingPerNodeMemoryBytes(clusterState);
        long existingTotalModelMemoryBytes = calculateExistingTotalModelMemoryBytes(assignments);
        int totalExistingProcessors = calculateTotalExistingAndUsedProcessors(assignments);
        int minNodes = 3; // TODO understand why this value
        long extraPerNodeModelMemoryBytes = 0;
        int extraPerNodeProcessors = 0;
        long extraModelMemoryBytes = 0;
        int extraProcessors = 0;
        long removeNodeMemoryInBytes = 0;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraPerNodeModelMemoryBytes,
            extraPerNodeProcessors,
            extraModelMemoryBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

    private static TestCase WhenStoppedTrainedModelDeployment_ThenNoScale_GivenAllNodesAreStillRequired(int seed) {
        String testDescription = "test scaling when the existing deployments require the same nodes when as small deployment is stopped";

        // Generic settings
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments;
        DiscoveryNodes nodes = createDiscoveryNode(8, 4);
        int numAssignments = 2;
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);
        // assignment 1 - has 3 allocations
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(3, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTED);
            tmaBuilder.addRoutingEntry(NODE_NAME_PREFIX + 0, new RoutingInfo(3, 3, RoutingState.STARTED, null));
            assignments.put("TrainedModelAssignment-" + seed + "-" + 0, tmaBuilder.build());
        }
        // assignment 2 - is stopping, has 1 allocation
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(1, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STOPPING);
            tmaBuilder.clearNodeRoutingTable();
        }
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state
        ClusterState clusterState = createClusterState(trainedModelAssignmentMetadata, nodes);

        // expected stats:
        int existingNodes = clusterState.nodes().getSize();
        long existingPerNodeMemoryBytes = calculateExistingPerNodeMemoryBytes(clusterState);
        long existingTotalModelMemoryBytes = calculateExistingTotalModelMemoryBytes(assignments);
        int totalExistingProcessors = calculateTotalExistingAndUsedProcessors(assignments);
        int minNodes = 3; // TODO understand why this value
        long extraPerNodeModelMemoryBytes = 0;
        int extraPerNodeProcessors = 0;
        long extraModelMemoryBytes = 0;
        int extraProcessors = 0;
        long removeNodeMemoryInBytes = 0;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraPerNodeModelMemoryBytes,
            extraPerNodeProcessors,
            extraModelMemoryBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

    private static TestCase WhenStopTrainedModelDeployment_ThenNoScale_GivenAllNodesAreStillRequired(int seed) {
        String testDescription = "test scaling when the existing deployments require the same nodes after a small deployment was removed";

        // Generic settings
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments;
        DiscoveryNodes nodes = createDiscoveryNode(8, 4);
        int numAssignments = 1;
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);
        // assignment 1 - has 3 allocations
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(3, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTED);
            tmaBuilder.addRoutingEntry(NODE_NAME_PREFIX + 0, new RoutingInfo(3, 3, RoutingState.STARTED, null));
            assignments.put("TrainedModelAssignment-" + seed + "-" + 0, tmaBuilder.build());
        }
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state
        ClusterState clusterState = createClusterState(trainedModelAssignmentMetadata, nodes);

        // expected stats:
        int existingNodes = clusterState.nodes().getSize();
        long existingPerNodeMemoryBytes = calculateExistingPerNodeMemoryBytes(clusterState);
        long existingTotalModelMemoryBytes = calculateExistingTotalModelMemoryBytes(assignments);
        int totalExistingProcessors = calculateTotalExistingAndUsedProcessors(assignments);
        int minNodes = 3; // TODO understand why this value
        long extraPerNodeModelMemoryBytes = 0;
        int extraPerNodeProcessors = 0;
        long extraModelMemoryBytes = 0;
        int extraProcessors = 0;
        long removeNodeMemoryInBytes = 0;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraPerNodeModelMemoryBytes,
            extraPerNodeProcessors,
            extraModelMemoryBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

    private static TestCase WhenStopTrainedModelDeployment_ThenScaledown_GivenDeploymentRequiredAWholeNode(int seed) {
        String testDescription = "test scaling down when the removed deployment required a whole node";
        // Generic settings
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments;
        DiscoveryNodes nodes = createSmallDiscoveryNodes(4, 16); // 16 allocations were required, but
        // the existing assignments now only require 12 allocations, so 2 nodes should be removed, but only 1 node will be removed at a time
        int numAssignments = 2;
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);
        // assignment 1 - has 12 allocations
        {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(12, seed);
            TrainedModelAssignment.Builder tmaBuilder = TrainedModelAssignment.Builder.empty(taskParams, null);
            tmaBuilder.setAssignmentState(AssignmentState.STARTED);
            tmaBuilder.addRoutingEntry(NODE_NAME_PREFIX + 0, new RoutingInfo(12, 12, RoutingState.STARTED, null));
            assignments.put("TrainedModelAssignment-" + seed + "-" + 0, tmaBuilder.build());
        }
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state
        ClusterState clusterState = createClusterState(trainedModelAssignmentMetadata, nodes);

        // expected stats:
        int existingNodes = clusterState.nodes().getSize();
        long existingPerNodeMemoryBytes = calculateExistingPerNodeMemoryBytes(clusterState);
        long existingTotalModelMemoryBytes = calculateExistingTotalModelMemoryBytes(assignments);
        int totalExistingProcessors = calculateTotalExistingAndUsedProcessors(assignments);
        int minNodes = 3; // TODO understand why this value
        long extraPerNodeModelMemoryBytes = 0;
        int extraPerNodeProcessors = 0;
        long extraModelMemoryBytes = 0;
        int extraProcessors = 0;
        long removeNodeMemoryInBytes = calculateExistingPerNodeMemoryBytes(clusterState); // we need to remove a whole node
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            existingNodes,
            existingPerNodeMemoryBytes,
            existingTotalModelMemoryBytes,
            totalExistingProcessors,
            minNodes,
            extraPerNodeModelMemoryBytes,
            extraPerNodeProcessors,
            extraModelMemoryBytes,
            extraProcessors,
            removeNodeMemoryInBytes,
            perNodeMemoryOverheadInBytes
        );

        ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(testDescription, expectedStats);
        return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    }

}
