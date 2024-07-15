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
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
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
    private final TestCase testCase;

    @ParametersFactory(shuffle = true)
    public static Iterable<Object[]> parameterizedTestCases() {
        List<TestCase> testCases = List.of(WhenStartTrainedModelDeployment_ThenScaleUp_GivenNoExistingDeployments()
        // TODO
        // WhenStartTrainedModelDeployment_ThenScaleUp_GivenExistingDeployments(),
        // WhenStartTrainedModelDeployment_ThenNoScale_GivenExistingDeployments(),
        //
        // WhenUpdateTrainedModelDeployment_ThenScaleUp_GivenDeploymentGetsLarger(),
        // WhenUpdateTrainedModelDeployment_ThenNoScale_GivenDeploymentGetsLargerAndNodesAreSufficient(),
        // WhenUpdateTrainedModelDeployment_ThenNoScale_GivenDeploymentGetsSmallerButAllNodesAreStillRequired(),
        // WhenUpdateTrainedModelDeployment_ThenScaleDown_GivenDeploymentGetsSmaller(),
        //
        // WhenStopTrainedModelDeployment_ThenNoScale_GivenAllNodesAreStillRequired(),
        // WhenStopTrainedModelDeployment_ThenScaledown_GivenDeploymentRequiredAWholeNode()
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
            public void onResponse(MlAutoscalingStats mlAutoscalingStats) {
                assertEquals(message, expectedStats, mlAutoscalingStats);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Unexpected failure" + e);
            }
        };
    }

    static ClusterState createClusterStateWithoutNodes(TrainedModelAssignmentMetadata trainedModelAssignmentMetadata) {
        return createClusterState(0, ByteSizeValue.ofGb(0), 0, ByteSizeValue.ofGb(0), trainedModelAssignmentMetadata);
    }

    /**
     * Create a cluster state with nodes of the same size which together have enough resources to satisfy the requirements.
     * If any of the parameters are zero, then the cluster state will have zero nodes.
     * @param minProcessorsPerNode
     * @param minMemoryPerNode
     * @param totalProcessors
     * @param totalMemory
     * @return a cluster state with the required nodes and metadata
     */
    static ClusterState createClusterState(
        int minProcessorsPerNode,
        ByteSizeValue minMemoryPerNode,
        int totalProcessors,
        ByteSizeValue totalMemory,
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata
    ) {
        ClusterState.Builder csBuilder = new ClusterState.Builder(ClusterState.EMPTY_STATE);

        Metadata.Builder metadataBuilder = Metadata.builder();
        // TODO PersistentTasksCustomMetadata is required for jobs other than TrainedModels
        // .customs(Map.of(PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata.builder().build()))
        if (trainedModelAssignmentMetadata != null) {
            metadataBuilder.putCustom(TrainedModelAssignmentMetadata.NAME, trainedModelAssignmentMetadata);
        }

        Metadata metadata = metadataBuilder.build();
        csBuilder.metadata(metadata);

        if (minProcessorsPerNode == 0
            || minMemoryPerNode.equals(ByteSizeValue.ZERO)
            || totalProcessors == 0
            || totalMemory.equals(ByteSizeValue.ZERO)) {

            DiscoveryNodes nodes = (DiscoveryNodes) createMlNodesOfUniformSize(
                minProcessorsPerNode,
                minMemoryPerNode,
                totalProcessors,
                totalMemory
            );
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
    static Iterable<DiscoveryNode> createMlNodesOfUniformSize(
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
        LinkedList<DiscoveryNode> nodes = new LinkedList<>();

        while (assignedProcessors < totalProcessors || assignedMemory.getBytes() < totalMemory.getBytes()) {
            nodes.add(buildDiscoveryNode(numProcessorsPerNode, String.valueOf(memoryPerNode.getBytes())));
            assignedProcessors += (int) numProcessorsPerNode;
            assignedMemory = ByteSizeValue.add(memoryPerNode, assignedMemory);
        }

        return nodes;
    }

    private static DiscoveryNode buildDiscoveryNode(double numProcessorsPerNode, String memoryPerNode) {
        Map<String, String> attributes = Map.of(
            MACHINE_MEMORY_NODE_ATTR,
            memoryPerNode,
            MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
            String.valueOf(numProcessorsPerNode),
            MAX_JVM_SIZE_NODE_ATTR,
            Long.toString(Runtime.getRuntime().maxMemory())
        );

        return DiscoveryNodeUtils.builder(randomAlphaOfLength(10)).attributes(attributes).roles(Set.of(DiscoveryNodeRole.ML_ROLE)).build();
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

    private static StartTrainedModelDeploymentAction.TaskParams createTaskParams(int numAllocations) {

        String modelId = randomAlphaOfLength(10);
        String deploymentId = randomAlphaOfLength(10);
        long modelBytes = ByteSizeValue.ofGb(2).getBytes();
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

    private static Map<String, TrainedModelAssignment> createModelAssignments(int numAssignments, int[] numAllocationsPerAssignment) {
        Map<String, TrainedModelAssignment> assignments = new HashMap<>(numAssignments);

        for (int i = 0; i < numAssignments; i++) {
            StartTrainedModelDeploymentAction.TaskParams taskParams = createTaskParams(numAllocationsPerAssignment[i]);
            assignments.put(
                "TrainedModelAssignment-" + randomAlphaOfLength(10),
                TrainedModelAssignment.Builder.empty(taskParams, null).build()
            );
        }

        return assignments;
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

    private static TestCase WhenStartTrainedModelDeployment_ThenScaleUp_GivenNoExistingDeployments() {
        String testDescription = "test scaling from zero";

        // generic parameters
        ClusterSettings clusterSettings = createClusterSettings();
        MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
        Settings settings = createSettings();

        // TrainedModelAssignments
        int numAllocationsRequested = 1;
        Map<String, TrainedModelAssignment> assignments = createModelAssignments(1, new int[] { numAllocationsRequested });
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = new TrainedModelAssignmentMetadata(assignments);

        // Cluster state starts with zero nodes
        ClusterState clusterState = createClusterStateWithoutNodes(trainedModelAssignmentMetadata);

        // expected stats:
        int nodes = 1;
        long perNodeMemoryInBytes = ByteSizeValue.ofGb(2).getBytes();
        long modelMemoryInBytesSum = ByteSizeValue.ofGb(2).getBytes();
        int processorsSum = 1;
        int minNodes = 1;
        long extraSingleNodeModelMemoryInBytes = 0;
        int extraSingleNodeProcessors = 1;
        long extraModelMemoryInBytes = ByteSizeValue.ofGb(2).getBytes();
        int extraProcessors = 1;
        long removeNodeMemoryInBytes = 0;
        long perNodeMemoryOverheadInBytes = MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        MlAutoscalingStats expectedStats = new MlAutoscalingStats(
            nodes,
            perNodeMemoryInBytes,
            modelMemoryInBytesSum,
            processorsSum,
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

    // private static TestCase WhenStartTrainedModelDeployment_ThenScaleUp_GivenExistingDeployments() {
    // String testDescription = "test scaling up with existing deployment";
    // // Cluster state
    // int minProcessorsPerNode = randomIntBetween(1, 32);
    // ByteSizeValue minMemoryPerNode = ByteSizeValue.ofGb(randomIntBetween(1, 64));
    // int totalProcessors = randomIntBetween(minProcessorsPerNode, 32);
    // ByteSizeValue totalMemory = ByteSizeValue.ofBytes(
    // randomLongBetween(minMemoryPerNode.getBytes(), ByteSizeValue.ofGb(64).getBytes())
    // );
    // ClusterState clusterState = createClusterState(minProcessorsPerNode, minMemoryPerNode, totalProcessors, totalMemory);
    // ClusterSettings clusterSettings = createClusterSettings();
    // MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
    // Settings settings = createSettings();
    // ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(expectedStats);
    // return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    // }
    //
    // private static TestCase WhenUpdateTrainedModelDeployment_ThenScaleUp_GivenDeploymentGetsLarger() {
    // String testDescription = "test scaling up when updating existing deployment to be larger";
    // ClusterState clusterState = createClusterState();
    // ClusterSettings clusterSettings = createClusterSettings();
    // MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
    // Settings settings = createSettings();
    // ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(expectedStats);
    // return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    // }
    //
    // private static TestCase WhenUpdateTrainedModelDeployment_ThenNoScale_GivenDeploymentGetsLargerAndNodesAreSufficient() {
    // String testDescription = "test scaling when updating existing deployment to be larger but still fits in existing nodes";
    // ClusterState clusterState = createClusterState();
    // ClusterSettings clusterSettings = createClusterSettings();
    // MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
    // Settings settings = createSettings();
    // ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(expectedStats);
    // return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    // }
    //
    // private static TestCase WhenUpdateTrainedModelDeployment_ThenNoScale_GivenDeploymentGetsSmallerButAllNodesAreStillRequired() {
    // String testDescription = "test scaling up when updating existing deployment to be smaller but all nodes are still required";
    // ClusterState clusterState = createClusterState();
    // ClusterSettings clusterSettings = createClusterSettings();
    // MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
    // Settings settings = createSettings();
    // ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(expectedStats);
    // return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    // }
    //
    // private static TestCase WhenUpdateTrainedModelDeployment_ThenScaleDown_GivenDeploymentGetsSmaller() {
    // String testDescription = "test scaling down when updating existing deployment to be smaller";
    // ClusterState clusterState = createClusterState();
    // ClusterSettings clusterSettings = createClusterSettings();
    // MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
    // Settings settings = createSettings();
    // ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(expectedStats);
    // return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    // }
    //
    // private static TestCase WhenStartTrainedModelDeployment_ThenNoScale_GivenExistingDeployments() {
    // String testDescription = "test scaling when existing nodes have room for the new deployment";
    // ClusterState clusterState = createClusterState();
    // ClusterSettings clusterSettings = createClusterSettings();
    // MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
    // Settings settings = createSettings();
    // ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(expectedStats);
    // return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    // }
    //
    // private static TestCase WhenStopTrainedModelDeployment_ThenNoScale_GivenAllNodesAreStillRequired() {
    // String testDescription = "test scaling when the existing deployments require the same nodes after a small deployment is removed";
    // ClusterState clusterState = createClusterState();
    // ClusterSettings clusterSettings = createClusterSettings();
    // MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
    // Settings settings = createSettings();
    // ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(expectedStats);
    // return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    // }
    //
    // private static TestCase WhenStopTrainedModelDeployment_ThenScaledown_GivenDeploymentRequiredAWholeNode() {
    // String testDescription = "test scaling down when the removed deployment required a whole node";
    // ClusterState clusterState = createClusterState();
    // ClusterSettings clusterSettings = createClusterSettings();
    // MlMemoryTracker mlMemoryTracker = createMlMemoryTracker();
    // Settings settings = createSettings();
    // ActionListener<MlAutoscalingStats> verificationListener = createVerificationListener(expectedStats);
    // return new TestCase(testDescription, clusterState, clusterSettings, mlMemoryTracker, settings, verificationListener);
    // }

}
