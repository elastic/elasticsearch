/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.Before;

import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlProcessorAutoscalingDeciderTests extends ESTestCase {

    private ScaleTimer scaleTimer;

    @Before
    public void setup() {
        scaleTimer = new ScaleTimer(System::currentTimeMillis);
    }

    public void testScale_GivenCurrentCapacityIsUsedExactly() {
        String modelId1 = "model-id-1";
        String modelId2 = "model-id-2";

        String mlNodeId1 = "ml-node-id-1";
        String mlNodeId2 = "ml-node-id-2";
        String dataNodeId = "data-node-id";
        DiscoveryNode mlNode1 = buildNode(mlNodeId1, true, 7.8);
        DiscoveryNode mlNode2 = buildNode(mlNodeId2, true, 7.6);
        DiscoveryNode dataNode = buildNode(dataNodeId, false, 24.0);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(mlNode1).add(mlNode2).add(dataNode).build())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                modelId1,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId1,
                                        modelId1,
                                        42L,
                                        2,
                                        3,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                ).addRoutingEntry(mlNodeId1, new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                            )
                            .addNewAssignment(
                                modelId2,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId2,
                                        modelId2,
                                        42L,
                                        10,
                                        1,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                )
                                    .addRoutingEntry(mlNodeId1, new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                                    .addRoutingEntry(mlNodeId2, new RoutingInfo(8, 8, RoutingState.STARTED, ""))
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        MlProcessorAutoscalingDecider decider = newDecider();

        MlProcessorAutoscalingCapacity capacity = decider.scale(
            Settings.EMPTY,
            newContext(clusterState),
            new MlAutoscalingContext(clusterState),
            1
        );

        assertThat(capacity.nodeProcessors(), equalTo(Processors.of(7.8)));
        assertThat(capacity.tierProcessors(), equalTo(Processors.of(15.4)));
        assertThat(capacity.reason(), equalTo("passing currently perceived capacity as it is fully used"));
    }

    public void testScale_GivenUnsatisfiedDeployments() {
        String modelId1 = "model-id-1";
        String modelId2 = "model-id-2";

        String mlNodeId1 = "ml-node-id-1";
        String mlNodeId2 = "ml-node-id-2";
        String dataNodeId = "data-node-id";
        DiscoveryNode mlNode1 = buildNode(mlNodeId1, true, 4);
        DiscoveryNode mlNode2 = buildNode(mlNodeId2, true, 4);
        DiscoveryNode dataNode = buildNode(dataNodeId, false, 24);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(mlNode1).add(mlNode2).add(dataNode).build())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                modelId1,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId1,
                                        modelId1,
                                        42L,
                                        1,
                                        8,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                )
                            )
                            .addNewAssignment(
                                modelId2,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId2,
                                        modelId2,
                                        42L,
                                        3,
                                        4,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                )
                                    .addRoutingEntry(mlNodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                                    .addRoutingEntry(mlNodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        MlProcessorAutoscalingDecider decider = newDecider();

        MlProcessorAutoscalingCapacity capacity = decider.scale(
            Settings.EMPTY,
            newContext(clusterState),
            new MlAutoscalingContext(clusterState),
            1
        );

        assertThat(capacity.nodeProcessors(), equalTo(Processors.of(8.0)));
        assertThat(capacity.tierProcessors(), equalTo(Processors.of(20.0)));
        assertThat(capacity.reason(), equalTo("requesting scale up as there are unsatisfied deployments"));
    }

    public void testScale_GivenUnsatisfiedDeploymentIsLowPriority_ShouldNotScaleUp() {
        String modelId1 = "model-id-1";
        String modelId2 = "model-id-2";

        String mlNodeId1 = "ml-node-id-1";
        String mlNodeId2 = "ml-node-id-2";
        String dataNodeId = "data-node-id";
        DiscoveryNode mlNode1 = buildNode(mlNodeId1, true, 4);
        DiscoveryNode mlNode2 = buildNode(mlNodeId2, true, 4);
        DiscoveryNode dataNode = buildNode(dataNodeId, false, 24);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(mlNode1).add(mlNode2).add(dataNode).build())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                modelId1,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId1,
                                        modelId1,
                                        42L,
                                        1,
                                        1,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.LOW,
                                        0L,
                                        0L
                                    )
                                )
                            )
                            .addNewAssignment(
                                modelId2,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId2,
                                        modelId2,
                                        42L,
                                        2,
                                        4,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                )
                                    .addRoutingEntry(mlNodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                                    .addRoutingEntry(mlNodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        MlProcessorAutoscalingDecider decider = newDecider();

        MlProcessorAutoscalingCapacity capacity = decider.scale(
            Settings.EMPTY,
            newContext(clusterState),
            new MlAutoscalingContext(clusterState),
            1
        );

        assertThat(capacity.nodeProcessors(), equalTo(Processors.of(4.0)));
        assertThat(capacity.tierProcessors(), equalTo(Processors.of(8.0)));
        assertThat(capacity.reason(), equalTo("passing currently perceived capacity as it is fully used"));
    }

    public void testScale_GivenMoreThanHalfProcessorsAreUsed() {
        String modelId1 = "model-id-1";
        String modelId2 = "model-id-2";

        String mlNodeId1 = "ml-node-id-1";
        String mlNodeId2 = "ml-node-id-2";
        String dataNodeId = "data-node-id";
        DiscoveryNode mlNode1 = buildNode(mlNodeId1, true, 3.8);
        DiscoveryNode mlNode2 = buildNode(mlNodeId2, true, 3.8);
        DiscoveryNode dataNode = buildNode(dataNodeId, false, 24);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(mlNode1).add(mlNode2).add(dataNode).build())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                modelId1,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId1,
                                        modelId1,
                                        42L,
                                        2,
                                        2,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                ).addRoutingEntry(mlNodeId1, new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                            )
                            .addNewAssignment(
                                modelId2,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId2,
                                        modelId2,
                                        42L,
                                        1,
                                        1,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                ).addRoutingEntry(mlNodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        MlProcessorAutoscalingDecider decider = newDecider();

        MlProcessorAutoscalingCapacity capacity = decider.scale(
            Settings.EMPTY,
            newContext(clusterState),
            new MlAutoscalingContext(clusterState),
            1
        );

        assertThat(capacity.nodeProcessors(), equalTo(Processors.of(3.8)));
        assertThat(capacity.tierProcessors(), equalTo(Processors.of(7.6)));
        assertThat(
            capacity.reason(),
            equalTo("not scaling down as model assignments require more than half of the ML tier's allocated processors")
        );

        // test with allocated processor scaling
        capacity = decider.scale(Settings.EMPTY, newContext(clusterState), new MlAutoscalingContext(clusterState), 2);

        assertThat(capacity.nodeProcessors(), equalTo(Processors.of(1.9)));
        assertThat(capacity.tierProcessors(), equalTo(Processors.of(3.8)));
        assertThat(
            capacity.reason(),
            equalTo("not scaling down as model assignments require more than half of the ML tier's allocated processors")
        );
    }

    public void testScale_GivenDownScalePossible_DelayNotSatisfied() {
        String modelId1 = "model-id-1";
        String modelId2 = "model-id-2";

        String mlNodeId1 = "ml-node-id-1";
        String mlNodeId2 = "ml-node-id-2";
        String dataNodeId = "data-node-id";
        DiscoveryNode mlNode1 = buildNode(mlNodeId1, true, 7.9);
        DiscoveryNode mlNode2 = buildNode(mlNodeId2, true, 7.9);
        DiscoveryNode dataNode = buildNode(dataNodeId, false, 24);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(mlNode1).add(mlNode2).add(dataNode).build())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                modelId1,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId1,
                                        modelId1,
                                        42L,
                                        2,
                                        2,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                ).addRoutingEntry(mlNodeId1, new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                            )
                            .addNewAssignment(
                                modelId2,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId2,
                                        modelId2,
                                        42L,
                                        1,
                                        1,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                ).addRoutingEntry(mlNodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        MlProcessorAutoscalingDecider decider = newDecider();
        scaleTimer.markScale();

        MlProcessorAutoscalingCapacity capacity = decider.scale(
            Settings.EMPTY,
            newContext(clusterState),
            new MlAutoscalingContext(clusterState),
            1
        );

        assertThat(capacity.nodeProcessors(), equalTo(Processors.of(7.9)));
        assertThat(capacity.tierProcessors(), equalTo(Processors.of(15.8)));
        assertThat(capacity.reason(), containsString("Passing currently perceived capacity as down scale delay has not been satisfied"));
    }

    public void testScale_GivenDownScalePossible_DelaySatisfied() {
        String modelId1 = "model-id-1";
        String modelId2 = "model-id-2";

        String mlNodeId1 = "ml-node-id-1";
        String mlNodeId2 = "ml-node-id-2";
        String dataNodeId = "data-node-id";
        DiscoveryNode mlNode1 = buildNode(mlNodeId1, true, 8);
        DiscoveryNode mlNode2 = buildNode(mlNodeId2, true, 8);
        DiscoveryNode dataNode = buildNode(dataNodeId, false, 24);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(mlNode1).add(mlNode2).add(dataNode).build())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                modelId1,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId1,
                                        modelId1,
                                        42L,
                                        2,
                                        2,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                ).addRoutingEntry(mlNodeId1, new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                            )
                            .addNewAssignment(
                                modelId2,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId2,
                                        modelId2,
                                        42L,
                                        1,
                                        1,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.NORMAL,
                                        0L,
                                        0L
                                    )
                                ).addRoutingEntry(mlNodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        TimeMachine timeMachine = new TimeMachine();
        scaleTimer = new ScaleTimer(timeMachine);
        MlProcessorAutoscalingDecider decider = newDecider();
        scaleTimer.markScale();
        scaleTimer.markDownScaleAndGetMillisLeftFromDelay(Settings.EMPTY);
        timeMachine.setOffset(TimeValue.timeValueHours(1));

        MlProcessorAutoscalingCapacity capacity = decider.scale(
            Settings.EMPTY,
            newContext(clusterState),
            new MlAutoscalingContext(clusterState),
            1
        );

        assertThat(capacity.nodeProcessors(), equalTo(Processors.of(2.0)));
        assertThat(capacity.tierProcessors(), equalTo(Processors.of(5.0)));
        assertThat(capacity.reason(), containsString("requesting scale down as tier and/or node size could be smaller"));
    }

    public void testScale_GivenLowPriorityDeploymentsOnly() {
        String modelId1 = "model-id-1";
        String modelId2 = "model-id-2";

        String mlNodeId1 = "ml-node-id-1";
        String mlNodeId2 = "ml-node-id-2";
        String dataNodeId = "data-node-id";
        DiscoveryNode mlNode1 = buildNode(mlNodeId1, true, 4);
        DiscoveryNode mlNode2 = buildNode(mlNodeId2, true, 4);
        DiscoveryNode dataNode = buildNode(dataNodeId, false, 24);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(mlNode1).add(mlNode2).add(dataNode).build())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                modelId1,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId1,
                                        modelId1,
                                        42L,
                                        1,
                                        1,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.LOW,
                                        0L,
                                        0L
                                    )
                                ).addRoutingEntry(mlNodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            )
                            .addNewAssignment(
                                modelId2,
                                TrainedModelAssignment.Builder.empty(
                                    new StartTrainedModelDeploymentAction.TaskParams(
                                        modelId2,
                                        modelId2,
                                        42L,
                                        1,
                                        1,
                                        1024,
                                        ByteSizeValue.ONE,
                                        Priority.LOW,
                                        0L,
                                        0L
                                    )
                                ).addRoutingEntry(mlNodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        TimeMachine timeMachine = new TimeMachine();
        scaleTimer = new ScaleTimer(timeMachine);
        MlProcessorAutoscalingDecider decider = newDecider();
        scaleTimer.markScale();
        scaleTimer.markDownScaleAndGetMillisLeftFromDelay(Settings.EMPTY);
        timeMachine.setOffset(TimeValue.timeValueHours(1));

        MlProcessorAutoscalingCapacity capacity = decider.scale(
            Settings.EMPTY,
            newContext(clusterState),
            new MlAutoscalingContext(clusterState),
            1
        );

        assertThat(capacity.nodeProcessors(), equalTo(Processors.ZERO));
        assertThat(capacity.tierProcessors(), equalTo(Processors.of(0.1)));
        assertThat(capacity.reason(), equalTo("requesting scale down as tier and/or node size could be smaller"));
    }

    private static DiscoveryNode buildNode(String name, boolean isML, double allocatedProcessors) {
        return DiscoveryNodeUtils.builder(name)
            .name(name)
            .attributes(
                Map.of(
                    MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
                    String.valueOf(10),
                    MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
                    String.valueOf(allocatedProcessors)
                )
            )
            .roles(isML ? DiscoveryNodeRole.roles() : Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .build();
    }

    private MlProcessorAutoscalingDecider newDecider() {
        return new MlProcessorAutoscalingDecider(scaleTimer);
    }

    private AutoscalingDeciderContext newContext(ClusterState clusterState) {
        AutoscalingDeciderContext context = mock(AutoscalingDeciderContext.class);
        when(context.state()).thenReturn(clusterState);
        return context;
    }

    private static class TimeMachine implements LongSupplier {

        private long offsetMillis;

        void setOffset(TimeValue timeValue) {
            this.offsetMillis = timeValue.millis();
        }

        @Override
        public long getAsLong() {
            return System.currentTimeMillis() + offsetMillis;
        }
    }
}
