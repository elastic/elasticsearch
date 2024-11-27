/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import static java.lang.Math.min;
import static org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator.JVM_SIZE_KNOT_POINT;
import static org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator.STATIC_JVM_UPPER_THRESHOLD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlAutoscalingDeciderServiceTests extends ESTestCase {

    private static final long[] NODE_TIERS_NO_MONITORING = new long[] {
        ByteSizeValue.ofGb(1).getBytes(),
        ByteSizeValue.ofGb(2).getBytes(),
        ByteSizeValue.ofGb(4).getBytes(),
        ByteSizeValue.ofGb(8).getBytes(),
        ByteSizeValue.ofGb(16).getBytes(),
        ByteSizeValue.ofGb(32).getBytes(),
        ByteSizeValue.ofGb(64).getBytes(),
        ByteSizeValue.ofGb(15).getBytes(),
        ByteSizeValue.ofGb(30).getBytes(),
        ByteSizeValue.ofGb(60).getBytes() };

    // When monitoring is enabled Filebeat and Metricbeat are given a memory allowance of 360MB,
    // and this is deducted from the raw node size.
    private static final long MONITORING_ALLOWANCE_BYTES = ByteSizeValue.ofMb(360).getBytes();

    private static final long[] NODE_TIERS_WITH_MONITORING = Arrays.stream(NODE_TIERS_NO_MONITORING)
        .map(m -> m - MONITORING_ALLOWANCE_BYTES)
        .toArray();

    private static final long BYTES_IN_4MB = ByteSizeValue.ofMb(4).getBytes();

    // Must match the logic used in MachineDependentHeap.MachineNodeRole.ML_ONLY
    // (including rounding down to a multiple of 4 megabytes before multiplying
    // back up).
    private static long mlOnlyNodeJvmBytes(long systemMemoryBytes) {
        // 40% of memory up to 16GB, plus 10% of memory above that, up to an absolute maximum of 31GB
        long unroundedBytes = (systemMemoryBytes <= JVM_SIZE_KNOT_POINT)
            ? (long) (systemMemoryBytes * 0.4)
            : (long) min(JVM_SIZE_KNOT_POINT * 0.4 + (systemMemoryBytes - JVM_SIZE_KNOT_POINT) * 0.1, STATIC_JVM_UPPER_THRESHOLD);
        return (unroundedBytes / BYTES_IN_4MB) * BYTES_IN_4MB;
    }

    public static final List<Tuple<Long, Long>> AUTO_NODE_TIERS_NO_MONITORING = Arrays.stream(NODE_TIERS_NO_MONITORING)
        .mapToObj(m -> Tuple.tuple(m, mlOnlyNodeJvmBytes(m)))
        .toList();

    public static final List<Tuple<Long, Long>> AUTO_NODE_TIERS_WITH_MONITORING = Arrays.stream(NODE_TIERS_WITH_MONITORING)
        .mapToObj(m -> Tuple.tuple(m, mlOnlyNodeJvmBytes(m)))
        .toList();

    private static final long TEST_JOB_SIZE = ByteSizeValue.ofMb(200).getBytes();

    private NodeLoadDetector nodeLoadDetector;
    private NodeRealAvailabilityZoneMapper nodeRealAvailabilityZoneMapper;
    private ClusterService clusterService;
    private Settings settings;
    private TimeMachine timeSupplier;
    private MlMemoryTracker mlMemoryTracker;

    @Before
    public void setup() {
        mlMemoryTracker = mock(MlMemoryTracker.class);
        when(mlMemoryTracker.isRecentlyRefreshed()).thenReturn(true);
        when(mlMemoryTracker.asyncRefresh()).thenReturn(true);
        when(mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(any())).thenReturn(TEST_JOB_SIZE);
        when(mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(any())).thenReturn(TEST_JOB_SIZE);
        when(mlMemoryTracker.getTrainedModelAssignmentMemoryRequirement(any())).thenReturn(TEST_JOB_SIZE);
        when(mlMemoryTracker.getJobMemoryRequirement(any(), any())).thenReturn(TEST_JOB_SIZE);
        nodeLoadDetector = mock(NodeLoadDetector.class);
        when(nodeLoadDetector.getMlMemoryTracker()).thenReturn(mlMemoryTracker);
        when(nodeLoadDetector.detectNodeLoad(any(), any(), anyInt(), anyInt(), anyBoolean())).thenReturn(
            NodeLoad.builder("any").setUseMemory(true).incAssignedAnomalyDetectorMemory(ByteSizeValue.ofGb(1).getBytes()).build()
        );
        nodeRealAvailabilityZoneMapper = mock(NodeRealAvailabilityZoneMapper.class);
        clusterService = mock(ClusterService.class);
        settings = Settings.EMPTY;
        timeSupplier = new TimeMachine();
        ClusterSettings cSettings = new ClusterSettings(
            settings,
            Set.of(
                MachineLearning.MAX_MACHINE_MEMORY_PERCENT,
                MachineLearning.MAX_OPEN_JOBS_PER_NODE,
                MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT,
                MachineLearning.MAX_ML_NODE_SIZE,
                MachineLearning.ALLOCATED_PROCESSORS_SCALE,
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(cSettings);
    }

    public void testScale_whenNotOnMaster() {
        MlAutoscalingDeciderService service = buildService();
        service.offMaster();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> service.scale(Settings.EMPTY, mock(AutoscalingDeciderContext.class))
        );
        assertThat(iae.getMessage(), equalTo("request for scaling information is only allowed on the master node"));
    }

    public void testScale_GivenNoML_AndPresentMLNodes() {
        MlAutoscalingDeciderService service = buildService();
        service.onMaster();

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(buildNode("ml-1", ByteSizeValue.ofGb(4), 8))
                    .add(buildNode("ml-2", ByteSizeValue.ofGb(4), 8))
                    .build()
            )
            .build();

        AutoscalingDeciderResult result = service.scale(
            Settings.EMPTY,
            new DeciderContext(
                clusterState,
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(8), null),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4), null)
                )
            )
        );

        // First call doesn't downscale as delay has not been satisfied
        assertThat(result.reason().summary(), containsString("down scale delay has not been satisfied"));

        // Let's move time forward 1 hour
        timeSupplier.setOffset(TimeValue.timeValueHours(1));

        result = service.scale(
            Settings.EMPTY,
            new DeciderContext(
                clusterState,
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(8), null),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4), null)
                )
            )
        );

        assertThat(result.requiredCapacity().total().memory().getBytes(), equalTo(0L));
        assertThat(result.requiredCapacity().node().memory().getBytes(), equalTo(0L));
    }

    public void testScale_GivenNoML_AndNoMLNodes() {
        MlAutoscalingDeciderService service = buildService();
        service.onMaster();

        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(DiscoveryNodes.builder().build()).build();

        AutoscalingDeciderResult result = service.scale(
            Settings.EMPTY,
            new DeciderContext(
                clusterState,
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(8), null),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4), null)
                )
            )
        );

        // First call doesn't downscale as delay has not been satisfied
        assertThat(result.reason().summary(), containsString("Passing currently perceived capacity as no scaling changes are necessary"));
    }

    public void testScale_GivenUndeterminedMemory_ShouldReturnNullCapacity() {
        MlAutoscalingDeciderService service = buildService();
        service.onMaster();

        String jobId = "a_job";
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask(jobId, randomFrom("ml-1", "ml-2"), JobState.OPENED, tasksBuilder);
        Metadata.Builder metadata = Metadata.builder();
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build());

        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(buildNode("ml-1", ByteSizeValue.ofGb(4), 8))
                    .add(buildNode("ml-2", ByteSizeValue.ofGb(4), 8))
                    .build()
            )
            .metadata(metadata)
            .build();

        // Making the memory tracker return 0 for an AD job memory results to the decider return
        // undetermined capacity.
        when(mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(jobId)).thenReturn(0L);

        AutoscalingDeciderResult result = service.scale(
            Settings.EMPTY,
            new DeciderContext(
                clusterState,
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(8), null),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4), null)
                )
            )
        );

        assertThat(
            result.reason().summary(),
            containsString(
                "[memory_decider] Passing currently perceived capacity as there are running analytics "
                    + "and anomaly jobs or deployed models, but their assignment explanations are unexpected or their "
                    + "memory usage estimates are inaccurate."
            )
        );
        assertThat(result.requiredCapacity(), is(nullValue()));
    }

    public void testScale_GivenModelWithZeroAllocations() {
        MlAutoscalingDeciderService service = buildService();
        service.onMaster();

        ClusterState clusterState = new ClusterState.Builder(new ClusterName("cluster")).metadata(
            Metadata.builder()
                .putCustom(
                    TrainedModelAssignmentMetadata.NAME,
                    new TrainedModelAssignmentMetadata(
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
                            ).setAssignmentState(AssignmentState.STARTED).build()
                        )
                    )
                )
                .build()
        ).nodes(DiscoveryNodes.builder().add(buildNode("ml-node", ByteSizeValue.ofGb(4), 8)).build()).build();

        AutoscalingDeciderResult result = service.scale(
            Settings.EMPTY,
            new DeciderContext(
                clusterState,
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4), null),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4), null)
                )
            )
        );
        // First call doesn't downscale as delay has not been satisfied
        assertThat(result.reason().summary(), containsString("down scale delay has not been satisfied"));

        // Let's move time forward 1 hour
        timeSupplier.setOffset(TimeValue.timeValueHours(1));

        result = service.scale(
            Settings.EMPTY,
            new DeciderContext(
                clusterState,
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4), null),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4), null)
                )
            )
        );
        assertThat(result.reason().summary(), equalTo("Requesting scale down as tier and/or node size could be smaller"));
        assertThat(result.requiredCapacity().total().memory().getBytes(), equalTo(0L));
        assertThat(result.requiredCapacity().node().memory().getBytes(), equalTo(0L));
    }

    public void testScale_GivenTrainedModelAllocationAndNoMlNode() {
        MlAutoscalingDeciderService service = buildService();
        service.onMaster();

        ClusterState clusterState = new ClusterState.Builder(new ClusterName("cluster")).metadata(
            Metadata.builder()
                .putCustom(
                    TrainedModelAssignmentMetadata.NAME,
                    new TrainedModelAssignmentMetadata(
                        Map.of(
                            "model",
                            TrainedModelAssignment.Builder.empty(
                                new StartTrainedModelDeploymentAction.TaskParams(
                                    "model",
                                    "model-deployment",
                                    400,
                                    1,
                                    2,
                                    100,
                                    null,
                                    Priority.NORMAL,
                                    0L,
                                    0L
                                ),
                                new AdaptiveAllocationsSettings(true, 0, 4)
                            ).setAssignmentState(AssignmentState.STARTING).build()
                        )
                    )
                )
                .build()
        ).build();

        AutoscalingDeciderResult result = service.scale(
            Settings.EMPTY,
            new DeciderContext(
                clusterState,
                new AutoscalingCapacity(AutoscalingCapacity.AutoscalingResources.ZERO, AutoscalingCapacity.AutoscalingResources.ZERO)
            )
        );

        assertThat(result.reason().summary(), containsString("requesting scale up"));
        assertThat(result.requiredCapacity().total().memory().getBytes(), greaterThan(TEST_JOB_SIZE));
        assertThat(result.requiredCapacity().total().processors().count(), equalTo(2.0));
        assertThat(result.requiredCapacity().node().memory().getBytes(), greaterThan(TEST_JOB_SIZE));
        assertThat(result.requiredCapacity().node().processors().count(), equalTo(2.0));
    }

    private DiscoveryNode buildNode(String id, ByteSizeValue machineMemory, int allocatedProcessors) {
        return DiscoveryNodeUtils.create(
            id,
            buildNewFakeTransportAddress(),
            Map.of(
                MachineLearning.MACHINE_MEMORY_NODE_ATTR,
                String.valueOf(machineMemory.getBytes()),
                MachineLearning.MAX_JVM_SIZE_NODE_ATTR,
                String.valueOf(MlAutoscalingDeciderServiceTests.mlOnlyNodeJvmBytes(machineMemory.getBytes())),
                MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR,
                String.valueOf(allocatedProcessors)
            ),
            Set.of(DiscoveryNodeRole.ML_ROLE)
        );
    }

    private MlAutoscalingDeciderService buildService() {
        return new MlAutoscalingDeciderService(nodeLoadDetector, settings, nodeRealAvailabilityZoneMapper, clusterService, timeSupplier);
    }

    static class DeciderContext implements AutoscalingDeciderContext {

        private final ClusterState state;
        private final AutoscalingCapacity capacity;

        DeciderContext(ClusterState state, AutoscalingCapacity capacity) {
            this.state = state;
            this.capacity = capacity;
        }

        @Override
        public ClusterState state() {
            return state;
        }

        @Override
        public AutoscalingCapacity currentCapacity() {
            return capacity;
        }

        @Override
        public Set<DiscoveryNode> nodes() {
            return null;
        }

        @Override
        public Set<DiscoveryNodeRole> roles() {
            return null;
        }

        @Override
        public ClusterInfo info() {
            return null;
        }

        @Override
        public SnapshotShardSizeInfo snapshotShardSizeInfo() {
            return null;
        }

        @Override
        public void ensureNotCancelled() {

        }
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
