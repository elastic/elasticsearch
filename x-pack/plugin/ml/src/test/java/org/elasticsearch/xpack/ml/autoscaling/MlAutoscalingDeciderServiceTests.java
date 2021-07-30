/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;
import org.junit.Before;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlAutoscalingDeciderServiceTests extends ESTestCase {

    private static final long DEFAULT_NODE_SIZE = ByteSizeValue.ofGb(20).getBytes();
    private static final long DEFAULT_JVM_SIZE = ByteSizeValue.ofMb((long)(DEFAULT_NODE_SIZE * 0.25)).getBytes();
    private static final long DEFAULT_JOB_SIZE = ByteSizeValue.ofMb(200).getBytes();
    private static final long OVERHEAD = ByteSizeValue.ofMb(30).getBytes();
    private NodeLoadDetector nodeLoadDetector;
    private ClusterService clusterService;
    private Settings settings;
    private Supplier<Long> timeSupplier;
    private MlMemoryTracker mlMemoryTracker;

    @Before
    public void setup() {
        mlMemoryTracker = mock(MlMemoryTracker.class);
        when(mlMemoryTracker.isRecentlyRefreshed(any())).thenReturn(true);
        when(mlMemoryTracker.asyncRefresh()).thenReturn(true);
        when(mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(any())).thenReturn(DEFAULT_JOB_SIZE);
        when(mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(any())).thenReturn(DEFAULT_JOB_SIZE);
        nodeLoadDetector = mock(NodeLoadDetector.class);
        when(nodeLoadDetector.getMlMemoryTracker()).thenReturn(mlMemoryTracker);
        when(nodeLoadDetector.detectNodeLoad(any(), anyBoolean(), any(), anyInt(), anyInt(), anyBoolean()))
            .thenReturn(NodeLoad.builder("any")
                .setUseMemory(true)
                .incAssignedJobMemory(ByteSizeValue.ofGb(1).getBytes())
                .build());
        clusterService = mock(ClusterService.class);
        settings = Settings.EMPTY;
        timeSupplier = System::currentTimeMillis;
        ClusterSettings cSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(MachineLearning.MAX_MACHINE_MEMORY_PERCENT,
                MachineLearning.MAX_OPEN_JOBS_PER_NODE,
                MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT));
        when(clusterService.getClusterSettings()).thenReturn(cSettings);
    }

    public void testScale_whenNotOnMaster() {
        MlAutoscalingDeciderService service = buildService();
        service.offMaster();
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
            () -> service.scale(Settings.EMPTY,
                mock(AutoscalingDeciderContext.class)));
        assertThat(iae.getMessage(), equalTo("request for scaling information is only allowed on the master node"));
    }

    public void testScaleUp_withNoJobsWaiting() {
        MlAutoscalingDeciderService service = buildService();
        service.onMaster();

        assertThat(service.checkForScaleUp(
            0,
            0,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            NativeMemoryCapacity.ZERO,
            MlScalingReason.builder()),
            equalTo(Optional.empty()));
    }

    public void testScaleUp_withWaitingJobsAndAutoMemoryAndNoRoomInNodes() {
        when(mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(any())).thenReturn(ByteSizeValue.ofGb(2).getBytes());
        when(mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(any())).thenReturn(ByteSizeValue.ofGb(2).getBytes());
        List<String> jobTasks = Arrays.asList("waiting_job", "waiting_job_2");
        List<String> analytics = Arrays.asList("analytics_waiting");
        List<NodeLoad> fullyLoadedNode = Arrays.asList(NodeLoad.builder("any")
            .setMaxMemory(ByteSizeValue.ofGb(1).getBytes())
            .setUseMemory(true)
            .incAssignedJobMemory(ByteSizeValue.ofGb(1).getBytes()).build());
        MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder()
            .setPassedConfiguration(Settings.EMPTY)
            .setCurrentMlCapacity(AutoscalingCapacity.ZERO);
        MlAutoscalingDeciderService service = buildService();
        service.setUseAuto(true);
        { // No time in queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(
                0,
                0,
                fullyLoadedNode,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            AutoscalingDeciderResult result = decision.get();
            long allowedBytesForMlNode = NativeMemoryCalculator.allowedBytesForMl(
                result.requiredCapacity().node().memory().getBytes(),
                30,
                true
            );
            long allowedBytesForMlTier = NativeMemoryCalculator.allowedBytesForMl(
                result.requiredCapacity().total().memory().getBytes(),
                30,
                true
            );
            assertThat(allowedBytesForMlNode, greaterThanOrEqualTo(ByteSizeValue.ofGb(2).getBytes() + OVERHEAD));
            assertThat(allowedBytesForMlTier, greaterThanOrEqualTo(ByteSizeValue.ofGb(2).getBytes() * 3 + OVERHEAD));
        }
        { // we allow one job in the analytics queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(0, 1,
                fullyLoadedNode,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            AutoscalingDeciderResult result = decision.get();
            long allowedBytesForMlNode = NativeMemoryCalculator.allowedBytesForMl(
                result.requiredCapacity().node().memory().getBytes(),
                30,
                true
            );
            long allowedBytesForMlTier = NativeMemoryCalculator.allowedBytesForMl(
                result.requiredCapacity().total().memory().getBytes(),
                30,
                true
            );
            assertThat(allowedBytesForMlNode, greaterThanOrEqualTo(ByteSizeValue.ofGb(2).getBytes() + OVERHEAD));
            assertThat(allowedBytesForMlTier, greaterThanOrEqualTo(ByteSizeValue.ofGb(2).getBytes() * 2 + OVERHEAD));
        }
        { // we allow one job in the anomaly queue and analytics queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(1, 1,
                fullyLoadedNode,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            AutoscalingDeciderResult result = decision.get();
            long allowedBytesForMlNode = NativeMemoryCalculator.allowedBytesForMl(
                result.requiredCapacity().node().memory().getBytes(),
                30,
                true
            );
            long allowedBytesForMlTier = NativeMemoryCalculator.allowedBytesForMl(
                result.requiredCapacity().total().memory().getBytes(),
                30,
                true
            );
            assertThat(allowedBytesForMlNode, greaterThanOrEqualTo(ByteSizeValue.ofGb(2).getBytes() + OVERHEAD));
            assertThat(allowedBytesForMlTier, greaterThanOrEqualTo(ByteSizeValue.ofGb(2).getBytes() + OVERHEAD));
        }
    }

    public void testScaleUp_withWaitingJobsAndRoomInNodes() {
        List<String> jobTasks = Arrays.asList("waiting_job", "waiting_job_2");
        List<String> analytics = Arrays.asList("analytics_waiting");
        MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder()
            .setPassedConfiguration(Settings.EMPTY)
            .setCurrentMlCapacity(AutoscalingCapacity.ZERO);
        List<NodeLoad> nodesWithRoom = Arrays.asList(NodeLoad.builder("partially_filled")
            .setMaxMemory(ByteSizeValue.ofMb(430).getBytes())
            .setUseMemory(true)
            .setMaxJobs(10)
            .incNumAssignedJobs()
            .incAssignedJobMemory(ByteSizeValue.ofMb(230).getBytes()).build(),
            NodeLoad.builder("not_filled")
                .setMaxMemory(ByteSizeValue.ofMb(230).getBytes())
                .setMaxJobs(10)
                .setUseMemory(true).build());
        MlAutoscalingDeciderService service = buildService();
        service.setMaxMachineMemoryPercent(25);
        { // No time in queue, should be able to assign all but one job given the current node load
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(0, 0,
                nodesWithRoom,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertTrue(decision.isPresent());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo((DEFAULT_JOB_SIZE + OVERHEAD) * 4));
            assertThat(decision.get().requiredCapacity().total().memory().getBytes(),
                equalTo(4 * (DEFAULT_JOB_SIZE + OVERHEAD)));
        }
        { // we allow one job in the analytics queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(0, 1,
                nodesWithRoom,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isPresent());
        }
        { // we allow one job in the anomaly queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(1, 0,
                nodesWithRoom,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isPresent());
        }
    }

    public void testScaleUp_withWaitingJobsAndNoRoomInNodes() {
        List<String> jobTasks = Arrays.asList("waiting_job", "waiting_job_2");
        List<String> analytics = Arrays.asList("analytics_waiting");
        MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder()
            .setPassedConfiguration(Settings.EMPTY)
            .setCurrentMlCapacity(AutoscalingCapacity.ZERO);
        List<NodeLoad> fullyLoadedNode = Arrays.asList(NodeLoad.builder("any")
            .setMaxMemory(ByteSizeValue.ofGb(1).getBytes())
            .setUseMemory(true)
            .incAssignedJobMemory(ByteSizeValue.ofGb(1).getBytes()).build());
        MlAutoscalingDeciderService service = buildService();
        service.setMaxMachineMemoryPercent(25);
        { // No time in queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(0, 0,
                fullyLoadedNode,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo((DEFAULT_JOB_SIZE + OVERHEAD) * 4));
            assertThat(decision.get().requiredCapacity().total().memory().getBytes(),
                equalTo(4 * (3 * DEFAULT_JOB_SIZE + OVERHEAD)));
        }
        { // we allow one job in the analytics queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(0, 1,
                fullyLoadedNode,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(4 * (DEFAULT_JOB_SIZE + OVERHEAD)));
            assertThat(decision.get().requiredCapacity().total().memory().getBytes(),
                equalTo(4 * (2 * DEFAULT_JOB_SIZE + OVERHEAD)));
        }
        { // we allow one job in the anomaly queue and analytics queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(1, 1,
                fullyLoadedNode,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(4 * (DEFAULT_JOB_SIZE + OVERHEAD)));
            assertThat(decision.get().requiredCapacity().total().memory().getBytes(), equalTo(4 * (DEFAULT_JOB_SIZE + OVERHEAD)));
        }
    }

    public void testScaleUp_withWaitingJobs_WithFutureCapacity() {
        List<String> jobTasks = Arrays.asList("waiting_job", "waiting_job_2");
        List<String> analytics = Arrays.asList("analytics_waiting");
        MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder()
            .setPassedConfiguration(Settings.EMPTY)
            .setCurrentMlCapacity(AutoscalingCapacity.ZERO);
        List<NodeLoad> fullyLoadedNode = Arrays.asList(NodeLoad.builder("any")
            .setMaxMemory(ByteSizeValue.ofGb(1).getBytes())
            .setUseMemory(true)
            .incAssignedJobMemory(ByteSizeValue.ofGb(1).getBytes()).build());
        MlAutoscalingDeciderService service = buildService();
        service.setMaxMachineMemoryPercent(25);
        { // with null future capacity and current capacity has a small node
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(2, 1,
                fullyLoadedNode,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(DEFAULT_JOB_SIZE * 4));
            assertThat(decision.get().requiredCapacity().total().memory().getBytes(), equalTo(DEFAULT_JOB_SIZE * 4));
        }
        {
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(2, 1,
                fullyLoadedNode,
                jobTasks,
                analytics,
                new NativeMemoryCapacity(ByteSizeValue.ofGb(3).getBytes(), ByteSizeValue.ofGb(1).getBytes()),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(2).getBytes(), ByteSizeValue.ofGb(2).getBytes()),
                reasonBuilder);
            assertTrue(decision.isEmpty());
        }
        {
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(2, 1,
                fullyLoadedNode,
                jobTasks,
                analytics,
                new NativeMemoryCapacity(ByteSizeValue.ofMb(1).getBytes(), ByteSizeValue.ofMb(1).getBytes()),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(2).getBytes(), ByteSizeValue.ofGb(2).getBytes()),
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(ByteSizeValue.ofGb(8).getBytes()));
            assertThat(decision.get().requiredCapacity().total().memory().getBytes(), equalTo(ByteSizeValue.ofMb(8992).getBytes()));
        }
    }

    public void testScaleDown() {
        List<NodeLoad> nodeLoads = Arrays.asList(
            NodeLoad.builder("foo").setMaxMemory(DEFAULT_NODE_SIZE).incAssignedJobMemory(ByteSizeValue.ofGb(1).getBytes()).build(),
            NodeLoad.builder("bar").setMaxMemory(DEFAULT_NODE_SIZE).incAssignedJobMemory(ByteSizeValue.ofGb(1).getBytes()).build(),
            NodeLoad.builder("baz").setMaxMemory(DEFAULT_NODE_SIZE).incAssignedJobMemory(ByteSizeValue.ofGb(1).getBytes()).build()
        );

        MlAutoscalingDeciderService service = buildService();
        service.setMaxMachineMemoryPercent(25);
        MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder()
            .setPassedConfiguration(Settings.EMPTY)
            .setCurrentMlCapacity(AutoscalingCapacity.ZERO);
        {//Current capacity allows for smaller node
            Optional<AutoscalingDeciderResult> result = service.checkForScaleDown(nodeLoads,
                ByteSizeValue.ofMb(100).getBytes(),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(3).getBytes(), ByteSizeValue.ofGb(1).getBytes()),
                reasonBuilder);
            assertThat(result.isEmpty(), is(false));
            AutoscalingDeciderResult autoscalingDeciderResult = result.get();
            assertThat(autoscalingDeciderResult.requiredCapacity().node().memory().getBytes(),
                equalTo((ByteSizeValue.ofMb(100).getBytes() + OVERHEAD) * 4));
            assertThat(autoscalingDeciderResult.requiredCapacity().total().memory().getBytes(),
                equalTo(ByteSizeValue.ofGb(12).getBytes()));
        }
        {// Current capacity allows for smaller tier
            Optional<AutoscalingDeciderResult> result = service.checkForScaleDown(nodeLoads,
                ByteSizeValue.ofMb(100).getBytes(),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(4).getBytes(), ByteSizeValue.ofGb(1).getBytes()),
                reasonBuilder);
            assertThat(result.isEmpty(), is(false));
            AutoscalingDeciderResult autoscalingDeciderResult = result.get();
            assertThat(autoscalingDeciderResult.requiredCapacity().node().memory().getBytes(),
                equalTo((ByteSizeValue.ofMb(100).getBytes() + OVERHEAD) * 4));
            assertThat(autoscalingDeciderResult.requiredCapacity().total().memory().getBytes(),
                equalTo(ByteSizeValue.ofGb(12).getBytes()));
        }
        {// Scale down is not really possible
            Optional<AutoscalingDeciderResult> result = service.checkForScaleDown(nodeLoads,
                ByteSizeValue.ofMb(100).getBytes(),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(3).getBytes(), ByteSizeValue.ofMb(100).getBytes()),
                reasonBuilder);
            assertThat(result.isEmpty(), is(true));
        }
    }

    public void testEnsureScaleDown() {
        assertThat(
            MlAutoscalingDeciderService.ensureScaleDown(
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(8)),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(1))
                ),
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4)),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(2))
                )
            ), equalTo(new AutoscalingCapacity(
                new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4)),
                new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(1))
            ))
        );

        assertThat(
            MlAutoscalingDeciderService.ensureScaleDown(
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(8)),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(3))
                ),
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4)),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(2))
                )
            ), equalTo(new AutoscalingCapacity(
                new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4)),
                new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(2))
            ))
        );

        assertThat(
            MlAutoscalingDeciderService.ensureScaleDown(
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(4)),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(3))
                ),
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(3)),
                    new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(2))
                )
            ), equalTo(new AutoscalingCapacity(
                new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(3)),
                new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofGb(2))
            ))
        );
    }

    public void testFutureAvailableCapacity() {
        nodeLoadDetector = new NodeLoadDetector(mlMemoryTracker);
        MlAutoscalingDeciderService service = buildService();
        service.onMaster();
        service.setUseAuto(true);
        boolean waitingAnalytics = randomBoolean();
        boolean waitingAnomalyJobs = waitingAnalytics == false || randomBoolean();
        int maxWaitingAnalytics = randomIntBetween(1, 2);
        int maxWaitingAnomaly = randomIntBetween(1, 2);
        List<String> assignedAnomalyJobs = randomList(0, 2, () -> randomAlphaOfLength(10));
        List<String> batchAnomalyJobs = randomList(0, 2, () -> randomAlphaOfLength(10));
        List<String> assignedAnalyticsJobs = randomList(0, 2, () -> randomAlphaOfLength(10));
        ClusterState clusterState = clusterState(
            assignedAnomalyJobs,
            batchAnomalyJobs,
            assignedAnalyticsJobs,
            waitingAnomalyJobs ? randomList(1, maxWaitingAnomaly, () -> randomAlphaOfLength(10)) : Collections.emptyList(),
            waitingAnalytics ? randomList(1, maxWaitingAnalytics, () -> randomAlphaOfLength(10)) : Collections.emptyList()
        );

        Optional<NativeMemoryCapacity> nativeMemoryCapacity = service.calculateFutureAvailableCapacity(
            clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE),
            Duration.ofMillis(10),
            clusterState.getNodes().mastersFirstStream().collect(Collectors.toList()),
            clusterState
        );
        assertThat(nativeMemoryCapacity.isEmpty(), is(false));
        assertThat(nativeMemoryCapacity.get().getNode(), greaterThanOrEqualTo(DEFAULT_JOB_SIZE));
        assertThat(nativeMemoryCapacity.get().getNode(),
            lessThanOrEqualTo(NativeMemoryCalculator.allowedBytesForMl(DEFAULT_NODE_SIZE, 20, true)));
        assertThat(nativeMemoryCapacity.get().getTier(),
            greaterThanOrEqualTo(DEFAULT_JOB_SIZE * (assignedAnalyticsJobs.size() + batchAnomalyJobs.size())));
        assertThat(nativeMemoryCapacity.get().getTier(),
            lessThanOrEqualTo(3 * (NativeMemoryCalculator.allowedBytesForMl(DEFAULT_NODE_SIZE, 20, true))));
    }

    public void testScale_WithNoScaleUpButWaitingJobs() {
        nodeLoadDetector = new NodeLoadDetector(mlMemoryTracker);
        MlAutoscalingDeciderService service = buildService();
        service.onMaster();
        service.setUseAuto(true);
        boolean waitingAnalytics = randomBoolean();
        boolean waitingAnomalyJobs = waitingAnalytics == false || randomBoolean();
        int maxWaitingAnalytics = randomIntBetween(1, 2);
        int maxWaitingAnomaly = randomIntBetween(1, 2);
        ClusterState clusterState = clusterState(
            randomList(0, 2, () -> randomAlphaOfLength(10)),
            randomList(0, 2, () -> randomAlphaOfLength(10)),
            randomList(0, 2, () -> randomAlphaOfLength(10)),
            waitingAnomalyJobs ? randomList(1, maxWaitingAnomaly, () -> randomAlphaOfLength(10)) : Collections.emptyList(),
            waitingAnalytics ? randomList(1, maxWaitingAnalytics, () -> randomAlphaOfLength(10)) : Collections.emptyList()
        );

        Settings settings = Settings.builder()
            .put(MlAutoscalingDeciderService.NUM_ANALYTICS_JOBS_IN_QUEUE.getKey(), maxWaitingAnalytics)
            .put(MlAutoscalingDeciderService.NUM_ANOMALY_JOBS_IN_QUEUE.getKey(), maxWaitingAnomaly)
            .build();
        AutoscalingCapacity autoscalingCapacity = new AutoscalingCapacity(
            new AutoscalingCapacity.AutoscalingResources(ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1)),
            new AutoscalingCapacity.AutoscalingResources(ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1))
        );

        DeciderContext deciderContext = new DeciderContext(clusterState, autoscalingCapacity);

        AutoscalingDeciderResult result = service.scale(settings, deciderContext);
        assertThat(
            result.reason().summary(),
            containsString("but the number in the queue is less than the configured maximum allowed")
        );
        assertThat(result.requiredCapacity(), equalTo(autoscalingCapacity));
    }

    private MlAutoscalingDeciderService buildService() {
        return new MlAutoscalingDeciderService(nodeLoadDetector, settings, clusterService, timeSupplier);
    }

    private static ClusterState clusterState(List<String> anomalyTasks,
                                             List<String> batchAnomalyTasks,
                                             List<String> analyticsTasks,
                                             List<String> waitingAnomalyTasks,
                                             List<String> waitingAnalyticsTasks) {
        List<String> nodeNames = Arrays.asList("_node_id1", "_node_id2", "_node_id3");
        List<DiscoveryNode> nodeList = withMlNodes(nodeNames.toArray(String[]::new));
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        for (DiscoveryNode node : nodeList) {
            nodesBuilder.add(node);
        }
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        for (String jobId : anomalyTasks) {
            OpenJobPersistentTasksExecutorTests.addJobTask(jobId,
                randomFrom(nodeNames),
                randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
                tasksBuilder);
        }
        for (String jobId : batchAnomalyTasks) {
            String nodeAssignment = randomFrom(nodeNames);
            OpenJobPersistentTasksExecutorTests.addJobTask(jobId,
                nodeAssignment,
                randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
                tasksBuilder);
            StartDatafeedAction.DatafeedParams dfParams =new StartDatafeedAction.DatafeedParams(jobId + "-datafeed", 0);
            dfParams.setEndTime(new Date().getTime());
            tasksBuilder.addTask(
                MlTasks.datafeedTaskId(jobId + "-datafeed"),
                MlTasks.DATAFEED_TASK_NAME,
                dfParams,
                new PersistentTasksCustomMetadata.Assignment(nodeAssignment, "test"));
        }
        for (String analyticsId : analyticsTasks) {
            addAnalyticsTask(analyticsId,
                randomFrom(nodeNames),
                randomFrom(
                    DataFrameAnalyticsState.STARTED,
                    DataFrameAnalyticsState.REINDEXING,
                    DataFrameAnalyticsState.ANALYZING,
                    DataFrameAnalyticsState.STOPPING,
                    DataFrameAnalyticsState.STARTING
                ),
                tasksBuilder);
        }
        for (String job : waitingAnalyticsTasks) {
            addAnalyticsTask(job, null, null, tasksBuilder);
        }
        for (String job : waitingAnomalyTasks) {
            addJobTask(job, null, null, tasksBuilder);
        }
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();
        ClusterState.Builder cs = ClusterState.builder(new ClusterName("_name"));
        cs.nodes(nodesBuilder);
        Metadata.Builder metadata = Metadata.builder();
        metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        cs.metadata(metadata);
        return cs.build();
    }

    private static List<DiscoveryNode> withMlNodes(String... nodeName) {
        return Arrays.stream(nodeName)
            .map(n -> new DiscoveryNode(
                n,
                buildNewFakeTransportAddress(),
                MapBuilder.<String, String>newMapBuilder()
                    .put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, String.valueOf(DEFAULT_NODE_SIZE))
                    .put(MachineLearning.MAX_JVM_SIZE_NODE_ATTR, String.valueOf(DEFAULT_JVM_SIZE))
                    .put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, String.valueOf(10))
                    .map(),
                new HashSet<>(Arrays.asList(DiscoveryNodeRole.MASTER_ROLE)),
                Version.CURRENT))
            .collect(Collectors.toList());
    }

    public static void addAnalyticsTask(String jobId,
                                        String nodeId,
                                        DataFrameAnalyticsState jobState,
                                        PersistentTasksCustomMetadata.Builder builder) {
        builder.addTask(
            MlTasks.dataFrameAnalyticsTaskId(jobId),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams(jobId, Version.CURRENT, true),
            nodeId == null ? AWAITING_LAZY_ASSIGNMENT : new PersistentTasksCustomMetadata.Assignment(nodeId, "test assignment")
        );
        if (jobState != null) {
            builder.updateTaskState(
                MlTasks.dataFrameAnalyticsTaskId(jobId),
                new DataFrameAnalyticsTaskState(jobState, builder.getLastAllocationId(), null)
            );
        }
    }

    public static void addJobTask(String jobId,
                                  String nodeId,
                                  JobState jobState,
                                  PersistentTasksCustomMetadata.Builder builder) {
        builder.addTask(
            MlTasks.jobTaskId(jobId),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams(jobId),
            nodeId == null ? AWAITING_LAZY_ASSIGNMENT : new PersistentTasksCustomMetadata.Assignment(nodeId, "test assignment"));
        if (jobState != null) {
            builder.updateTaskState(MlTasks.jobTaskId(jobId),
                new JobTaskState(jobState, builder.getLastAllocationId(), null));
        }
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
    }

}
