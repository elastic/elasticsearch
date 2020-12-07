/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlAutoscalingDeciderServiceTests extends ESTestCase {

    private static final long DEFAULT_NODE_SIZE = ByteSizeValue.ofGb(2).getBytes();
    private static final long DEFAULT_JVM_SIZE = ByteSizeValue.ofMb((long)(DEFAULT_NODE_SIZE * 0.25)).getBytes();
    private static final long DEFAULT_JOB_SIZE = ByteSizeValue.ofMb(200).getBytes();
    private static final long OVERHEAD = ByteSizeValue.ofMb(30).getBytes();
    private NodeLoadDetector nodeLoadDetector;
    private ClusterService clusterService;
    private Settings settings;
    private Supplier<Long> timeSupplier;

    @Before
    public void setup() {
        MlMemoryTracker mlMemoryTracker = mock(MlMemoryTracker.class);
        when(mlMemoryTracker.asyncRefresh()).thenReturn(true);
        when(mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(any())).thenReturn(DEFAULT_JOB_SIZE);
        when(mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(any())).thenReturn(DEFAULT_JOB_SIZE);
        nodeLoadDetector = mock(NodeLoadDetector.class);
        when(nodeLoadDetector.getMlMemoryTracker()).thenReturn(mlMemoryTracker);
        clusterService = mock(ClusterService.class);
        settings = Settings.EMPTY;
        timeSupplier = System::currentTimeMillis;
        ClusterSettings cSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, MachineLearning.MAX_OPEN_JOBS_PER_NODE));
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

        assertThat(service.checkForScaleUp(0, 0,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            NativeMemoryCapacity.ZERO,
            MlScalingReason.builder()),
            equalTo(Optional.empty()));
    }

    public void testScaleUp_withWaitingJobs() {
        List<String> jobTasks = Arrays.asList("waiting_job", "waiting_job_2");
        List<String> analytics = Arrays.asList("analytics_waiting");
        MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder()
            .setPassedConfiguration(Settings.EMPTY)
            .setCurrentMlCapacity(AutoscalingCapacity.ZERO);
        MlAutoscalingDeciderService service = buildService();
        service.setMaxMachineMemoryPercent(25);
        { // No time in queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(0, 0,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo((DEFAULT_JOB_SIZE + OVERHEAD) * 4));
            assertThat(decision.get().requiredCapacity().tier().memory().getBytes(), equalTo(12 * DEFAULT_JOB_SIZE));
        }
        { // we allow one job in the analytics queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(0, 1,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(4 * (DEFAULT_JOB_SIZE + OVERHEAD)));
            assertThat(decision.get().requiredCapacity().tier().memory().getBytes(), equalTo(8 * DEFAULT_JOB_SIZE));
        }
        { // we allow one job in the anomaly queue and analytics queue
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(1, 1,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(4 * (DEFAULT_JOB_SIZE + OVERHEAD)));
            assertThat(decision.get().requiredCapacity().tier().memory().getBytes(), equalTo(4 * DEFAULT_JOB_SIZE));
        }
    }

    public void testScaleUp_withWaitingJobs_WithFutureCapacity() {
        List<String> jobTasks = Arrays.asList("waiting_job", "waiting_job_2");
        List<String> analytics = Arrays.asList("analytics_waiting");
        MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder()
            .setPassedConfiguration(Settings.EMPTY)
            .setCurrentMlCapacity(AutoscalingCapacity.ZERO);
        MlAutoscalingDeciderService service = buildService();
        service.setMaxMachineMemoryPercent(25);
        { // with null future capacity and current capacity has a small node
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(2, 1,
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(DEFAULT_JOB_SIZE * 4));
            assertThat(decision.get().requiredCapacity().tier().memory().getBytes(), equalTo(DEFAULT_JOB_SIZE * 4));
        }
        {
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(2, 1,
                jobTasks,
                analytics,
                new NativeMemoryCapacity(ByteSizeValue.ofGb(3).getBytes(), ByteSizeValue.ofGb(1).getBytes()),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(2).getBytes(), ByteSizeValue.ofGb(2).getBytes()),
                reasonBuilder);
            assertTrue(decision.isEmpty());
        }
        {
            Optional<AutoscalingDeciderResult> decision = service.checkForScaleUp(2, 1,
                jobTasks,
                analytics,
                new NativeMemoryCapacity(ByteSizeValue.ofMb(1).getBytes(), ByteSizeValue.ofMb(1).getBytes()),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(2).getBytes(), ByteSizeValue.ofGb(2).getBytes()),
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(ByteSizeValue.ofGb(8).getBytes()));
            assertThat(decision.get().requiredCapacity().tier().memory().getBytes(), equalTo(ByteSizeValue.ofMb(8992).getBytes()));
        }
    }

    public void testScaleDown_WithDetectionError() {
        List<DiscoveryNode> nodes = withMlNodes("foo", "bar", "baz");
        DiscoveryNode badNode = randomFrom(nodes);
        NodeLoad badLoad = NodeLoad.builder(badNode.getId()).setError("bad node").build();
        when(nodeLoadDetector.detectNodeLoad(any(), anyBoolean(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
            .thenReturn(NodeLoad.builder(badNode.getId()).setUseMemory(true).build());
        when(nodeLoadDetector.detectNodeLoad(any(), anyBoolean(), eq(badNode), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
            .thenReturn(badLoad);

        MlAutoscalingDeciderService service = buildService();
        MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder();
        assertThat(service.checkForScaleDown(nodes,
            ClusterState.EMPTY_STATE,
            Long.MAX_VALUE,
            new NativeMemoryCapacity(ByteSizeValue.ofGb(3).getBytes(), ByteSizeValue.ofGb(1).getBytes()),
            reasonBuilder).isEmpty(), is(true));
    }

    public void testScaleDown_WhenMemoryIsInaccurate() {
        List<DiscoveryNode> nodes = withMlNodes("foo", "bar", "baz");
        DiscoveryNode badNode = randomFrom(nodes);
        NodeLoad badLoad = NodeLoad.builder(badNode.getId()).setUseMemory(false).build();
        when(nodeLoadDetector.detectNodeLoad(any(), anyBoolean(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
            .thenReturn(NodeLoad.builder(badNode.getId()).setUseMemory(true).build());
        when(nodeLoadDetector.detectNodeLoad(any(), anyBoolean(), eq(badNode), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
            .thenReturn(badLoad);

        MlAutoscalingDeciderService service = buildService();
        MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder();
        expectThrows(AssertionError.class, () -> service.checkForScaleDown(nodes,
            ClusterState.EMPTY_STATE,
            Long.MAX_VALUE,
            new NativeMemoryCapacity(ByteSizeValue.ofGb(3).getBytes(), ByteSizeValue.ofGb(1).getBytes()),
            reasonBuilder));
    }

    public void testScaleDown() {
        List<DiscoveryNode> nodes = withMlNodes("foo", "bar", "baz");
        when(nodeLoadDetector.detectNodeLoad(any(), anyBoolean(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean()))
            .thenReturn(NodeLoad.builder("any")
                .setUseMemory(true)
                .incAssignedJobMemory(ByteSizeValue.ofGb(1).getBytes())
                .build());
        MlAutoscalingDeciderService service = buildService();
        service.setMaxMachineMemoryPercent(25);
        MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder()
            .setPassedConfiguration(Settings.EMPTY)
            .setCurrentMlCapacity(AutoscalingCapacity.ZERO);
        {//Current capacity allows for smaller node
            Optional<AutoscalingDeciderResult> result = service.checkForScaleDown(nodes,
                ClusterState.EMPTY_STATE,
                ByteSizeValue.ofMb(100).getBytes(),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(3).getBytes(), ByteSizeValue.ofGb(1).getBytes()),
                reasonBuilder);
            assertThat(result.isEmpty(), is(false));
            AutoscalingDeciderResult autoscalingDeciderResult = result.get();
            assertThat(autoscalingDeciderResult.requiredCapacity().node().memory().getBytes(),
                equalTo((ByteSizeValue.ofMb(100).getBytes() + OVERHEAD) * 4));
            assertThat(autoscalingDeciderResult.requiredCapacity().tier().memory().getBytes(),
                equalTo(ByteSizeValue.ofGb(12).getBytes()));
        }
        {// Current capacity allows for smaller tier
            Optional<AutoscalingDeciderResult> result = service.checkForScaleDown(nodes,
                ClusterState.EMPTY_STATE,
                ByteSizeValue.ofMb(100).getBytes(),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(4).getBytes(), ByteSizeValue.ofMb(100).getBytes()),
                reasonBuilder);
            assertThat(result.isEmpty(), is(false));
            AutoscalingDeciderResult autoscalingDeciderResult = result.get();
            assertThat(autoscalingDeciderResult.requiredCapacity().node().memory().getBytes(),
                equalTo((ByteSizeValue.ofMb(100).getBytes() + OVERHEAD) * 4));
            assertThat(autoscalingDeciderResult.requiredCapacity().tier().memory().getBytes(),
                equalTo(ByteSizeValue.ofGb(12).getBytes()));
        }
        {// Scale down is not really possible
            Optional<AutoscalingDeciderResult> result = service.checkForScaleDown(nodes,
                ClusterState.EMPTY_STATE,
                ByteSizeValue.ofMb(100).getBytes(),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(3).getBytes(), ByteSizeValue.ofMb(100).getBytes()),
                reasonBuilder);
            assertThat(result.isEmpty(), is(true));
        }
    }

    private MlAutoscalingDeciderService buildService() {
        return new MlAutoscalingDeciderService(nodeLoadDetector, settings, clusterService, timeSupplier);
    }

    private static List<DiscoveryNode> withMlNodes(String... nodeName) {
        return Arrays.stream(nodeName)
            .map(n -> new DiscoveryNode(
                n,
                buildNewFakeTransportAddress(),
                MapBuilder.<String, String>newMapBuilder()
                    .put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, String.valueOf(DEFAULT_NODE_SIZE))
                    .put(MachineLearning.MAX_JVM_SIZE_NODE_ATTR, String.valueOf(DEFAULT_JVM_SIZE))
                    .map(),
                new HashSet<>(Arrays.asList(DiscoveryNodeRole.MASTER_ROLE)),
                Version.CURRENT))
            .collect(Collectors.toList());
    }

}
