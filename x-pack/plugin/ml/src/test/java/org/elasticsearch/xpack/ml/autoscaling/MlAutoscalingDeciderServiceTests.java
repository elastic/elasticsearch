/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlAutoscalingDeciderServiceTests extends ESTestCase {

    private static final Function<String, Assignment> WAITING_ASSIGNMENT_FUNC = (s) -> AWAITING_LAZY_ASSIGNMENT;
    private static final long DEFAULT_NODE_SIZE = new ByteSizeValue(1, ByteSizeUnit.GB).getBytes();
    private static final long DEFAULT_JOB_SIZE = new ByteSizeValue(1, ByteSizeUnit.GB).getBytes();
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
            () -> service.scale(new MlAutoscalingDeciderConfiguration(0, 0, TimeValue.ZERO),
                mock(AutoscalingDeciderContext.class)));
        assertThat(iae.getMessage(), equalTo("request for scaling information is only allowed on the master node"));
    }

    public void testScaleUp_withNoJobsWaiting() {
        MlAutoscalingDeciderService service = buildService();
        service.onMaster();

        assertThat(service.checkForScaleUp(
            new MlAutoscalingDeciderConfiguration(0, 0, TimeValue.ZERO),
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
        MlAutoscalingDeciderService service = buildService();
        service.setMaxMachineMemoryPercent(25);
        { // No time in queue
            MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder();
            Optional<AutoscalingDecision> decision = service.checkForScaleUp(
                new MlAutoscalingDeciderConfiguration(0, 0, TimeValue.ZERO),
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(DEFAULT_JOB_SIZE * 4));
            assertThat(decision.get().requiredCapacity().tier().memory().getBytes(), equalTo(12 * DEFAULT_JOB_SIZE));
        }
        { // we allow one job in the analytics queue
            MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder();
            Optional<AutoscalingDecision> decision = service.checkForScaleUp(
                new MlAutoscalingDeciderConfiguration(0, 1, TimeValue.ZERO),
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(4 * DEFAULT_JOB_SIZE));
            assertThat(decision.get().requiredCapacity().tier().memory().getBytes(), equalTo(8 * DEFAULT_JOB_SIZE));
        }
        { // we allow one job in the anomaly queue and analytics queue
            MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder();
            Optional<AutoscalingDecision> decision = service.checkForScaleUp(
                new MlAutoscalingDeciderConfiguration(1, 1, TimeValue.ZERO),
                jobTasks,
                analytics,
                null,
                NativeMemoryCapacity.ZERO,
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(4 * DEFAULT_JOB_SIZE));
            assertThat(decision.get().requiredCapacity().tier().memory().getBytes(), equalTo(4 * DEFAULT_JOB_SIZE));
        }
    }

    public void testScaleUp_withWaitingJobs_WithFutureCapacity() {
        List<String> jobTasks = Arrays.asList("waiting_job", "waiting_job_2");
        List<String> analytics = Arrays.asList("analytics_waiting");
        MlAutoscalingDeciderService service = buildService();
        service.setMaxMachineMemoryPercent(25);
        { // with null future capacity and current capacity has a small node
            MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder();
            Optional<AutoscalingDecision> decision = service.checkForScaleUp(
                new MlAutoscalingDeciderConfiguration(2, 1, TimeValue.ZERO),
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
            MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder();
            Optional<AutoscalingDecision> decision = service.checkForScaleUp(
                new MlAutoscalingDeciderConfiguration(2, 1, TimeValue.ZERO),
                jobTasks,
                analytics,
                new NativeMemoryCapacity(ByteSizeValue.ofGb(3).getBytes(), ByteSizeValue.ofGb(1).getBytes()),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(2).getBytes(), ByteSizeValue.ofGb(2).getBytes()),
                reasonBuilder);
            assertTrue(decision.isEmpty());
        }
        {
            MlScalingReason.Builder reasonBuilder = new MlScalingReason.Builder();
            Optional<AutoscalingDecision> decision = service.checkForScaleUp(
                new MlAutoscalingDeciderConfiguration(2, 1, TimeValue.ZERO),
                jobTasks,
                analytics,
                new NativeMemoryCapacity(ByteSizeValue.ofMb(1).getBytes(), ByteSizeValue.ofMb(1).getBytes()),
                new NativeMemoryCapacity(ByteSizeValue.ofGb(2).getBytes(), ByteSizeValue.ofGb(2).getBytes()),
                reasonBuilder);
            assertFalse(decision.isEmpty());
            assertThat(decision.get().requiredCapacity().node().memory().getBytes(), equalTo(ByteSizeValue.ofGb(8).getBytes()));
            assertThat(decision.get().requiredCapacity().tier().memory().getBytes(), equalTo(ByteSizeValue.ofGb(12).getBytes()));
        }
    }

    public void testScaleDown() {

    }

    private MlAutoscalingDeciderService buildService() {
        return new MlAutoscalingDeciderService(nodeLoadDetector, settings, clusterService, timeSupplier);
    }

    private static List<DiscoveryNode> withMlNodes(String... nodeName) {
        return Arrays.stream(nodeName)
            .map(n -> new DiscoveryNode(
                n,
                buildNewFakeTransportAddress(),
                Collections.singletonMap(MachineLearning.MACHINE_MEMORY_NODE_ATTR, String.valueOf(DEFAULT_NODE_SIZE)),
                new HashSet<>(Arrays.asList(DiscoveryNodeRole.MASTER_ROLE)),
                Version.CURRENT))
            .collect(Collectors.toList());
    }

    private static List<PersistentTask<?>> anomalyTasks(Function<String, Assignment> assignmentFunction, String... jobIds) {
        return Arrays.stream(jobIds).map(jobId ->
            new PersistentTask<>(
                MlTasks.jobTaskId(jobId),
                MlTasks.JOB_TASK_NAME,
                new OpenJobAction.JobParams(jobId),
                randomLongBetween(1, 1000),
                assignmentFunction.apply(jobId))
        ).collect(Collectors.toList());
    }

    private static List<PersistentTask<?>> analyticsTasks(Function<String, Assignment> assignmentFunction, String... analyticsIds) {
        return Arrays.stream(analyticsIds).map(id ->
            new PersistentTask<>(
                MlTasks.dataFrameAnalyticsTaskId(id),
                MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
                new StartDataFrameAnalyticsAction.TaskParams(id, Version.CURRENT, Collections.emptyList(), true),
                randomLongBetween(1, 1000),
                assignmentFunction.apply(id))
        ).collect(Collectors.toList());
    }

}
