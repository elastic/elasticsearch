/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisionType;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlAutoscalingDeciderServiceTests extends ESTestCase {

    private static final Function<String, Assignment> WAITING_ASSIGNMENT_FUNC = (s) -> AWAITING_LAZY_ASSIGNMENT;
    private NodeLoadDetector nodeLoadDetector;
    private ClusterService clusterService;
    private Settings settings;
    private Supplier<Long> timeSupplier;

    @Before
    public void setup() {
        MlMemoryTracker mlMemoryTracker = mock(MlMemoryTracker.class);
        when(mlMemoryTracker.asyncRefresh()).thenReturn(true);
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
            () -> service.scale(new MlAutoscalingDeciderConfiguration(1, TimeValue.ZERO, TimeValue.ZERO),
                mock(AutoscalingDeciderContext.class)));
        assertThat(iae.getMessage(), equalTo("request for scaling information is only allowed on the master node"));
    }

    public void testScaleUp_withNoJobsWaiting() {
        int minNodes = randomIntBetween(1, 10);
        int numAnomaly = randomIntBetween(1, 20);
        int numAnalytics = randomIntBetween(1, 20);
        String[] nodeNames = Stream.generate(() -> randomAlphaOfLength(10)).limit(minNodes).toArray(String[]::new);
        List<DiscoveryNode> nodes = withMlNodes(nodeNames);
        List<PersistentTask<?>> jobTasks = anomalyTasks((s) -> new Assignment(randomFrom(nodeNames), ""),
            Stream.generate(() -> randomAlphaOfLength(10)).limit(numAnomaly).toArray(String[]::new));
        List<PersistentTask<?>> analytics = analyticsTasks((s) -> new Assignment(randomFrom(nodeNames), ""),
            Stream.generate(() -> randomAlphaOfLength(10)).limit(numAnalytics).toArray(String[]::new));

        MlAutoscalingDeciderService service = buildService();
        service.onMaster();

        AutoscalingDecision decision =  service.checkForScaleUp(
            new MlAutoscalingDeciderConfiguration(minNodes, TimeValue.ZERO, TimeValue.ZERO),
            nodes,
            jobTasks,
            analytics,
            1000L);
        assertThat(decision.type(), equalTo(AutoscalingDecisionType.NO_SCALE));
    }

    public void testScaleUp_withBelowMinNodes() {
        int minNodes = randomIntBetween(1, 10);
        int numAnomaly = randomIntBetween(1, 20);
        int numAnalytics = randomIntBetween(1, 20);
        String[] nodeNames = Stream.generate(() -> randomAlphaOfLength(10)).limit(minNodes - 1).toArray(String[]::new);
        List<DiscoveryNode> nodes = withMlNodes(nodeNames);
        List<PersistentTask<?>> jobTasks = anomalyTasks((s) -> new Assignment(randomFrom(nodeNames), ""),
            Stream.generate(() -> randomAlphaOfLength(10)).limit(numAnomaly).toArray(String[]::new));
        List<PersistentTask<?>> analytics = analyticsTasks((s) -> new Assignment(randomFrom(nodeNames), ""),
            Stream.generate(() -> randomAlphaOfLength(10)).limit(numAnalytics).toArray(String[]::new));

        MlAutoscalingDeciderService service = buildService();
        service.onMaster();

        AutoscalingDecision decision =  service.checkForScaleUp(
            new MlAutoscalingDeciderConfiguration(minNodes, TimeValue.ZERO, TimeValue.ZERO),
            nodes,
            jobTasks,
            analytics,
            1000L);
        assertThat(decision.type(), equalTo(AutoscalingDecisionType.SCALE_UP));
        assertThat(decision.reason(), containsString("number of machine learning nodes"));
    }

    public void testScaleUp_withWaitingAnomalyJobs() {
        int minNodes = randomIntBetween(1, 10);
        String[] nodeNames = Stream.generate(() -> randomAlphaOfLength(10)).limit(minNodes).toArray(String[]::new);
        List<DiscoveryNode> nodes = withMlNodes(nodeNames);
        List<PersistentTask<?>> jobTasks = anomalyTasks(
            (s) -> s.contains("waiting") ? AWAITING_LAZY_ASSIGNMENT : new Assignment(randomFrom(nodeNames), ""),
            "assigned1", "waiting_job", "waiting_job_2");
        List<PersistentTask<?>> analytics = analyticsTasks(
            (s) -> s.contains("waiting") ? AWAITING_LAZY_ASSIGNMENT : new Assignment(randomFrom(nodeNames), ""),
            "analytics_assigned", "analytics_waiting");
        MlAutoscalingDeciderService service = buildService();
        { // No time in queue
            AutoscalingDecision decision = service.checkForScaleUp(
                new MlAutoscalingDeciderConfiguration(minNodes, TimeValue.ZERO, TimeValue.ZERO),
                nodes,
                jobTasks,
                analytics,
                100L);
            assertThat(decision.type(), equalTo(AutoscalingDecisionType.SCALE_UP));
            assertThat(decision.reason(), containsString("have been waiting for assignment too long"));
            assertThat(decision.reason(), containsString("waiting_job"));
            assertThat(decision.reason(), containsString("waiting_job_2"));
            assertThat(decision.reason(), containsString("analytics_waiting"));
        }
        { // Allowed time in queue
            MlAutoscalingDeciderConfiguration config = new MlAutoscalingDeciderConfiguration(minNodes,
                TimeValue.timeValueMillis(200),
                TimeValue.timeValueMillis(300));
            AutoscalingDecision decision = service.checkForScaleUp(
                config,
                nodes,
                jobTasks,
                analytics,
                100L);

            assertThat(decision.type(), equalTo(AutoscalingDecisionType.NO_SCALE));

            decision = service.checkForScaleUp(
                config,
                nodes,
                jobTasks,
                analytics,
                101L);
            assertThat(decision.type(), equalTo(AutoscalingDecisionType.SCALE_UP));
            assertThat(decision.reason(), containsString("have been waiting for assignment too long"));
            assertThat(decision.reason(), containsString("waiting_job"));
            assertThat(decision.reason(), containsString("waiting_job_2"));
            assertThat(decision.reason(), not(containsString("analytics_waiting")));

            decision = service.checkForScaleUp(
                config,
                nodes,
                jobTasks,
                analytics,
                301L);
            assertThat(decision.type(), equalTo(AutoscalingDecisionType.SCALE_UP));
            assertThat(decision.reason(), containsString("have been waiting for assignment too long"));
            assertThat(decision.reason(), containsString("waiting_job"));
            assertThat(decision.reason(), containsString("waiting_job_2"));
            assertThat(decision.reason(), containsString("analytics_waiting"));
        }
    }

    public void testScaleDown() {

    }

    private MlAutoscalingDeciderService buildService() {
        return new MlAutoscalingDeciderService(nodeLoadDetector, settings, clusterService, timeSupplier);
    }

    private static List<DiscoveryNode> withMlNodes(String... nodeName) {
        return Arrays.stream(nodeName)
            .map(n -> new DiscoveryNode(n, buildNewFakeTransportAddress(), Version.CURRENT))
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
