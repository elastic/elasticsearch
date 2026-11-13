/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.decider.EnableAssignmentDecider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.ml.MlAssignmentNotifier.StuckDatafeed;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.junit.Before;

import java.net.InetAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlAssignmentNotifierDatafeedWatchdogTests extends ESTestCase {

    private static final String JOB_ID = "job-1";
    private static final String DATAFEED_ID = "df-1";
    private static final String ML_NODE = "ml-1";
    private static final Duration JUST_OVER_THRESHOLD = MlAssignmentNotifier.LONG_TIME_UNASSIGNED_INTERVAL.plusMinutes(1);
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private MlAssignmentNotifier notifier;

    @Before
    public void setupNotifier() {
        threadPool = mock(ThreadPool.class);
        clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(anyString())).thenReturn(executorService);
        notifier = new MlAssignmentNotifier(
            mock(AnomalyDetectionAuditor.class),
            mock(DataFrameAnalyticsAuditor.class),
            threadPool,
            clusterService,
            resolver
        );
    }

    public void testUnassignedDatafeedPastThresholdWithOpenJobShouldBeReported() {
        ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED), ML_NODE);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0, state)), empty());
        List<StuckDatafeed> stuck = notifier.findStartedDatafeedsWithoutTask(T0.plus(JUST_OVER_THRESHOLD), state);

        assertThat(ids(stuck), contains(DATAFEED_ID));
        assertThat(stuck.get(0).minutesWithoutTask(), equalTo(16L));
        assertThat(stuck.get(0).jobId(), equalTo(JOB_ID));
    }

    public void testStartingDatafeedPastThresholdWithOpenJobShouldBeReported() {
        for (DatafeedState datafeedState : new DatafeedState[] { DatafeedState.STARTING, null }) {
            MlAssignmentNotifier fresh = newNotifier();
            ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, datafeedState), ML_NODE);

            assertThat(ids(fresh.findStartedDatafeedsWithoutTask(T0, state)), empty());
            assertThat(ids(fresh.findStartedDatafeedsWithoutTask(T0.plus(JUST_OVER_THRESHOLD), state)), contains(DATAFEED_ID));
        }
    }

    public void testUnassignedDatafeedBelowThresholdShouldNotBeReported() {
        ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED), ML_NODE);

        notifier.findStartedDatafeedsWithoutTask(T0, state);

        assertThat(
            ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(MlAssignmentNotifier.LONG_TIME_UNASSIGNED_INTERVAL), state)),
            empty()
        );
    }

    public void testReportedDatafeedShouldBeRepeatedOnlyAfterReportInterval() {
        ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED), ML_NODE);
        Instant firstReport = T0.plus(JUST_OVER_THRESHOLD);

        notifier.findStartedDatafeedsWithoutTask(T0, state);
        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(firstReport, state)), contains(DATAFEED_ID));

        assertThat(
            ids(notifier.findStartedDatafeedsWithoutTask(firstReport.plus(MlAssignmentNotifier.MIN_REPORT_INTERVAL), state)),
            empty()
        );
        assertThat(
            ids(notifier.findStartedDatafeedsWithoutTask(firstReport.plus(MlAssignmentNotifier.MIN_REPORT_INTERVAL).plusSeconds(1), state)),
            contains(DATAFEED_ID)
        );
    }

    public void testJobNotOpenedShouldNotReportDatafeed() {
        List<Tasks> scenarios = List.of(
            new Tasks().job(ML_NODE, JobState.OPENING).datafeed(null, DatafeedState.STARTED),
            // job waiting for capacity or lazily unassigned
            new Tasks().job(null, JobState.OPENED).datafeed(null, DatafeedState.STARTED),
            new Tasks().job(null, null).datafeed(null, DatafeedState.STARTED),
            // job closing or failed
            new Tasks().job(ML_NODE, JobState.CLOSING).datafeed(null, DatafeedState.STARTED),
            new Tasks().job(ML_NODE, JobState.FAILED).datafeed(null, DatafeedState.STARTED),
            // no job task at all
            new Tasks().datafeed(null, DatafeedState.STARTED)
        );
        for (Tasks scenario : scenarios) {
            MlAssignmentNotifier fresh = newNotifier();
            ClusterState state = state(scenario, ML_NODE);
            fresh.findStartedDatafeedsWithoutTask(T0, state);
            assertThat(ids(fresh.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), state)), empty());
        }
    }

    public void testJobOpenedOnNodeThatLeftShouldNotReportDatafeed() {
        ClusterState state = state(new Tasks().job("gone-node", JobState.OPENED).datafeed(null, DatafeedState.STARTED), ML_NODE);

        notifier.findStartedDatafeedsWithoutTask(T0, state);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), state)), empty());
    }

    public void testStoppingOrStoppedDatafeedShouldNotBeReported() {
        for (DatafeedState datafeedState : new DatafeedState[] { DatafeedState.STOPPING, DatafeedState.STOPPED }) {
            MlAssignmentNotifier fresh = newNotifier();
            ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, datafeedState), ML_NODE);
            fresh.findStartedDatafeedsWithoutTask(T0, state);
            assertThat(ids(fresh.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), state)), empty());
        }
    }

    public void testDatafeedAssignedToLiveNodeShouldNotBeReported() {
        ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(ML_NODE, DatafeedState.STARTED), ML_NODE);

        notifier.findStartedDatafeedsWithoutTask(T0, state);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), state)), empty());
    }

    public void testDatafeedAssignedToAbsentNodePastThresholdShouldBeReported() {
        ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed("gone-node", DatafeedState.STARTED), ML_NODE);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0, state)), empty());

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(JUST_OVER_THRESHOLD), state)), contains(DATAFEED_ID));
    }

    public void testDatafeedReassignedWithinThresholdShouldRestartTimer() {
        ClusterState absent = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed("gone-node", DatafeedState.STARTED), ML_NODE);
        ClusterState reassigned = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(ML_NODE, DatafeedState.STARTED), ML_NODE);

        notifier.findStartedDatafeedsWithoutTask(T0, absent);
        notifier.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofMinutes(5)), reassigned);
        // absent again 14 minutes later: a new window starts, so nothing is reported 16 minutes after the original sighting
        notifier.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofMinutes(14)), absent);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(JUST_OVER_THRESHOLD), absent)), empty());
    }

    public void testMasterChangeShouldResetTimers() {
        ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED), ML_NODE);
        notifier.findStartedDatafeedsWithoutTask(T0, state);

        ClusterState notMaster = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).masterNodeId(null).build())
            .build();
        notifier.clusterChanged(new ClusterChangedEvent("test", notMaster, state));

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(JUST_OVER_THRESHOLD), state)), empty());
    }

    public void testDatafeedBlockedBySourceIndexShouldNotBeReported() {
        ClusterState state = state(
            new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED, List.of("missing-index-*")),
            ML_NODE
        );

        notifier.findStartedDatafeedsWithoutTask(T0, state);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), state)), empty());
    }

    public void testJobNodeShuttingDownShouldNotReportDatafeed() {
        SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(ML_NODE)
            .setNodeEphemeralId(ML_NODE)
            .setType(SingleNodeShutdownMetadata.Type.RESTART)
            .setReason("test")
            .setStartedAtMillis(1L)
            .build();
        ClusterState state = state(
            new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED),
            Metadata.builder().putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(Map.of(ML_NODE, shutdown))),
            ML_NODE
        );

        notifier.findStartedDatafeedsWithoutTask(T0, state);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), state)), empty());
    }

    public void testUpgradeOrResetModeShouldNotReportDatafeed() {
        for (MlMetadata mlMetadata : List.of(
            new MlMetadata.Builder().isUpgradeMode(true).build(),
            new MlMetadata.Builder().isResetMode(true).build()
        )) {
            MlAssignmentNotifier fresh = newNotifier();
            ClusterState state = state(
                new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED),
                Metadata.builder().putCustom(MlMetadata.TYPE, mlMetadata),
                ML_NODE
            );
            fresh.findStartedDatafeedsWithoutTask(T0, state);
            assertThat(ids(fresh.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), state)), empty());
        }
    }

    public void testPersistentTaskAssignmentDisabledInClusterSettingsShouldNotReportDatafeed() {
        Settings disabled = Settings.builder()
            .put(EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey(), "none")
            .build();
        ClusterState state = state(
            new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED),
            Metadata.builder().persistentSettings(disabled),
            ML_NODE
        );

        notifier.findStartedDatafeedsWithoutTask(T0, state);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), state)), empty());
    }

    public void testPersistentTaskAssignmentDisabledInNodeSettingsShouldNotReportDatafeed() {
        when(clusterService.getSettings()).thenReturn(
            Settings.builder().put(EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey(), "none").build()
        );
        ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED), ML_NODE);

        notifier.findStartedDatafeedsWithoutTask(T0, state);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), state)), empty());
    }

    public void testPersistentTaskAssignmentReenabledInClusterSettingsShouldReportDatafeed() {
        when(clusterService.getSettings()).thenReturn(
            Settings.builder().put(EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey(), "none").build()
        );
        Settings enabled = Settings.builder().put(EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey(), "all").build();
        ClusterState state = state(
            new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED),
            Metadata.builder().persistentSettings(enabled),
            ML_NODE
        );

        notifier.findStartedDatafeedsWithoutTask(T0, state);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(JUST_OVER_THRESHOLD), state)), contains(DATAFEED_ID));
    }

    public void testStateNotRecoveredShouldNotReportDatafeedAndShouldClearTimers() {
        ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED), ML_NODE);
        ClusterState notRecovered = ClusterState.builder(state)
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        notifier.findStartedDatafeedsWithoutTask(T0, state);

        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), notRecovered)), empty());

        // timers were cleared while the state was not recovered, so the window restarts here
        assertThat(ids(notifier.findStartedDatafeedsWithoutTask(T0.plus(Duration.ofHours(10)), state)), empty());
    }

    public void testWatchdogFailureShouldNotSuppressJobUnassignedReport() {
        AtomicReference<Instant> now = new AtomicReference<>(T0);
        Clock clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(invocation -> now.get());
        MlAssignmentNotifier clocked = new MlAssignmentNotifier(
            mock(AnomalyDetectionAuditor.class),
            mock(DataFrameAnalyticsAuditor.class),
            threadPool,
            clusterService,
            resolver,
            clock
        );
        // A datafeed without an index list makes the node selector throw; job-2 is independently unassigned for a long time.
        ClusterState state = state(
            new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED, null).job("job-2", null, JobState.OPENED),
            ML_NODE
        );

        MockLog.assertThatLogger(() -> {
            now.set(T0.plusSeconds(31));
            clocked.clusterChanged(new ClusterChangedEvent("test", state, state));
            now.set(now.get().plus(JUST_OVER_THRESHOLD));
            clocked.clusterChanged(new ClusterChangedEvent("test", state, state));
        },
            MlAssignmentNotifier.class,
            new MockLog.PatternSeenEventExpectation(
                "job report still logged",
                MlAssignmentNotifier.class.getCanonicalName(),
                Level.WARN,
                ".*ML persistent tasks unassigned for a long time \\[.*job-2.*"
            )
        );
    }

    public void testClusterChangedPastThresholdShouldLogErrorWithToken() {
        AtomicReference<Instant> now = new AtomicReference<>(T0);
        Clock clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(invocation -> now.get());
        MlAssignmentNotifier clocked = new MlAssignmentNotifier(
            mock(AnomalyDetectionAuditor.class),
            mock(DataFrameAnalyticsAuditor.class),
            threadPool,
            clusterService,
            resolver,
            clock
        );
        ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENED).datafeed(null, DatafeedState.STARTED), ML_NODE);

        MockLog.assertThatLogger(() -> {
            now.set(T0.plusSeconds(31));
            clocked.clusterChanged(new ClusterChangedEvent("test", state, state));
            now.set(now.get().plus(JUST_OVER_THRESHOLD));
            clocked.clusterChanged(new ClusterChangedEvent("test", state, state));
        },
            MlAssignmentNotifier.class,
            new MockLog.PatternSeenEventExpectation(
                "stuck datafeed reported",
                MlAssignmentNotifier.class.getCanonicalName(),
                Level.ERROR,
                "\\[ml-invariant:started_datafeed_without_task\\] Datafeed \\[df-1\\].*\\[16\\] minutes.*job \\[job-1\\].*"
            )
        );
    }

    public void testClusterChangedWithHealthyDatafeedsShouldNotLogErrorOrWarn() {
        AtomicReference<Instant> now = new AtomicReference<>(T0);
        Clock clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(invocation -> now.get());
        MlAssignmentNotifier clocked = new MlAssignmentNotifier(
            mock(AnomalyDetectionAuditor.class),
            mock(DataFrameAnalyticsAuditor.class),
            threadPool,
            clusterService,
            resolver,
            clock
        );
        // job opening while its datafeed waits, as after a node drain
        ClusterState state = state(new Tasks().job(ML_NODE, JobState.OPENING).datafeed(null, DatafeedState.STARTED), ML_NODE);

        MockLog.assertThatLogger(() -> {
            now.set(T0.plusSeconds(31));
            clocked.clusterChanged(new ClusterChangedEvent("test", state, state));
            now.set(now.get().plus(Duration.ofHours(10)));
            clocked.clusterChanged(new ClusterChangedEvent("test", state, state));
        },
            MlAssignmentNotifier.class,
            new MockLog.PatternNotSeenEventExpectation(
                "no invariant at ERROR",
                MlAssignmentNotifier.class.getCanonicalName(),
                Level.ERROR,
                ".*"
            )
        );
    }

    private MlAssignmentNotifier newNotifier() {
        return new MlAssignmentNotifier(
            mock(AnomalyDetectionAuditor.class),
            mock(DataFrameAnalyticsAuditor.class),
            threadPool,
            clusterService,
            resolver
        );
    }

    private static List<String> ids(List<StuckDatafeed> stuck) {
        return stuck.stream().map(StuckDatafeed::datafeedId).toList();
    }

    private static ClusterState state(Tasks tasks, String... liveNodeIds) {
        return state(tasks, Metadata.builder(), liveNodeIds);
    }

    private static ClusterState state(Tasks tasks, Metadata.Builder metadata, String... liveNodeIds) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("master", new TransportAddress(InetAddress.getLoopbackAddress(), 9300)))
            .localNodeId("master")
            .masterNodeId("master");
        int port = 9301;
        for (String nodeId : liveNodeIds) {
            nodes.add(DiscoveryNodeUtils.create(nodeId, new TransportAddress(InetAddress.getLoopbackAddress(), port++)));
        }
        return ClusterState.builder(new ClusterName("watchdog-test"))
            .metadata(metadata.putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build()))
            .nodes(nodes)
            .build();
    }

    /** Persistent tasks for one job and its datafeed; a null node leaves the task unassigned and a null state leaves it stateless. */
    private static final class Tasks {
        private final PersistentTasksCustomMetadata.Builder builder = PersistentTasksCustomMetadata.builder();

        Tasks job(String node, JobState state) {
            return job(JOB_ID, node, state);
        }

        Tasks job(String jobId, String node, JobState state) {
            builder.addTask(
                MlTasks.jobTaskId(jobId),
                MlTasks.JOB_TASK_NAME,
                new OpenJobAction.JobParams(jobId),
                node == null ? new Assignment(null, "awaiting capacity") : new Assignment(node, "")
            );
            if (state != null) {
                builder.updateTaskState(MlTasks.jobTaskId(jobId), new JobTaskState(state, builder.getLastAllocationId(), null, null));
            }
            return this;
        }

        Tasks datafeed(String node, DatafeedState state) {
            return datafeed(node, state, List.of());
        }

        Tasks datafeed(String node, DatafeedState state, List<String> indices) {
            StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams(DATAFEED_ID, 0L);
            params.setJobId(JOB_ID);
            params.setDatafeedIndices(indices);
            builder.addTask(
                MlTasks.datafeedTaskId(DATAFEED_ID),
                MlTasks.DATAFEED_TASK_NAME,
                params,
                node == null ? new Assignment(null, "no node") : new Assignment(node, "")
            );
            if (state != null) {
                builder.updateTaskState(MlTasks.datafeedTaskId(DATAFEED_ID), state);
            }
            return this;
        }

        PersistentTasksCustomMetadata build() {
            return builder.build();
        }
    }
}
