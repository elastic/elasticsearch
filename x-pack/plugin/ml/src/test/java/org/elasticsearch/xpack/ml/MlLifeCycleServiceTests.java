/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunner;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.process.MlController;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.net.InetAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.xpack.ml.MlLifeCycleService.MAX_GRACEFUL_SHUTDOWN_TIME;
import static org.elasticsearch.xpack.ml.MlLifeCycleService.isNodeSafeToShutdown;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class MlLifeCycleServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private DatafeedRunner datafeedRunner;
    private MlController mlController;
    private AutodetectProcessManager autodetectProcessManager;
    private DataFrameAnalyticsManager analyticsManager;
    private MlMemoryTracker memoryTracker;

    @Before
    public void setupMocks() {
        clusterService = mock(ClusterService.class);
        datafeedRunner = mock(DatafeedRunner.class);
        mlController = mock(MlController.class);
        autodetectProcessManager = mock(AutodetectProcessManager.class);
        analyticsManager = mock(DataFrameAnalyticsManager.class);
        memoryTracker = mock(MlMemoryTracker.class);
    }

    public void testIsNodeSafeToShutdown() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();

        tasksBuilder.addTask(
            MlTasks.jobTaskId("job-1"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("job-1"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("df1"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("df1", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.dataFrameAnalyticsTaskId("job-2"),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams("foo-2", Version.CURRENT, true),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.snapshotUpgradeTaskId("job-3", "snapshot-3"),
            MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
            new SnapshotUpgradeTaskParams("job-3", "snapshot-3"),
            new PersistentTasksCustomMetadata.Assignment("node-3", "test assignment")
        );

        Metadata metadata = Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()).build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();

        Instant shutdownStartTime = Instant.now();

        // We might be asked if it's safe to shut down immediately after being asked to shut down
        Clock clock = Clock.fixed(shutdownStartTime, ZoneId.systemDefault());
        assertThat(isNodeSafeToShutdown("node-1", clusterState, shutdownStartTime, clock), is(false)); // has AD job
        assertThat(isNodeSafeToShutdown("node-2", clusterState, shutdownStartTime, clock), is(true)); // has DFA job
        assertThat(isNodeSafeToShutdown("node-3", clusterState, shutdownStartTime, clock), is(false)); // has snapshot upgrade
        assertThat(isNodeSafeToShutdown("node-4", clusterState, shutdownStartTime, clock), is(true)); // has no ML tasks

        // Results should also be the same if we're asked if it's safe to shut down before being asked to shut down
        assertThat(isNodeSafeToShutdown("node-1", clusterState, null, Clock.systemUTC()), is(false)); // has AD job
        assertThat(isNodeSafeToShutdown("node-2", clusterState, null, Clock.systemUTC()), is(true)); // has DFA job
        assertThat(isNodeSafeToShutdown("node-3", clusterState, null, Clock.systemUTC()), is(false)); // has snapshot upgrade
        assertThat(isNodeSafeToShutdown("node-4", clusterState, null, Clock.systemUTC()), is(true)); // has no ML tasks

        // Results should still be the same 1 minute into shutdown
        clock = Clock.fixed(clock.instant().plus(Duration.ofMinutes(1)), ZoneId.systemDefault());
        assertThat(isNodeSafeToShutdown("node-1", clusterState, shutdownStartTime, clock), is(false)); // has AD job
        assertThat(isNodeSafeToShutdown("node-2", clusterState, shutdownStartTime, clock), is(true)); // has DFA job
        assertThat(isNodeSafeToShutdown("node-3", clusterState, shutdownStartTime, clock), is(false)); // has snapshot upgrade
        assertThat(isNodeSafeToShutdown("node-4", clusterState, shutdownStartTime, clock), is(true)); // has no ML tasks

        // After the timeout we should always report it's safe to shut down
        clock = Clock.fixed(clock.instant().plus(MAX_GRACEFUL_SHUTDOWN_TIME), ZoneId.systemDefault());
        assertThat(isNodeSafeToShutdown("node-1", clusterState, shutdownStartTime, clock), is(true)); // has AD job
        assertThat(isNodeSafeToShutdown("node-2", clusterState, shutdownStartTime, clock), is(true)); // has DFA job
        assertThat(isNodeSafeToShutdown("node-3", clusterState, shutdownStartTime, clock), is(true)); // has snapshot upgrade
        assertThat(isNodeSafeToShutdown("node-4", clusterState, shutdownStartTime, clock), is(true)); // has no ML tasks
    }

    public void testIsNodeSafeToShutdownGivenFailedTasks() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();

        tasksBuilder.addTask(
            MlTasks.jobTaskId("job-1"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("job-1"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.updateTaskState(MlTasks.jobTaskId("job-1"), new JobTaskState(JobState.FAILED, 1, "testing"));
        tasksBuilder.addTask(
            MlTasks.dataFrameAnalyticsTaskId("job-2"),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams("foo-2", Version.CURRENT, true),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );
        tasksBuilder.updateTaskState(
            MlTasks.dataFrameAnalyticsTaskId("job-2"),
            new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.FAILED, 2, "testing")
        );
        tasksBuilder.addTask(
            MlTasks.snapshotUpgradeTaskId("job-3", "snapshot-3"),
            MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
            new SnapshotUpgradeTaskParams("job-3", "snapshot-3"),
            new PersistentTasksCustomMetadata.Assignment("node-3", "test assignment")
        );
        tasksBuilder.updateTaskState(
            MlTasks.snapshotUpgradeTaskId("job-3", "snapshot-3"),
            new SnapshotUpgradeTaskState(SnapshotUpgradeState.FAILED, 3, "testing")
        );

        Metadata metadata = Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()).build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();

        // For these tests it shouldn't matter when shutdown started or what the time is now, because it's always safe to shut down
        Instant shutdownStartTime = randomFrom(Instant.now(), null);
        Clock clock = Clock.fixed(randomFrom(Instant.now(), Instant.now().plus(Duration.ofDays(1))), ZoneId.systemDefault());
        assertThat(isNodeSafeToShutdown("node-1", clusterState, shutdownStartTime, clock), is(true)); // has failed AD job
        assertThat(isNodeSafeToShutdown("node-2", clusterState, shutdownStartTime, clock), is(true)); // has failed DFA job
        assertThat(isNodeSafeToShutdown("node-3", clusterState, shutdownStartTime, clock), is(true)); // has failed snapshot upgrade
        assertThat(isNodeSafeToShutdown("node-4", clusterState, shutdownStartTime, clock), is(true)); // has no ML tasks
    }

    public void testSignalGracefulShutdownIncludingLocalNode() {

        MlLifeCycleService mlLifeCycleService = new MlLifeCycleService(
            clusterService,
            datafeedRunner,
            mlController,
            autodetectProcessManager,
            analyticsManager,
            memoryTracker
        );

        // Local node is node-2 here
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "node-1-name",
                    "node-1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Collections.emptyMap(),
                    DiscoveryNodeRole.roles()
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "node-2-name",
                    "node-2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    Collections.emptyMap(),
                    DiscoveryNodeRole.roles()
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "node-3-name",
                    "node-3",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                    Collections.emptyMap(),
                    DiscoveryNodeRole.roles()
                )
            )
            .masterNodeId("node-1")
            .localNodeId("node-2");
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(nodesBuilder).build();

        Collection<String> shutdownNodeIds = randomBoolean()
            ? Collections.singleton("node-2")
            : Arrays.asList("node-1", "node-2", "node-3");

        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        mlLifeCycleService.signalGracefulShutdown(clusterState, shutdownNodeIds, clock);
        assertThat(mlLifeCycleService.getShutdownStartTime("node-2"), is(clock.instant()));

        verify(datafeedRunner).vacateAllDatafeedsOnThisNode("previously assigned node [node-2-name] is shutting down");
        verify(autodetectProcessManager).vacateOpenJobsOnThisNode();
        verifyNoMoreInteractions(datafeedRunner, mlController, autodetectProcessManager, analyticsManager, memoryTracker);

        // Calling the method again should not advance the start time
        Clock advancedClock = Clock.fixed(clock.instant().plus(Duration.ofMinutes(1)), ZoneId.systemDefault());
        mlLifeCycleService.signalGracefulShutdown(clusterState, shutdownNodeIds, advancedClock);
        assertThat(mlLifeCycleService.getShutdownStartTime("node-2"), is(clock.instant()));
    }

    public void testSignalGracefulShutdownExcludingLocalNode() {

        MlLifeCycleService mlLifeCycleService = new MlLifeCycleService(
            clusterService,
            datafeedRunner,
            mlController,
            autodetectProcessManager,
            analyticsManager,
            memoryTracker
        );

        // Local node is node-2 here
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.create(
                    "node-1-name",
                    "node-1",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                    Collections.emptyMap(),
                    DiscoveryNodeRole.roles()
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "node-2-name",
                    "node-2",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                    Collections.emptyMap(),
                    DiscoveryNodeRole.roles()
                )
            )
            .add(
                DiscoveryNodeUtils.create(
                    "node-3-name",
                    "node-3",
                    new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                    Collections.emptyMap(),
                    DiscoveryNodeRole.roles()
                )
            )
            .masterNodeId("node-1")
            .localNodeId("node-2");
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(nodesBuilder).build();

        Collection<String> shutdownNodeIds = randomBoolean() ? Collections.singleton("node-1") : Arrays.asList("node-1", "node-3");

        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        mlLifeCycleService.signalGracefulShutdown(clusterState, shutdownNodeIds, clock);
        // The local node, node-2, is definitely not shutting down, but we should have still recorded
        // what time node-1 started shutting down, as we may get asked about it later
        assertThat(mlLifeCycleService.getShutdownStartTime("node-2"), nullValue());
        assertThat(mlLifeCycleService.getShutdownStartTime("node-1"), is(clock.instant()));

        verifyNoMoreInteractions(datafeedRunner, mlController, autodetectProcessManager, analyticsManager, memoryTracker);
    }
}
