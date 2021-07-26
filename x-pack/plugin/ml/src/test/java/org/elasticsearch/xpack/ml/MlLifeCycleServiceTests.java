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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
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
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunner;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class MlLifeCycleServiceTests extends ESTestCase {

    private static final Set<DiscoveryNodeRole> ROLES = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE, MachineLearning.ML_ROLE)));

    private Settings commonSettings;
    private ClusterService clusterService;
    private DatafeedRunner datafeedRunner;
    private AutodetectProcessManager autodetectProcessManager;
    private DataFrameAnalyticsManager analyticsManager;
    private MlMemoryTracker memoryTracker;

    @Before
    public void setupMocks() {
        commonSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false)
                .build();
        clusterService = mock(ClusterService.class);
        datafeedRunner = mock(DatafeedRunner.class);
        autodetectProcessManager = mock(AutodetectProcessManager.class);
        analyticsManager = mock(DataFrameAnalyticsManager.class);
        memoryTracker = mock(MlMemoryTracker.class);
    }

    public void testIsNodeSafeToShutdown() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();

        tasksBuilder.addTask(MlTasks.jobTaskId("job-1"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("job-1"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(MlTasks.datafeedTaskId("df1"), MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("df1", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(MlTasks.dataFrameAnalyticsTaskId("job-2"), MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams("foo-2", Version.CURRENT, true),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment"));
        tasksBuilder.addTask(MlTasks.snapshotUpgradeTaskId("job-3", "snapshot-3"), MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
            new SnapshotUpgradeTaskParams("job-3", "snapshot-3"),
            new PersistentTasksCustomMetadata.Assignment("node-3", "test assignment"));

        Metadata metadata = Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()).build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();

        assertThat(MlLifeCycleService.isNodeSafeToShutdown("node-1", clusterState), is(false)); // has AD job
        assertThat(MlLifeCycleService.isNodeSafeToShutdown("node-2", clusterState), is(true)); // has DFA job
        assertThat(MlLifeCycleService.isNodeSafeToShutdown("node-3", clusterState), is(false)); // has snapshot upgrade
        assertThat(MlLifeCycleService.isNodeSafeToShutdown("node-4", clusterState), is(true)); // has no ML tasks
    }

    public void testIsNodeSafeToShutdownGivenFailedTasks() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();

        tasksBuilder.addTask(MlTasks.jobTaskId("job-1"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("job-1"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment"));
        tasksBuilder.updateTaskState(MlTasks.jobTaskId("job-1"), new JobTaskState(JobState.FAILED, 1, "testing"));
        tasksBuilder.addTask(MlTasks.dataFrameAnalyticsTaskId("job-2"), MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams("foo-2", Version.CURRENT, true),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment"));
        tasksBuilder.updateTaskState(MlTasks.dataFrameAnalyticsTaskId("job-2"),
            new DataFrameAnalyticsTaskState(DataFrameAnalyticsState.FAILED, 2, "testing"));
        tasksBuilder.addTask(MlTasks.snapshotUpgradeTaskId("job-3", "snapshot-3"), MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
            new SnapshotUpgradeTaskParams("job-3", "snapshot-3"),
            new PersistentTasksCustomMetadata.Assignment("node-3", "test assignment"));
        tasksBuilder.updateTaskState(MlTasks.snapshotUpgradeTaskId("job-3", "snapshot-3"),
            new SnapshotUpgradeTaskState(SnapshotUpgradeState.FAILED, 3, "testing"));

        Metadata metadata = Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()).build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();

        assertThat(MlLifeCycleService.isNodeSafeToShutdown("node-1", clusterState), is(true)); // has failed AD job
        assertThat(MlLifeCycleService.isNodeSafeToShutdown("node-2", clusterState), is(true)); // has failed DFA job
        assertThat(MlLifeCycleService.isNodeSafeToShutdown("node-3", clusterState), is(true)); // has failed snapshot upgrade
        assertThat(MlLifeCycleService.isNodeSafeToShutdown("node-4", clusterState), is(true)); // has no ML tasks
    }

    public void testSignalGracefulShutdownIncludingLocalNode() {

        MlLifeCycleService mlLifeCycleService = new MlLifeCycleService(TestEnvironment.newEnvironment(commonSettings), clusterService,
            datafeedRunner, autodetectProcessManager, analyticsManager, memoryTracker);

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node-1-name", "node-1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                Collections.emptyMap(), ROLES, Version.CURRENT))
            .add(new DiscoveryNode("node-2-name", "node-2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                Collections.emptyMap(), ROLES, Version.CURRENT))
            .add(new DiscoveryNode("node-3-name", "node-3", new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                Collections.emptyMap(), ROLES, Version.CURRENT))
            .masterNodeId("node-1")
            .localNodeId("node-2");
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(nodesBuilder).build();

        Collection<String> shutdownNodeIds =
            randomBoolean() ? Collections.singleton("node-2") : Arrays.asList("node-1", "node-2", "node-3");

        mlLifeCycleService.signalGracefulShutdown(clusterState, shutdownNodeIds);

        verify(datafeedRunner).vacateAllDatafeedsOnThisNode("previously assigned node [node-2-name] is shutting down");
        verify(autodetectProcessManager).vacateOpenJobsOnThisNode();
        verifyNoMoreInteractions(datafeedRunner, autodetectProcessManager, analyticsManager, memoryTracker);
    }

    public void testSignalGracefulShutdownExcludingLocalNode() {

        MlLifeCycleService mlLifeCycleService = new MlLifeCycleService(TestEnvironment.newEnvironment(commonSettings), clusterService,
            datafeedRunner, autodetectProcessManager, analyticsManager, memoryTracker);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node-1-name", "node-1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                Collections.emptyMap(), ROLES, Version.CURRENT))
            .add(new DiscoveryNode("node-2-name", "node-2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                Collections.emptyMap(), ROLES, Version.CURRENT))
            .add(new DiscoveryNode("node-3-name", "node-3", new TransportAddress(InetAddress.getLoopbackAddress(), 9302),
                Collections.emptyMap(), ROLES, Version.CURRENT))
            .masterNodeId("node-1")
            .localNodeId("node-2");
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(nodesBuilder).build();

        Collection<String> shutdownNodeIds =
            randomBoolean() ? Collections.singleton("node-1") : Arrays.asList("node-1", "node-3");

        mlLifeCycleService.signalGracefulShutdown(clusterState, shutdownNodeIds);

        verifyNoMoreInteractions(datafeedRunner, autodetectProcessManager, analyticsManager, memoryTracker);
    }
}
