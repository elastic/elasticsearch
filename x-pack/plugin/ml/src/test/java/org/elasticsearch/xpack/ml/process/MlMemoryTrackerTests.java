/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.junit.Before;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MlMemoryTrackerTests extends ESTestCase {

    private JobManager jobManager;
    private JobResultsProvider jobResultsProvider;
    private DataFrameAnalyticsConfigProvider configProvider;
    private MlMemoryTracker memoryTracker;

    @Before
    public void setup() {

        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.singleton(PersistentTasksClusterService.CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING)
        );
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = ClusterState.EMPTY_STATE;
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable r = (Runnable) invocation.getArguments()[0];
            r.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(anyString())).thenReturn(executorService);
        jobManager = mock(JobManager.class);
        jobResultsProvider = mock(JobResultsProvider.class);
        configProvider = mock(DataFrameAnalyticsConfigProvider.class);
        memoryTracker = new MlMemoryTracker(Settings.EMPTY, clusterService, threadPool, jobManager, jobResultsProvider, configProvider);
    }

    public void testRefreshAll() {

        boolean isMaster = randomBoolean();
        if (isMaster) {
            memoryTracker.onMaster();
        } else {
            memoryTracker.offMaster();
        }

        Map<String, PersistentTasksCustomMetadata.PersistentTask<?>> tasks = new HashMap<>();

        int numAnomalyDetectorJobTasks = randomIntBetween(2, 5);
        for (int i = 1; i <= numAnomalyDetectorJobTasks; ++i) {
            String jobId = "job" + i;
            PersistentTasksCustomMetadata.PersistentTask<?> task = makeTestAnomalyDetectorTask(jobId);
            tasks.put(task.getId(), task);
        }

        // One snapshot upgrade is for a running job, one for a job that isn't running
        int numSnapshotUpgradeTasks = 2;
        for (int i = numAnomalyDetectorJobTasks; i < numAnomalyDetectorJobTasks + numSnapshotUpgradeTasks; ++i) {
            String jobId = "job" + i;
            String snapshotId = Long.toString(randomLongBetween(1000000000L, 9999999999L));
            PersistentTasksCustomMetadata.PersistentTask<?> task = makeTestSnapshotUpgradeTask(jobId, snapshotId);
            tasks.put(task.getId(), task);
        }

        List<String> allIds = new ArrayList<>();
        int numDataFrameAnalyticsTasks = randomIntBetween(2, 5);
        for (int i = 1; i <= numDataFrameAnalyticsTasks; ++i) {
            String id = "analytics" + i;
            allIds.add(id);
            PersistentTasksCustomMetadata.PersistentTask<?> task = makeTestDataFrameAnalyticsTask(id, false);
            tasks.put(task.getId(), task);
        }

        PersistentTasksCustomMetadata persistentTasks = new PersistentTasksCustomMetadata(
            numAnomalyDetectorJobTasks + numSnapshotUpgradeTasks + numDataFrameAnalyticsTasks,
            tasks
        );

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<Long> listener = (Consumer<Long>) invocation.getArguments()[3];
            listener.accept(randomLongBetween(1000, 1000000));
            return null;
        }).when(jobResultsProvider).getEstablishedMemoryUsage(anyString(), any(), any(), any(), any());

        if (isMaster) {
            memoryTracker.refresh(persistentTasks, ActionListener.wrap(aVoid -> {}, ESTestCase::assertNull));
        } else {
            AtomicReference<Exception> exception = new AtomicReference<>();
            memoryTracker.refresh(persistentTasks, ActionListener.wrap(e -> fail("Expected failure response"), exception::set));
            assertEquals("Request to refresh anomaly detector memory requirement on non-master node", exception.get().getMessage());
        }

        if (isMaster) {
            for (int i = 1; i < numAnomalyDetectorJobTasks + numSnapshotUpgradeTasks; ++i) {
                String jobId = "job" + i;
                // This should only be called once even for the job where there's both a job task and a snapshot upgrade task
                verify(jobResultsProvider, times(1)).getEstablishedMemoryUsage(eq(jobId), any(), any(), any(), any());
            }
            verify(configProvider, times(1)).getConfigsForJobsWithTasksLeniently(eq(new HashSet<>(allIds)), any());
        } else {
            verify(jobResultsProvider, never()).getEstablishedMemoryUsage(anyString(), any(), any(), any(), any());
        }
    }

    public void testRefreshWithSkips() {

        boolean isMaster = randomBoolean();
        if (isMaster) {
            memoryTracker.onMaster();
        } else {
            memoryTracker.offMaster();
        }

        Map<String, PersistentTasksCustomMetadata.PersistentTask<?>> tasks = new HashMap<>();

        Set<String> toSkip = new HashSet<>();

        int numAnomalyDetectorJobTasks = randomIntBetween(2, 5);
        for (int i = 1; i <= numAnomalyDetectorJobTasks; ++i) {
            String jobId = "job" + i;
            if (randomBoolean()) {
                toSkip.add(jobId);
            }
            PersistentTasksCustomMetadata.PersistentTask<?> task = makeTestAnomalyDetectorTask(jobId);
            tasks.put(task.getId(), task);
        }

        // One snapshot upgrade is for a running job, one for a job that isn't running
        int numSnapshotUpgradeTasks = 2;
        for (int i = numAnomalyDetectorJobTasks; i < numAnomalyDetectorJobTasks + numSnapshotUpgradeTasks; ++i) {
            String jobId = "job" + i;
            String snapshotId = Long.toString(randomLongBetween(1000000000L, 9999999999L));
            PersistentTasksCustomMetadata.PersistentTask<?> task = makeTestSnapshotUpgradeTask(jobId, snapshotId);
            tasks.put(task.getId(), task);
        }

        List<String> allIds = new ArrayList<>();
        int numDataFrameAnalyticsTasks = randomIntBetween(2, 5);
        for (int i = 1; i <= numDataFrameAnalyticsTasks; ++i) {
            String id = "analytics" + i;
            allIds.add(id);
            PersistentTasksCustomMetadata.PersistentTask<?> task = makeTestDataFrameAnalyticsTask(id, false);
            tasks.put(task.getId(), task);
        }

        PersistentTasksCustomMetadata persistentTasks = new PersistentTasksCustomMetadata(
            numAnomalyDetectorJobTasks + numSnapshotUpgradeTasks + numDataFrameAnalyticsTasks,
            tasks
        );

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<Long> listener = (Consumer<Long>) invocation.getArguments()[3];
            listener.accept(randomLongBetween(1000, 1000000));
            return null;
        }).when(jobResultsProvider).getEstablishedMemoryUsage(anyString(), any(), any(), any(), any());

        if (isMaster) {
            memoryTracker.refresh(persistentTasks, toSkip, ActionListener.wrap(aVoid -> {}, ESTestCase::assertNull));
        } else {
            AtomicReference<Exception> exception = new AtomicReference<>();
            memoryTracker.refresh(persistentTasks, toSkip, ActionListener.wrap(e -> fail("Expected failure response"), exception::set));
            assertEquals("Request to refresh anomaly detector memory requirement on non-master node", exception.get().getMessage());
        }

        if (isMaster) {
            for (int i = 1; i < numAnomalyDetectorJobTasks + numSnapshotUpgradeTasks; ++i) {
                String jobId = "job" + i;
                if (toSkip.contains(jobId)) {
                    verify(jobResultsProvider, never()).getEstablishedMemoryUsage(eq(jobId), any(), any(), any(), any());
                } else {
                    // This should only be called once even for the job where there's both a job task and a snapshot upgrade task
                    verify(jobResultsProvider, times(1)).getEstablishedMemoryUsage(eq(jobId), any(), any(), any(), any());
                }
            }
            verify(configProvider, times(1)).getConfigsForJobsWithTasksLeniently(eq(new HashSet<>(allIds)), any());
        } else {
            verify(jobResultsProvider, never()).getEstablishedMemoryUsage(anyString(), any(), any(), any(), any());
        }
    }

    public void testRefreshAllFailure() {

        Map<String, PersistentTasksCustomMetadata.PersistentTask<?>> tasks = new HashMap<>();

        int numAnomalyDetectorJobTasks = randomIntBetween(2, 5);
        for (int i = 1; i <= numAnomalyDetectorJobTasks; ++i) {
            String jobId = "job" + i;
            PersistentTasksCustomMetadata.PersistentTask<?> task = makeTestAnomalyDetectorTask(jobId);
            tasks.put(task.getId(), task);
        }

        int numSnapshotUpgradeTasks = randomIntBetween(1, 3);
        for (int i = numAnomalyDetectorJobTasks; i < numAnomalyDetectorJobTasks + numSnapshotUpgradeTasks; ++i) {
            String jobId = "job" + i;
            PersistentTasksCustomMetadata.PersistentTask<?> task = makeTestAnomalyDetectorTask(jobId);
            tasks.put(task.getId(), task);
        }

        int numDataFrameAnalyticsTasks = randomIntBetween(2, 5);
        for (int i = 1; i <= numDataFrameAnalyticsTasks; ++i) {
            String id = "analytics" + i;
            PersistentTasksCustomMetadata.PersistentTask<?> task = makeTestDataFrameAnalyticsTask(id, false);
            tasks.put(task.getId(), task);
        }

        PersistentTasksCustomMetadata persistentTasks = new PersistentTasksCustomMetadata(
            numAnomalyDetectorJobTasks + numSnapshotUpgradeTasks + numDataFrameAnalyticsTasks,
            tasks
        );

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<Long> listener = (Consumer<Long>) invocation.getArguments()[3];
            listener.accept(randomLongBetween(1000, 1000000));
            return null;
        }).when(jobResultsProvider).getEstablishedMemoryUsage(anyString(), any(), any(), any(), any());

        // First run a refresh using a component that calls the onFailure method of the listener

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<List<DataFrameAnalyticsConfig>> listener = (ActionListener<List<DataFrameAnalyticsConfig>>) invocation
                .getArguments()[1];
            listener.onFailure(new IllegalArgumentException("computer says no"));
            return null;
        }).when(configProvider).getConfigsForJobsWithTasksLeniently(any(), any());

        AtomicBoolean gotErrorResponse = new AtomicBoolean(false);
        memoryTracker.refresh(
            persistentTasks,
            ActionListener.wrap(aVoid -> fail("Expected error response"), e -> gotErrorResponse.set(true))
        );
        assertTrue(gotErrorResponse.get());

        // Now run another refresh using a component that calls the onResponse method of the listener - this
        // proves that the ML memory tracker has not been permanently blocked up by the previous failure

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<List<DataFrameAnalyticsConfig>> listener = (ActionListener<List<DataFrameAnalyticsConfig>>) invocation
                .getArguments()[1];
            listener.onResponse(Collections.emptyList());
            return null;
        }).when(configProvider).getConfigsForJobsWithTasksLeniently(any(), any());

        AtomicReference<Exception> exception = new AtomicReference<>();
        memoryTracker.refresh(persistentTasks, ActionListener.wrap(e -> fail("Expected failure response"), exception::set));
        assertEquals("Request to refresh anomaly detector memory requirement on non-master node", exception.get().getMessage());
    }

    public void testRefreshOneAnomalyDetectorJob() {

        boolean isMaster = randomBoolean();
        if (isMaster) {
            memoryTracker.onMaster();
        } else {
            memoryTracker.offMaster();
        }

        String jobId = "job";
        boolean haveEstablishedModelMemory = randomBoolean();

        long modelBytes = 1024 * 1024;
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<Long> listener = (Consumer<Long>) invocation.getArguments()[3];
            listener.accept(haveEstablishedModelMemory ? modelBytes : 0L);
            return null;
        }).when(jobResultsProvider).getEstablishedMemoryUsage(eq(jobId), any(), any(), any(), any());

        boolean simulateVeryOldJob = randomBoolean();
        long recentJobModelMemoryLimitMb = 2;
        Job job = mock(Job.class);
        when(job.getAnalysisLimits()).thenReturn(simulateVeryOldJob ? null : new AnalysisLimits(recentJobModelMemoryLimitMb, 4L));
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Job> listener = (ActionListener<Job>) invocation.getArguments()[1];
            listener.onResponse(job);
            return null;
        }).when(jobManager).getJob(eq(jobId), any());

        if (isMaster) {
            AtomicReference<Long> refreshedMemoryRequirement = new AtomicReference<>();
            memoryTracker.refreshAnomalyDetectorJobMemory(
                jobId,
                ActionListener.wrap(refreshedMemoryRequirement::set, ESTestCase::assertNull)
            );
            if (haveEstablishedModelMemory) {
                assertEquals(
                    Long.valueOf(modelBytes + Job.PROCESS_MEMORY_OVERHEAD.getBytes()),
                    memoryTracker.getAnomalyDetectorJobMemoryRequirement(jobId)
                );
            } else {
                long expectedModelMemoryLimit = simulateVeryOldJob
                    ? AnalysisLimits.PRE_6_1_DEFAULT_MODEL_MEMORY_LIMIT_MB
                    : recentJobModelMemoryLimitMb;
                assertEquals(
                    Long.valueOf(ByteSizeValue.ofMb(expectedModelMemoryLimit).getBytes() + Job.PROCESS_MEMORY_OVERHEAD.getBytes()),
                    memoryTracker.getAnomalyDetectorJobMemoryRequirement(jobId)
                );
            }
            assertEquals(memoryTracker.getAnomalyDetectorJobMemoryRequirement(jobId), refreshedMemoryRequirement.get());
        } else {
            AtomicReference<Exception> exception = new AtomicReference<>();
            memoryTracker.refreshAnomalyDetectorJobMemory(
                jobId,
                ActionListener.wrap(e -> fail("Expected failure response"), exception::set)
            );
            assertEquals("Request to refresh anomaly detector memory requirement on non-master node", exception.get().getMessage());
            assertNull(memoryTracker.getAnomalyDetectorJobMemoryRequirement(jobId));
        }

        memoryTracker.removeAnomalyDetectorJob(jobId);
        assertNull(memoryTracker.getAnomalyDetectorJobMemoryRequirement(jobId));
    }

    public void testStop() {

        memoryTracker.onMaster();
        memoryTracker.stop();

        AtomicReference<Exception> exception = new AtomicReference<>();
        memoryTracker.refreshAnomalyDetectorJobMemory("job", ActionListener.wrap(ESTestCase::assertNull, exception::set));

        assertNotNull(exception.get());
        assertThat(exception.get(), instanceOf(EsRejectedExecutionException.class));
        assertEquals("Couldn't run ML memory update - node is shutting down", exception.get().getMessage());
    }

    public void testMaxDuration() {
        assertThat(MlMemoryTracker.max(Duration.ofMinutes(1), Duration.ofMinutes(2)), equalTo(Duration.ofMinutes(2)));
        assertThat(MlMemoryTracker.max(Duration.ofMinutes(4), Duration.ofMinutes(3)), equalTo(Duration.ofMinutes(4)));
        assertThat(MlMemoryTracker.max(Duration.ofMinutes(5), Duration.ofMinutes(5)), equalTo(Duration.ofMinutes(5)));
    }

    private PersistentTasksCustomMetadata.PersistentTask<OpenJobAction.JobParams> makeTestAnomalyDetectorTask(String jobId) {
        return new PersistentTasksCustomMetadata.PersistentTask<>(
            MlTasks.jobTaskId(jobId),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams(jobId),
            0,
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        );
    }

    private PersistentTasksCustomMetadata.PersistentTask<SnapshotUpgradeTaskParams> makeTestSnapshotUpgradeTask(
        String jobId,
        String snapshotId
    ) {
        return new PersistentTasksCustomMetadata.PersistentTask<>(
            MlTasks.snapshotUpgradeTaskId(jobId, snapshotId),
            MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
            new SnapshotUpgradeTaskParams(jobId, snapshotId),
            0,
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        );
    }

    private PersistentTasksCustomMetadata.PersistentTask<StartDataFrameAnalyticsAction.TaskParams> makeTestDataFrameAnalyticsTask(
        String id,
        boolean allowLazyStart
    ) {
        return new PersistentTasksCustomMetadata.PersistentTask<>(
            MlTasks.dataFrameAnalyticsTaskId(id),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams(id, Version.CURRENT, allowLazyStart),
            0,
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        );
    }

}
