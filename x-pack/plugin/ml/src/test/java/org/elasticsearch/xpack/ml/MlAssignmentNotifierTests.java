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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.junit.Before;

import java.net.InetAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests.addJobTask;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MlAssignmentNotifierTests extends ESTestCase {

    private AnomalyDetectionAuditor anomalyDetectionAuditor;
    private DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor;
    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Before
    public void setupMocks() {
        anomalyDetectionAuditor = mock(AnomalyDetectionAuditor.class);
        dataFrameAnalyticsAuditor = mock(DataFrameAnalyticsAuditor.class);
        clusterService = mock(ClusterService.class);
        threadPool = mock(ThreadPool.class);

        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(anyString())).thenReturn(executorService);
    }

    public void testClusterChanged_assign() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(
            anomalyDetectionAuditor,
            dataFrameAnalyticsAuditor,
            threadPool,
            clusterService
        );

        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, new PersistentTasksCustomMetadata(0L, Collections.emptyMap()))
            )
            .build();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", "_node_id", null, tasksBuilder);
        Metadata metadata = Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()).build();
        ClusterState newState = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata)
            // set local node master
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300)))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        if (anomalyDetectionAuditor.includeNodeInfo()) {
            verify(anomalyDetectionAuditor, times(1)).info("job_id", "Opening job on node [_node_id]");
        } else {
            verify(anomalyDetectionAuditor, times(1)).info("job_id", "Opening job");
        }

        // no longer master
        newState = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300)))
                    .localNodeId("_node_id")
            )
            .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        if (anomalyDetectionAuditor.includeNodeInfo()) {
            verifyNoMoreInteractions(anomalyDetectionAuditor);
        }
    }

    public void testClusterChanged_unassign() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(
            anomalyDetectionAuditor,
            dataFrameAnalyticsAuditor,
            threadPool,
            clusterService
        );

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", "_node_id", null, tasksBuilder);
        Metadata metadata = Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()).build();
        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata)
            // set local node master
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200)))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", null, null, tasksBuilder);
        metadata = Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()).build();
        ClusterState newState = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata)
            // set local node master
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200)))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        if (anomalyDetectionAuditor.includeNodeInfo()) {
            verify(anomalyDetectionAuditor, times(1)).info("job_id", "Job unassigned from node [_node_id]");
        } else {
            verify(anomalyDetectionAuditor, times(1)).info("job_id", "Job relocating.");
        }

        verify(anomalyDetectionAuditor, times(2)).includeNodeInfo();

        // no longer master
        newState = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200)))
                    .localNodeId("_node_id")
            )
            .build();

        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verifyNoMoreInteractions(anomalyDetectionAuditor);
    }

    public void testClusterChanged_multipleProjects() {
        final Clock clock = mock(Clock.class);

        final Instant startInstant = Instant.now();
        when(clock.instant()).thenReturn(startInstant);
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(
            anomalyDetectionAuditor,
            dataFrameAnalyticsAuditor,
            threadPool,
            clusterService,
            clock
        );

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", null, JobState.OPENED, tasksBuilder);

        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata.Builder metadataBuilder = Metadata.builder()
            .put(ProjectMetadata.builder(projectId).putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()).build());
        for (int p = randomIntBetween(1, 5); p > 0; p--) {
            metadataBuilder.put(ProjectMetadata.builder(randomUniqueProjectId()));
        }
        final ClusterState previous = ClusterState.builder(ClusterName.DEFAULT).metadata(metadataBuilder.build()).build();
        final ClusterState newState = ClusterState.builder(previous)
            // set local node master
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200)))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();

        // Force the event to happen far enough in the future to trigger the unassigned checker
        // This will track the current state (which jobs are unassigned) but won't trigger a warning
        final Instant firstEventInstant = startInstant.plus(MlAssignmentNotifier.MIN_CHECK_UNASSIGNED_INTERVAL).plusSeconds(1);
        when(clock.instant()).thenReturn(firstEventInstant);
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));

        final MockLog log = MockLog.capture(MlAssignmentNotifier.class);
        log.addExpectation(
            new MockLog.PatternSeenEventExpectation(
                "expect-warning-log",
                MlAssignmentNotifier.class.getName(),
                Level.WARN,
                Pattern.quote("In project [" + projectId + "] ML persistent tasks unassigned for a long time [") + ".*"
            )
        );

        // Force an event in the future that will trigger a warning message
        final Instant secondEventInstant = firstEventInstant.plus(MlAssignmentNotifier.LONG_TIME_UNASSIGNED_INTERVAL).plusSeconds(1);
        when(clock.instant()).thenReturn(secondEventInstant);
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));

        log.assertAllExpectationsMatched();
    }

    public void testClusterChanged_noPersistentTaskChanges() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(
            anomalyDetectionAuditor,
            dataFrameAnalyticsAuditor,
            threadPool,
            clusterService
        );

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", null, null, tasksBuilder);
        Metadata metadata = Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()).build();
        ClusterState previous = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

        ClusterState newState = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata)
            // set local node master
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200)))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();

        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verifyNoMoreInteractions(anomalyDetectionAuditor);

        // no longer master
        newState = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200)))
                    .localNodeId("_node_id")
            )
            .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
    }

    public void testAuditUnassignedMlTasks() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(
            anomalyDetectionAuditor,
            dataFrameAnalyticsAuditor,
            threadPool,
            clusterService
        );

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", null, null, tasksBuilder);
        Metadata metadata = Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()).build();
        ClusterState newState = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata)
            // set local node master
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200)))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();
        notifier.auditUnassignedMlTasks(
            Metadata.DEFAULT_PROJECT_ID,
            newState.nodes(),
            newState.metadata().getProject().custom(PersistentTasksCustomMetadata.TYPE)
        );
        if (anomalyDetectionAuditor.includeNodeInfo()) {
            verify(anomalyDetectionAuditor, times(1)).warning("job_id", "No node found to open job. Reasons [test assignment]");
        } else {
            // need to account for includeNodeInfo being called here, in the test, and also in anomalyDetectionAuditor
            verify(anomalyDetectionAuditor, times(2)).includeNodeInfo();
        }
    }

    public void testFindLongTimeUnassignedTasks() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(
            anomalyDetectionAuditor,
            dataFrameAnalyticsAuditor,
            threadPool,
            clusterService
        );

        Instant now = Instant.now();
        Instant eightHoursAgo = now.minus(Duration.ofHours(8));
        Instant sevenHoursAgo = eightHoursAgo.plus(Duration.ofHours(1));
        Instant twoHoursAgo = sevenHoursAgo.plus(Duration.ofHours(5));
        Instant tomorrow = now.plus(Duration.ofHours(24));

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job1", "node1", JobState.OPENED, tasksBuilder);
        addJobTask("job2", "node1", JobState.OPENED, tasksBuilder);
        addJobTask("job3", null, JobState.OPENED, tasksBuilder);
        addJobTask("job4", null, JobState.OPENED, tasksBuilder);
        addJobTask("job5", null, JobState.OPENED, tasksBuilder);
        List<String> itemsToReport = notifier.findLongTimeUnassignedTasks(eightHoursAgo, tasksBuilder.build());
        // Nothing reported because unassigned jobs only just detected
        assertThat(itemsToReport, empty());

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job1", null, JobState.OPENED, tasksBuilder);
        addJobTask("job2", "node1", JobState.OPENED, tasksBuilder);
        addJobTask("job3", null, JobState.OPENED, tasksBuilder);
        addJobTask("job4", "node2", JobState.OPENED, tasksBuilder);
        addJobTask("job5", null, JobState.OPENED, tasksBuilder);
        itemsToReport = notifier.findLongTimeUnassignedTasks(sevenHoursAgo, tasksBuilder.build());
        // Jobs 3 and 5 still unassigned so should get reported, job 4 now assigned, job 1 only just detected unassigned
        assertThat(
            itemsToReport,
            containsInAnyOrder("[xpack/ml/job]/[job3] unassigned for [3600] seconds", "[xpack/ml/job]/[job5] unassigned for [3600] seconds")
        );

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job1", null, JobState.OPENED, tasksBuilder);
        addJobTask("job2", null, JobState.OPENED, tasksBuilder);
        addJobTask("job3", null, JobState.OPENED, tasksBuilder);
        addJobTask("job4", "node2", JobState.OPENED, tasksBuilder);
        addJobTask("job5", null, JobState.OPENED, tasksBuilder);
        itemsToReport = notifier.findLongTimeUnassignedTasks(twoHoursAgo, tasksBuilder.build());
        // Jobs 3 and 5 still unassigned but reported less than 6 hours ago, job 1 still unassigned so gets reported now,
        // job 2 only just detected unassigned
        assertThat(itemsToReport, contains("[xpack/ml/job]/[job1] unassigned for [18000] seconds"));

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job1", null, JobState.OPENED, tasksBuilder);
        addJobTask("job2", null, JobState.OPENED, tasksBuilder);
        addJobTask("job3", null, JobState.OPENED, tasksBuilder);
        addJobTask("job4", null, JobState.OPENED, tasksBuilder);
        addJobTask("job5", "node1", JobState.OPENED, tasksBuilder);
        itemsToReport = notifier.findLongTimeUnassignedTasks(now, tasksBuilder.build());
        // Job 3 still unassigned and reported more than 6 hours ago, job 1 still unassigned but reported less than 6 hours ago,
        // job 2 still unassigned so gets reported now, job 4 only just detected unassigned, job 5 now assigned
        assertThat(
            itemsToReport,
            containsInAnyOrder(
                "[xpack/ml/job]/[job2] unassigned for [7200] seconds",
                "[xpack/ml/job]/[job3] unassigned for [28800] seconds"
            )
        );

        tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job1", null, JobState.FAILED, tasksBuilder);
        addJobTask("job2", null, JobState.FAILED, tasksBuilder);
        addJobTask("job3", null, JobState.FAILED, tasksBuilder);
        addJobTask("job4", null, JobState.FAILED, tasksBuilder);
        addJobTask("job5", "node1", JobState.FAILED, tasksBuilder);
        itemsToReport = notifier.findLongTimeUnassignedTasks(tomorrow, tasksBuilder.build());
        // We still have unassigned jobs, but now all the jobs are failed, so none should be reported as unassigned
        // as it doesn't make any difference whether they're assigned or not and autoscaling will ignore them
        assertThat(itemsToReport, empty());
    }

    public void testFindLongTimeUnassignedTasks_WithNullState() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(
            anomalyDetectionAuditor,
            dataFrameAnalyticsAuditor,
            threadPool,
            clusterService
        );
        var now = Instant.now();
        var sevenHoursAgo = now.minus(Duration.ofHours(7));
        var eightHoursAgo = now.minus(Duration.ofHours(8));

        {
            // run once with valid state to add unassigned job to the history
            PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
            addJobTask("job1", null, JobState.OPENED, tasksBuilder);
            List<String> itemsToReport = notifier.findLongTimeUnassignedTasks(eightHoursAgo, tasksBuilder.build());
            // Nothing reported because unassigned jobs only just detected
            assertThat(itemsToReport, empty());
        }
        {
            PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
            addJobTask("job1", null, null, tasksBuilder); // this time the job has no state
            // one hour later the job would be detected as unassigned if not for the missing state
            List<String> itemsToReport = notifier.findLongTimeUnassignedTasks(sevenHoursAgo, tasksBuilder.build());
            assertThat(itemsToReport, empty());
        }
    }
}
