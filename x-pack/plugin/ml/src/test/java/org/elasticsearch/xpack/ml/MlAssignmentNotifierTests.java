/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests.addJobTask;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
        threadPool = mock(ThreadPool.class);

        ExecutorService executorService = mock(ExecutorService.class);
        org.mockito.Mockito.doAnswer(invocation -> {
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
                    .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verify(anomalyDetectionAuditor, times(1)).info("job_id", "Opening job on node [_node_id]");

        // no longer master
        newState = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata)
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT))
                    .localNodeId("_node_id")
            )
            .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verifyNoMoreInteractions(anomalyDetectionAuditor);
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
                    .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
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
                    .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verify(anomalyDetectionAuditor, times(1)).info("job_id", "Job unassigned from node [_node_id]");

        // no longer master
        newState = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata)
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                    .localNodeId("_node_id")
            )
            .build();

        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verifyNoMoreInteractions(anomalyDetectionAuditor);
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
                    .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
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
                    .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
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
                    .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();
        notifier.auditUnassignedMlTasks(newState.nodes(), newState.metadata().custom(PersistentTasksCustomMetadata.TYPE));
        verify(anomalyDetectionAuditor, times(1)).warning("job_id", "No node found to open job. Reasons [test assignment]");
    }
}
