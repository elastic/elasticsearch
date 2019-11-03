/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.xpack.ml.action.TransportOpenJobActionTests.addJobTask;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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
    private MlConfigMigrator configMigrator;

    @Before
    @SuppressWarnings("unchecked")
    private void setupMocks() {
        anomalyDetectionAuditor = mock(AnomalyDetectionAuditor.class);
        dataFrameAnalyticsAuditor = mock(DataFrameAnalyticsAuditor.class);
        clusterService = mock(ClusterService.class);
        threadPool = mock(ThreadPool.class);
        configMigrator = mock(MlConfigMigrator.class);
        threadPool = mock(ThreadPool.class);

        ExecutorService executorService = mock(ExecutorService.class);
        org.elasticsearch.mock.orig.Mockito.doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(anyString())).thenReturn(executorService);

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[1];
            listener.onResponse(Boolean.TRUE);
            return null;
        }).when(configMigrator).migrateConfigs(any(ClusterState.class), any(ActionListener.class));
    }

    public void testClusterChanged_info() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(anomalyDetectionAuditor, dataFrameAnalyticsAuditor, threadPool,
            configMigrator, clusterService);

        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE,
                        new PersistentTasksCustomMetaData(0L, Collections.emptyMap())))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "_node_id", null, tasksBuilder);
        MetaData metaData = MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()).build();
        ClusterState newState = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                // set local node master
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verify(anomalyDetectionAuditor, times(1)).info(eq("job_id"), any());
        verify(configMigrator, times(1)).migrateConfigs(eq(newState), any());

        // no longer master
        newState = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT)))
                .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verifyNoMoreInteractions(anomalyDetectionAuditor);
    }

    public void testClusterChanged_warning() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(anomalyDetectionAuditor, dataFrameAnalyticsAuditor, threadPool,
            configMigrator, clusterService);

        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE,
                        new PersistentTasksCustomMetaData(0L, Collections.emptyMap())))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", null, null, tasksBuilder);
        MetaData metaData = MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()).build();
        ClusterState newState = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                // set local node master
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verify(anomalyDetectionAuditor, times(1)).warning(eq("job_id"), any());
        verify(configMigrator, times(1)).migrateConfigs(eq(newState), any());

        // no longer master
        newState = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT)))
                .build();

        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verifyNoMoreInteractions(anomalyDetectionAuditor);
    }

    public void testClusterChanged_noPersistentTaskChanges() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(anomalyDetectionAuditor, dataFrameAnalyticsAuditor, threadPool,
            configMigrator, clusterService);

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", null, null, tasksBuilder);
        MetaData metaData = MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()).build();
        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                .build();

        ClusterState newState = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                // set local node master
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .build();

        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verify(configMigrator, times(1)).migrateConfigs(any(), any());
        verifyNoMoreInteractions(anomalyDetectionAuditor);

        // no longer master
        newState = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT)))
                .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", newState, previous));
        verifyNoMoreInteractions(configMigrator);
    }
}
