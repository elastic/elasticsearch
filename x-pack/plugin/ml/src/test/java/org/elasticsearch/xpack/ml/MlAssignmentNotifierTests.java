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
import org.elasticsearch.xpack.ml.notifications.Auditor;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MlAssignmentNotifierTests extends ESTestCase {

    private Auditor auditor;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private MlConfigMigrator configMigrator;

    @Before
    @SuppressWarnings("unchecked")
    private void setupMocks() {
        auditor = mock(Auditor.class);
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
        }).when(configMigrator).migrateConfigsWithoutTasks(any(ClusterState.class), any(ActionListener.class));
    }

    public void testClusterChanged_info() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(auditor, threadPool, configMigrator, clusterService);
        notifier.onMaster();

        DiscoveryNode node =
                new DiscoveryNode("node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT);
        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE,
                        new PersistentTasksCustomMetaData(0L, Collections.emptyMap())))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", "node_id", null, tasksBuilder);
        MetaData metaData = MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()).build();
        ClusterState state = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder().add(node))
                .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", state, previous));
        verify(auditor, times(1)).info(eq("job_id"), any());
        verify(configMigrator, times(1)).migrateConfigsWithoutTasks(eq(state), any());

        notifier.offMaster();
        notifier.clusterChanged(new ClusterChangedEvent("_test", state, previous));
        verifyNoMoreInteractions(auditor);
    }

    public void testClusterChanged_warning() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(auditor, threadPool, configMigrator, clusterService);
        notifier.onMaster();

        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE,
                        new PersistentTasksCustomMetaData(0L, Collections.emptyMap())))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", null, null, tasksBuilder);
        MetaData metaData = MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()).build();
        ClusterState state = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                .build();
        notifier.clusterChanged(new ClusterChangedEvent("_test", state, previous));
        verify(auditor, times(1)).warning(eq("job_id"), any());
        verify(configMigrator, times(1)).migrateConfigsWithoutTasks(eq(state), any());

        notifier.offMaster();
        notifier.clusterChanged(new ClusterChangedEvent("_test", state, previous));
        verifyNoMoreInteractions(auditor);
    }

    public void testClusterChanged_noPersistentTaskChanges() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(auditor, threadPool, configMigrator, clusterService);
        notifier.onMaster();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", null, null, tasksBuilder);
        MetaData metaData = MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()).build();
        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                .build();

        ClusterState current = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData)
                .build();

        notifier.clusterChanged(new ClusterChangedEvent("_test", current, previous));
        verify(configMigrator, never()).migrateConfigsWithoutTasks(any(), any());

        notifier.offMaster();
        verify(configMigrator, never()).migrateConfigsWithoutTasks(any(), any());
    }

    public void testMigrateNotTriggered_GivenPre66Nodes() {
        MlAssignmentNotifier notifier = new MlAssignmentNotifier(auditor, threadPool, configMigrator, clusterService);
        notifier.onMaster();

        ClusterState previous = ClusterState.builder(new ClusterName("_name"))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", null, null, tasksBuilder);
        MetaData metaData = MetaData.builder().putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()).build();

        // mixed 6.5 and 6.6 nodes
        ClusterState current = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.V_6_5_0))
                        .add(new DiscoveryNode("node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301), Version.V_6_6_0)))
                .metaData(metaData)
                .build();

        notifier.clusterChanged(new ClusterChangedEvent("_test", current, previous));
        verify(configMigrator, never()).migrateConfigsWithoutTasks(any(), any());

        current = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.V_6_6_0))
                        .add(new DiscoveryNode("node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301), Version.V_6_6_0)))
                .metaData(metaData)
                .build();

        // all 6.6 nodes
        notifier.clusterChanged(new ClusterChangedEvent("_test", current, previous));
        verify(configMigrator, times(1)).migrateConfigsWithoutTasks(any(), any());
    }
}
