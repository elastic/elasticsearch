/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MLMetadataField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.junit.Before;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.mock.orig.Mockito.doAnswer;
import static org.elasticsearch.mock.orig.Mockito.times;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MlInitializationServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("my_cluster");

    private ThreadPool threadPool;
    private ExecutorService executorService;
    private ClusterService clusterService;
    private Client client;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        executorService = mock(ExecutorService.class);
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);

        ScheduledFuture scheduledFuture = mock(ScheduledFuture.class);
        when(threadPool.schedule(any(), any(), any())).thenReturn(scheduledFuture);

        when(clusterService.getClusterName()).thenReturn(CLUSTER_NAME);
    }

    public void testInitialize() throws Exception {
        MlInitializationService initializationService = new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(1)).submitStateUpdateTask(eq("install-ml-metadata"), any());
        assertThat(initializationService.getDailyMaintenanceService().isStarted(), is(true));
    }

    public void testInitialize_noMasterNode() throws Exception {
        MlInitializationService initializationService = new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT)))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(0)).submitStateUpdateTask(eq("install-ml-metadata"), any());
        assertThat(initializationService.getDailyMaintenanceService(), is(nullValue()));
    }

    public void testInitialize_alreadyInitialized() throws Exception {
        MlInitializationService initializationService = new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder()
                        .putCustom(MLMetadataField.TYPE, new MlMetadata.Builder().build()))
                .build();
        MlDailyMaintenanceService initialDailyMaintenanceService = mock(MlDailyMaintenanceService.class);
        initializationService.setDailyMaintenanceService(initialDailyMaintenanceService);
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(0)).submitStateUpdateTask(eq("install-ml-metadata"), any());
        assertSame(initialDailyMaintenanceService, initializationService.getDailyMaintenanceService());
    }

    public void testInitialize_onlyOnce() throws Exception {
        MlInitializationService initializationService = new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(1)).submitStateUpdateTask(eq("install-ml-metadata"), any());
    }

    public void testInitialize_reintialiseAfterFailure() throws Exception {
        MlInitializationService initializationService = new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client);

        // Fail the first cluster state update
        AtomicBoolean onFailureCalled = new AtomicBoolean(false);
        Mockito.doAnswer(invocation -> {
            ClusterStateUpdateTask task = (ClusterStateUpdateTask) invocation.getArguments()[1];
            task.onFailure("mock a failure", new IllegalStateException());
            onFailureCalled.set(true);
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("install-ml-metadata"), any(ClusterStateUpdateTask.class));

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));
        assertTrue("Something went wrong mocking the cluster update task", onFailureCalled.get());
        verify(clusterService, times(1)).submitStateUpdateTask(eq("install-ml-metadata"), any(ClusterStateUpdateTask.class));

        // 2nd update succeeds
        AtomicReference<ClusterState> clusterStateHolder = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            ClusterStateUpdateTask task = (ClusterStateUpdateTask) invocation.getArguments()[1];
            clusterStateHolder.set(task.execute(cs));
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("install-ml-metadata"), any(ClusterStateUpdateTask.class));

        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));
        assertTrue("Something went wrong mocking the sucessful cluster update task", clusterStateHolder.get() != null);
        verify(clusterService, times(2)).submitStateUpdateTask(eq("install-ml-metadata"), any(ClusterStateUpdateTask.class));

        // 3rd update won't be called as ML Metadata has been installed
        initializationService.clusterChanged(new ClusterChangedEvent("_source", clusterStateHolder.get(), clusterStateHolder.get()));
        verify(clusterService, times(2)).submitStateUpdateTask(eq("install-ml-metadata"), any(ClusterStateUpdateTask.class));
    }

    public void testNodeGoesFromMasterToNonMasterAndBack() throws Exception {
        MlInitializationService initializationService = new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client);
        MlDailyMaintenanceService initialDailyMaintenanceService = mock(MlDailyMaintenanceService.class);
        initializationService.setDailyMaintenanceService(initialDailyMaintenanceService);

        ClusterState masterCs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder())
                .build();
        ClusterState noMasterCs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT)))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", noMasterCs, masterCs));

        verify(initialDailyMaintenanceService).stop();

        initializationService.clusterChanged(new ClusterChangedEvent("_source", masterCs, noMasterCs));
        MlDailyMaintenanceService finalDailyMaintenanceService = initializationService.getDailyMaintenanceService();
        assertNotSame(initialDailyMaintenanceService, finalDailyMaintenanceService);
        assertThat(initializationService.getDailyMaintenanceService().isStarted(), is(true));
    }
}
