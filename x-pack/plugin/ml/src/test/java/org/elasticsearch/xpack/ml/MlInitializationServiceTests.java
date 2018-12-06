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
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.junit.Before;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;

import static org.elasticsearch.mock.orig.Mockito.doAnswer;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
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

    public void testInitialize() {
        MlInitializationService initializationService = new MlInitializationService(threadPool, clusterService, client);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        assertThat(initializationService.getDailyMaintenanceService().isStarted(), is(true));
    }

    public void testInitialize_noMasterNode() {
        MlInitializationService initializationService = new MlInitializationService(threadPool, clusterService, client);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT)))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        assertThat(initializationService.getDailyMaintenanceService(), is(nullValue()));
    }

    public void testInitialize_alreadyInitialized() {
        MlInitializationService initializationService = new MlInitializationService(threadPool, clusterService, client);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().build()))
                .build();
        MlDailyMaintenanceService initialDailyMaintenanceService = mock(MlDailyMaintenanceService.class);
        initializationService.setDailyMaintenanceService(initialDailyMaintenanceService);
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        assertSame(initialDailyMaintenanceService, initializationService.getDailyMaintenanceService());
    }

    public void testNodeGoesFromMasterToNonMasterAndBack() {
        MlInitializationService initializationService = new MlInitializationService(threadPool, clusterService, client);
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
