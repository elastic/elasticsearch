/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.MoreExecutors;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 */
public class WatcherLifeCycleServiceTest extends ElasticsearchTestCase {

    private ThreadPool threadPool;
    private WatcherService watcherService;
    private ClusterService clusterService;
    private IndicesService indicesService;
    private WatcherLifeCycleService lifeCycleService;

    @Before
    public void prepareServices() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(MoreExecutors.newDirectExecutorService());
        watcherService = mock(WatcherService.class);
        clusterService = mock(ClusterService.class);
        indicesService = mock(IndicesService.class);
        lifeCycleService = new WatcherLifeCycleService(ImmutableSettings.EMPTY, clusterService, indicesService, threadPool, watcherService);
    }

    @Test
    public void testStartAndStopCausedByClusterState() throws Exception {
        // starting... local node is master node
        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1");
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        when(watcherService.state()).thenReturn(WatcherService.State.STOPPED);
        when(watcherService.validate(clusterState)).thenReturn(true);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, times(1)).start(clusterState);
        verify(watcherService, never()).stop();

        // Trying to start a second time, but that should have no affect.
        when(watcherService.state()).thenReturn(WatcherService.State.STARTED);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, times(1)).start(clusterState);
        verify(watcherService, never()).stop();

        // Stopping because local node is no longer master node
        nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id2");
        ClusterState noMasterClusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", noMasterClusterState, noMasterClusterState));
        verify(watcherService, times(1)).stop();
        verify(watcherService, times(1)).start(clusterState);
    }

    @Test
    public void testStartWithStateNotRecoveredBlock() throws Exception {
        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1");
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
                .nodes(nodes).build();
        when(watcherService.state()).thenReturn(WatcherService.State.STOPPED);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, never()).start(any(ClusterState.class));
    }

    @Test
    public void testManualStartStop() {
        lifeCycleService.start();
        verify(watcherService, times(1)).start(any(ClusterState.class));
        verify(watcherService, never()).stop();

        lifeCycleService.stop();
        verify(watcherService, times(1)).start(any(ClusterState.class));
        verify(watcherService, times(1)).stop();

        // Starting via cluster state update, we shouldn't start because we have been stopped manually.
        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1");
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        when(watcherService.state()).thenReturn(WatcherService.State.STOPPED);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, times(1)).start(any(ClusterState.class));
        verify(watcherService, times(1)).stop();

        // we can only start, if we start manually
        lifeCycleService.start();
        verify(watcherService, times(2)).start(any(ClusterState.class));
        verify(watcherService, times(1)).stop();

        // stop watcher via cluster state update
        nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id2");
        clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        when(watcherService.state()).thenReturn(WatcherService.State.STOPPED);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, times(2)).start(any(ClusterState.class));
        verify(watcherService, times(2)).stop();

        // starting watcher via cluster state update, which should work, because we manually started before
        nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1");
        clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        when(watcherService.validate(clusterState)).thenReturn(true);
        when(watcherService.state()).thenReturn(WatcherService.State.STOPPED);
        lifeCycleService.clusterChanged(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, times(3)).start(any(ClusterState.class));
        verify(watcherService, times(2)).stop();
    }

}
