/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WatcherLifeCycleServiceTests extends ESTestCase {
    private ClusterService clusterService;
    private WatcherService watcherService;
    private WatcherLifeCycleService lifeCycleService;

    @Before
    public void prepareServices() {
        ThreadPool threadPool = mock(ThreadPool.class);
        final ExecutorService executorService = EsExecutors.newDirectExecutorService();
        when(threadPool.executor(anyString())).thenReturn(executorService);
        clusterService = mock(ClusterService.class);
        Answer<Object> answer = new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                AckedClusterStateUpdateTask updateTask = (AckedClusterStateUpdateTask) invocationOnMock.getArguments()[1];
                updateTask.onAllNodesAcked(null);
                return null;
            }
        };
        doAnswer(answer).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        watcherService = mock(WatcherService.class);
        lifeCycleService = new WatcherLifeCycleService(Settings.EMPTY, threadPool, clusterService, watcherService);
    }

    public void testStartAndStopCausedByClusterState() throws Exception {
        // starting... local node is master node
        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1");
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        when(watcherService.state()).thenReturn(WatcherState.STOPPED);
        when(watcherService.validate(clusterState)).thenReturn(true);
        lifeCycleService.applyClusterState(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, times(1)).start(clusterState);
        verify(watcherService, never()).stop();

        // Trying to start a second time, but that should have no affect.
        when(watcherService.state()).thenReturn(WatcherState.STARTED);
        lifeCycleService.applyClusterState(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, times(1)).start(clusterState);
        verify(watcherService, never()).stop();

        // Stopping because local node is no longer master node
        nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id2");
        ClusterState noMasterClusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        lifeCycleService.applyClusterState(new ClusterChangedEvent("any", noMasterClusterState, noMasterClusterState));
        verify(watcherService, times(1)).stop();
        verify(watcherService, times(1)).start(clusterState);
    }

    public void testStartWithStateNotRecoveredBlock() throws Exception {
        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1");
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
                .nodes(nodes).build();
        when(watcherService.state()).thenReturn(WatcherState.STOPPED);
        lifeCycleService.applyClusterState(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, never()).start(any(ClusterState.class));
    }

    public void testManualStartStop() throws Exception {
        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1");
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        when(clusterService.state()).thenReturn(clusterState);
        when(watcherService.validate(clusterState)).thenReturn(true);

        when(watcherService.state()).thenReturn(WatcherState.STOPPED);
        lifeCycleService.start();
        verify(watcherService, times(1)).start(any(ClusterState.class));
        verify(watcherService, never()).stop();

        when(watcherService.state()).thenReturn(WatcherState.STARTED);
        lifeCycleService.stop();
        verify(watcherService, times(1)).start(any(ClusterState.class));
        verify(watcherService, times(1)).stop();

        // Starting via cluster state update, we shouldn't start because we have been stopped manually.
        when(watcherService.state()).thenReturn(WatcherState.STOPPED);
        lifeCycleService.applyClusterState(new ClusterChangedEvent("any", clusterState, clusterState));
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
        when(watcherService.state()).thenReturn(WatcherState.STARTED);
        lifeCycleService.applyClusterState(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, times(2)).start(any(ClusterState.class));
        verify(watcherService, times(2)).stop();

        // starting watcher via cluster state update, which should work, because we manually started before
        nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1");
        clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        when(watcherService.validate(clusterState)).thenReturn(true);
        when(watcherService.state()).thenReturn(WatcherState.STOPPED);
        lifeCycleService.applyClusterState(new ClusterChangedEvent("any", clusterState, clusterState));
        verify(watcherService, times(3)).start(any(ClusterState.class));
        verify(watcherService, times(2)).stop();
    }

    public void testManualStartStopClusterStateNotValid() throws Exception {
        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1");
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        when(clusterService.state()).thenReturn(clusterState);
        when(watcherService.state()).thenReturn(WatcherState.STOPPED);
        when(watcherService.validate(clusterState)).thenReturn(false);


        lifeCycleService.start();
        verify(watcherService, never()).start(any(ClusterState.class));
        verify(watcherService, never()).stop();
    }

    public void testManualStartStopWatcherNotStopped() throws Exception {
        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1");
        ClusterState clusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes).build();
        when(clusterService.state()).thenReturn(clusterState);
        when(watcherService.state()).thenReturn(WatcherState.STOPPING);

        lifeCycleService.start();
        verify(watcherService, never()).validate(any(ClusterState.class));
        verify(watcherService, never()).start(any(ClusterState.class));
        verify(watcherService, never()).stop();
    }

    public void testWatchIndexDeletion() throws Exception {
        DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1").build();
        // old cluster state that contains watcher index
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        ClusterState oldClusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .metaData(new MetaData.Builder().put(IndexMetaData.builder(Watch.INDEX)
                        .settings(indexSettings).numberOfReplicas(0).numberOfShards(1)))
                .nodes(discoveryNodes).build();

        // new cluster state that does not contain watcher index
        ClusterState newClusterState = ClusterState.builder(new ClusterName("my-cluster")).nodes(discoveryNodes).build();
        when(watcherService.state()).thenReturn(WatcherState.STARTED);

        lifeCycleService.applyClusterState(new ClusterChangedEvent("any", newClusterState, oldClusterState));
        verify(watcherService, never()).start(any(ClusterState.class));
        verify(watcherService, never()).stop();
        verify(watcherService, times(1)).watchIndexDeletedOrClosed();
    }

    public void testWatchIndexClosing() throws Exception {
        DiscoveryNodes discoveryNodes = new DiscoveryNodes.Builder().masterNodeId("id1").localNodeId("id1").build();
        // old cluster state that contains watcher index
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        ClusterState oldClusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .metaData(new MetaData.Builder().put(IndexMetaData.builder(Watch.INDEX)
                        .settings(indexSettings).numberOfReplicas(0).numberOfShards(1)))
                .nodes(discoveryNodes).build();

        // new cluster state with a closed watcher index
        ClusterState newClusterState = ClusterState.builder(new ClusterName("my-cluster"))
                .metaData(new MetaData.Builder().put(IndexMetaData.builder(Watch.INDEX).state(IndexMetaData.State.CLOSE)
                .settings(indexSettings).numberOfReplicas(0).numberOfShards(1)))
                .nodes(discoveryNodes).build();
        when(watcherService.state()).thenReturn(WatcherState.STARTED);

        lifeCycleService.applyClusterState(new ClusterChangedEvent("any", newClusterState, oldClusterState));
        verify(watcherService, never()).start(any(ClusterState.class));
        verify(watcherService, never()).stop();
        verify(watcherService, times(1)).watchIndexDeletedOrClosed();
    }

}
