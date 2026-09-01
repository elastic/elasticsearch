/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SystemViewsTests extends ESTestCase {

    private static final PutViewAction.Request EXPECTED_REQUEST = new PutViewAction.Request(
        MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
        MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
        new View(SystemViews.VIEWS.entrySet().iterator().next().getKey(), SystemViews.VIEWS.entrySet().iterator().next().getValue())
    );

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private ViewService viewService;
    private SystemViews systemViews;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        viewService = mock(ViewService.class);
        systemViews = new SystemViews(clusterService, threadPool, viewService);
    }

    @After
    public void teardown() {
        clusterService.close();
        terminate(threadPool);
    }

    public void testDoesNothingBeforeGatewayRecovery() {
        systemViews.clusterChanged(clusterChangedEvent(false, true));
        flushThreadPoolExecutor(threadPool, ThreadPool.Names.GENERIC);
        verify(viewService, never()).putView(any(), any(), any());
    }

    public void testDoesNothingWhenNotMaster() {
        systemViews.clusterChanged(clusterChangedEvent(true, false));
        flushThreadPoolExecutor(threadPool, ThreadPool.Names.GENERIC);
        verify(viewService, never()).putView(any(), any(), any());
    }

    public void testCreatesViewOnMaster() {
        systemViews.clusterChanged(clusterChangedEvent(true, true));
        flushThreadPoolExecutor(threadPool, ThreadPool.Names.GENERIC);
        verify(viewService, times(1)).putView(eq(ProjectId.DEFAULT), eq(EXPECTED_REQUEST), any());
    }

    public void testIsIdempotent() {
        systemViews.clusterChanged(clusterChangedEvent(true, true));
        flushThreadPoolExecutor(threadPool, ThreadPool.Names.GENERIC);
        systemViews.clusterChanged(clusterChangedEvent(true, true));
        flushThreadPoolExecutor(threadPool, ThreadPool.Names.GENERIC);
        verify(viewService, times(1)).putView(eq(ProjectId.DEFAULT), eq(EXPECTED_REQUEST), any());
    }

    public void testUpdatesViewWhenDefinitionDrifts() {
        // The view already exists but with a stale query, so the bootstrap must overwrite it with the current definition.
        when(viewService.get(any(), any())).thenReturn(new View(EXPECTED_REQUEST.view().name(), "FROM this |s a stale query"));
        systemViews.clusterChanged(clusterChangedEvent(true, true));
        flushThreadPoolExecutor(threadPool, ThreadPool.Names.GENERIC);
        verify(viewService, times(1)).putView(eq(ProjectId.DEFAULT), eq(EXPECTED_REQUEST), any());
    }

    public void testUpdatesViewWhenDefinitionIsUpToDate() {
        // The view already exists but with a stale query, so the bootstrap must overwrite it with the current definition.
        when(viewService.get(any(), any())).thenReturn(EXPECTED_REQUEST.view());
        systemViews.clusterChanged(clusterChangedEvent(true, true));
        verify(viewService, never()).putView(any(), any(), any());
    }

    private static ClusterChangedEvent clusterChangedEvent(boolean gatewayRecovered, boolean localNodeMaster) {
        DiscoveryNode localNode = DiscoveryNodeUtils.create("local_node");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder().add(localNode).localNodeId("local_node");
        if (localNodeMaster) {
            nodes.masterNodeId("local_node");
        }
        ClusterBlocks.Builder blocks = ClusterBlocks.builder();
        if (gatewayRecovered == false) {
            blocks.addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
        }
        ClusterState state = ClusterState.builder(new ClusterName("cluster_name")).nodes(nodes).blocks(blocks).build();
        return new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE);
    }
}
