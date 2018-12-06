/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class
WatcherTransportActionTests extends ESTestCase {

    private MyTransportAction transportAction;
    private ClusterService clusterService;
    private TransportService transportService;

    @Before
    public void createTransportAction() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any())).thenReturn(EsExecutors.newDirectExecutorService());
        clusterService = mock(ClusterService.class);
        transportService = mock(TransportService.class);
        transportAction = new MyTransportAction(transportService, threadPool, clusterService);
    }

    public void testThatRequestIsExecutedLocallyWithDistributedExecutionEnabled() throws Exception {
        DiscoveryNodes nodes = new DiscoveryNodes.Builder()
                .masterNodeId("master_node").localNodeId("data_node")
                .add(newNode("master_node", Version.CURRENT))
                .add(newNode("data_node", Version.CURRENT))
                .build();

        Index watchIndex = new Index(Watch.INDEX, "foo");
        ShardId shardId = new ShardId(watchIndex, 0);
        IndexRoutingTable routingTable = IndexRoutingTable.builder(watchIndex)
                .addShard(TestShardRouting.newShardRouting(shardId, "data_node", true, STARTED)).build();

        ClusterState state = ClusterState.builder(new ClusterName("my-cluster"))
                .nodes(nodes)
                .routingTable(RoutingTable.builder().add(routingTable).build())
                .build();

        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(state.nodes().getLocalNode());
        MyActionRequest request = new MyActionRequest();
        PlainActionFuture<MyActionResponse> future = PlainActionFuture.newFuture();

        Task task = request.createTask(1, "type", "action", new TaskId("parent", 0), Collections.emptyMap());
        transportAction.doExecute(task, request, future);
        MyActionResponse response = future.actionGet(1000);
        assertThat(response.request, is(request));
    }

    public void testThatRequestIsExecutedByMasterWithDistributedExecutionDisabled() throws Exception {
        DiscoveryNodes nodes = new DiscoveryNodes.Builder()
                .masterNodeId("master_node").localNodeId("master_node")
                .add(newNode("master_node", VersionUtils.randomVersionBetween(random(), Version.V_5_6_0, Version.V_6_0_0_alpha2)))
                .build();
        ClusterState state = ClusterState.builder(new ClusterName("my-cluster")).nodes(nodes).build();

        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(state.nodes().getLocalNode());
        MyActionRequest request = new MyActionRequest();
        PlainActionFuture<MyActionResponse> future = PlainActionFuture.newFuture();

        Task task = request.createTask(1, "type", "action", new TaskId("parent", 0), Collections.emptyMap());
        transportAction.doExecute(task, request, future);
        MyActionResponse response = future.actionGet(1000);
        assertThat(response.request, is(request));
    }

    public void testThatRequestIsForwardedToMasterWithDistributedExecutionDisabled() throws Exception {
        DiscoveryNodes nodes = new DiscoveryNodes.Builder()
                .masterNodeId("master_node").localNodeId("non_master_node")
                .add(newNode("master_node", VersionUtils.randomVersionBetween(random(), Version.V_5_6_0, Version.V_6_0_0_alpha2)))
                .add(newNode("non_master_node", Version.CURRENT))
                .build();

        ClusterState state = ClusterState.builder(new ClusterName("my-cluster")).nodes(nodes).build();

        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(state.nodes().getLocalNode());
        MyActionRequest request = new MyActionRequest();
        Task task = request.createTask(1, "type", "action", new TaskId("parent", 0), Collections.emptyMap());
        transportAction.doExecute(task, request, PlainActionFuture.newFuture());
        // dont wait for the future here, we would need to stub the action listener of the sendRequest call

        ArgumentCaptor<DiscoveryNode> nodeArgumentCaptor = ArgumentCaptor.forClass(DiscoveryNode.class);
        verify(transportService).sendRequest(nodeArgumentCaptor.capture(), eq("my_action_name"), eq(request), any());
        assertThat(nodeArgumentCaptor.getValue().getId(), is("master_node"));
    }

    private static DiscoveryNode newNode(String nodeName, Version version) {
        return new DiscoveryNode(nodeName, ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                new HashSet<>(asList(DiscoveryNode.Role.values())), version);
    }

    private final class MyTransportAction extends WatcherTransportAction<MyActionRequest, MyActionResponse> {

        MyTransportAction(TransportService transportService, ThreadPool threadPool, ClusterService clusterService) {
            super(Settings.EMPTY, "my_action_name", transportService, threadPool, new ActionFilters(Collections.emptySet()),
                    new IndexNameExpressionResolver(Settings.EMPTY), new XPackLicenseState(Settings.EMPTY), clusterService,
                    MyActionRequest::new, MyActionResponse::new);
        }

        @Override
        protected void masterOperation(MyActionRequest request, ClusterState state,
                                       ActionListener<MyActionResponse> listener) throws Exception {
            listener.onResponse(new MyActionResponse(request));
        }
    }

    private static final class MyActionResponse extends ActionResponse {

        MyActionRequest request;

        MyActionResponse(MyActionRequest request) {
            super();
            this.request = request;
        }

        MyActionResponse() {}
    }

    private static final class MyActionRequest extends MasterNodeRequest<MyActionRequest> {

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }
}