/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.activate;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.test.WatchExecutionContextMockBuilder;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransportActivateWatchActionTests extends ESTestCase {

    private TransportActivateWatchAction action;
    private Watch watch = new WatchExecutionContextMockBuilder("watch_id").buildMock().watch();
    private ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private TriggerService triggerService = mock(TriggerService.class);
    private ClusterService clusterService = mock(ClusterService.class);

    @Before
    public void setupAction() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        TransportService transportService = mock(TransportService.class);

        WatchParser parser = mock(WatchParser.class);
        when(parser.parseWithSecrets(eq("watch_id"), eq(true), anyObject(), anyObject(), anyObject(), anyLong(), anyLong()))
            .thenReturn(watch);

        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        // mock an update response that calls the listener
        doAnswer(invocation -> {
            UpdateRequest request = (UpdateRequest) invocation.getArguments()[0];
            ActionListener<UpdateResponse> listener = (ActionListener) invocation.getArguments()[1];

            ShardId shardId = new ShardId(new Index(Watch.INDEX, "uuid"), 0);
            listener.onResponse(new UpdateResponse(shardId, request.type(), request.id(), request.version(),
                DocWriteResponse.Result.UPDATED));

            return null;
        }).when(client).update(any(), any());

        // mock an get response that calls the listener
        doAnswer(invocation -> {
            GetRequest request = (GetRequest) invocation.getArguments()[0];
            ActionListener<GetResponse> listener = (ActionListener) invocation.getArguments()[1];

            GetResult getResult = new GetResult(request.index(), request.type(), request.id(), 20, 1, request.version(), true, null,
                Collections.emptyMap());
            listener.onResponse(new GetResponse(getResult));

            return null;
        }).when(client).get(any(), any());

        action = new TransportActivateWatchAction(Settings.EMPTY, transportService, threadPool,
            new ActionFilters(Collections.emptySet()), new IndexNameExpressionResolver(Settings.EMPTY), new ClockMock(),
            new XPackLicenseState(Settings.EMPTY), parser, clusterService, client, triggerService);
    }

    // when running in distributed mode, watches are only triggered by the indexing operation listener
    public void testWatchesAreNotTriggeredWhenDistributed() throws Exception {
        boolean watchActivated = randomBoolean();
        ActivateWatchRequest request = new ActivateWatchRequest("watch_id", watchActivated);
        ActionListener<ActivateWatchResponse> listener = PlainActionFuture.newFuture();

        // add a few nodes, with current versions
        ClusterState clusterState = ClusterState.builder(new ClusterName("my_cluster"))
            .nodes(DiscoveryNodes.builder()
                .masterNodeId("node_1")
                .localNodeId(randomFrom("node_1", "node_2"))
                .add(newNode("node_1", Version.CURRENT))
                .add(newNode("node_2", Version.CURRENT)))
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        mockWatchStatus(watchActivated);

        action.masterOperation(request, clusterState, listener);

        verifyNoMoreInteractions(triggerService);
    }

    public void testWatchesAreNotTriggeredOnNonMasterWhenNotDistributed() throws Exception {
        boolean watchActivated = randomBoolean();
        ActivateWatchRequest request = new ActivateWatchRequest("watch_id", watchActivated);
        ActionListener<ActivateWatchResponse> listener = PlainActionFuture.newFuture();

        // add a few nodes, with current versions
        ClusterState clusterState = ClusterState.builder(new ClusterName("my_cluster"))
            .nodes(DiscoveryNodes.builder()
                .masterNodeId("node_2")
                .localNodeId("node_1")
                .add(newNode("node_1", Version.CURRENT))
                .add(newNode("node_2", Version.V_5_6_10)))
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        mockWatchStatus(watchActivated);

        action.masterOperation(request, clusterState, listener);

        verifyNoMoreInteractions(triggerService);
    }

    // we trigger on the master node only, not on any other node
    public void testWatchesAreTriggeredOnMasterWhenNotDistributed() throws Exception {
        boolean watchActivated = randomBoolean();
        ActivateWatchRequest request = new ActivateWatchRequest("watch_id", watchActivated);
        ActionListener<ActivateWatchResponse> listener = PlainActionFuture.newFuture();

        // add a few nodes, with current versions
        ClusterState clusterState = ClusterState.builder(new ClusterName("my_cluster"))
            .nodes(DiscoveryNodes.builder()
                .masterNodeId("node_1")
                .localNodeId("node_1")
                .add(newNode("node_1", Version.CURRENT))
                .add(newNode("node_2", Version.V_5_6_10)))
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        mockWatchStatus(watchActivated);

        action.masterOperation(request, clusterState, listener);

        if (watchActivated) {
            verify(triggerService).add(eq(watch));
        } else {
            verify(triggerService).remove(eq("watch_id"));
        }
    }

    private void mockWatchStatus(boolean active) {
        WatchStatus status = mock(WatchStatus.class);
        WatchStatus.State state = new WatchStatus.State(active, DateTime.now(DateTimeZone.UTC));
        when(status.state()).thenReturn(state);
        when(watch.status()).thenReturn(status);
    }

    private static DiscoveryNode newNode(String nodeId, Version version) {
        return new DiscoveryNode(nodeId, ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
            new HashSet<>(asList(DiscoveryNode.Role.values())), version);
    }
}
