/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.put;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.ActionFilters;
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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.test.WatchExecutionContextMockBuilder;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TransportPutWatchActionTests extends ESTestCase {

    private TransportPutWatchAction action;
    private final Watch watch = new WatchExecutionContextMockBuilder("_id").buildMock().watch();
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final ClusterService clusterService = mock(ClusterService.class);
    private final TriggerService triggerService = mock(TriggerService.class);
    private final ActionListener<PutWatchResponse> listener = ActionListener.wrap(r -> {}, e -> assertThat(e, is(nullValue())));

    @Before
    public void setupAction() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        TransportService transportService = mock(TransportService.class);

        WatchParser parser = mock(WatchParser.class);
        when(parser.parseWithSecrets(eq("_id"), eq(false), anyObject(), anyObject(), anyObject(), anyBoolean(), anyLong(), anyLong()))
            .thenReturn(watch);
        WatchStatus status = mock(WatchStatus.class);
        WatchStatus.State state = new WatchStatus.State(true, DateTime.now(DateTimeZone.UTC));
        when(status.state()).thenReturn(state);
        when(watch.status()).thenReturn(status);

        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        // mock an index response that calls the listener
        doAnswer(invocation -> {
            UpdateRequest request = (UpdateRequest) invocation.getArguments()[0];
            ActionListener<UpdateResponse> listener = (ActionListener) invocation.getArguments()[1];

            ShardId shardId = new ShardId(new Index(Watch.INDEX, "uuid"), 0);
            listener.onResponse(new UpdateResponse(shardId, request.type(), request.id(), request.version(),
                DocWriteResponse.Result.UPDATED));

            return null;
        }).when(client).update(any(), any());

        action = new TransportPutWatchAction(Settings.EMPTY, transportService, threadPool,
                new ActionFilters(Collections.emptySet()), new IndexNameExpressionResolver(Settings.EMPTY), new ClockMock(),
                new XPackLicenseState(Settings.EMPTY), parser, client, clusterService, triggerService);
    }

    public void testHeadersAreFilteredWhenPuttingWatches() throws Exception {
        // set up threadcontext with some arbitrary info
        String headerName = randomFrom(ClientHelper.SECURITY_HEADER_FILTERS);
        threadContext.putHeader(headerName, randomAlphaOfLength(10));
        threadContext.putHeader(randomAlphaOfLength(10), "doesntmatter");

        PutWatchRequest putWatchRequest = new PutWatchRequest();
        putWatchRequest.setId("_id");

        ClusterState state = ClusterState.builder(new ClusterName("my_cluster"))
            .nodes(DiscoveryNodes.builder()
                .masterNodeId("node_1")
                .localNodeId(randomFrom("node_1", "node_2"))
                .add(newNode("node_1", Version.CURRENT))
                .add(newNode("node_2", Version.CURRENT)))
            .build();
        when(clusterService.state()).thenReturn(state);

        action.masterOperation(putWatchRequest, state, listener);

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        verify(watch.status()).setHeaders(captor.capture());
        Map<String, String> capturedHeaders = captor.getValue();
        assertThat(capturedHeaders.keySet(), hasSize(1));
        assertThat(capturedHeaders, hasKey(headerName));
    }

    public void testWatchesAreNeverTriggeredWhenDistributed() throws Exception {
        PutWatchRequest putWatchRequest = new PutWatchRequest();
        putWatchRequest.setId("_id");

        ClusterState clusterState = ClusterState.builder(new ClusterName("my_cluster"))
            .nodes(DiscoveryNodes.builder()
                .masterNodeId("node_1")
                .localNodeId(randomFrom("node_1", "node_2"))
                .add(newNode("node_1", Version.CURRENT))
                .add(newNode("node_2", Version.CURRENT)))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        action.masterOperation(putWatchRequest, clusterState, listener);

        verifyZeroInteractions(triggerService);
    }

    public void testWatchesAreNotTriggeredOnNonMasterWhenNotDistributed() throws Exception {
        PutWatchRequest putWatchRequest = new PutWatchRequest();
        putWatchRequest.setId("_id");

        ClusterState clusterState = ClusterState.builder(new ClusterName("my_cluster"))
            .nodes(DiscoveryNodes.builder()
                .masterNodeId("node_2")
                .localNodeId("node_1")
                .add(newNode("node_1", Version.CURRENT))
                .add(newNode("node_2", Version.V_5_6_10)))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        action.masterOperation(putWatchRequest, clusterState, listener);

        verifyZeroInteractions(triggerService);
    }

    public void testWatchesAreTriggeredOnMasterWhenNotDistributed() throws Exception {
        PutWatchRequest putWatchRequest = new PutWatchRequest();
        putWatchRequest.setId("_id");
        putWatchRequest.setVersion(randomLongBetween(1, 100));

        ClusterState clusterState = ClusterState.builder(new ClusterName("my_cluster"))
            .nodes(DiscoveryNodes.builder()
                .masterNodeId("node_1")
                .localNodeId("node_1")
                .add(newNode("node_1", Version.CURRENT))
                .add(newNode("node_2", Version.V_5_6_10)))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        action.masterOperation(putWatchRequest, clusterState, listener);

        verify(triggerService).add(eq(watch));
    }

    private static DiscoveryNode newNode(String nodeId, Version version) {
        return new DiscoveryNode(nodeId, ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
            new HashSet<>(asList(DiscoveryNode.Role.values())), version);
    }
}
