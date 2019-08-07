/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.ack;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.watcher.WatcherMetaData;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionSnapshot;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.ClockHolder;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.junit.Before;

import java.time.Clock;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportAckWatchActionTests extends ESTestCase {

    private TransportAckWatchAction action;
    private Client client;

    @Before
    public void setupAction() {
        TransportService transportService = mock(TransportService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        WatchParser watchParser = mock(WatchParser.class);
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        action = new TransportAckWatchAction(transportService, new ActionFilters(Collections.emptySet()),
            new ClockHolder(Clock.systemUTC()), new XPackLicenseState(Settings.EMPTY),
            watchParser, client, createClusterService(threadPool));
    }

    public void testWatchNotFound() {
        String watchId = "my_watch_id";
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocation.getArguments()[1];
            listener.onResponse(new GetResponse(new GetResult(Watch.INDEX, MapperService.SINGLE_MAPPING_NAME, watchId, UNASSIGNED_SEQ_NO,
                0, -1, false, BytesArray.EMPTY, Collections.emptyMap(), Collections.emptyMap())));
            return null;
        }).when(client).get(anyObject(), anyObject());

        doAnswer(invocation -> {
            ContextPreservingActionListener listener = (ContextPreservingActionListener) invocation.getArguments()[2];
            listener.onResponse(new WatcherStatsResponse(new ClusterName("clusterName"), new WatcherMetaData(false),
                Collections.emptyList(), Collections.emptyList()));
            return null;
        }).when(client).execute(eq(WatcherStatsAction.INSTANCE), anyObject(), anyObject());

        AckWatchRequest ackWatchRequest = new AckWatchRequest(watchId);
        PlainActionFuture<AckWatchResponse> listener = PlainActionFuture.newFuture();
        action.doExecute(ackWatchRequest, listener);

        ExecutionException exception = expectThrows(ExecutionException.class, listener::get);
        ElasticsearchException e = (ElasticsearchException) exception.getCause();
        assertThat(e.getMessage(), is("Watch with id [" + watchId + "] does not exist"));
    }

    public void testThatWatchCannotBeAckedWhileRunning() {
        String watchId = "my_watch_id";

        doAnswer(invocation -> {
            ContextPreservingActionListener listener = (ContextPreservingActionListener) invocation.getArguments()[2];
            DiscoveryNode discoveryNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
            WatcherStatsResponse.Node node = new WatcherStatsResponse.Node(discoveryNode);
            WatchExecutionSnapshot snapshot = mock(WatchExecutionSnapshot.class);
            when(snapshot.watchId()).thenReturn(watchId);
            node.setSnapshots(Collections.singletonList(snapshot));
            listener.onResponse(new WatcherStatsResponse(new ClusterName("clusterName"),
                new WatcherMetaData(false), Collections.singletonList(node), Collections.emptyList()));
            return null;
        }).when(client).execute(eq(WatcherStatsAction.INSTANCE), anyObject(), anyObject());

        AckWatchRequest ackWatchRequest = new AckWatchRequest(watchId);
        PlainActionFuture<AckWatchResponse> listener = PlainActionFuture.newFuture();
        action.doExecute(ackWatchRequest, listener);

        ExecutionException exception = expectThrows(ExecutionException.class, listener::get);
        ElasticsearchException e = (ElasticsearchException) exception.getCause();
        assertThat(e.getMessage(), is("watch[my_watch_id] is running currently, cannot ack until finished"));
        assertThat(e.status(), is(RestStatus.CONFLICT));
    }
}
