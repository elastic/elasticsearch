/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.watcher.WatcherMetadata;
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
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TransportAckWatchActionTests extends ESTestCase {

    private TransportAckWatchAction action;
    private Client client;

    @Before
    public void setupAction() {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        WatchParser watchParser = mock(WatchParser.class);
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        action = new TransportAckWatchAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            new ClockHolder(Clock.systemUTC()),
            TestUtils.newTestLicenseState(),
            watchParser,
            client
        );
    }

    public void testWatchNotFound() {
        String watchId = "my_watch_id";
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) invocation.getArguments()[1];
            listener.onResponse(
                new GetResponse(
                    new GetResult(
                        Watch.INDEX,
                        watchId,
                        UNASSIGNED_SEQ_NO,
                        0,
                        -1,
                        false,
                        BytesArray.EMPTY,
                        Collections.emptyMap(),
                        Collections.emptyMap()
                    )
                )
            );
            return null;
        }).when(client).get(any(), any());

        doAnswer(invocation -> {
            ContextPreservingActionListener listener = (ContextPreservingActionListener) invocation.getArguments()[2];
            listener.onResponse(
                new WatcherStatsResponse(
                    new ClusterName("clusterName"),
                    new WatcherMetadata(false),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            );
            return null;
        }).when(client).execute(eq(WatcherStatsAction.INSTANCE), any(), any());

        AckWatchRequest ackWatchRequest = new AckWatchRequest(watchId);
        PlainActionFuture<AckWatchResponse> listener = new PlainActionFuture<>();
        action.doExecute(ackWatchRequest, listener);

        ExecutionException exception = expectThrows(ExecutionException.class, listener::get);
        ElasticsearchException e = (ElasticsearchException) exception.getCause();
        assertThat(e.getMessage(), is("Watch with id [" + watchId + "] does not exist"));
    }

    public void testThatWatchCannotBeAckedWhileRunning() {
        String watchId = "my_watch_id";

        doAnswer(invocation -> {
            ContextPreservingActionListener listener = (ContextPreservingActionListener) invocation.getArguments()[2];
            DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("node_2");
            WatcherStatsResponse.Node node = new WatcherStatsResponse.Node(discoveryNode);
            WatchExecutionSnapshot snapshot = mock(WatchExecutionSnapshot.class);
            when(snapshot.watchId()).thenReturn(watchId);
            node.setSnapshots(Collections.singletonList(snapshot));
            listener.onResponse(
                new WatcherStatsResponse(
                    new ClusterName("clusterName"),
                    new WatcherMetadata(false),
                    Collections.singletonList(node),
                    Collections.emptyList()
                )
            );
            return null;
        }).when(client).execute(eq(WatcherStatsAction.INSTANCE), any(), any());

        AckWatchRequest ackWatchRequest = new AckWatchRequest(watchId);
        PlainActionFuture<AckWatchResponse> listener = new PlainActionFuture<>();
        action.doExecute(ackWatchRequest, listener);

        ExecutionException exception = expectThrows(ExecutionException.class, listener::get);
        ElasticsearchException e = (ElasticsearchException) exception.getCause();
        assertThat(e.getMessage(), is("watch[my_watch_id] is running currently, cannot ack until finished"));
        assertThat(e.status(), is(RestStatus.CONFLICT));
    }
}
