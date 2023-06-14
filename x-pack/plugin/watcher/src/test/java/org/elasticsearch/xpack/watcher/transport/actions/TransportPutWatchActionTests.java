/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.actions;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.ClockHolder;
import org.elasticsearch.xpack.watcher.test.WatchExecutionContextMockBuilder;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutWatchActionTests extends ESTestCase {

    private TransportPutWatchAction action;
    private Watch watch = new WatchExecutionContextMockBuilder("_id").buildMock().watch();
    private ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    @Before
    public void setupAction() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        TransportService transportService = mock(TransportService.class);

        WatchParser parser = mock(WatchParser.class);
        when(parser.parseWithSecrets(eq("_id"), eq(false), any(), any(), any(), anyBoolean(), anyLong(), anyLong())).thenReturn(watch);

        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        // mock an index response that calls the listener
        doAnswer(invocation -> {
            IndexRequest request = (IndexRequest) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<IndexResponse> listener = (ActionListener) invocation.getArguments()[2];

            ShardId shardId = new ShardId(new Index(Watch.INDEX, "uuid"), 0);
            listener.onResponse(new IndexResponse(shardId, request.id(), 1, 1, 1, true));

            return null;
        }).when(client).execute(any(), any(), any());

        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.getMinTransportVersion()).thenReturn(TransportVersion.current());

        action = new TransportPutWatchAction(
            transportService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            new ClockHolder(new ClockMock()),
            TestUtils.newTestLicenseState(),
            parser,
            client,
            clusterService
        );
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testHeadersAreFilteredWhenPuttingWatches() throws Exception {
        // set up threadcontext with some arbitrary info
        String headerName = randomFrom(ClientHelper.SECURITY_HEADER_FILTERS);
        if (Set.of(AuthenticationField.AUTHENTICATION_KEY, SecondaryAuthentication.THREAD_CTX_KEY).contains(headerName)) {
            threadContext.putHeader(
                headerName,
                Authentication.newRealmAuthentication(new User("dummy"), new Authentication.RealmRef("name", "type", "node")).encode()
            );
        } else {
            threadContext.putHeader(headerName, randomAlphaOfLength(10));
        }
        threadContext.putHeader(randomAlphaOfLength(10), "doesntmatter");

        PutWatchRequest putWatchRequest = new PutWatchRequest();
        putWatchRequest.setId("_id");
        action.doExecute(putWatchRequest, ActionListener.wrap(r -> {}, e -> assertThat(e, is(nullValue()))));

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        verify(watch.status()).setHeaders(captor.capture());
        Map<String, String> capturedHeaders = captor.getValue();
        assertThat(capturedHeaders.keySet(), hasSize(1));
        assertThat(capturedHeaders, hasKey(headerName));
    }
}
