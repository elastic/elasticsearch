/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LifecyclePolicyClientTests extends ESTestCase {

    public void testExecuteWithHeadersAsyncNoHeaders() throws InterruptedException {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        final CountDownLatch latch = new CountDownLatch(2);
        final ActionListener<SearchResponse> listener = ActionTestUtils.assertNoFailureListener(v -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            latch.countDown();
        });

        doAnswer(invocationOnMock -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            latch.countDown();
            ((ActionListener<?>) invocationOnMock.getArguments()[2]).onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        SearchRequest request = new SearchRequest("foo");

        final var policyClient = new LifecyclePolicySecurityClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN, Map.of());
        policyClient.execute(TransportSearchAction.TYPE, request, listener);

        latch.await();
    }

    public void testExecuteWithHeadersAsyncWrongHeaders() throws InterruptedException {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        final CountDownLatch latch = new CountDownLatch(2);
        final ActionListener<SearchResponse> listener = ActionTestUtils.assertNoFailureListener(v -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            latch.countDown();
        });

        doAnswer(invocationOnMock -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            latch.countDown();
            ((ActionListener<?>) invocationOnMock.getArguments()[2]).onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        SearchRequest request = new SearchRequest("foo");
        Map<String, String> headers = Maps.newMapWithExpectedSize(1);
        headers.put("foo", "foo");
        headers.put("bar", "bar");

        final var policyClient = new LifecyclePolicySecurityClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN, headers);
        policyClient.execute(TransportSearchAction.TYPE, request, listener);

        latch.await();
    }

    public void testExecuteWithHeadersAsyncWithHeaders() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Client client = mock(Client.class);
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        final CountDownLatch latch = new CountDownLatch(2);
        final ActionListener<SearchResponse> listener = ActionTestUtils.assertNoFailureListener(v -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            latch.countDown();
        });

        doAnswer(invocationOnMock -> {
            assertThat(threadContext.getHeaders().size(), equalTo(2));
            assertThat(threadContext.getHeaders().get("es-security-runas-user"), equalTo("foo"));
            assertThat(threadContext.getHeaders().get("_xpack_security_authentication"), equalTo("bar"));
            latch.countDown();
            ((ActionListener<?>) invocationOnMock.getArguments()[2]).onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        SearchRequest request = new SearchRequest("foo");
        Map<String, String> headers = Maps.newMapWithExpectedSize(1);
        headers.put("es-security-runas-user", "foo");
        headers.put("_xpack_security_authentication", "bar");

        final var policyClient = new LifecyclePolicySecurityClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN, headers);
        policyClient.execute(TransportSearchAction.TYPE, request, listener);

        latch.await();
    }
}
