/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyObject;
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
        final ActionListener<SearchResponse> listener = ActionListener.wrap(v -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            latch.countDown();
        }, e -> fail(e.getMessage()));

        doAnswer(invocationOnMock -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            latch.countDown();
            ((ActionListener<?>) invocationOnMock.getArguments()[2]).onResponse(null);
            return null;
        }).when(client).execute(anyObject(), anyObject(), anyObject());

        SearchRequest request = new SearchRequest("foo");

        try (LifecyclePolicySecurityClient policyClient = new LifecyclePolicySecurityClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN,
                Collections.emptyMap())) {
            policyClient.execute(SearchAction.INSTANCE, request, listener);
        }

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
        final ActionListener<SearchResponse> listener = ActionListener.wrap(v -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            latch.countDown();
        }, e -> fail(e.getMessage()));

        doAnswer(invocationOnMock -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            latch.countDown();
            ((ActionListener<?>) invocationOnMock.getArguments()[2]).onResponse(null);
            return null;
        }).when(client).execute(anyObject(), anyObject(), anyObject());

        SearchRequest request = new SearchRequest("foo");
        Map<String, String> headers = new HashMap<>(1);
        headers.put("foo", "foo");
        headers.put("bar", "bar");

        try (LifecyclePolicySecurityClient policyClient = new LifecyclePolicySecurityClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN,
                headers)) {
            policyClient.execute(SearchAction.INSTANCE, request, listener);
        }

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
        final ActionListener<SearchResponse> listener = ActionListener.wrap(v -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            latch.countDown();
        }, e -> fail(e.getMessage()));

        doAnswer(invocationOnMock -> {
            assertThat(threadContext.getHeaders().size(), equalTo(2));
            assertThat(threadContext.getHeaders().get("es-security-runas-user"), equalTo("foo"));
            assertThat(threadContext.getHeaders().get("_xpack_security_authentication"), equalTo("bar"));
            latch.countDown();
            ((ActionListener<?>) invocationOnMock.getArguments()[2]).onResponse(null);
            return null;
        }).when(client).execute(anyObject(), anyObject(), anyObject());

        SearchRequest request = new SearchRequest("foo");
        Map<String, String> headers = new HashMap<>(1);
        headers.put("es-security-runas-user", "foo");
        headers.put("_xpack_security_authentication", "bar");

        try (LifecyclePolicySecurityClient policyClient = new LifecyclePolicySecurityClient(client, ClientHelper.INDEX_LIFECYCLE_ORIGIN,
                headers)) {
            policyClient.execute(SearchAction.INSTANCE, request, listener);
        }

        latch.await();
    }
}
