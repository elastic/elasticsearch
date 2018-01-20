/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClientHelperTests extends ESTestCase {

    public void testStashContext() {
        final String origin = randomAlphaOfLengthBetween(4, 16);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        final boolean setOtherValues = randomBoolean();
        if (setOtherValues) {
            threadContext.putTransient("foo", "bar");
            threadContext.putHeader("foo", "bar");
        }

        assertNull(threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
        ThreadContext.StoredContext storedContext = ClientHelper.stashWithOrigin(threadContext, origin);
        assertEquals(origin, threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
        assertNull(threadContext.getTransient("foo"));
        assertNull(threadContext.getTransient("bar"));

        storedContext.close();
        assertNull(threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));

        if (setOtherValues) {
            assertEquals("bar", threadContext.getTransient("foo"));
            assertEquals("bar", threadContext.getHeader("foo"));
        }
    }

    public void testExecuteAsyncWrapsListener() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final String headerName = randomAlphaOfLengthBetween(4, 16);
        final String headerValue = randomAlphaOfLengthBetween(4, 16);
        final String origin = randomAlphaOfLengthBetween(4, 16);
        final CountDownLatch latch = new CountDownLatch(2);
        final ActionListener<ClusterHealthResponse> listener = ActionListener.wrap(v -> {
            assertNull(threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
            assertEquals(headerValue, threadContext.getHeader(headerName));
            latch.countDown();
        }, e -> fail(e.getMessage()));

        final ClusterHealthRequest request = new ClusterHealthRequest();
        threadContext.putHeader(headerName, headerValue);

        ClientHelper.executeAsyncWithOrigin(threadContext, origin, request, listener, (req, listener1) -> {
            assertSame(request, req);
            assertEquals(origin, threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
            assertNull(threadContext.getHeader(headerName));
            latch.countDown();
            listener1.onResponse(null);
        });

        latch.await();
    }

    public void testExecuteWithClient() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Client client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        final String headerName = randomAlphaOfLengthBetween(4, 16);
        final String headerValue = randomAlphaOfLengthBetween(4, 16);
        final String origin = randomAlphaOfLengthBetween(4, 16);
        final CountDownLatch latch = new CountDownLatch(2);
        final ActionListener<ClusterHealthResponse> listener = ActionListener.wrap(v -> {
            assertNull(threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
            assertEquals(headerValue, threadContext.getHeader(headerName));
            latch.countDown();
        }, e -> fail(e.getMessage()));

        doAnswer(invocationOnMock -> {
            assertEquals(origin, threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
            assertNull(threadContext.getHeader(headerName));
            latch.countDown();
            ((ActionListener)invocationOnMock.getArguments()[2]).onResponse(null);
            return null;
        }).when(client).execute(anyObject(), anyObject(), anyObject());

        threadContext.putHeader(headerName, headerValue);
        ClientHelper.executeAsyncWithOrigin(client, origin, ClusterHealthAction.INSTANCE, new ClusterHealthRequest(), listener);

        latch.await();
    }

    public void testClientWithOrigin() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Client client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.settings()).thenReturn(Settings.EMPTY);

        final String headerName = randomAlphaOfLengthBetween(4, 16);
        final String headerValue = randomAlphaOfLengthBetween(4, 16);
        final String origin = randomAlphaOfLengthBetween(4, 16);
        final CountDownLatch latch = new CountDownLatch(2);
        final ActionListener<ClusterHealthResponse> listener = ActionListener.wrap(v -> {
            assertNull(threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
            assertEquals(headerValue, threadContext.getHeader(headerName));
            latch.countDown();
        }, e -> fail(e.getMessage()));


        doAnswer(invocationOnMock -> {
            assertEquals(origin, threadContext.getTransient(ClientHelper.ACTION_ORIGIN_TRANSIENT_NAME));
            assertNull(threadContext.getHeader(headerName));
            latch.countDown();
            ((ActionListener)invocationOnMock.getArguments()[2]).onResponse(null);
            return null;
        }).when(client).execute(anyObject(), anyObject(), anyObject());

        threadContext.putHeader(headerName, headerValue);
        Client clientWithOrigin = ClientHelper.clientWithOrigin(client, origin);
        clientWithOrigin.execute(null, null, listener);
        latch.await();
    }
}
