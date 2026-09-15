/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class DefaultRestInterceptorChainTests extends ESTestCase {

    private final RestChannel channel = mock(RestChannel.class);
    private final NodeClient client = mock(NodeClient.class);
    private final RestHandler handler = mock(RestHandler.class);
    private final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();

    public void testEmptyChainCallsHandler() throws Exception {
        runChain(handler, List.of()).actionGet(10, TimeUnit.SECONDS);
        verify(handler).handleRequest(request, channel, client);
    }

    public void testHandlerExceptionPropagatesAsFailure() {
        var ex = new RuntimeException("boom");
        var throwingHandler = new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                throw ex;
            }

            @Override
            public List<Route> routes() {
                return List.of();
            }
        };

        var future = runChain(throwingHandler, List.of());
        assertSame(ex, expectThrows(RuntimeException.class, () -> future.actionGet(10, TimeUnit.SECONDS)));
    }

    public void testInterceptorShortCircuitsWithoutCallingHandler() {
        runChain(handler, List.of((chain, listener) -> listener.onResponse(null))).actionGet(10, TimeUnit.SECONDS);

        verifyNoInteractions(handler);
    }

    public void testInterceptorProceedingReachesHandler() throws Exception {
        runChain(handler, List.of(RestInterceptorChain::proceed)).actionGet(10, TimeUnit.SECONDS);

        verify(handler).handleRequest(request, channel, client);
    }

    public void testMultipleInterceptorsCalledInOrder() {
        List<Integer> callOrder = new ArrayList<>();
        RestInterceptor first = (chain, listener) -> {
            callOrder.add(1);
            chain.proceed(listener);
        };
        RestInterceptor second = (chain, listener) -> {
            callOrder.add(2);
            chain.proceed(listener);
        };

        runChain(handler, List.of(first, second)).actionGet(10, TimeUnit.SECONDS);

        assertThat(callOrder, equalTo(List.of(1, 2)));
    }

    public void testInterceptorCanSubstituteHandlerAndChannel() throws Exception {
        RestHandler substituteHandler = mock(RestHandler.class);
        RestChannel substituteChannel = mock(RestChannel.class);
        RestInterceptor swapper = (chain, listener) -> chain.proceed(chain.request(), substituteChannel, substituteHandler, listener);

        runChain(handler, List.of(swapper)).actionGet(10, TimeUnit.SECONDS);

        verify(substituteHandler).handleRequest(request, substituteChannel, client);
        verifyNoInteractions(handler);
    }

    public void testChainAccessorsReflectConstructorValues() {
        var chain = new DefaultRestInterceptorChain(request, channel, handler, client, List.of());
        assertSame(request, chain.request());
        assertSame(channel, chain.channel());
        assertSame(handler, chain.handler());
    }

    private PlainActionFuture<Void> runChain(RestHandler handler, List<RestInterceptor> interceptors) {
        var chain = new DefaultRestInterceptorChain(request, channel, handler, client, interceptors);
        var future = new PlainActionFuture<Void>();
        chain.proceed(future);
        return future;
    }
}
