/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCounted;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.NullDispatcher;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.contains;

/**
 * This test just tests whether the pipelining works in general without any connection to the Elasticsearch handler
 */
public class Netty4HttpServerPipeliningTests extends ESTestCase {
    private NetworkService networkService;
    private ThreadPool threadPool;

    @Before
    public void setup() {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    public void testThatHttpPipeliningWorks() throws Exception {
        final Settings settings = Settings.builder().put("http.port", "0").build();
        try (HttpServerTransport httpServerTransport = new CustomNettyHttpServerTransport(settings)) {
            httpServerTransport.start();
            final TransportAddress transportAddress = randomFrom(httpServerTransport.boundAddress().boundAddresses());

            final int numberOfRequests = randomIntBetween(4, 16);
            final List<String> requests = new ArrayList<>(numberOfRequests);
            for (int i = 0; i < numberOfRequests; i++) {
                if (rarely()) {
                    requests.add("/slow/" + i);
                } else {
                    requests.add("/" + i);
                }
            }

            try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
                Collection<FullHttpResponse> responses = nettyHttpClient.get(transportAddress.address(), requests.toArray(new String[] {}));
                try {
                    Collection<String> responseBodies = Netty4HttpClient.returnHttpResponseBodies(responses);
                    assertThat(responseBodies, contains(requests.toArray()));
                } finally {
                    responses.forEach(ReferenceCounted::release);
                }
            }
        }
    }

    class CustomNettyHttpServerTransport extends Netty4HttpServerTransport {

        private final ExecutorService executorService = Executors.newCachedThreadPool();

        CustomNettyHttpServerTransport(final Settings settings) {
            super(
                settings,
                Netty4HttpServerPipeliningTests.this.networkService,
                Netty4HttpServerPipeliningTests.this.threadPool,
                xContentRegistry(),
                new NullDispatcher(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                new SharedGroupFactory(settings),
                Tracer.NOOP,
                TLSConfig.noTLS(),
                null,
                randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
            );
        }

        @Override
        protected void doClose() {
            executorService.shutdown();
            super.doClose();
        }

        @Override
        public void incomingRequest(HttpRequest httpRequest, HttpChannel httpChannel) {
            executorService.submit(() -> {
                final Netty4HttpRequest pipelinedRequest = (Netty4HttpRequest) httpRequest;
                try {
                    final String uri = pipelinedRequest.uri();

                    final ByteBuf buffer = Unpooled.copiedBuffer(uri, StandardCharsets.UTF_8);

                    HttpResponse response = pipelinedRequest.createResponse(
                        RestStatus.OK,
                        new BytesArray(uri.getBytes(StandardCharsets.UTF_8))
                    );
                    response.addHeader("content-length", Integer.toString(buffer.readableBytes()));

                    final boolean slow = uri.matches("/slow/\\d+");
                    if (slow) {
                        try {
                            Thread.sleep(scaledRandomIntBetween(500, 1000));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        assert uri.matches("/\\d+");
                    }

                    httpChannel.sendResponse(response, ActionListener.noop());
                } finally {
                    pipelinedRequest.release();
                }
            });
        }
    }

}
