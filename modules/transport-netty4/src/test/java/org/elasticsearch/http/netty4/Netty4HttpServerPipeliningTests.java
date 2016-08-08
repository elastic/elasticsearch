/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

/**
 * This test just tests, if he pipelining works in general with out any connection the Elasticsearch handler
 */
public class Netty4HttpServerPipeliningTests extends ESTestCase {
    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Settings.EMPTY, Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    public void testThatHttpPipeliningWorksWhenEnabled() throws Exception {
        final Settings settings = Settings.builder()
            .put("http.pipelining", true)
            .put("http.port", "0")
            .build();
        try (final HttpServerTransport httpServerTransport = new CustomNettyHttpServerTransport(settings)) {
            httpServerTransport.start();
            final InetSocketTransportAddress transportAddress =
                (InetSocketTransportAddress) randomFrom(httpServerTransport.boundAddress().boundAddresses());

            final int numberOfRequests = randomIntBetween(4, 16);
            final List<String> requests = new ArrayList<>(numberOfRequests);
            for (int i = 0; i < numberOfRequests; i++) {
                if (rarely()) {
                    requests.add("/slow?sleep=" + scaledRandomIntBetween(500, 1000));
                } else {
                    requests.add("/" + i);
                }
            }

            try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
                Collection<FullHttpResponse> responses = nettyHttpClient.get(transportAddress.address(), requests.toArray(new String[]{}));
                Collection<String> responseBodies = Netty4HttpClient.returnHttpResponseBodies(responses);
                assertThat(responseBodies, contains(requests.toArray()));
            }
        }
    }

    public void testThatHttpPipeliningCanBeDisabled() throws Exception {
        final Settings settings = Settings.builder()
            .put("http.pipelining", false)
            .put("http.port", "0")
            .build();
        try (final HttpServerTransport httpServerTransport = new CustomNettyHttpServerTransport(settings)) {
            httpServerTransport.start();
            final InetSocketTransportAddress transportAddress =
                (InetSocketTransportAddress) randomFrom(httpServerTransport.boundAddress().boundAddresses());

            final int numberOfRequests = randomIntBetween(4, 16);
            final int numberOfSlowRequests = scaledRandomIntBetween(1, numberOfRequests);
            final List<String> requests = new ArrayList<>(numberOfRequests);
            for (int i = 0; i < numberOfRequests - numberOfSlowRequests; i++) {
                requests.add("/" + i);
            }
            for (int i = 0; i < numberOfSlowRequests; i++) {
                requests.add("/slow?sleep=" + sleep(i));
            }

            try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
                Collection<FullHttpResponse> responses = nettyHttpClient.get(transportAddress.address(), requests.toArray(new String[]{}));
                List<String> responseBodies = new ArrayList<>(Netty4HttpClient.returnHttpResponseBodies(responses));
                // we cannot be sure about the order of the fast requests, but the slow ones should have to be last
                assertThat(responseBodies, hasSize(numberOfRequests));
                for (int i = 0; i < numberOfSlowRequests; i++) {
                    assertThat(responseBodies.get(numberOfRequests - numberOfSlowRequests + i), equalTo("/slow?sleep=" + sleep(i)));
                }
            }
        }
    }


    private int sleep(int index) {
        return 500 + 100 * (index + 1);
    }

    class CustomNettyHttpServerTransport extends Netty4HttpServerTransport {

        private final ExecutorService executorService = Executors.newCachedThreadPool();

        CustomNettyHttpServerTransport(final Settings settings) {
            super(settings,
                Netty4HttpServerPipeliningTests.this.networkService,
                Netty4HttpServerPipeliningTests.this.bigArrays,
                Netty4HttpServerPipeliningTests.this.threadPool);
        }

        @Override
        public ChannelHandler configureServerChannelHandler() {
            return new CustomHttpChannelHandler(this, executorService, Netty4HttpServerPipeliningTests.this.threadPool.getThreadContext());
        }

        @Override
        protected void doClose() {
            executorService.shutdown();
            super.doClose();
        }

    }

    private class CustomHttpChannelHandler extends Netty4HttpServerTransport.HttpChannelHandler {

        private final ExecutorService executorService;

        CustomHttpChannelHandler(Netty4HttpServerTransport transport, ExecutorService executorService, ThreadContext threadContext) {
            super(transport, randomBoolean(), threadContext);
            this.executorService = executorService;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            ch.pipeline().replace("handler", "handler", new PossiblySlowUpstreamHandler(executorService));
        }

    }

    class PossiblySlowUpstreamHandler extends SimpleChannelInboundHandler<Object> {

        private final ExecutorService executorService;

        PossiblySlowUpstreamHandler(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            executorService.submit(new PossiblySlowRunnable(ctx, msg));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.info("Caught exception", cause);
            ctx.channel().close().sync();
        }

    }

    class PossiblySlowRunnable implements Runnable {

        private ChannelHandlerContext ctx;
        private HttpPipelinedRequest pipelinedRequest;
        private FullHttpRequest fullHttpRequest;

        PossiblySlowRunnable(ChannelHandlerContext ctx, Object msg) {
            this.ctx = ctx;
            if (msg instanceof HttpPipelinedRequest) {
                this.pipelinedRequest = (HttpPipelinedRequest) msg;
            } else if (msg instanceof FullHttpRequest) {
                this.fullHttpRequest = (FullHttpRequest) msg;
            }
        }

        @Override
        public void run() {
            final String uri;
            if (pipelinedRequest != null && pipelinedRequest.last() instanceof FullHttpRequest) {
                uri = ((FullHttpRequest) pipelinedRequest.last()).uri();
            } else {
                uri = fullHttpRequest.uri();
            }

            final ByteBuf buffer = Unpooled.copiedBuffer(uri, StandardCharsets.UTF_8);

            final DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buffer);
            httpResponse.headers().add(HttpHeaderNames.CONTENT_LENGTH, buffer.readableBytes());

            final QueryStringDecoder decoder = new QueryStringDecoder(uri);

            final int timeout =
                uri.startsWith("/slow") && decoder.parameters().containsKey("sleep") ?
                    Integer.valueOf(decoder.parameters().get("sleep").get(0)) : 0;
            if (timeout > 0) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            if (pipelinedRequest != null) {
                ctx.writeAndFlush(pipelinedRequest.createHttpResponse(httpResponse, ctx.channel().newPromise()));
            } else {
                ctx.writeAndFlush(httpResponse);
            }
        }

    }

}
