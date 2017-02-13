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
package org.elasticsearch.http.netty3;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.NullDispatcher;
import org.elasticsearch.http.netty3.Netty3HttpServerTransport.HttpChannelPipelineFactory;
import org.elasticsearch.http.netty3.pipelining.OrderedDownstreamChannelEvent;
import org.elasticsearch.http.netty3.pipelining.OrderedUpstreamMessageEvent;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.http.netty3.Netty3HttpClient.returnHttpResponseBodies;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * This test just tests, if he pipelining works in general with out any connection the elasticsearch handler
 */
public class Netty3HttpServerPipeliningTests extends ESTestCase {
    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private CustomNetty3HttpServerTransport httpServerTransport;

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
        if (httpServerTransport != null) {
            httpServerTransport.close();
        }
    }

    public void testThatHttpPipeliningWorksWhenEnabled() throws Exception {
        Settings settings = Settings.builder()
                               .put("http.pipelining", true)
                               .put("http.port", "0")
                               .build();
        httpServerTransport = new CustomNetty3HttpServerTransport(settings);
        httpServerTransport.start();
        InetSocketTransportAddress transportAddress =
            (InetSocketTransportAddress) randomFrom(httpServerTransport.boundAddress().boundAddresses());

        final int numberOfRequests = randomIntBetween(4, 16);
        final List<String> requests = new ArrayList<>(numberOfRequests);
        for (int i = 0; i < numberOfRequests; i++) {
            if (rarely()) {
                requests.add("/slow/" + i);
            } else {
                requests.add("/" + i);
            }
        }
        try (Netty3HttpClient nettyHttpClient = new Netty3HttpClient()) {
            Collection<HttpResponse> responses = nettyHttpClient.get(transportAddress.address(), requests.toArray(new String[]{}));
            Collection<String> responseBodies = returnHttpResponseBodies(responses);
            assertThat(responseBodies, contains(requests.toArray()));
        }
    }

    public void testThatHttpPipeliningCanBeDisabled() throws Exception {
        Settings settings = Settings.builder()
                                .put("http.pipelining", false)
                                .put("http.port", "0")
                                .build();
        httpServerTransport = new CustomNetty3HttpServerTransport(settings);
        httpServerTransport.start();
        InetSocketTransportAddress transportAddress =
            (InetSocketTransportAddress) randomFrom(httpServerTransport.boundAddress().boundAddresses());

        final int numberOfRequests = randomIntBetween(4, 16);
        final Set<Integer> slowIds = new HashSet<>();
        final List<String> requests = new ArrayList<>(numberOfRequests);
        for (int i = 0; i < numberOfRequests; i++) {
            if (rarely()) {
                requests.add("/slow/" + i);
                slowIds.add(i);
            } else {
                requests.add("/" + i);
            }
        }

        try (Netty3HttpClient nettyHttpClient = new Netty3HttpClient()) {
            Collection<HttpResponse> responses = nettyHttpClient.get(transportAddress.address(), requests.toArray(new String[]{}));
            List<String> responseBodies = new ArrayList<>(returnHttpResponseBodies(responses));
            // we cannot be sure about the order of the responses, but the slow ones should come last
            assertThat(responseBodies, hasSize(numberOfRequests));
            for (int i = 0; i < numberOfRequests - slowIds.size(); i++) {
                assertThat(responseBodies.get(i), matches("/\\d+"));
            }

            final Set<Integer> ids = new HashSet<>();
            for (int i = 0; i < slowIds.size(); i++) {
                final String response = responseBodies.get(numberOfRequests - slowIds.size() + i);
                assertThat(response, matches("/slow/\\d+"));
                assertTrue(ids.add(Integer.parseInt(response.split("/")[2])));
            }

            assertThat(ids, equalTo(slowIds));
        }
    }

    class CustomNetty3HttpServerTransport extends Netty3HttpServerTransport {

        private final ExecutorService executorService;

        CustomNetty3HttpServerTransport(Settings settings) {
            super(settings, Netty3HttpServerPipeliningTests.this.networkService,
                Netty3HttpServerPipeliningTests.this.bigArrays, Netty3HttpServerPipeliningTests.this.threadPool, xContentRegistry(),
                    new NullDispatcher());
            this.executorService = Executors.newFixedThreadPool(5);
        }

        @Override
        public ChannelPipelineFactory configureServerChannelPipelineFactory() {
            return new CustomHttpChannelPipelineFactory(this, executorService, Netty3HttpServerPipeliningTests.this.threadPool
                .getThreadContext());
        }

        @Override
        public void stop() {
            executorService.shutdownNow();
            super.stop();
        }
    }

    private class CustomHttpChannelPipelineFactory extends HttpChannelPipelineFactory {

        private final ExecutorService executorService;

        CustomHttpChannelPipelineFactory(Netty3HttpServerTransport transport, ExecutorService executorService,
                                                ThreadContext threadContext) {
            super(transport, randomBoolean(), threadContext);
            this.executorService = executorService;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            pipeline.replace("handler", "handler", new PossiblySlowUpstreamHandler(executorService));
            return pipeline;
        }
    }

    class PossiblySlowUpstreamHandler extends SimpleChannelUpstreamHandler {

        private final ExecutorService executorService;

        PossiblySlowUpstreamHandler(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
            executorService.submit(new PossiblySlowRunnable(ctx, e));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            logger.info("Caught exception", e.getCause());
            e.getChannel().close();
        }
    }

    class PossiblySlowRunnable implements Runnable {

        private ChannelHandlerContext ctx;
        private MessageEvent e;

        PossiblySlowRunnable(ChannelHandlerContext ctx, MessageEvent e) {
            this.ctx = ctx;
            this.e = e;
        }

        @Override
        public void run() {
            HttpRequest request;
            OrderedUpstreamMessageEvent oue = null;
            if (e instanceof OrderedUpstreamMessageEvent) {
                oue = (OrderedUpstreamMessageEvent) e;
                request = (HttpRequest) oue.getMessage();
            } else {
                request = (HttpRequest) e.getMessage();
            }

            ChannelBuffer buffer = ChannelBuffers.copiedBuffer(request.getUri(), StandardCharsets.UTF_8);

            DefaultHttpResponse httpResponse = new DefaultHttpResponse(HTTP_1_1, OK);
            httpResponse.headers().add(CONTENT_LENGTH, buffer.readableBytes());
            httpResponse.setContent(buffer);

            final boolean slow = request.getUri().matches("/slow/\\d+");
            if (slow) {
                try {
                    Thread.sleep(scaledRandomIntBetween(500, 1000));
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            } else {
                assert request.getUri().matches("/\\d+");
            }

            if (oue != null) {
                ctx.sendDownstream(new OrderedDownstreamChannelEvent(oue, 0, true, httpResponse));
            } else {
                ctx.getChannel().write(httpResponse);
            }
        }
    }
}
