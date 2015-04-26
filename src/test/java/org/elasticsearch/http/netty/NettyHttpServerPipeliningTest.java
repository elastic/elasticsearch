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
package org.elasticsearch.http.netty;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty.pipelining.OrderedDownstreamChannelEvent;
import org.elasticsearch.http.netty.pipelining.OrderedUpstreamMessageEvent;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.cache.recycler.MockBigArrays;
import org.elasticsearch.test.cache.recycler.MockPageCacheRecycler;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.http.netty.NettyHttpClient.returnHttpResponseBodies;
import static org.elasticsearch.http.netty.NettyHttpServerTransport.HttpChannelPipelineFactory;
import static org.hamcrest.Matchers.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * This test just tests, if he pipelining works in general with out any connection the elasticsearch handler
 */
public class NettyHttpServerPipeliningTest extends ElasticsearchTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockPageCacheRecycler mockPageCacheRecycler;
    private MockBigArrays bigArrays;
    private CustomNettyHttpServerTransport httpServerTransport;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(ImmutableSettings.EMPTY);
        threadPool = new ThreadPool("test");
        mockPageCacheRecycler = new MockPageCacheRecycler(ImmutableSettings.EMPTY, threadPool);
        bigArrays = new MockBigArrays(mockPageCacheRecycler, new NoneCircuitBreakerService());
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

    @Test
    public void testThatHttpPipeliningWorksWhenEnabled() throws Exception {
        Settings settings = settingsBuilder().put("http.pipelining", true).build();
        httpServerTransport = new CustomNettyHttpServerTransport(settings);
        httpServerTransport.start();
        InetSocketTransportAddress transportAddress = (InetSocketTransportAddress) httpServerTransport.boundAddress().boundAddress();

        List<String> requests = Arrays.asList("/firstfast", "/slow?sleep=500", "/secondfast", "/slow?sleep=1000", "/thirdfast");
        try (NettyHttpClient nettyHttpClient = new NettyHttpClient()) {
            Collection<HttpResponse> responses = nettyHttpClient.sendRequests(transportAddress.address(), requests.toArray(new String[]{}));
            Collection<String> responseBodies = returnHttpResponseBodies(responses);
            assertThat(responseBodies, contains("/firstfast", "/slow?sleep=500", "/secondfast", "/slow?sleep=1000", "/thirdfast"));
        }
    }

    @Test
    public void testThatHttpPipeliningCanBeDisabled() throws Exception {
        Settings settings = settingsBuilder().put("http.pipelining", false).build();
        httpServerTransport = new CustomNettyHttpServerTransport(settings);
        httpServerTransport.start();
        InetSocketTransportAddress transportAddress = (InetSocketTransportAddress) httpServerTransport.boundAddress().boundAddress();

        List<String> requests = Arrays.asList("/slow?sleep=1000", "/firstfast", "/secondfast", "/thirdfast", "/slow?sleep=500");
        try (NettyHttpClient nettyHttpClient = new NettyHttpClient()) {
            Collection<HttpResponse> responses = nettyHttpClient.sendRequests(transportAddress.address(), requests.toArray(new String[]{}));
            List<String> responseBodies = Lists.newArrayList(returnHttpResponseBodies(responses));
            // we cannot be sure about the order of the fast requests, but the slow ones should have to be last
            assertThat(responseBodies, hasSize(5));
            assertThat(responseBodies.get(3), is("/slow?sleep=500"));
            assertThat(responseBodies.get(4), is("/slow?sleep=1000"));
        }
    }

    class CustomNettyHttpServerTransport extends NettyHttpServerTransport {

        private final ExecutorService executorService;

        public CustomNettyHttpServerTransport(Settings settings) {
            super(settings, NettyHttpServerPipeliningTest.this.networkService, NettyHttpServerPipeliningTest.this.bigArrays);
            this.executorService = Executors.newFixedThreadPool(5);
        }

        @Override
        public ChannelPipelineFactory configureServerChannelPipelineFactory() {
            return new CustomHttpChannelPipelineFactory(this, executorService);
        }

        @Override
        public HttpServerTransport stop() throws ElasticsearchException {
            executorService.shutdownNow();
            return super.stop();
        }
    }

    private class CustomHttpChannelPipelineFactory extends HttpChannelPipelineFactory {

        private final ExecutorService executorService;

        public CustomHttpChannelPipelineFactory(NettyHttpServerTransport transport, ExecutorService executorService) {
            super(transport, randomBoolean());
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

        public PossiblySlowUpstreamHandler(ExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
            executorService.submit(new PossiblySlowRunnable(ctx, e));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            e.getCause().printStackTrace();
            e.getChannel().close();
        }
    }

    class PossiblySlowRunnable implements Runnable {

        private ChannelHandlerContext ctx;
        private MessageEvent e;

        public PossiblySlowRunnable(ChannelHandlerContext ctx, MessageEvent e) {
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

            ChannelBuffer buffer = ChannelBuffers.copiedBuffer(request.getUri(), Charsets.UTF_8);

            DefaultHttpResponse httpResponse = new DefaultHttpResponse(HTTP_1_1, OK);
            httpResponse.headers().add(CONTENT_LENGTH, buffer.readableBytes());
            httpResponse.setContent(buffer);

            QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());

            final int timeout = request.getUri().startsWith("/slow") && decoder.getParameters().containsKey("sleep") ? Integer.valueOf(decoder.getParameters().get("sleep").get(0)) : 0;
            if (timeout > 0) {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException();
                }
            }

            if (oue != null) {
                ctx.sendDownstream(new OrderedDownstreamChannelEvent(oue, 0, true, httpResponse));
            } else {
                ctx.getChannel().write(httpResponse);
            }
        }
    }
}
