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

package org.elasticsearch.http.netty4.pipelining;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.hamcrest.core.Is.is;

public class Netty4HttpPipeliningHandlerTests extends ESTestCase {

    private ExecutorService executorService = Executors.newFixedThreadPool(randomIntBetween(4, 8));
    private Map<String, CountDownLatch> waitingRequests = new ConcurrentHashMap<>();
    private Map<String, CountDownLatch> finishingRequests = new ConcurrentHashMap<>();

    @After
    public void tearDown() throws Exception {
        waitingRequests.keySet().forEach(this::finishRequest);
        shutdownExecutorService();
        super.tearDown();
    }

    private CountDownLatch finishRequest(String url) {
        waitingRequests.get(url).countDown();
        return finishingRequests.get(url);
    }

    private void shutdownExecutorService() throws InterruptedException {
        if (!executorService.isShutdown()) {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testThatPipeliningWorksWithFastSerializedRequests() throws InterruptedException {
        final int numberOfRequests = randomIntBetween(2, 128);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new HttpPipeliningHandler(numberOfRequests), new WorkEmulatorHandler());

        for (int i = 0; i < numberOfRequests; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + String.valueOf(i)));
        }

        final List<CountDownLatch> latches = new ArrayList<>();
        for (final String url : waitingRequests.keySet()) {
            latches.add(finishRequest(url));
        }

        for (final CountDownLatch latch : latches) {
            latch.await();
        }

        embeddedChannel.flush();

        for (int i = 0; i < numberOfRequests; i++) {
            assertReadHttpMessageHasContent(embeddedChannel, String.valueOf(i));
        }

        assertTrue(embeddedChannel.isOpen());
    }

    public void testThatPipeliningWorksWhenSlowRequestsInDifferentOrder() throws InterruptedException {
        final int numberOfRequests = randomIntBetween(2, 128);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new HttpPipeliningHandler(numberOfRequests), new WorkEmulatorHandler());

        for (int i = 0; i < numberOfRequests; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + String.valueOf(i)));
        }

        // random order execution
        final List<String> urls = new ArrayList<>(waitingRequests.keySet());
        Randomness.shuffle(urls);
        final List<CountDownLatch> latches = new ArrayList<>();
        for (final String url : urls) {
            latches.add(finishRequest(url));
        }

        for (final CountDownLatch latch : latches) {
            latch.await();
        }

        embeddedChannel.flush();

        for (int i = 0; i < numberOfRequests; i++) {
            assertReadHttpMessageHasContent(embeddedChannel, String.valueOf(i));
        }

        assertTrue(embeddedChannel.isOpen());
    }

    public void testThatPipeliningWorksWithChunkedRequests() throws InterruptedException {
        final int numberOfRequests = randomIntBetween(2, 128);
        final EmbeddedChannel embeddedChannel =
            new EmbeddedChannel(
                new AggregateUrisAndHeadersHandler(),
                new HttpPipeliningHandler(numberOfRequests),
                new WorkEmulatorHandler());

        for (int i = 0; i < numberOfRequests; i++) {
            final DefaultHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/" + i);
            embeddedChannel.writeInbound(request);
            embeddedChannel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
        }

        final List<CountDownLatch> latches = new ArrayList<>();
        for (int i = numberOfRequests - 1; i >= 0; i--) {
            latches.add(finishRequest(Integer.toString(i)));
        }

        for (final CountDownLatch latch : latches) {
            latch.await();
        }

        embeddedChannel.flush();

        for (int i = 0; i < numberOfRequests; i++) {
            assertReadHttpMessageHasContent(embeddedChannel, Integer.toString(i));
        }

        assertTrue(embeddedChannel.isOpen());
    }

    public void testThatPipeliningClosesConnectionWithTooManyEvents() throws InterruptedException {
        final int numberOfRequests = randomIntBetween(2, 128);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new HttpPipeliningHandler(numberOfRequests), new WorkEmulatorHandler());

        for (int i = 0; i < 1 + numberOfRequests + 1; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + Integer.toString(i)));
        }

        final List<CountDownLatch> latches = new ArrayList<>();
        final List<Integer> requests = IntStream.range(1, numberOfRequests + 1).mapToObj(r -> r).collect(Collectors.toList());
        Randomness.shuffle(requests);

        for (final Integer request : requests) {
            latches.add(finishRequest(request.toString()));
        }

        for (final CountDownLatch latch : latches) {
            latch.await();
        }

        finishRequest(Integer.toString(numberOfRequests + 1)).await();

        embeddedChannel.flush();

        assertFalse(embeddedChannel.isOpen());
    }


    private void assertReadHttpMessageHasContent(EmbeddedChannel embeddedChannel, String expectedContent) {
        FullHttpResponse response = (FullHttpResponse) embeddedChannel.outboundMessages().poll();
        assertNotNull("Expected response to exist, maybe you did not wait long enough?", response);
        assertNotNull("Expected response to have content " + expectedContent, response.content());
        String data = new String(ByteBufUtil.getBytes(response.content()), StandardCharsets.UTF_8);
        assertThat(data, is(expectedContent));
    }

    private FullHttpRequest createHttpRequest(String uri) {
        return new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, uri);
    }

    private static class AggregateUrisAndHeadersHandler extends SimpleChannelInboundHandler<HttpRequest> {

        static final Queue<String> QUEUE_URI = new LinkedTransferQueue<>();

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpRequest request) throws Exception {
            QUEUE_URI.add(request.uri());
        }

    }

    private class WorkEmulatorHandler extends SimpleChannelInboundHandler<HttpPipelinedRequest> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final HttpPipelinedRequest pipelinedRequest) throws Exception {
            final QueryStringDecoder decoder;
            if (pipelinedRequest.last() instanceof FullHttpRequest) {
                final FullHttpRequest fullHttpRequest = (FullHttpRequest) pipelinedRequest.last();
                decoder = new QueryStringDecoder(fullHttpRequest.uri());
            } else {
                decoder = new QueryStringDecoder(AggregateUrisAndHeadersHandler.QUEUE_URI.poll());
            }

            final String uri = decoder.path().replace("/", "");
            final ByteBuf content = Unpooled.copiedBuffer(uri, StandardCharsets.UTF_8);
            final DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, OK, content);
            httpResponse.headers().add(CONTENT_LENGTH, content.readableBytes());

            final CountDownLatch waitingLatch = new CountDownLatch(1);
            waitingRequests.put(uri, waitingLatch);
            final CountDownLatch finishingLatch = new CountDownLatch(1);
            finishingRequests.put(uri, finishingLatch);

            executorService.submit(() -> {
                try {
                    waitingLatch.await(1000, TimeUnit.SECONDS);
                    final ChannelPromise promise = ctx.newPromise();
                    ctx.write(pipelinedRequest.createHttpResponse(httpResponse, promise), promise);
                    finishingLatch.countDown();
                } catch (InterruptedException e) {
                    fail(e.toString());
                }
            });
        }

    }

}
