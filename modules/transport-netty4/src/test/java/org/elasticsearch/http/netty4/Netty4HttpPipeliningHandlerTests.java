/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.bytes.ZeroBytesReference;
import org.elasticsearch.common.network.ThreadWatchdog;
import org.elasticsearch.common.network.ThreadWatchdogHelper;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.transport.netty4.NettyAllocator;
import org.junit.After;

import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class Netty4HttpPipeliningHandlerTests extends ESTestCase {

    private final ExecutorService handlerService = Executors.newFixedThreadPool(randomIntBetween(4, 8));
    private final ExecutorService eventLoopService = Executors.newFixedThreadPool(1);
    private final Map<String, CountDownLatch> waitingRequests = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> finishingRequests = new ConcurrentHashMap<>();

    @After
    public void tearDown() throws Exception {
        waitingRequests.keySet().forEach(this::finishRequest);
        terminateExecutorService(handlerService);
        terminateExecutorService(eventLoopService);
        super.tearDown();
    }

    private void terminateExecutorService(ExecutorService executorService) throws InterruptedException {
        if (executorService.isShutdown() == false) {
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private CountDownLatch finishRequest(String url) {
        waitingRequests.get(url).countDown();
        return finishingRequests.get(url);
    }

    public void testThatPipeliningWorksWithFastSerializedRequests() throws InterruptedException {
        final int numberOfRequests = randomIntBetween(2, 128);
        final EmbeddedChannel embeddedChannel = makeEmbeddedChannelWithSimulatedWork(numberOfRequests);

        for (int i = 0; i < numberOfRequests; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + i));
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

    private EmbeddedChannel makeEmbeddedChannelWithSimulatedWork(int numberOfRequests) {
        return new EmbeddedChannel(new Netty4HttpPipeliningHandler(numberOfRequests, null, new ThreadWatchdog.ActivityTracker()) {
            @Override
            protected void handlePipelinedRequest(ChannelHandlerContext ctx, Netty4HttpRequest pipelinedRequest) {
                ctx.fireChannelRead(pipelinedRequest);
            }
        }, new WorkEmulatorHandler());
    }

    public void testThatPipeliningWorksWhenSlowRequestsInDifferentOrder() throws InterruptedException {
        final int numberOfRequests = randomIntBetween(2, 128);
        final EmbeddedChannel embeddedChannel = makeEmbeddedChannelWithSimulatedWork(numberOfRequests);

        for (int i = 0; i < numberOfRequests; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + i));
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

    public void testThatPipeliningClosesConnectionWithTooManyEvents() throws InterruptedException {
        final int numberOfRequests = randomIntBetween(2, 128);
        final EmbeddedChannel embeddedChannel = makeEmbeddedChannelWithSimulatedWork(numberOfRequests);

        for (int i = 0; i < 1 + numberOfRequests + 1; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + i));
        }

        final List<CountDownLatch> latches = new ArrayList<>();
        final List<Integer> requests = IntStream.range(1, numberOfRequests + 1).boxed().collect(Collectors.toList());
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

    public void testPipeliningRequestsAreReleased() {
        final int numberOfRequests = 10;
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            new Netty4HttpPipeliningHandler(numberOfRequests + 1, null, new ThreadWatchdog.ActivityTracker())
        );

        for (int i = 0; i < numberOfRequests; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + i));
        }

        Netty4HttpRequest inbound;
        ArrayList<Netty4HttpRequest> requests = new ArrayList<>();
        while ((inbound = embeddedChannel.readInbound()) != null) {
            requests.add(inbound);
        }

        ArrayList<ChannelPromise> promises = new ArrayList<>();
        for (int i = 1; i < requests.size(); ++i) {
            ChannelPromise promise = embeddedChannel.newPromise();
            promises.add(promise);
            Netty4HttpRequest pipelinedRequest = requests.get(i);
            Netty4FullHttpResponse resp = pipelinedRequest.createResponse(RestStatus.OK, BytesArray.EMPTY);
            embeddedChannel.writeAndFlush(resp, promise);
        }

        for (ChannelPromise promise : promises) {
            assertFalse(promise.isDone());
        }
        embeddedChannel.close().syncUninterruptibly();
        for (ChannelPromise promise : promises) {
            assertTrue(promise.isDone());
            assertTrue(promise.cause() instanceof ClosedChannelException);
        }
    }

    public void testSmallFullResponsesAreSentDirectly() {
        final List<Object> messagesSeen = new ArrayList<>();
        final var embeddedChannel = new EmbeddedChannel(capturingHandler(messagesSeen), getTestHttpHandler());
        embeddedChannel.writeInbound(createHttpRequest("/test"));
        final Netty4HttpRequest request = embeddedChannel.readInbound();
        final var maxSize = (int) NettyAllocator.suggestedMaxAllocationSize() / 2;
        final var content = new ZeroBytesReference(between(0, maxSize));
        final var response = request.createResponse(RestStatus.OK, content);
        assertThat(response, instanceOf(FullHttpResponse.class));
        final var promise = embeddedChannel.newPromise();
        embeddedChannel.writeAndFlush(response, promise);
        assertTrue(promise.isDone());
        assertThat(messagesSeen, hasSize(1));
        assertSame(response, messagesSeen.get(0));
    }

    public void testLargeFullResponsesAreSplit() {
        final List<Object> messagesSeen = new ArrayList<>();
        final var embeddedChannel = new EmbeddedChannel(capturingHandler(messagesSeen), getTestHttpHandler());
        embeddedChannel.writeInbound(createHttpRequest("/test"));
        final Netty4HttpRequest request = embeddedChannel.readInbound();
        final var minSize = (int) NettyAllocator.suggestedMaxAllocationSize();
        final var content = new ZeroBytesReference(between(minSize, (int) (minSize * 1.5)));
        final var response = request.createResponse(RestStatus.OK, content);
        assertThat(response, instanceOf(FullHttpResponse.class));
        final var promise = embeddedChannel.newPromise();
        embeddedChannel.writeAndFlush(response, promise);
        assertTrue(promise.isDone());
        assertThat(messagesSeen, hasSize(3));
        final var headersMessage = asInstanceOf(DefaultHttpResponse.class, messagesSeen.get(0));
        assertEquals(RestStatus.OK.getStatus(), headersMessage.status().code());
        assertThat(headersMessage, not(instanceOf(FullHttpResponse.class)));
        final var chunk1 = asInstanceOf(DefaultHttpContent.class, messagesSeen.get(1));
        final var chunk2 = asInstanceOf(DefaultLastHttpContent.class, messagesSeen.get(2));
        assertEquals(content.length(), chunk1.content().readableBytes() + chunk2.content().readableBytes());
        assertThat(chunk1, not(instanceOf(FullHttpResponse.class)));
        assertThat(chunk2, not(instanceOf(FullHttpResponse.class)));
    }

    public void testDecoderErrorSurfacedAsNettyInboundError() {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(getTestHttpHandler());
        // a request with a decoder error
        final DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        Exception cause = new ElasticsearchException("Boom");
        request.setDecoderResult(DecoderResult.failure(cause));
        embeddedChannel.writeInbound(request);
        final Netty4HttpRequest nettyRequest = embeddedChannel.readInbound();
        assertThat(nettyRequest.getInboundException(), sameInstance(cause));
    }

    public void testResumesChunkedMessage() {
        final List<Object> messagesSeen = new ArrayList<>();
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(capturingHandler(messagesSeen), getTestHttpHandler());
        embeddedChannel.writeInbound(createHttpRequest("/chunked"));
        final Netty4HttpRequest request = embeddedChannel.readInbound();
        final BytesReference chunk = new BytesArray(randomByteArrayOfLength(embeddedChannel.config().getWriteBufferHighWaterMark() + 1));
        final int chunks = randomIntBetween(2, 10);
        final HttpResponse response = request.createResponse(RestStatus.OK, getRepeatedChunkResponseBody(chunks, chunk));
        final ChannelPromise promise = embeddedChannel.newPromise();
        embeddedChannel.write(response, promise);
        assertFalse("should not be fully flushed right away", promise.isDone());
        assertThat(messagesSeen, hasSize(2));
        embeddedChannel.flush();
        assertTrue(promise.isDone());
        assertThat(messagesSeen, hasSize(chunks + 1));
        assertChunkedMessageAtIndex(messagesSeen, 0, chunks, chunk);
    }

    public void testResumesAfterChunkedMessage() {
        final List<Object> messagesSeen = new ArrayList<>();
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(capturingHandler(messagesSeen), getTestHttpHandler());
        embeddedChannel.writeInbound(createHttpRequest("/chunked"));
        embeddedChannel.writeInbound(createHttpRequest("/chunked2"));
        final Netty4HttpRequest request1 = embeddedChannel.readInbound();
        final Netty4HttpRequest request2 = embeddedChannel.readInbound();

        final int chunks1 = randomIntBetween(2, 10);
        final int chunks2 = randomIntBetween(2, 10);
        final BytesReference chunk = new BytesArray(randomByteArrayOfLength(embeddedChannel.config().getWriteBufferHighWaterMark() + 1));
        final HttpResponse response1 = request1.createResponse(RestStatus.OK, getRepeatedChunkResponseBody(chunks1, chunk));
        final HttpResponse response2 = request2.createResponse(RestStatus.OK, getRepeatedChunkResponseBody(chunks2, chunk));
        final ChannelPromise promise1 = embeddedChannel.newPromise();
        final ChannelPromise promise2 = embeddedChannel.newPromise();
        if (randomBoolean()) {
            // randomly write messages out of order
            embeddedChannel.write(response2, promise2);
            embeddedChannel.write(response1, promise1);
        } else {
            embeddedChannel.write(response1, promise1);
            embeddedChannel.write(response2, promise2);
        }
        assertFalse("should not be fully flushed right away", promise1.isDone());
        assertThat(messagesSeen, hasSize(2));
        embeddedChannel.flush();
        assertTrue(promise1.isDone());
        assertThat(messagesSeen, hasSize(chunks1 + chunks2 + 2));
        assertChunkedMessageAtIndex(messagesSeen, 0, chunks1, chunk);
        assertChunkedMessageAtIndex(messagesSeen, chunks1 + 1, chunks2, chunk);
        assertTrue(promise2.isDone());
    }

    public void testResumesSingleAfterChunkedMessage() {
        final List<Object> messagesSeen = new ArrayList<>();
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(capturingHandler(messagesSeen), getTestHttpHandler());
        embeddedChannel.writeInbound(createHttpRequest("/chunked"));
        embeddedChannel.writeInbound(createHttpRequest("/single"));
        final Netty4HttpRequest request1 = embeddedChannel.readInbound();
        final Netty4HttpRequest request2 = embeddedChannel.readInbound();

        final int chunks1 = randomIntBetween(2, 10);
        final BytesReference chunk = new BytesArray(randomByteArrayOfLength(embeddedChannel.config().getWriteBufferHighWaterMark() + 1));
        final HttpResponse response1 = request1.createResponse(RestStatus.OK, getRepeatedChunkResponseBody(chunks1, chunk));
        final BytesReference single = new BytesArray(
            randomByteArrayOfLength(randomIntBetween(1, embeddedChannel.config().getWriteBufferHighWaterMark()))
        );
        final HttpResponse response2 = request2.createResponse(RestStatus.OK, single);
        final ChannelPromise promise1 = embeddedChannel.newPromise();
        final ChannelPromise promise2 = embeddedChannel.newPromise();
        if (randomBoolean()) {
            // randomly write messages out of order
            embeddedChannel.write(response2, promise2);
            embeddedChannel.write(response1, promise1);
        } else {
            embeddedChannel.write(response1, promise1);
            embeddedChannel.write(response2, promise2);
        }
        assertFalse("should not be fully flushed right away", promise1.isDone());
        assertThat(messagesSeen, hasSize(2));
        embeddedChannel.flush();
        assertTrue(promise1.isDone());
        assertThat(messagesSeen, hasSize(chunks1 + 1 + 1));
        assertChunkedMessageAtIndex(messagesSeen, 0, chunks1, chunk);
        assertThat(messagesSeen.get(chunks1 + 1), instanceOf(Netty4FullHttpResponse.class));
        assertContentAtIndexEquals(messagesSeen, chunks1 + 1, single);
        assertTrue(promise2.isDone());
    }

    public void testChunkedResumesAfterSingleMessage() {
        final List<Object> messagesSeen = new ArrayList<>();
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(capturingHandler(messagesSeen), getTestHttpHandler());
        embeddedChannel.writeInbound(createHttpRequest("/chunked"));
        final Netty4HttpRequest request1 = embeddedChannel.readInbound();
        embeddedChannel.writeInbound(createHttpRequest("/chunked2"));
        final Netty4HttpRequest request2 = embeddedChannel.readInbound();

        final int chunks2 = randomIntBetween(2, 10);
        final BytesReference chunk = new BytesArray(randomByteArrayOfLength(embeddedChannel.config().getWriteBufferHighWaterMark() + 1));
        final HttpResponse response1 = request1.createResponse(RestStatus.OK, chunk);
        final HttpResponse response2 = request2.createResponse(RestStatus.OK, getRepeatedChunkResponseBody(chunks2, chunk));
        final ChannelPromise promise1 = embeddedChannel.newPromise();
        final ChannelPromise promise2 = embeddedChannel.newPromise();
        if (randomBoolean()) {
            // randomly write messages out of order
            embeddedChannel.write(response2, promise2);
            assertTrue(embeddedChannel.isWritable());
            embeddedChannel.write(response1, promise1);
            assertFalse(embeddedChannel.isWritable());
        } else {
            embeddedChannel.write(response1, promise1);
            assertFalse(embeddedChannel.isWritable());
            embeddedChannel.write(response2, promise2);
        }
        assertFalse("should not be fully flushed right away", promise1.isDone());
        assertThat("unexpected [" + messagesSeen + "]", messagesSeen, hasSize(1));
        embeddedChannel.flush();
        assertTrue(promise1.isDone());
        assertThat(messagesSeen, hasSize(chunks2 + 2));
        assertThat(messagesSeen.get(0), instanceOf(Netty4FullHttpResponse.class));
        assertChunkedMessageAtIndex(messagesSeen, 1, chunks2, chunk);
        assertTrue(promise2.isDone());
    }

    public void testChunkedWithSmallChunksResumesAfterSingleMessage() {
        final List<Object> messagesSeen = new ArrayList<>();
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(capturingHandler(messagesSeen), getTestHttpHandler());
        embeddedChannel.writeInbound(createHttpRequest("/chunked"));
        final Netty4HttpRequest request1 = embeddedChannel.readInbound();
        embeddedChannel.writeInbound(createHttpRequest("/chunked2"));
        final Netty4HttpRequest request2 = embeddedChannel.readInbound();

        final int chunks2 = randomIntBetween(2, 10);
        final HttpResponse response1 = request1.createResponse(
            RestStatus.OK,
            new BytesArray(randomByteArrayOfLength(embeddedChannel.config().getWriteBufferHighWaterMark() + 1))
        );
        final BytesReference chunk = new BytesArray(randomByteArrayOfLength(randomIntBetween(10, 512)));
        final HttpResponse response2 = request2.createResponse(RestStatus.OK, getRepeatedChunkResponseBody(chunks2, chunk));
        final ChannelPromise promise1 = embeddedChannel.newPromise();
        final ChannelPromise promise2 = embeddedChannel.newPromise();
        if (randomBoolean()) {
            // randomly write messages out of order
            embeddedChannel.write(response2, promise2);
            assertTrue(embeddedChannel.isWritable());
            embeddedChannel.write(response1, promise1);
            assertFalse(embeddedChannel.isWritable());
        } else {
            embeddedChannel.write(response1, promise1);
            assertFalse(embeddedChannel.isWritable());
            embeddedChannel.write(response2, promise2);
        }
        assertFalse("should not be fully flushed right away", promise1.isDone());
        assertThat("unexpected [" + messagesSeen + "]", messagesSeen, hasSize(1));
        embeddedChannel.flush();
        assertTrue(promise1.isDone());
        assertThat(messagesSeen, hasSize(chunks2 + 2));
        assertThat(messagesSeen.get(0), instanceOf(Netty4FullHttpResponse.class));
        assertChunkedMessageAtIndex(messagesSeen, 1, chunks2, chunk);
        assertTrue(promise2.isDone());
    }

    public void testPipeliningRequestsAreReleasedAfterFailureOnChunked() {
        final List<Object> messagesSeen = new ArrayList<>();
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(capturingHandler(messagesSeen), getTestHttpHandler());
        embeddedChannel.writeInbound(createHttpRequest("/chunked"));
        final Netty4HttpRequest chunkedResponseRequest = embeddedChannel.readInbound();

        final BytesReference chunk = new BytesArray(randomByteArrayOfLength(embeddedChannel.config().getWriteBufferHighWaterMark() + 1));
        final HttpResponse chunkedResponse = chunkedResponseRequest.createResponse(
            RestStatus.OK,
            getRepeatedChunkResponseBody(randomIntBetween(2, 10), chunk)
        );
        final ChannelPromise chunkedWritePromise = embeddedChannel.newPromise();
        embeddedChannel.write(chunkedResponse, chunkedWritePromise);

        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + i));
        }

        Netty4HttpRequest inbound;
        ArrayList<Netty4HttpRequest> requests = new ArrayList<>();
        while ((inbound = embeddedChannel.readInbound()) != null) {
            requests.add(inbound);
        }

        ArrayList<ChannelPromise> promises = new ArrayList<>();
        for (Netty4HttpRequest request : requests) {
            ChannelPromise promise = embeddedChannel.newPromise();
            promises.add(promise);
            Netty4FullHttpResponse resp = request.createResponse(RestStatus.OK, BytesArray.EMPTY);
            embeddedChannel.write(resp, promise);
        }
        assertFalse(chunkedWritePromise.isDone());
        for (ChannelPromise promise : promises) {
            assertFalse(promise.isDone());
        }
        embeddedChannel.close().syncUninterruptibly();
        assertDoneWithClosedChannel(chunkedWritePromise);
        for (ChannelPromise promise : promises) {
            assertDoneWithClosedChannel(promise);
        }
        // we wrote the first chunk and its headers only
        assertThat(messagesSeen, hasSize(2));
        assertThat(messagesSeen.get(0), instanceOf(Netty4ChunkedHttpResponse.class));
        assertThat(messagesSeen.get(1), instanceOf(DefaultHttpContent.class));
    }

    public void testActivityTracking() {
        final var watchdog = new ThreadWatchdog();
        final var activityTracker = watchdog.getActivityTrackerForCurrentThread();
        final var requestHandled = new AtomicBoolean();
        final var handler = new Netty4HttpPipeliningHandler(Integer.MAX_VALUE, mock(Netty4HttpServerTransport.class), activityTracker) {
            @Override
            protected void handlePipelinedRequest(ChannelHandlerContext ctx, Netty4HttpRequest pipelinedRequest) {
                // thread is not idle while handling the request
                assertThat(ThreadWatchdogHelper.getStuckThreadNames(watchdog), empty());
                assertThat(ThreadWatchdogHelper.getStuckThreadNames(watchdog), equalTo(List.of(Thread.currentThread().getName())));
                ctx.fireChannelRead(pipelinedRequest);
                assertTrue(requestHandled.compareAndSet(false, true));
            }
        };

        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new ChannelDuplexHandler(), handler);
        embeddedChannel.writeInbound(createHttpRequest("/test"));
        assertTrue(requestHandled.get());

        // thread is now idle
        assertThat(ThreadWatchdogHelper.getStuckThreadNames(watchdog), empty());
        assertThat(ThreadWatchdogHelper.getStuckThreadNames(watchdog), empty());
    }

    // assert that a message of the given number of repeated chunks is found at the given index in the list and each chunk is equal to
    // the given BytesReference
    private static void assertChunkedMessageAtIndex(List<Object> messagesSeen, int index, int chunks, BytesReference chunkBytes) {
        assertThat(messagesSeen.get(index), instanceOf(Netty4ChunkedHttpResponse.class));
        for (int i = index + 1; i < chunks; i++) {
            assertThat(messagesSeen.get(i), instanceOf(DefaultHttpContent.class));
            assertContentAtIndexEquals(messagesSeen, i, chunkBytes);
        }
        assertThat(messagesSeen.get(index + chunks), instanceOf(LastHttpContent.class));
    }

    private static void assertContentAtIndexEquals(List<Object> messagesSeen, int index, BytesReference single) {
        assertEquals(Netty4Utils.toBytesReference(((ByteBufHolder) messagesSeen.get(index)).content()), single);
    }

    private static void assertDoneWithClosedChannel(ChannelPromise chunkedWritePromise) {
        assertTrue(chunkedWritePromise.isDone());
        assertThat(chunkedWritePromise.cause(), instanceOf(ClosedChannelException.class));
    }

    private Netty4HttpPipeliningHandler getTestHttpHandler() {
        return new Netty4HttpPipeliningHandler(
            Integer.MAX_VALUE,
            mock(Netty4HttpServerTransport.class),
            new ThreadWatchdog.ActivityTracker()
        ) {
            @Override
            protected void handlePipelinedRequest(ChannelHandlerContext ctx, Netty4HttpRequest pipelinedRequest) {
                ctx.fireChannelRead(pipelinedRequest);
            }
        };
    }

    private static ChunkedRestResponseBodyPart getRepeatedChunkResponseBody(int chunkCount, BytesReference chunk) {
        return new ChunkedRestResponseBodyPart() {

            private int remaining = chunkCount;

            @Override
            public boolean isPartComplete() {
                return remaining == 0;
            }

            @Override
            public boolean isLastPart() {
                return true;
            }

            @Override
            public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
                fail("no continuations here");
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) {
                assertThat(remaining, greaterThan(0));
                remaining--;
                return ReleasableBytesReference.wrap(chunk);
            }

            @Override
            public String getResponseContentTypeString() {
                return "application/octet-stream";
            }
        };
    }

    private static ChannelDuplexHandler capturingHandler(List<Object> messagesSeen) {
        return new ChannelDuplexHandler() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                assertTrue(ctx.channel().isWritable());
                messagesSeen.add(msg);
                super.write(ctx, msg, promise);
            }
        };
    }

    private void assertReadHttpMessageHasContent(EmbeddedChannel embeddedChannel, String expectedContent) {
        FullHttpResponse response = (FullHttpResponse) embeddedChannel.outboundMessages().poll();
        assertNotNull("Expected response to exist, maybe you did not wait long enough?", response);
        assertNotNull("Expected response to have content " + expectedContent, response.content());
        String data = new String(ByteBufUtil.getBytes(response.content()), StandardCharsets.UTF_8);
        assertThat(data, is(expectedContent));
    }

    private DefaultFullHttpRequest createHttpRequest(String uri) {
        return new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, uri);
    }

    private class WorkEmulatorHandler extends SimpleChannelInboundHandler<Netty4HttpRequest> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, Netty4HttpRequest request) {
            final QueryStringDecoder decoder = new QueryStringDecoder(request.uri());

            final String uri = decoder.path().replace("/", "");
            final BytesReference content = new BytesArray(uri.getBytes(StandardCharsets.UTF_8));
            HttpResponse httpResponse = request.createResponse(RestStatus.OK, content);
            httpResponse.addHeader(CONTENT_LENGTH.toString(), Integer.toString(content.length()));

            final CountDownLatch waitingLatch = new CountDownLatch(1);
            waitingRequests.put(uri, waitingLatch);
            final CountDownLatch finishingLatch = new CountDownLatch(1);
            finishingRequests.put(uri, finishingLatch);

            handlerService.submit(() -> {
                try {
                    assertTrue(waitingLatch.await(1000, TimeUnit.SECONDS));
                    final ChannelPromise promise = ctx.newPromise();
                    eventLoopService.submit(() -> {
                        ctx.write(httpResponse, promise);
                        finishingLatch.countDown();
                    });
                } catch (InterruptedException e) {
                    fail(e.toString());
                }
            });
        }
    }
}
