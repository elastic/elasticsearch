/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedStream;
import io.netty.handler.stream.ChunkedWriteHandler;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpBodyTracer;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_AGE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class Netty4IncrementalRequestHandlingIT extends ESNetty4IntegTestCase {

    private static final int MAX_CONTENT_LENGTH = ByteSizeUnit.MB.toIntBytes(10);

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // reduce max content length just to cut down test duration
            .put(SETTING_HTTP_MAX_CONTENT_LENGTH.getKey(), ByteSizeValue.of(MAX_CONTENT_LENGTH, ByteSizeUnit.BYTES))
            // disable time-based expiry of channel stats since we assert that the total request size accumulates
            .put(SETTING_HTTP_CLIENT_STATS_MAX_CLOSED_CHANNEL_AGE.getKey(), TimeValue.MAX_VALUE)
            .build();
    }

    // ensure empty http content has single 0 size chunk
    public void testEmptyContent() throws Exception {
        try (var clientContext = newClientContext()) {
            var totalRequests = randomIntBetween(1, 10);
            for (int requestIndex = 0; requestIndex < totalRequests; requestIndex++) {
                var opaqueId = clientContext.newOpaqueId();

                // send request with empty content
                clientContext.channel().writeAndFlush(fullHttpRequest(opaqueId, Unpooled.EMPTY_BUFFER));
                var handler = clientContext.awaitRestChannelAccepted(opaqueId);

                // should receive a single empty chunk
                try (var chunk = handler.getNextChunk()) {
                    assertTrue(chunk.isLast());
                    assertEquals(0, chunk.length());
                }
                assertFalse(handler.isClosed());

                // send response to process following request
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));
                safeAwait(handler.closedLatch);
            }
            for (int requestIndex = 0; requestIndex < totalRequests; requestIndex++) {
                clientContext.getNextResponse().release();
            }
        }
    }

    // ensures content integrity, no losses or re-ordering
    public void testReceiveAllChunks() throws Exception {
        try (var clientContext = newClientContext()) {
            var totalRequests = randomIntBetween(1, 10);
            for (int requestIndex = 0; requestIndex < totalRequests; requestIndex++) {
                var opaqueId = clientContext.newOpaqueId();

                // this dataset will be compared with one on server side
                var dataSize = randomIntBetween(1, MAX_CONTENT_LENGTH);
                var sendData = Unpooled.wrappedBuffer(randomByteArrayOfLength(dataSize));
                sendData.retain();
                clientContext.channel().writeAndFlush(fullHttpRequest(opaqueId, sendData));

                var handler = clientContext.awaitRestChannelAccepted(opaqueId);

                var receivedData = Unpooled.buffer(dataSize);
                while (true) {
                    try (var chunk = handler.getNextChunk()) {
                        receivedData.writeBytes(Netty4Utils.toByteBuf(chunk.data()));
                        if (chunk.isLast()) {
                            break;
                        }
                    }
                }

                assertFalse(handler.isClosed());
                assertEquals("sent and received payloads are not the same", sendData, receivedData);
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));
                safeAwait(handler.closedLatch);
            }
            for (int requestIndex = 0; requestIndex < totalRequests; requestIndex++) {
                clientContext.getNextResponse().release();
            }
        }
    }

    // ensures that all received chunks are released when connection closed and handler notified
    public void testClientConnectionCloseMidStream() throws Exception {
        try (var clientContext = newClientContext()) {
            var opaqueId = clientContext.newOpaqueId();

            // write less than the complete request body
            final var requestContentLength = between(2, ByteSizeUnit.KB.toIntBytes(10));
            clientContext.channel().write(httpRequest(opaqueId, requestContentLength));
            final var requestTransmittedLength = between(1, requestContentLength - 1);
            clientContext.channel().writeAndFlush(randomContent(requestTransmittedLength, false));

            // await stream handler is ready and request full content
            var handler = clientContext.awaitRestChannelAccepted(opaqueId);

            assertFalse(handler.isClosed());

            // terminate client connection
            clientContext.channel().close();
            // read the transmitted bytes and notice the channel being closed
            assertEquals(requestTransmittedLength, handler.readUntilClose());

            assertTrue(handler.isClosed());
        }
    }

    // ensures that all received chunks are released when server decides to close connection
    public void testServerCloseConnectionMidStream() throws Exception {
        try (var clientContext = newClientContext()) {
            var opaqueId = clientContext.newOpaqueId();

            // write less than the complete request body
            final var requestContentLength = between(2, ByteSizeUnit.KB.toIntBytes(10));
            clientContext.channel().write(httpRequest(opaqueId, requestContentLength));
            final var requestTransmittedLength = between(1, requestContentLength - 1);
            clientContext.channel().writeAndFlush(randomContent(requestTransmittedLength, false));

            // await stream handler is ready and request full content
            var handler = clientContext.awaitRestChannelAccepted(opaqueId);
            assertFalse(handler.isClosed());

            // terminate connection on server and wait resources are released
            final var exceptionFuture = new PlainActionFuture<Exception>();
            assertNull(handler.nextChunkListenerRef.getAndSet(ActionTestUtils.assertNoSuccessListener(exceptionFuture::onResponse)));
            handler.channel.request().getHttpChannel().close();
            assertThat(safeGet(exceptionFuture), instanceOf(ClosedChannelException.class));
            assertTrue(handler.isClosed());
        }
    }

    public void testServerExceptionMidStream() throws Exception {
        try (var clientContext = newClientContext()) {
            var opaqueId = clientContext.newOpaqueId();

            // write less than the complete request body
            final var requestContentLength = between(2, ByteSizeUnit.KB.toIntBytes(10));
            clientContext.channel().write(httpRequest(opaqueId, requestContentLength));
            final var requestTransmittedLength = between(1, requestContentLength - 1);
            clientContext.channel().writeAndFlush(randomContent(requestTransmittedLength, false));

            // await stream handler is ready and request full content
            var handler = clientContext.awaitRestChannelAccepted(opaqueId);
            assertFalse(handler.isClosed());

            // terminate connection on server and wait resources are released
            final var exceptionFuture = new PlainActionFuture<Exception>();
            assertNull(handler.nextChunkListenerRef.getAndSet(ActionTestUtils.assertNoSuccessListener(exceptionFuture::onResponse)));
            handler.shouldThrowInsideHandleChunk = true;
            handler.stream.next();
            // simulated exception inside handleChunk
            final var exception = asInstanceOf(RuntimeException.class, safeGet(exceptionFuture));
            assertEquals(ServerRequestHandler.SIMULATED_EXCEPTION_MESSAGE, exception.getMessage());
            safeAwait(handler.closedLatch);
        }
    }

    // ensure that client's socket buffers data when server is not consuming data
    public void testClientBackpressure() throws Exception {
        try (var clientContext = newClientContext()) {
            var opaqueId = clientContext.newOpaqueId();
            var payloadSize = MAX_CONTENT_LENGTH;
            var totalParts = 10;
            var partSize = payloadSize / totalParts;
            clientContext.channel().writeAndFlush(httpRequest(opaqueId, payloadSize));
            for (int i = 0; i < totalParts; i++) {
                clientContext.channel().writeAndFlush(randomContent(partSize, false));
            }
            assertFalse(
                "should not flush last content immediately",
                clientContext.channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).isDone()
            );

            var handler = clientContext.awaitRestChannelAccepted(opaqueId);

            // some data flushes from channel into OS buffer and won't be visible here, usually 4-8Mb
            var osBufferOffset = ByteSizeUnit.MB.toIntBytes(10);

            // incrementally read data on server side and ensure client side buffer drains accordingly
            for (int readBytes = 0; readBytes <= payloadSize; readBytes += partSize) {
                var minBufSize = Math.max(payloadSize - readBytes - osBufferOffset, 0);
                var maxBufSize = Math.max(payloadSize - readBytes, 0);
                // it is hard to tell that client's channel is no logger flushing data
                // it might take a few busy-iterations before channel buffer flush to OS
                // and bytesBeforeWritable will stop changing
                assertBusy(() -> {
                    var bufSize = clientContext.channel().bytesBeforeWritable();
                    assertTrue(
                        "client's channel buffer should be in range [" + minBufSize + "," + maxBufSize + "], got " + bufSize,
                        bufSize >= minBufSize && bufSize <= maxBufSize
                    );
                });
                handler.readBytes(partSize);
            }
            assertTrue(handler.receivedLastChunk);
        }
    }

    // ensures that server reply 100-continue on acceptable request size
    public void test100Continue() throws Exception {
        try (var clientContext = newClientContext()) {
            var totalRequests = randomIntBetween(1, 10);
            for (int requestIndex = 0; requestIndex < totalRequests; requestIndex++) {
                var opaqueId = clientContext.newOpaqueId();
                var acceptableContentLength = randomIntBetween(0, MAX_CONTENT_LENGTH);

                // send request header and await 100-continue
                final var request = httpRequest(opaqueId, acceptableContentLength);
                HttpUtil.set100ContinueExpected(request, true);
                clientContext.channel().writeAndFlush(request);
                final var continueResponse = clientContext.getNextResponse();
                assertEquals(HttpResponseStatus.CONTINUE, continueResponse.status());
                continueResponse.release();

                // send content
                clientContext.channel().writeAndFlush(randomContent(acceptableContentLength, true));

                // consume content and reply 200
                final var handler = clientContext.awaitRestChannelAccepted(opaqueId);
                final var consumed = handler.readAllBytes();
                assertEquals(acceptableContentLength, consumed);
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));

                final var finalResponse = clientContext.getNextResponse();
                assertEquals(HttpResponseStatus.OK, finalResponse.status());
                finalResponse.release();
            }
        }
    }

    // ensures that server reply 413-too-large on oversized request with expect-100-continue
    public void test413TooLargeOnExpect100Continue() throws Exception {
        try (var clientContext = newClientContext()) {
            var totalRequests = randomIntBetween(1, 10);
            for (int requestIndex = 0; requestIndex < totalRequests; requestIndex++) {
                var opaqueId = clientContext.newOpaqueId();

                // send request header and await 413 too large
                final var request = httpRequest(opaqueId, MAX_CONTENT_LENGTH + 1);
                HttpUtil.set100ContinueExpected(request, true);
                clientContext.channel().writeAndFlush(request);
                final var response = clientContext.getNextResponse();
                assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());
                response.release();

                // terminate request
                clientContext.channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            }
        }
    }

    // ensures that oversized chunked encoded request has maxContentLength limit and returns 413
    public void testOversizedChunkedEncoding() throws Exception {
        try (
            var clientContext = newClientContext(
                internalCluster().getRandomNodeName(),
                t -> {/* ignore exception from e.g. server closing socket */}
            )
        ) {
            var opaqueId = clientContext.newOpaqueId();
            final var requestBodyStream = new HttpChunkedInput(
                new ChunkedStream(new ByteBufInputStream(Unpooled.wrappedBuffer(randomByteArrayOfLength(MAX_CONTENT_LENGTH + 1)))),
                LastHttpContent.EMPTY_LAST_CONTENT
            );
            final var request = httpRequest(opaqueId, 0);
            HttpUtil.setTransferEncodingChunked(request, true);

            clientContext.channel().pipeline().addLast(new ChunkedWriteHandler());
            clientContext.channel().writeAndFlush(request);
            clientContext.channel().writeAndFlush(requestBodyStream);
            var handler = clientContext.awaitRestChannelAccepted(opaqueId);
            assertThat(handler.readUntilClose(), lessThanOrEqualTo(MAX_CONTENT_LENGTH));

            var response = clientContext.getNextResponse();
            assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());
            response.release();
        }
    }

    // ensures that we don't leak buffers in stream on 400-bad-request
    // some bad requests are dispatched from rest-controller before reaching rest handler
    // test relies on netty's buffer leak detection
    public void testBadRequestReleaseQueuedChunks() throws Exception {
        try (var clientContext = newClientContext()) {
            var totalRequests = randomIntBetween(1, 10);
            for (int requestIndex = 0; requestIndex < totalRequests; requestIndex++) {
                var opaqueId = clientContext.newOpaqueId();

                final var contentSize = randomIntBetween(0, MAX_CONTENT_LENGTH);
                final var request = httpRequest(opaqueId, contentSize);
                final var content = randomContent(contentSize, true);

                // set unacceptable content-type
                request.headers().set(CONTENT_TYPE, "unknown");
                clientContext.channel().writeAndFlush(request);
                clientContext.channel().writeAndFlush(content);

                final var response = clientContext.getNextResponse();
                assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
                response.release();
            }
        }
    }

    /**
     * ensures that {@link org.elasticsearch.http.HttpClientStatsTracker} counts streamed content bytes
     */
    public void testHttpClientStats() throws Exception {
        try (var clientContext = newClientContext()) {
            // need to offset starting point, since we reuse cluster and other tests already sent some data
            var totalBytesSent = clientContext.transportStatsRequestBytesSize();

            final var totalRequests = between(1, 10);
            for (var requestIndex = 0; requestIndex < totalRequests; requestIndex++) {
                var opaqueId = clientContext.newOpaqueId();
                final var contentSize = randomIntBetween(0, MAX_CONTENT_LENGTH);
                totalBytesSent += contentSize;
                clientContext.channel().writeAndFlush(httpRequest(opaqueId, contentSize));
                clientContext.channel().writeAndFlush(randomContent(contentSize, true));
                final var handler = clientContext.awaitRestChannelAccepted(opaqueId);
                assertEquals(contentSize, handler.readAllBytes());
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));
                assertEquals(totalBytesSent, clientContext.transportStatsRequestBytesSize());
            }
            for (int requestIndex = 0; requestIndex < totalRequests; requestIndex++) {
                clientContext.getNextResponse().release();
            }
        }
    }

    // ensures that we log parts of http body and final line
    @TestLogging(
        reason = "testing TRACE logging",
        value = "org.elasticsearch.http.HttpTracer:TRACE,org.elasticsearch.http.HttpBodyTracer:TRACE"
    )
    public void testHttpBodyLogging() throws Exception {
        assertHttpBodyLogging(clientContext -> {
            var opaqueId = clientContext.newOpaqueId();
            clientContext.channel()
                .writeAndFlush(fullHttpRequest(opaqueId, Unpooled.wrappedBuffer(randomByteArrayOfLength(ByteSizeUnit.KB.toIntBytes(8)))));
            final var handler = clientContext.awaitRestChannelAccepted(opaqueId);
            handler.readAllBytes();
        });
    }

    // ensures that we log some parts of body and final line when connection is closed in the middle
    @TestLogging(
        reason = "testing TRACE logging",
        value = "org.elasticsearch.http.HttpTracer:TRACE,org.elasticsearch.http.HttpBodyTracer:TRACE"
    )
    public void testHttpBodyLoggingChannelClose() throws Exception {
        assertHttpBodyLogging(clientContext -> {
            var opaqueId = clientContext.newOpaqueId();

            // write less than the complete request body, but still enough to get multiple log lines
            final var requestContentLength = between(ByteSizeUnit.KB.toIntBytes(8) + 1, ByteSizeUnit.KB.toIntBytes(16));
            clientContext.channel().write(httpRequest(opaqueId, requestContentLength));
            final var requestTransmittedLength = between(ByteSizeUnit.KB.toIntBytes(8), requestContentLength - 1);
            clientContext.channel().writeAndFlush(randomContent(requestTransmittedLength, false));

            final var handler = clientContext.awaitRestChannelAccepted(opaqueId);
            assertEquals(requestTransmittedLength, handler.readBytes(requestTransmittedLength));
            clientContext.channel().close();
            assertEquals(0, handler.readUntilClose());
            assertTrue(handler.isClosed());
        });
    }

    // asserts that we emit at least one logging event for a part and last line
    // http body should be large enough to split across multiple lines, > 4kb
    private void assertHttpBodyLogging(Consumer<ClientContext> test) throws Exception {
        try (var clientContext = newClientContext()) {
            final Runnable testRunnable = () -> {
                try {
                    test.accept(clientContext);
                } catch (Exception e) {
                    fail(e);
                }
            };
            MockLog.assertThatLogger(
                testRunnable,
                HttpBodyTracer.class,
                new MockLog.SeenEventExpectation(
                    "request part",
                    HttpBodyTracer.class.getCanonicalName(),
                    Level.TRACE,
                    "* request body [part *]*"
                ),
                new MockLog.SeenEventExpectation(
                    "request end",
                    HttpBodyTracer.class.getCanonicalName(),
                    Level.TRACE,
                    "* request body (gzip compressed, base64-encoded, and split into * parts on preceding log lines;*)"
                )
            );
        }
    }

    public void testBulkIndexingRequestSplitting() throws Exception {
        final var watermarkBytes = between(100, 200);
        final var tinyNode = internalCluster().startCoordinatingOnlyNode(
            Settings.builder()
                .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK.getKey(), ByteSizeValue.ofBytes(watermarkBytes))
                .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK_SIZE.getKey(), ByteSizeValue.ofBytes(watermarkBytes))
                .build()
        );

        try (var clientContext = newClientContext(tinyNode, cause -> ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError(cause)))) {
            final var request = new DefaultHttpRequest(HTTP_1_1, POST, "/_bulk");
            request.headers().add(CONTENT_TYPE, APPLICATION_JSON);
            HttpUtil.setTransferEncodingChunked(request, true);

            final var channel = clientContext.channel();
            channel.writeAndFlush(request);

            final var indexName = randomIdentifier();
            final var indexCreatedListener = ClusterServiceUtils.addTemporaryStateListener(
                cs -> Iterators.filter(
                    cs.metadata().indicesAllProjects().iterator(),
                    indexMetadata -> indexMetadata.getIndex().getName().equals(indexName)
                ).hasNext()
            );

            indexCreatedListener.addListener(ActionListener.running(() -> logger.info("--> index created")));

            final var valueLength = between(10, 30);
            final var docSizeBytes = "{'field':''}".length() + valueLength;
            final var itemCount = between(watermarkBytes / docSizeBytes + 1, 300); // enough to split at least once
            assertThat(itemCount * docSizeBytes, greaterThan(watermarkBytes));
            for (int i = 0; i < itemCount; i++) {
                channel.write(new DefaultHttpContent(Unpooled.wrappedBuffer(Strings.format("""
                    {"index":{"_index":"%s"}}
                    {"field":"%s"}
                    """, indexName, randomAlphaOfLength(valueLength)).getBytes(StandardCharsets.UTF_8))));
            }

            channel.flush();
            safeAwait(indexCreatedListener); // index must be created before we finish sending the request

            channel.writeAndFlush(new DefaultLastHttpContent());
            final var response = clientContext.getNextResponse();
            try {
                assertEquals(RestStatus.OK.getStatus(), response.status().code());
                final ObjectPath responseBody;
                final var copy = response.content().copy(); // Netty4Utils doesn't handle direct buffers, so copy to heap first
                try {
                    responseBody = ObjectPath.createFromXContent(JsonXContent.jsonXContent, Netty4Utils.toBytesReference(copy));
                } finally {
                    copy.release();
                }
                assertFalse(responseBody.evaluate("errors"));
                assertEquals(itemCount, responseBody.evaluateArraySize("items"));
                for (int i = 0; i < itemCount; i++) {
                    assertEquals(
                        RestStatus.CREATED.getStatus(),
                        (int) asInstanceOf(int.class, responseBody.evaluateExact("items", Integer.toString(i), "index", "status"))
                    );
                }
            } finally {
                response.release();
            }
        }
    }

    static FullHttpRequest fullHttpRequest(String opaqueId, ByteBuf content) {
        var request = new DefaultFullHttpRequest(HTTP_1_1, POST, ControlServerRequestPlugin.ROUTE, Unpooled.wrappedBuffer(content));
        request.headers().add(CONTENT_LENGTH, content.readableBytes());
        request.headers().add(CONTENT_TYPE, APPLICATION_JSON);
        request.headers().add(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);
        return request;
    }

    static HttpRequest httpRequest(String opaqueId, int contentLength) {
        var request = new DefaultHttpRequest(HTTP_1_1, POST, ControlServerRequestPlugin.ROUTE);
        request.headers().add(CONTENT_LENGTH, contentLength);
        request.headers().add(CONTENT_TYPE, APPLICATION_JSON);
        request.headers().add(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);
        return request;
    }

    static HttpContent randomContent(int size, boolean isLast) {
        var buf = Unpooled.wrappedBuffer(randomByteArrayOfLength(size));
        if (isLast) {
            return new DefaultLastHttpContent(buf);
        } else {
            return new DefaultHttpContent(buf);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(ControlServerRequestPlugin.class), super.nodePlugins());
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    private static final LongSupplier idGenerator = new AtomicLong()::getAndIncrement;

    private ClientContext newClientContext() throws Exception {
        return newClientContext(
            internalCluster().getRandomNodeName(),
            cause -> ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError(cause))
        );
    }

    private ClientContext newClientContext(String nodeName, Consumer<Throwable> exceptionHandler) throws Exception {
        var clientResponseQueue = new LinkedBlockingDeque<FullHttpResponse>(16);
        final var httpServerTransport = internalCluster().getInstance(HttpServerTransport.class, nodeName);
        var remoteAddr = randomFrom(httpServerTransport.boundAddress().boundAddresses());
        var handlersByOpaqueId = internalCluster().getInstance(HandlersByOpaqueId.class, nodeName);
        var bootstrap = new Bootstrap().group(new NioEventLoopGroup(1))
            .channel(NioSocketChannel.class)
            .remoteAddress(remoteAddr.getAddress(), remoteAddr.getPort())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    var p = ch.pipeline();
                    p.addLast(new HttpClientCodec());
                    p.addLast(new HttpObjectAggregator(ByteSizeUnit.MB.toIntBytes(4)));
                    p.addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
                            msg.retain();
                            clientResponseQueue.add(msg);
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            exceptionHandler.accept(cause);
                        }
                    });
                }
            });
        var channel = bootstrap.connect().sync().channel();
        return new ClientContext(
            getTestName() + "-" + randomIdentifier() + "-" + idGenerator.getAsLong(),
            httpServerTransport,
            handlersByOpaqueId,
            bootstrap,
            channel,
            clientResponseQueue
        );
    }

    /**
     * Collects together the objects that make up the client and surrounding context in which each test runs.
     */
    static final class ClientContext implements AutoCloseable {
        private final String contextName;
        private final HttpServerTransport httpServerTransport;
        private final HandlersByOpaqueId handlersByOpaqueId;
        private final Bootstrap bootstrap;
        private final Channel channel;
        private final BlockingDeque<FullHttpResponse> responseQueue;

        ClientContext(
            String contextName,
            HttpServerTransport httpServerTransport,
            HandlersByOpaqueId handlersByOpaqueId,
            Bootstrap bootstrap,
            Channel channel,
            BlockingDeque<FullHttpResponse> responseQueue
        ) {
            this.contextName = contextName;
            this.httpServerTransport = httpServerTransport;
            this.handlersByOpaqueId = handlersByOpaqueId;
            this.bootstrap = bootstrap;
            this.channel = channel;
            this.responseQueue = responseQueue;
        }

        String newOpaqueId() {
            return contextName + "-" + idGenerator.getAsLong();
        }

        @Override
        public void close() {
            safeGet(channel.close());
            safeGet(bootstrap.config().group().shutdownGracefully(0, 0, TimeUnit.SECONDS));
            assertThat(responseQueue, empty());
            handlersByOpaqueId.removeHandlers(contextName);
        }

        FullHttpResponse getNextResponse() {
            try {
                var response = responseQueue.poll(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
                assertNotNull("queue is empty", response);
                return response;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }

        ServerRequestHandler awaitRestChannelAccepted(String opaqueId) {
            var handler = safeAwait(handlersByOpaqueId.getHandlerFor(opaqueId));
            safeAwait(handler.channelAccepted);
            return handler;
        }

        long transportStatsRequestBytesSize() {
            var bytes = 0L;
            for (final var clientStats : httpServerTransport.stats().clientStats()) {
                bytes += clientStats.requestSizeBytes();
            }
            return bytes;
        }

        Channel channel() {
            return channel;
        }
    }

    /** A streaming request handler which allows tests to consume exactly the data (bytes or chunks) they expect.  */
    static class ServerRequestHandler implements BaseRestHandler.RequestBodyChunkConsumer {
        final SubscribableListener<Void> channelAccepted = new SubscribableListener<>();
        final String opaqueId;
        private final AtomicReference<ActionListener<Chunk>> nextChunkListenerRef = new AtomicReference<>();
        final Netty4HttpRequestBodyStream stream;
        RestChannel channel;
        boolean receivedLastChunk = false;
        final CountDownLatch closedLatch = new CountDownLatch(1);
        volatile boolean shouldThrowInsideHandleChunk = false;

        ServerRequestHandler(String opaqueId, Netty4HttpRequestBodyStream stream) {
            this.opaqueId = opaqueId;
            this.stream = stream;
        }

        static final String SIMULATED_EXCEPTION_MESSAGE = "simulated exception inside handleChunk";

        boolean isClosed() {
            return closedLatch.getCount() == 0;
        }

        @Override
        public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
            Transports.assertTransportThread();
            assertFalse("should not get any chunks after close", isClosed());
            final var nextChunkListener = nextChunkListenerRef.getAndSet(null);
            assertNotNull("next chunk must be explicitly requested", nextChunkListener);
            if (shouldThrowInsideHandleChunk) {
                // Must close the chunk. This is the contract of this method.
                chunk.close();
                final var exception = new RuntimeException(SIMULATED_EXCEPTION_MESSAGE);
                nextChunkListener.onFailure(exception);
                throw exception;
            }
            nextChunkListener.onResponse(new Chunk(chunk, isLast));
        }

        @Override
        public void accept(RestChannel channel) {
            this.channel = channel;
            channelAccepted.onResponse(null);
        }

        @Override
        public void streamClose() {
            Transports.assertTransportThread();
            closedLatch.countDown();
            final var nextChunkListener = nextChunkListenerRef.getAndSet(null);
            if (nextChunkListener != null) {
                // might get a chunk and then a close in one read event, in which case the chunk consumes the listener
                nextChunkListener.onFailure(new ClosedChannelException());
            }
        }

        void sendResponse(RestResponse response) {
            channel.sendResponse(response);
        }

        Chunk getNextChunk() throws Exception {
            final var exception = new AtomicReference<Exception>();
            final var future = new PlainActionFuture<Chunk>();
            assertTrue(nextChunkListenerRef.compareAndSet(null, ActionListener.assertOnce(future.delegateResponse((l, e) -> {
                assertTrue(exception.compareAndSet(null, e));
                l.onResponse(null);
            }))));
            if (isClosed()) {
                // check streamClosed _after_ registering listener ref
                nextChunkListenerRef.set(null);
                if (randomBoolean()) {
                    stream.next(); // shouldn't do anything after close anyway
                }
                throw new ClosedChannelException();
            }
            stream.next();
            final var chunk = safeGet(future);
            if (exception.get() != null) {
                throw exception.get();
            }
            return chunk;
        }

        int readBytes(int bytes) {
            var consumed = 0;
            if (receivedLastChunk == false) {
                while ((bytes == -1 || consumed < bytes) && isClosed() == false) {
                    try (var chunk = getNextChunk()) {
                        consumed = Math.addExact(consumed, chunk.length());
                        if (chunk.isLast()) {
                            receivedLastChunk = true;
                            break;
                        }
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            }
            return consumed;
        }

        int readAllBytes() {
            return readBytes(-1);
        }

        int readUntilClose() {
            int handledBytes = 0;
            while (true) {
                try {
                    try (var chunk = getNextChunk()) {
                        handledBytes = Math.addExact(handledBytes, chunk.length());
                    }
                } catch (ClosedChannelException e) {
                    return handledBytes;
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
        }

        record Chunk(ReleasableBytesReference data, boolean isLast) implements Releasable {
            @Override
            public void close() {
                Releasables.closeExpectNoException(data);
            }

            public int length() {
                return data.length();
            }
        }
    }

    /** Collection of ServerRequestHandler instances keyed by the X-Opaque-Id header of the request that created it, on each node */
    public static class HandlersByOpaqueId extends AbstractLifecycleComponent {
        private final Map<String, SubscribableListener<ServerRequestHandler>> handlers = new ConcurrentHashMap<>();

        SubscribableListener<ServerRequestHandler> getHandlerFor(String opaqueId) {
            return handlers.computeIfAbsent(opaqueId, ignored -> new SubscribableListener<>());
        }

        void removeHandlers(String contextName) {
            for (var opaqueId : handlers.keySet()) {
                if (opaqueId.startsWith(contextName + "-")) {
                    var handler = safeAwait(handlers.get(opaqueId));
                    handler.channel.request().getHttpChannel().close();
                    handlers.remove(opaqueId);
                }
            }
        }

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {
            assertThat(handlers, anEmptyMap()); // should not have leaked any handlers at node shutdown
        }

        @Override
        protected void doClose() {}
    }

    /** Exposes a RestHandler over which the test has full control */
    public static class ControlServerRequestPlugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/request-stream";

        private final HandlersByOpaqueId handlersByOpaqueId = new HandlersByOpaqueId();

        @Override
        public Collection<?> createComponents(PluginServices services) {
            return List.of(handlersByOpaqueId);
        }

        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new BaseRestHandler() {
                @Override
                public String getName() {
                    return ROUTE;
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.POST, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    var stream = (Netty4HttpRequestBodyStream) request.contentStream();
                    var opaqueId = request.getHeaders().get(Task.X_OPAQUE_ID_HTTP_HEADER).get(0);
                    var handler = new ServerRequestHandler(opaqueId, stream);
                    handlersByOpaqueId.getHandlerFor(opaqueId).onResponse(handler);
                    return handler;
                }
            });
        }
    }

}
