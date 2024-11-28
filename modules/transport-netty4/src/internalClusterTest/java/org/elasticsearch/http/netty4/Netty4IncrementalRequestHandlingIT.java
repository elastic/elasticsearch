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
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpBodyTracer;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpTransportSettings;
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
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class Netty4IncrementalRequestHandlingIT extends ESNetty4IntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        builder.put(HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.getKey(), new ByteSizeValue(50, ByteSizeUnit.MB));
        return builder.build();
    }

    // ensure empty http content has single 0 size chunk
    public void testEmptyContent() throws Exception {
        try (var ctx = setupClientCtx()) {
            var totalRequests = randomIntBetween(1, 10);
            for (int reqNo = 0; reqNo < totalRequests; reqNo++) {
                var opaqueId = opaqueId(reqNo);

                // send request with empty content
                ctx.clientChannel.writeAndFlush(fullHttpRequest(opaqueId, Unpooled.EMPTY_BUFFER));
                var handler = ctx.awaitRestChannelAccepted(opaqueId);
                handler.stream.next();

                // should receive a single empty chunk
                var recvChunk = safePoll(handler.recvChunks);
                assertTrue(recvChunk.isLast);
                assertEquals(0, recvChunk.chunk.length());
                recvChunk.chunk.close();
                assertFalse(handler.streamClosed);

                // send response to process following request
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));
                assertBusy(() -> assertTrue(handler.streamClosed));
            }
            assertBusy(() -> assertEquals("should receive all server responses", totalRequests, ctx.clientRespQueue.size()));
        }
    }

    // ensures content integrity, no loses and re-order
    public void testReceiveAllChunks() throws Exception {
        try (var ctx = setupClientCtx()) {
            var totalRequests = randomIntBetween(1, 10);
            for (int reqNo = 0; reqNo < totalRequests; reqNo++) {
                var opaqueId = opaqueId(reqNo);

                // this dataset will be compared with one on server side
                var dataSize = randomIntBetween(1024, maxContentLength());
                var sendData = Unpooled.wrappedBuffer(randomByteArrayOfLength(dataSize));
                sendData.retain();
                ctx.clientChannel.writeAndFlush(fullHttpRequest(opaqueId, sendData));

                var handler = ctx.awaitRestChannelAccepted(opaqueId);

                var recvData = Unpooled.buffer(dataSize);
                while (true) {
                    handler.stream.next();
                    var recvChunk = safePoll(handler.recvChunks);
                    try (recvChunk.chunk) {
                        recvData.writeBytes(Netty4Utils.toByteBuf(recvChunk.chunk));
                        if (recvChunk.isLast) {
                            break;
                        }
                    }
                }

                assertFalse(handler.streamClosed);
                assertEquals("sent and received payloads are not the same", sendData, recvData);
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));
                assertBusy(() -> assertTrue(handler.streamClosed));
            }
            assertBusy(() -> assertEquals("should receive all server responses", totalRequests, ctx.clientRespQueue.size()));
        }
    }

    // ensures that all received chunks are released when connection closed and handler notified
    public void testClientConnectionCloseMidStream() throws Exception {
        try (var ctx = setupClientCtx()) {
            var opaqueId = opaqueId(0);

            // write half of http request
            ctx.clientChannel.write(httpRequest(opaqueId, 2 * 1024));
            ctx.clientChannel.writeAndFlush(randomContent(1024, false));

            // await stream handler is ready and request full content
            var handler = ctx.awaitRestChannelAccepted(opaqueId);
            assertBusy(() -> assertNotEquals(0, handler.stream.bufSize()));

            assertFalse(handler.streamClosed);

            // terminate client connection
            ctx.clientChannel.close();
            // read the first half of the request
            handler.stream.next();
            // attempt to read more data and it should notice channel being closed eventually
            handler.stream.next();

            // wait for resources to be released
            assertBusy(() -> {
                assertEquals(0, handler.stream.bufSize());
                assertTrue(handler.streamClosed);
            });
        }
    }

    // ensures that all recieved chunks are released when server decides to close connection
    public void testServerCloseConnectionMidStream() throws Exception {
        try (var ctx = setupClientCtx()) {
            var opaqueId = opaqueId(0);

            // write half of http request
            ctx.clientChannel.write(httpRequest(opaqueId, 2 * 1024));
            ctx.clientChannel.writeAndFlush(randomContent(1024, false));

            // await stream handler is ready and request full content
            var handler = ctx.awaitRestChannelAccepted(opaqueId);
            assertBusy(() -> assertNotEquals(0, handler.stream.bufSize()));
            assertFalse(handler.streamClosed);

            // terminate connection on server and wait resources are released
            handler.channel.request().getHttpChannel().close();
            assertBusy(() -> {
                assertEquals(0, handler.stream.bufSize());
                assertTrue(handler.streamClosed);
            });
        }
    }

    public void testServerExceptionMidStream() throws Exception {
        try (var ctx = setupClientCtx()) {
            var opaqueId = opaqueId(0);

            // write half of http request
            ctx.clientChannel.write(httpRequest(opaqueId, 2 * 1024));
            ctx.clientChannel.writeAndFlush(randomContent(1024, false));

            // await stream handler is ready and request full content
            var handler = ctx.awaitRestChannelAccepted(opaqueId);
            assertBusy(() -> assertNotEquals(0, handler.stream.bufSize()));
            assertFalse(handler.streamClosed);

            handler.shouldThrowInsideHandleChunk = true;
            handler.stream.next();

            assertBusy(() -> {
                assertEquals(0, handler.stream.bufSize());
                assertTrue(handler.streamClosed);
            });
        }
    }

    // ensure that client's socket buffers data when server is not consuming data
    public void testClientBackpressure() throws Exception {
        try (var ctx = setupClientCtx()) {
            var opaqueId = opaqueId(0);
            var payloadSize = maxContentLength();
            var totalParts = 10;
            var partSize = payloadSize / totalParts;
            ctx.clientChannel.writeAndFlush(httpRequest(opaqueId, payloadSize));
            for (int i = 0; i < totalParts; i++) {
                ctx.clientChannel.writeAndFlush(randomContent(partSize, false));
            }
            assertFalse(
                "should not flush last content immediately",
                ctx.clientChannel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).isDone()
            );

            var handler = ctx.awaitRestChannelAccepted(opaqueId);

            // some data flushes from channel into OS buffer and won't be visible here, usually 4-8Mb
            var osBufferOffset = MBytes(10);

            // incrementally read data on server side and ensure client side buffer drains accordingly
            for (int readBytes = 0; readBytes <= payloadSize; readBytes += partSize) {
                var minBufSize = Math.max(payloadSize - readBytes - osBufferOffset, 0);
                var maxBufSize = Math.max(payloadSize - readBytes, 0);
                // it is hard to tell that client's channel is no logger flushing data
                // it might take a few busy-iterations before channel buffer flush to OS
                // and bytesBeforeWritable will stop changing
                assertBusy(() -> {
                    var bufSize = ctx.clientChannel.bytesBeforeWritable();
                    assertTrue(
                        "client's channel buffer should be in range [" + minBufSize + "," + maxBufSize + "], got " + bufSize,
                        bufSize >= minBufSize && bufSize <= maxBufSize
                    );
                });
                handler.readBytes(partSize);
            }
            assertTrue(handler.stream.hasLast());
        }
    }

    // ensures that server reply 100-continue on acceptable request size
    public void test100Continue() throws Exception {
        try (var ctx = setupClientCtx()) {
            for (int reqNo = 0; reqNo < randomIntBetween(2, 10); reqNo++) {
                var id = opaqueId(reqNo);
                var acceptableContentLength = randomIntBetween(0, maxContentLength());

                // send request header and await 100-continue
                var req = httpRequest(id, acceptableContentLength);
                HttpUtil.set100ContinueExpected(req, true);
                ctx.clientChannel.writeAndFlush(req);
                var resp = (FullHttpResponse) safePoll(ctx.clientRespQueue);
                assertEquals(HttpResponseStatus.CONTINUE, resp.status());
                resp.release();

                // send content
                var content = randomContent(acceptableContentLength, true);
                ctx.clientChannel.writeAndFlush(content);

                // consume content and reply 200
                var handler = ctx.awaitRestChannelAccepted(id);
                var consumed = handler.readAllBytes();
                assertEquals(acceptableContentLength, consumed);
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));

                resp = (FullHttpResponse) safePoll(ctx.clientRespQueue);
                assertEquals(HttpResponseStatus.OK, resp.status());
                resp.release();
            }
        }
    }

    // ensures that server reply 413-too-large on oversized request with expect-100-continue
    public void test413TooLargeOnExpect100Continue() throws Exception {
        try (var ctx = setupClientCtx()) {
            for (int reqNo = 0; reqNo < randomIntBetween(2, 10); reqNo++) {
                var id = opaqueId(reqNo);
                var oversized = maxContentLength() + 1;

                // send request header and await 413 too large
                var req = httpRequest(id, oversized);
                HttpUtil.set100ContinueExpected(req, true);
                ctx.clientChannel.writeAndFlush(req);
                var resp = (FullHttpResponse) safePoll(ctx.clientRespQueue);
                assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, resp.status());
                resp.release();

                // terminate request
                ctx.clientChannel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            }
        }
    }

    // ensures that oversized chunked encoded request has no limits at http layer
    // rest handler is responsible for oversized requests
    public void testOversizedChunkedEncodingNoLimits() throws Exception {
        try (var ctx = setupClientCtx()) {
            for (var reqNo = 0; reqNo < randomIntBetween(2, 10); reqNo++) {
                var id = opaqueId(reqNo);
                var contentSize = maxContentLength() + 1;
                var content = randomByteArrayOfLength(contentSize);
                var is = new ByteBufInputStream(Unpooled.wrappedBuffer(content));
                var chunkedIs = new ChunkedStream(is);
                var httpChunkedIs = new HttpChunkedInput(chunkedIs, LastHttpContent.EMPTY_LAST_CONTENT);
                var req = httpRequest(id, 0);
                HttpUtil.setTransferEncodingChunked(req, true);

                ctx.clientChannel.pipeline().addLast(new ChunkedWriteHandler());
                ctx.clientChannel.writeAndFlush(req);
                ctx.clientChannel.writeAndFlush(httpChunkedIs);
                var handler = ctx.awaitRestChannelAccepted(id);
                var consumed = handler.readAllBytes();
                assertEquals(contentSize, consumed);
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));

                var resp = (FullHttpResponse) safePoll(ctx.clientRespQueue);
                assertEquals(HttpResponseStatus.OK, resp.status());
                resp.release();
            }
        }
    }

    // ensures that we dont leak buffers in stream on 400-bad-request
    // some bad requests are dispatched from rest-controller before reaching rest handler
    // test relies on netty's buffer leak detection
    public void testBadRequestReleaseQueuedChunks() throws Exception {
        try (var ctx = setupClientCtx()) {
            for (var reqNo = 0; reqNo < randomIntBetween(2, 10); reqNo++) {
                var id = opaqueId(reqNo);
                var contentSize = randomIntBetween(0, maxContentLength());
                var req = httpRequest(id, contentSize);
                var content = randomContent(contentSize, true);

                // set unacceptable content-type
                req.headers().set(CONTENT_TYPE, "unknown");
                ctx.clientChannel.writeAndFlush(req);
                ctx.clientChannel.writeAndFlush(content);

                var resp = (FullHttpResponse) safePoll(ctx.clientRespQueue);
                assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
                resp.release();
            }
        }
    }

    private static long transportStatsRequestBytesSize(Ctx ctx) {
        var httpTransport = internalCluster().getInstance(HttpServerTransport.class, ctx.nodeName);
        var stats = httpTransport.stats().clientStats();
        var bytes = 0L;
        for (var s : stats) {
            bytes += s.requestSizeBytes();
        }
        return bytes;
    }

    /**
     * ensures that {@link org.elasticsearch.http.HttpClientStatsTracker} counts streamed content bytes
     */
    public void testHttpClientStats() throws Exception {
        try (var ctx = setupClientCtx()) {
            // need to offset starting point, since we reuse cluster and other tests already sent some data
            var totalBytesSent = transportStatsRequestBytesSize(ctx);

            for (var reqNo = 0; reqNo < randomIntBetween(2, 10); reqNo++) {
                var id = opaqueId(reqNo);
                var contentSize = randomIntBetween(0, maxContentLength());
                totalBytesSent += contentSize;
                ctx.clientChannel.writeAndFlush(httpRequest(id, contentSize));
                ctx.clientChannel.writeAndFlush(randomContent(contentSize, true));
                var handler = ctx.awaitRestChannelAccepted(id);
                handler.readAllBytes();
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));
                assertEquals(totalBytesSent, transportStatsRequestBytesSize(ctx));
            }
        }
    }

    /**
     * ensures that we log parts of http body and final line
     */
    @TestLogging(
        reason = "testing TRACE logging",
        value = "org.elasticsearch.http.HttpTracer:TRACE,org.elasticsearch.http.HttpBodyTracer:TRACE"
    )
    public void testHttpBodyLogging() throws Exception {
        assertHttpBodyLogging((ctx) -> () -> {
            try {
                var req = fullHttpRequest(opaqueId(0), randomByteBuf(8 * 1024));
                ctx.clientChannel.writeAndFlush(req);
                var handler = ctx.awaitRestChannelAccepted(opaqueId(0));
                handler.readAllBytes();
            } catch (Exception e) {
                fail(e);
            }
        });
    }

    /**
     * ensures that we log some parts of body and final line when connection is closed in the middle
     */
    @TestLogging(
        reason = "testing TRACE logging",
        value = "org.elasticsearch.http.HttpTracer:TRACE,org.elasticsearch.http.HttpBodyTracer:TRACE"
    )
    public void testHttpBodyLoggingChannelClose() throws Exception {
        assertHttpBodyLogging((ctx) -> () -> {
            try {
                var req = httpRequest(opaqueId(0), 2 * 8192);
                var halfContent = randomContent(8192, false);
                ctx.clientChannel.writeAndFlush(req);
                ctx.clientChannel.writeAndFlush(halfContent);
                var handler = ctx.awaitRestChannelAccepted(opaqueId(0));
                handler.readBytes(8192);
                ctx.clientChannel.close();
                handler.stream.next();
                assertBusy(() -> assertTrue(handler.streamClosed));
            } catch (Exception e) {
                fail(e);
            }
        });
    }

    // asserts that we emit at least one logging event for a part and last line
    // http body should be large enough to split across multiple lines, > 4kb
    private void assertHttpBodyLogging(Function<Ctx, Runnable> test) throws Exception {
        try (var ctx = setupClientCtx()) {
            MockLog.assertThatLogger(
                test.apply(ctx),
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

    private int maxContentLength() {
        return HttpHandlingSettings.fromSettings(internalCluster().getInstance(Settings.class)).maxContentLength();
    }

    private String opaqueId(int reqNo) {
        return getTestName() + "-" + reqNo;
    }

    static int MBytes(int m) {
        return m * 1024 * 1024;
    }

    static <T> T safePoll(BlockingDeque<T> queue) {
        try {
            var t = queue.poll(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            assertNotNull("queue is empty", t);
            return t;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    static FullHttpRequest fullHttpRequest(String opaqueId, ByteBuf content) {
        var req = new DefaultFullHttpRequest(HTTP_1_1, POST, ControlServerRequestPlugin.ROUTE, Unpooled.wrappedBuffer(content));
        req.headers().add(CONTENT_LENGTH, content.readableBytes());
        req.headers().add(CONTENT_TYPE, APPLICATION_JSON);
        req.headers().add(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);
        return req;
    }

    static HttpRequest httpRequest(String opaqueId, int contentLength) {
        return httpRequest(ControlServerRequestPlugin.ROUTE, opaqueId, contentLength);
    }

    static HttpRequest httpRequest(String uri, String opaqueId, int contentLength) {
        var req = new DefaultHttpRequest(HTTP_1_1, POST, uri);
        req.headers().add(CONTENT_LENGTH, contentLength);
        req.headers().add(CONTENT_TYPE, APPLICATION_JSON);
        req.headers().add(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);
        return req;
    }

    static HttpContent randomContent(int size, boolean isLast) {
        var buf = Unpooled.wrappedBuffer(randomByteArrayOfLength(size));
        if (isLast) {
            return new DefaultLastHttpContent(buf);
        } else {
            return new DefaultHttpContent(buf);
        }
    }

    static ByteBuf randomByteBuf(int size) {
        return Unpooled.wrappedBuffer(randomByteArrayOfLength(size));
    }

    Ctx setupClientCtx() throws Exception {
        var nodeName = internalCluster().getRandomNodeName();
        var clientRespQueue = new LinkedBlockingDeque<>(16);
        var bootstrap = bootstrapClient(nodeName, clientRespQueue);
        var channel = bootstrap.connect().sync().channel();
        return new Ctx(getTestName(), nodeName, bootstrap, channel, clientRespQueue);
    }

    Bootstrap bootstrapClient(String node, BlockingQueue<Object> queue) {
        var httpServer = internalCluster().getInstance(HttpServerTransport.class, node);
        var remoteAddr = randomFrom(httpServer.boundAddress().boundAddresses());
        return new Bootstrap().group(new NioEventLoopGroup(1))
            .channel(NioSocketChannel.class)
            .remoteAddress(remoteAddr.getAddress(), remoteAddr.getPort())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    var p = ch.pipeline();
                    p.addLast(new HttpClientCodec());
                    p.addLast(new HttpObjectAggregator(4096));
                    p.addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
                            msg.retain();
                            queue.add(msg);
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                            queue.add(cause);
                        }
                    });
                }
            });
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(ControlServerRequestPlugin.class), super.nodePlugins());
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    record Ctx(String testName, String nodeName, Bootstrap clientBootstrap, Channel clientChannel, BlockingDeque<Object> clientRespQueue)
        implements
            AutoCloseable {

        @Override
        public void close() throws Exception {
            safeGet(clientChannel.close());
            safeGet(clientBootstrap.config().group().shutdownGracefully());
            clientRespQueue.forEach(o -> { if (o instanceof FullHttpResponse resp) resp.release(); });
            for (var opaqueId : ControlServerRequestPlugin.handlers.keySet()) {
                if (opaqueId.startsWith(testName)) {
                    var handler = ControlServerRequestPlugin.handlers.get(opaqueId);
                    handler.recvChunks.forEach(c -> c.chunk.close());
                    handler.channel.request().getHttpChannel().close();
                    ControlServerRequestPlugin.handlers.remove(opaqueId);
                }
            }
        }

        ServerRequestHandler awaitRestChannelAccepted(String opaqueId) throws Exception {
            assertBusy(() -> assertTrue(ControlServerRequestPlugin.handlers.containsKey(opaqueId)));
            var handler = ControlServerRequestPlugin.handlers.get(opaqueId);
            safeAwait(handler.channelAccepted);
            return handler;
        }
    }

    static class ServerRequestHandler implements BaseRestHandler.RequestBodyChunkConsumer {
        final SubscribableListener<Void> channelAccepted = new SubscribableListener<>();
        final String opaqueId;
        final BlockingDeque<Chunk> recvChunks = new LinkedBlockingDeque<>();
        final Netty4HttpRequestBodyStream stream;
        RestChannel channel;
        boolean recvLast = false;
        volatile boolean streamClosed = false;
        volatile boolean shouldThrowInsideHandleChunk = false;

        ServerRequestHandler(String opaqueId, Netty4HttpRequestBodyStream stream) {
            this.opaqueId = opaqueId;
            this.stream = stream;
        }

        @Override
        public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
            Transports.assertTransportThread();
            if (shouldThrowInsideHandleChunk) {
                // Must close the chunk. This is the contract of this method.
                chunk.close();
                throw new RuntimeException("simulated exception inside handleChunk");
            }
            recvChunks.add(new Chunk(chunk, isLast));
        }

        @Override
        public void accept(RestChannel channel) throws Exception {
            this.channel = channel;
            channelAccepted.onResponse(null);
        }

        @Override
        public void streamClose() {
            streamClosed = true;
        }

        void sendResponse(RestResponse response) {
            channel.sendResponse(response);
        }

        int readBytes(int bytes) {
            var consumed = 0;
            if (recvLast == false) {
                while (consumed < bytes) {
                    stream.next();
                    var recvChunk = safePoll(recvChunks);
                    consumed += recvChunk.chunk.length();
                    recvChunk.chunk.close();
                    if (recvChunk.isLast) {
                        recvLast = true;
                        break;
                    }
                }
            }
            return consumed;
        }

        int readAllBytes() {
            return readBytes(Integer.MAX_VALUE);
        }

        record Chunk(ReleasableBytesReference chunk, boolean isLast) {}
    }

    // takes full control of rest handler from the outside
    public static class ControlServerRequestPlugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/request-stream";

        static final ConcurrentHashMap<String, ServerRequestHandler> handlers = new ConcurrentHashMap<>();

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
                    handlers.put(opaqueId, handler);
                    return handler;
                }
            });
        }
    }

}
