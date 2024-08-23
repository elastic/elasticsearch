/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
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
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;

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
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpServerTransport;
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
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class Netty4IncrementalRequestHandlingIT extends ESNetty4IntegTestCase {

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

                // send response to process following request
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));
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
                var dataSize = randomIntBetween(1024, 10 * 1024 * 1024);
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

                assertEquals("sent and received payloads are not the same", sendData, recvData);
                handler.sendResponse(new RestResponse(RestStatus.OK, ""));
            }
            assertBusy(() -> assertEquals("should receive all server responses", totalRequests, ctx.clientRespQueue.size()));
        }
    }

    // ensures that all queued chunks are released when connection closed
    public void testClientConnectionCloseMidStream() throws Exception {
        try (var ctx = setupClientCtx()) {
            var opaqueId = opaqueId(0);

            // write half of http request
            ctx.clientChannel.write(httpRequest(opaqueId, 2 * 1024));
            ctx.clientChannel.writeAndFlush(randomContent(1024, false));

            // await stream handler is ready and request full content
            var handler = ctx.awaitRestChannelAccepted(opaqueId);
            assertBusy(() -> assertEquals(1, handler.stream.queueSize()));

            // enable auto-read to receive channel close event
            handler.stream.channel().config().setAutoRead(true);

            // terminate connection and wait resources are released
            ctx.clientChannel.close();
            assertBusy(() -> assertEquals(0, handler.stream.queueSize()));
        }
    }

    // ensures that all queued chunks are released when server decides to close connection
    public void testServerCloseConnectionMidStream() throws Exception {
        try (var ctx = setupClientCtx()) {
            var opaqueId = opaqueId(0);

            // write half of http request
            ctx.clientChannel.write(httpRequest(opaqueId, 2 * 1024));
            ctx.clientChannel.writeAndFlush(randomContent(1024, false));

            // await stream handler is ready and request full content
            var handler = ctx.awaitRestChannelAccepted(opaqueId);
            assertBusy(() -> assertEquals(1, handler.stream.queueSize()));

            // terminate connection on server and wait resources are released
            handler.channel.request().getHttpChannel().close();
            assertBusy(() -> assertEquals(0, handler.stream.queueSize()));
        }
    }

    // ensure that client's socket buffers data when server is not consuming data
    public void testClientBackpressure() throws Exception {
        try (var ctx = setupClientCtx()) {
            var opaqueId = opaqueId(0);
            var payloadSize = MBytes(50);
            ctx.clientChannel.writeAndFlush(httpRequest(opaqueId, payloadSize));
            for (int i = 0; i < 5; i++) {
                ctx.clientChannel.writeAndFlush(randomContent(MBytes(10), false));
            }
            assertFalse(
                "should not flush last content immediately",
                ctx.clientChannel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).isDone()
            );

            var handler = ctx.awaitRestChannelAccepted(opaqueId);

            // Read buffers for socket and channel usually within few MBytes range all together.
            // This test assumes that buffers will not exceed 10 MBytes, in other words there should
            // be less than 10 MBytes in fly between http client's socket and rest handler. This
            // loop ensures that reading 10 MBytes of content on server side should free almost
            // same size in client's channel write buffer.
            for (int mb = 0; mb <= 50; mb += 10) {
                var minBufSize = payloadSize - MBytes(10 + mb);
                var maxBufSize = payloadSize - MBytes(mb);
                // it is hard to tell that client's channel is no logger flushing data
                // it might take a few busy-iterations before channel buffer flush to kernel
                // and bytesBeforeWritable will stop changing
                assertBusy(() -> {
                    var bufSize = ctx.clientChannel.bytesBeforeWritable();
                    assertTrue(
                        "client's channel buffer should be in range [" + minBufSize + "," + maxBufSize + "], got " + bufSize,
                        bufSize >= minBufSize && bufSize <= maxBufSize
                    );
                });
                handler.consumeBytes(MBytes(10));
            }
            assertTrue(handler.stream.hasLast());
        }
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

        ServerRequestHandler(String opaqueId, Netty4HttpRequestBodyStream stream) {
            this.opaqueId = opaqueId;
            this.stream = stream;
        }

        @Override
        public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
            recvChunks.add(new Chunk(chunk, isLast));
        }

        @Override
        public void accept(RestChannel channel) throws Exception {
            this.channel = channel;
            channelAccepted.onResponse(null);
        }

        void sendResponse(RestResponse response) {
            channel.sendResponse(response);
        }

        void consumeBytes(int bytes) {
            if (recvLast) {
                return;
            }
            while (bytes > 0) {
                stream.next();
                var recvChunk = safePoll(recvChunks);
                bytes -= recvChunk.chunk.length();
                recvChunk.chunk.close();
                if (recvChunk.isLast) {
                    recvLast = true;
                    break;
                }
            }
        }

        Future<?> onChannelThread(Callable<?> task) {
            return this.stream.channel().eventLoop().submit(task);
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
