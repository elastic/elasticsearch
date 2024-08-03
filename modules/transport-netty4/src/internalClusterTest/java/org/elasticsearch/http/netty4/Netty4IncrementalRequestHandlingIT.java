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

import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpChannel;
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
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
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
            ctx.clientChannel.writeAndFlush(fullRequest(Unpooled.EMPTY_BUFFER));
            var handler = ctx.awaitRestChannelAccepted();
            handler.stream.requestBytes(1);
            var recvChunk = safePoll(handler.recvChunks);
            assertTrue(recvChunk.isLast);
            assertEquals(0, recvChunk.chunk.length());
            recvChunk.chunk.close();
        }
    }

    // ensures content integrity, no loses and re-order
    public void testReceiveAllChunks() throws Exception {
        try (var ctx = setupClientCtx()) {
            var dataSize = randomIntBetween(1024, 10 * 1024 * 1024);
            var sendData = Unpooled.wrappedBuffer(randomByteArrayOfLength(dataSize));
            sendData.retain();
            ctx.clientChannel.writeAndFlush(fullRequest(sendData));

            var handler = ctx.awaitRestChannelAccepted();

            var gotAllChunks = false;
            var recvData = Unpooled.buffer(dataSize);
            while (gotAllChunks == false) {
                handler.stream.requestBytes(randomIntBetween(1024, 64 * 1024));
                var recvChunk = safePoll(handler.recvChunks);
                try (recvChunk.chunk) {
                    if (recvChunk.isLast) {
                        gotAllChunks = true;
                    }
                    recvData.writeBytes(BytesReference.toBytes(recvChunk.chunk));
                }
            }
            assertEquals(sendData, recvData);
        }
    }

    // ensure that client's socket is no longer writable if server not consuming data
    public void testClientBackpressure() throws Exception {
        try (var ctx = setupClientCtx()) {

            // a meaningful write payload that is larger than socket and channel buffers
            final var writeChunkSize = 16 * 1024;
            final var totalWriteChunks = randomIntBetween(1024, 2048); // 16-32MB
            final var totalWriteSize = writeChunkSize * totalWriteChunks;
            var writtenChunks = 0;

            ctx.clientChannel.writeAndFlush(notFullRequest(totalWriteSize));
            var handler = ctx.awaitRestChannelAccepted();

            // write chunks until channel is no longer writable
            while (writtenChunks <= totalWriteChunks && ctx.clientChannel.isWritable()) {
                ctx.clientChannel.writeAndFlush(randomContent(writeChunkSize, false));
                writtenChunks += 1;
            }
            assertTrue("should not be able to write all chunks", writtenChunks < totalWriteChunks);

            // read all written chunks and check client socket is writable again
            var totalReadSize = 0;
            var mustReadBytes = writtenChunks * writeChunkSize;
            while (mustReadBytes > 0) {
                handler.stream.requestBytes(writeChunkSize);
                var recvChunk = safePoll(handler.recvChunks);
                mustReadBytes -= recvChunk.chunk.length();
                totalReadSize += recvChunk.chunk.length();
                recvChunk.chunk.close();
            }

            assertBusy(() -> assertTrue("channel must be writable again", ctx.clientChannel.isWritable()));

            // write remaining chunks
            while (writtenChunks < totalWriteChunks) { // leave 1 for LastHttpContent
                writtenChunks += 1;
                ctx.clientChannel.writeAndFlush(randomContent(writeChunkSize, false));
            }
            ctx.clientChannel.writeAndFlush(randomContent(writeChunkSize, true));

            // consume remaining chunks
            var allConsumed = false;
            while (allConsumed == false) {
                handler.stream.requestBytes(writeChunkSize);
                var recvChunk = safePoll(handler.recvChunks);
                totalReadSize += recvChunk.chunk.length();
                allConsumed = recvChunk.isLast;
                recvChunk.chunk.close();
            }
            assertEquals("did not received all bytes", totalWriteSize, totalReadSize);

            // send OK response back to wrap it up
            handler.channel.sendResponse(new RestResponse(RestStatus.OK, ""));
            var resp = (FullHttpResponse) safePoll(ctx.clientRespQueue);
            assertEquals(200, resp.status().code());
            resp.release();
        }
    }

    static <T> T safePoll(BlockingDeque<T> queue) {
        try {
            var t = queue.poll(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            assertNotNull("queue is empty", t);
            return t;
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    static FullHttpRequest fullRequest(ByteBuf content) {
        var req = new DefaultFullHttpRequest(HTTP_1_1, POST, SingleRequestHandlerPlugin.ROUTE, Unpooled.wrappedBuffer(content));
        req.headers().add(CONTENT_LENGTH, content.readableBytes());
        req.headers().add(CONTENT_TYPE, APPLICATION_JSON);
        return req;
    }

    static HttpRequest notFullRequest(int contentLength) {
        var req = new DefaultHttpRequest(HTTP_1_1, POST, SingleRequestHandlerPlugin.ROUTE);
        req.headers().add(CONTENT_LENGTH, contentLength);
        req.headers().add(CONTENT_TYPE, APPLICATION_JSON);
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

    record Ctx(String nodeName, Bootstrap clientBootstrap, Channel clientChannel, BlockingDeque<Object> clientRespQueue)
        implements
            AutoCloseable {

        @Override
        public void close() throws Exception {
            safeGet(clientChannel.close());
            safeGet(clientBootstrap.config().group().shutdownGracefully());
            clientRespQueue.forEach(o -> { if (o instanceof FullHttpResponse resp) resp.release(); });
            for (var handler : SingleRequestHandlerPlugin.handlers) {
                handler.recvChunks.forEach(c -> c.chunk.close());
                handler.httpChannel.close();
            }
            SingleRequestHandlerPlugin.handlers.clear();
        }

        ServerRequestHandler awaitRestChannelAccepted() throws Exception {
            var handlers = SingleRequestHandlerPlugin.handlers;
            assertBusy(() -> { assertEquals(1, handlers.size()); });
            var handler = handlers.get(0);
            safeAwait(handler.channelAccepted);
            return handler;
        }
    }

    Ctx setupClientCtx() throws Exception {
        var nodeName = internalCluster().getRandomNodeName();
        var clientRespQueue = new LinkedBlockingDeque<>(16);
        var bootstrap = bootstrapClient(nodeName, clientRespQueue);
        var channel = bootstrap.connect().sync().channel();
        return new Ctx(nodeName, bootstrap, channel, clientRespQueue);
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
        return CollectionUtils.concatLists(List.of(SingleRequestHandlerPlugin.class), super.nodePlugins());
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    static class ServerRequestHandler implements BaseRestHandler.RequestBodyChunkConsumer {
        SubscribableListener<Void> channelAccepted = new SubscribableListener<>();
        RestChannel channel;
        HttpChannel httpChannel;
        HttpBody.Stream stream;
        BlockingDeque<Chunk> recvChunks = new LinkedBlockingDeque<>();

        record Chunk(ReleasableBytesReference chunk, boolean isLast) {}

        @Override
        public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
            recvChunks.add(new Chunk(chunk, isLast));
        }

        @Override
        public void accept(RestChannel channel) throws Exception {
            this.channel = channel;
            this.httpChannel = channel.request().getHttpChannel();
            this.stream = channel.request().contentStream();
            channelAccepted.onResponse(null);
        }
    }

    public static class SingleRequestHandlerPlugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/request-stream";

        static final List<ServerRequestHandler> handlers = Collections.synchronizedList(new ArrayList<>());

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
                    var handler = new ServerRequestHandler();
                    handlers.add(handler);
                    return handler;
                }
            });
        }
    }

}
