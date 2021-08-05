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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.NettyAllocator;

import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.fail;

/**
 * Tiny helper to send http requests over netty.
 */
class Netty4HttpClient implements Closeable {

    static Collection<String> returnHttpResponseBodies(Collection<FullHttpResponse> responses) {
        List<String> list = new ArrayList<>(responses.size());
        for (FullHttpResponse response : responses) {
            list.add(response.content().toString(StandardCharsets.UTF_8));
        }
        return list;
    }

    static Collection<String> returnOpaqueIds(Collection<FullHttpResponse> responses) {
        List<String> list = new ArrayList<>(responses.size());
        for (HttpResponse response : responses) {
            list.add(response.headers().get(Task.X_OPAQUE_ID));
        }
        return list;
    }

    private final Bootstrap clientBootstrap;

    Netty4HttpClient() {
        clientBootstrap = new Bootstrap()
            .channel(NettyAllocator.getChannelType())
            .option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator())
            .group(new NioEventLoopGroup(1));
    }

    public List<FullHttpResponse> get(SocketAddress remoteAddress, String... uris) throws InterruptedException {
        List<HttpRequest> requests = new ArrayList<>(uris.length);
        for (int i = 0; i < uris.length; i++) {
            final HttpRequest httpRequest = new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, uris[i]);
            httpRequest.headers().add(HOST, "localhost");
            httpRequest.headers().add("X-Opaque-ID", String.valueOf(i));
            httpRequest.headers().add("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
            requests.add(httpRequest);
        }
        return sendRequests(remoteAddress, requests);
    }

    public final Collection<FullHttpResponse> post(SocketAddress remoteAddress, List<Tuple<String, CharSequence>> urisAndBodies)
        throws InterruptedException {
        return processRequestsWithBody(HttpMethod.POST, remoteAddress, urisAndBodies);
    }

    public final FullHttpResponse send(SocketAddress remoteAddress, FullHttpRequest httpRequest) throws InterruptedException {
        List<FullHttpResponse> responses = sendRequests(remoteAddress, Collections.singleton(httpRequest));
        assert responses.size() == 1 : "expected 1 and only 1 http response";
        return responses.get(0);
    }

    public final Collection<FullHttpResponse> put(SocketAddress remoteAddress, List<Tuple<String, CharSequence>> urisAndBodies)
        throws InterruptedException {
        return processRequestsWithBody(HttpMethod.PUT, remoteAddress, urisAndBodies);
    }

    private List<FullHttpResponse> processRequestsWithBody(HttpMethod method, SocketAddress remoteAddress, List<Tuple<String,
        CharSequence>> urisAndBodies) throws InterruptedException {
        List<HttpRequest> requests = new ArrayList<>(urisAndBodies.size());
        for (Tuple<String, CharSequence> uriAndBody : urisAndBodies) {
            ByteBuf content = Unpooled.copiedBuffer(uriAndBody.v2(), StandardCharsets.UTF_8);
            HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uriAndBody.v1(), content);
            request.headers().add(HttpHeaderNames.HOST, "localhost");
            request.headers().add(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
            request.headers().add(HttpHeaderNames.CONTENT_TYPE, "application/json");
            requests.add(request);
        }
        return sendRequests(remoteAddress, requests);
    }

    private synchronized List<FullHttpResponse> sendRequests(
        final SocketAddress remoteAddress,
        final Collection<HttpRequest> requests) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(requests.size());
        final List<FullHttpResponse> content = Collections.synchronizedList(new ArrayList<>(requests.size()));

        clientBootstrap.handler(new CountDownLatchHandler(latch, content));

        ChannelFuture channelFuture = null;
        try {
            channelFuture = clientBootstrap.connect(remoteAddress);
            channelFuture.sync();

            for (HttpRequest request : requests) {
                channelFuture.channel().writeAndFlush(request);
            }
            if (latch.await(30L, TimeUnit.SECONDS) == false) {
                fail("Failed to get all expected responses.");
            }

        } finally {
            if (channelFuture != null) {
                channelFuture.channel().close().sync();
            }
        }

        return content;
    }

    @Override
    public void close() {
        clientBootstrap.config().group().shutdownGracefully().awaitUninterruptibly();
    }

    /**
     * helper factory which adds returned data to a list and uses a count down latch to decide when done
     */
    private static class CountDownLatchHandler extends ChannelInitializer<SocketChannel> {

        private final CountDownLatch latch;
        private final Collection<FullHttpResponse> content;

        CountDownLatchHandler(final CountDownLatch latch, final Collection<FullHttpResponse> content) {
            this.latch = latch;
            this.content = content;
        }

        @Override
        protected void initChannel(SocketChannel ch) {
            final int maxContentLength = new ByteSizeValue(100, ByteSizeUnit.MB).bytesAsInt();
            ch.pipeline().addLast(new HttpResponseDecoder());
            ch.pipeline().addLast(new HttpRequestEncoder());
            ch.pipeline().addLast(new HttpContentDecompressor());
            ch.pipeline().addLast(new HttpObjectAggregator(maxContentLength));
            ch.pipeline().addLast(new SimpleChannelInboundHandler<HttpObject>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
                    final FullHttpResponse response = (FullHttpResponse) msg;
                    // We copy the buffer manually to avoid a huge allocation on a pooled allocator. We have
                    // a test that tracks huge allocations, so we want to avoid them in this test code.
                    ByteBuf newContent = Unpooled.copiedBuffer(((FullHttpResponse) msg).content());
                    content.add(response.replace(newContent));
                    latch.countDown();
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                    super.exceptionCaught(ctx, cause);
                    latch.countDown();
                }
            });
        }

    }

}
