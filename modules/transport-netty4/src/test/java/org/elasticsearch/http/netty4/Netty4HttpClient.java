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
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.collect.Tuple;
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

    public Collection<FullHttpResponse> get(SocketAddress remoteAddress, String... uris) throws InterruptedException {
        Collection<HttpRequest> requests = new ArrayList<>(uris.length);
        for (int i = 0; i < uris.length; i++) {
            final HttpRequest httpRequest = new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, uris[i]);
            httpRequest.headers().add(HOST, "localhost");
            httpRequest.headers().add("X-Opaque-ID", String.valueOf(i));
            requests.add(httpRequest);
        }
        return sendRequests(remoteAddress, requests);
    }

    public final Collection<FullHttpResponse> post(SocketAddress remoteAddress, List<Tuple<String, CharSequence>> urisAndBodies)
        throws InterruptedException {
        return processRequestsWithBody(HttpMethod.POST, remoteAddress, urisAndBodies);
    }

    public final FullHttpResponse post(SocketAddress remoteAddress, FullHttpRequest httpRequest) throws InterruptedException {
        Collection<FullHttpResponse> responses = sendRequests(remoteAddress, Collections.singleton(httpRequest));
        assert responses.size() == 1 : "expected 1 and only 1 http response";
        return responses.iterator().next();
    }

    public final Collection<FullHttpResponse> put(SocketAddress remoteAddress, List<Tuple<String, CharSequence>> urisAndBodies)
        throws InterruptedException {
        return processRequestsWithBody(HttpMethod.PUT, remoteAddress, urisAndBodies);
    }

    private Collection<FullHttpResponse> processRequestsWithBody(HttpMethod method, SocketAddress remoteAddress, List<Tuple<String,
        CharSequence>> urisAndBodies) throws InterruptedException {
        Collection<HttpRequest> requests = new ArrayList<>(urisAndBodies.size());
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

    private synchronized Collection<FullHttpResponse> sendRequests(
        final SocketAddress remoteAddress,
        final Collection<HttpRequest> requests) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(requests.size());
        final Collection<FullHttpResponse> content = Collections.synchronizedList(new ArrayList<>(requests.size()));

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
        protected void initChannel(SocketChannel ch) throws Exception {
            final int maxContentLength = new ByteSizeValue(100, ByteSizeUnit.MB).bytesAsInt();
            ch.pipeline().addLast(new HttpResponseDecoder());
            ch.pipeline().addLast(new HttpRequestEncoder());
            ch.pipeline().addLast(new HttpObjectAggregator(maxContentLength));
            ch.pipeline().addLast(new SimpleChannelInboundHandler<HttpObject>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
                    final FullHttpResponse response = (FullHttpResponse) msg;
                    content.add(response.copy());
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
