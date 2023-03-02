/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.ExceptionsHelper;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Emulates a <a href="https://en.wikipedia.org/wiki/Proxy_server#Web_proxy_servers">Web Proxy Server</a>.
 * @see <a href="https://github.com/netty/netty/tree/4.1/example/src/main/java/io/netty/example/proxy">Netty Proxy Example</a>
 */
class WebProxyServer implements Closeable {

    private static final Set<String> BLOCKED_HEADERS = Stream.of("Host", "Proxy-Connection", "Proxy-Authenticate")
        .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));

    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private final ChannelFuture channelFuture;
    private final InetSocketAddress socketAddress;

    WebProxyServer(String upstreamHost, int upstreamPort) {
        try {
            channelFuture = new ServerBootstrap().group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast(new HttpServerCodec())
                            .addLast(new HttpObjectAggregator(Integer.MAX_VALUE))
                            .addLast(new ProxyFrontendHandler(upstreamHost, upstreamPort));
                    }
                })
                .bind(0)
                .sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        socketAddress = (InetSocketAddress) channelFuture.channel().localAddress();
    }

    int getPort() {
        return socketAddress.getPort();
    }

    String getHost() {
        return socketAddress.getHostString();
    }

    @Override
    public void close() throws IOException {
        channelFuture.channel().close().awaitUninterruptibly();
        bossGroup.shutdownGracefully().awaitUninterruptibly();
        workerGroup.shutdownGracefully().awaitUninterruptibly();
    }

    private static class ProxyFrontendHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private final String upstreamHost;
        private final int upstreamPort;
        private final String upstreamHostPort;

        private Channel outboundChannel;

        private ProxyFrontendHandler(String upstreamHost, int upstreamPort) {
            this.upstreamHost = upstreamHost;
            this.upstreamPort = upstreamPort;
            upstreamHostPort = "http://" + upstreamHost + ":" + upstreamPort;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            Channel inboundChannel = ctx.channel();
            outboundChannel = new Bootstrap().group(inboundChannel.eventLoop())
                .channel(inboundChannel.getClass())
                .option(ChannelOption.AUTO_READ, false) // Have to manually read data if we reuse the event loop
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast(new HttpClientCodec())
                            .addLast(new HttpObjectAggregator(Integer.MAX_VALUE))
                            .addLast(new ProxyBackendHandler(inboundChannel));
                    }
                })
                .connect(upstreamHost, upstreamPort)
                .addListener(future -> {
                    if (future.isSuccess()) {
                        inboundChannel.read();
                    } else {
                        inboundChannel.close();
                    }
                })
                .channel();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
            var upstreamHeaders = new DefaultHttpHeaders();
            for (var header : req.headers()) {
                if (BLOCKED_HEADERS.contains(header.getKey()) == false) {
                    upstreamHeaders.set(header.getKey(), header.getValue());
                }
            }
            upstreamHeaders.set("X-Via", "test-web-proxy-server");
            outboundChannel.writeAndFlush(
                new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1,
                    req.method(),
                    req.uri().replace(upstreamHostPort, ""),
                    req.content().retain(),
                    upstreamHeaders,
                    req.trailingHeaders()
                )
            ).addListener(channelFutureListener(ctx));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR));
            ctx.close();
        }
    }

    private static class ProxyBackendHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

        private final Channel inboundChannel;

        private ProxyBackendHandler(Channel inboundChannel) {
            this.inboundChannel = inboundChannel;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.read();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) throws Exception {
            // Proxy the response from the upstream server
            inboundChannel.writeAndFlush(response.retain()).addListener(channelFutureListener(ctx));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ExceptionsHelper.maybeDieOnAnotherThread(cause);
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR));
            ctx.close();
        }
    }

    private static ChannelFutureListener channelFutureListener(ChannelHandlerContext ctx) {
        return future -> {
            if (future.isSuccess()) {
                ctx.channel().read();
            } else {
                future.channel().close();
            }
        };
    }
}
