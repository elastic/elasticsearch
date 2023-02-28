/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpVersion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Emulates a <a href="https://en.wikipedia.org/wiki/Proxy_server#Web_proxy_servers">Web Proxy Server</a>
 */
class WebProxyServer extends MockHttpProxyServer {

    private static final Logger logger = LogManager.getLogger(WebProxyServer.class);

    private static final Set<String> BLOCKED_HEADERS = Stream.of("Host", "Proxy-Connection", "Proxy-Authenticate")
        .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));

    private final EventLoopGroup group = new NioEventLoopGroup(1);

    WebProxyServer(String upstreamHost, int upstreamPort) {
        String upstreamHostPort = "http://" + upstreamHost + ":" + upstreamPort;
        handler(() -> new SimpleChannelInboundHandler<FullHttpRequest>() {

            private Channel outboundChannel;
            private Channel inboundChannel;

            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                inboundChannel = ctx.channel();
                outboundChannel = new Bootstrap().group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                .addLast(new HttpClientCodec())
                                .addLast(new HttpObjectAggregator(Integer.MAX_VALUE))
                                .addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) throws Exception {
                                        // Proxy the response from the upstream server
                                        inboundChannel.writeAndFlush(response.retain());
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        logger.error("Proxy client error", cause);
                                        ctx.close();
                                    }
                                });
                        }
                    })
                    .connect(upstreamHost, upstreamPort)
                    .sync()
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
                );
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                logger.error("Proxy server error", cause);
                ctx.close();
            }
        });
    }

    @Override
    public void close() throws IOException {
        group.shutdownGracefully();
        super.close();
    }
}
