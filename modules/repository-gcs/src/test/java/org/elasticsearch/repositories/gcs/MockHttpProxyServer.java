/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Supplier;

/**
 * A mock HTTP Proxy server for testing of support of HTTP proxies in various SDKs
 */
class MockHttpProxyServer implements Closeable {

    private final EventLoopGroup group = new NioEventLoopGroup(1);

    private ChannelFuture channelFuture;
    private InetSocketAddress socketAddress;

    MockHttpProxyServer handler(Supplier<SimpleChannelInboundHandler<FullHttpRequest>> handler) {
        try {
            channelFuture = new ServerBootstrap().group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast(new HttpServerCodec())
                            .addLast(new HttpObjectAggregator(Integer.MAX_VALUE))
                            .addLast(handler.get());
                    }
                })
                .bind(0)
                .sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        socketAddress = (InetSocketAddress) channelFuture.channel().localAddress();
        return this;
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
        group.shutdownGracefully();
    }
}
