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
package org.elasticsearch.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CopyBytesSocketChannelTests extends ESTestCase {

    private final AtomicReference<CopyBytesSocketChannel> accepted = new AtomicReference<>();
    private NioEventLoopGroup eventLoopGroup;
    private InetSocketAddress serverAddress;
    private Channel serverChannel;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        eventLoopGroup = new NioEventLoopGroup(1);
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.channel(CopyBytesServerSocketChannel.class);
        serverBootstrap.group(eventLoopGroup);
        serverBootstrap.childHandler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                accepted.set((CopyBytesSocketChannel) ch);
            }
        });

        ChannelFuture bindFuture = serverBootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0));
        bindFuture.await(10, TimeUnit.SECONDS);
        serverAddress = (InetSocketAddress) bindFuture.channel().localAddress();
        assertTrue(bindFuture.isSuccess());
        serverChannel = bindFuture.channel();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        serverChannel.close().await(10, TimeUnit.SECONDS);
        eventLoopGroup.shutdownGracefully().await(15, TimeUnit.SECONDS);
    }

    public void testThing() throws Exception {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(VerifyingCopyChannel.class);
        bootstrap.handler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {

            }
        });

        ChannelFuture connectFuture = bootstrap.connect(serverAddress);
        connectFuture.await(10, TimeUnit.SECONDS);
        assertTrue(connectFuture.isSuccess());
        CopyBytesSocketChannel copyChannel = (CopyBytesSocketChannel) connectFuture.channel();
        try {
            assertBusy(() -> assertNotNull(accepted.get()));
        } finally {
            copyChannel.close();
        }
    }

    public static class VerifyingCopyChannel extends CopyBytesSocketChannel {

        public VerifyingCopyChannel() {
            super();
        }

        @Override
        protected int writeToSocketChannel(SocketChannel socketChannel, ByteBuffer ioBuffer) throws IOException {
            return 0;
        }

        @Override
        protected int readFromSocketChannel(SocketChannel socketChannel, ByteBuffer ioBuffer) throws IOException {
            return -1;
        }
    }
}
