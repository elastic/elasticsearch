/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CopyBytesSocketChannelTests extends ESTestCase {

    private final UnpooledByteBufAllocator alloc = new UnpooledByteBufAllocator(false);
    private final AtomicReference<CopyBytesSocketChannel> accepted = new AtomicReference<>();
    private final AtomicInteger serverBytesReceived = new AtomicInteger();
    private final AtomicInteger clientBytesReceived = new AtomicInteger();
    private final ConcurrentLinkedQueue<ByteBuf> serverReceived = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<ByteBuf> clientReceived = new ConcurrentLinkedQueue<>();
    private NioEventLoopGroup eventLoopGroup;
    private InetSocketAddress serverAddress;
    private Channel serverChannel;

    @Override
    @SuppressForbidden(reason = "calls getLocalHost")
    public void setUp() throws Exception {
        super.setUp();
        eventLoopGroup = new NioEventLoopGroup(1);
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.channel(CopyBytesServerSocketChannel.class);
        serverBootstrap.group(eventLoopGroup);
        serverBootstrap.option(ChannelOption.ALLOCATOR, alloc);
        serverBootstrap.childOption(ChannelOption.ALLOCATOR, alloc);
        serverBootstrap.childHandler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                accepted.set((CopyBytesSocketChannel) ch);
                ch.pipeline().addLast(new SimpleChannelInboundHandler<>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                        ByteBuf buffer = (ByteBuf) msg;
                        serverBytesReceived.addAndGet(buffer.readableBytes());
                        serverReceived.add(buffer.retain());
                    }
                });
            }
        });

        ChannelFuture bindFuture = serverBootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0));
        assertTrue(bindFuture.await(10, TimeUnit.SECONDS));
        serverAddress = (InetSocketAddress) bindFuture.channel().localAddress();
        bindFuture.isSuccess();
        serverChannel = bindFuture.channel();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        try {
            assertTrue(serverChannel.close().await(10, TimeUnit.SECONDS));
        } finally {
            eventLoopGroup.shutdownGracefully().await(10, TimeUnit.SECONDS);
        }
    }

    public void testSendAndReceive() throws Exception {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(VerifyingCopyChannel.class);
        bootstrap.option(ChannelOption.ALLOCATOR, alloc);
        bootstrap.handler(new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.pipeline().addLast(new SimpleChannelInboundHandler<>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                        ByteBuf buffer = (ByteBuf) msg;
                        clientBytesReceived.addAndGet(buffer.readableBytes());
                        clientReceived.add(buffer.retain());
                    }
                });
            }
        });

        ChannelFuture connectFuture = bootstrap.connect(serverAddress);
        connectFuture.await(10, TimeUnit.SECONDS);
        assertTrue(connectFuture.isSuccess());
        CopyBytesSocketChannel copyChannel = (CopyBytesSocketChannel) connectFuture.channel();
        ByteBuf clientData = generateData();
        ByteBuf serverData = generateData();

        try {
            assertBusy(() -> assertNotNull(accepted.get()));
            int clientBytesToWrite = clientData.readableBytes();
            ChannelFuture clientWriteFuture = copyChannel.writeAndFlush(clientData.retainedSlice());
            clientWriteFuture.await(10, TimeUnit.SECONDS);
            assertBusy(() -> assertEquals(clientBytesToWrite, serverBytesReceived.get()));

            int serverBytesToWrite = serverData.readableBytes();
            ChannelFuture serverWriteFuture = accepted.get().writeAndFlush(serverData.retainedSlice());
            assertTrue(serverWriteFuture.await(10, TimeUnit.SECONDS));
            assertBusy(() -> assertEquals(serverBytesToWrite, clientBytesReceived.get()));

            ByteBuf compositeServerReceived = Unpooled.wrappedBuffer(serverReceived.toArray(new ByteBuf[0]));
            assertEquals(clientData, compositeServerReceived);
            ByteBuf compositeClientReceived = Unpooled.wrappedBuffer(clientReceived.toArray(new ByteBuf[0]));
            assertEquals(serverData, compositeClientReceived);
        } finally {
            clientData.release();
            serverData.release();
            serverReceived.forEach(ByteBuf::release);
            clientReceived.forEach(ByteBuf::release);
            assertTrue(copyChannel.close().await(10, TimeUnit.SECONDS));
        }
    }

    private ByteBuf generateData() {
        return Unpooled.wrappedBuffer(randomAlphaOfLength(randomIntBetween(1 << 22, 1 << 23)).getBytes(StandardCharsets.UTF_8));
    }

    public static class VerifyingCopyChannel extends CopyBytesSocketChannel {

        public VerifyingCopyChannel() {
            super();
        }

        @Override
        protected int writeToSocketChannel(SocketChannel socketChannel, ByteBuffer ioBuffer) throws IOException {
            assertTrue("IO Buffer must be a direct byte buffer", ioBuffer.isDirect());
            int remaining = ioBuffer.remaining();
            int originalLimit = ioBuffer.limit();
            // If greater than a KB, possibly invoke a partial write.
            if (remaining > 1024) {
                if (randomBoolean()) {
                    int bytes = randomIntBetween(remaining / 2, remaining);
                    ioBuffer.limit(ioBuffer.position() + bytes);
                }
            }
            int written = socketChannel.write(ioBuffer);
            ioBuffer.limit(originalLimit);
            return written;
        }

        @Override
        protected int readFromSocketChannel(SocketChannel socketChannel, ByteBuffer ioBuffer) throws IOException {
            assertTrue("IO Buffer must be a direct byte buffer", ioBuffer.isDirect());
            return socketChannel.read(ioBuffer);
        }
    }
}
