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

package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

public class NettyAdaptorTests extends ESTestCase {

    public void testBasicRead() {
        TenIntsToStringsHandler handler = new TenIntsToStringsHandler();
        NettyAdaptor nettyAdaptor = new NettyAdaptor(handler);
        ByteBuffer message = ByteBuffer.allocate(40);
        for (int i = 0; i < 10; ++i) {
            message.putInt(i);
        }
        message.flip();
        ByteBuffer[] buffers = {message};
        assertEquals(40, nettyAdaptor.read(buffers));
        assertEquals("0123456789", handler.result);
    }

    public void testBasicReadWithExcessData() {
        TenIntsToStringsHandler handler = new TenIntsToStringsHandler();
        NettyAdaptor nettyAdaptor = new NettyAdaptor(handler);
        ByteBuffer message = ByteBuffer.allocate(52);
        for (int i = 0; i < 13; ++i) {
            message.putInt(i);
        }
        message.flip();
        ByteBuffer[] buffers = {message};
        assertEquals(40, nettyAdaptor.read(buffers));
        assertEquals("0123456789", handler.result);
    }

    public void testUncaughtReadExceptionsBubbleUp() {
        NettyAdaptor nettyAdaptor = new NettyAdaptor(new TenIntsToStringsHandler());
        ByteBuffer message = ByteBuffer.allocate(40);
        for (int i = 0; i < 9; ++i) {
            message.putInt(i);
        }
        message.flip();
        ByteBuffer[] buffers = {message};
        expectThrows(IllegalStateException.class, () -> nettyAdaptor.read(buffers));
    }

    public void testWriteInsidePipelineIsCaptured() {
        TenIntsToStringsHandler tenIntsToStringsHandler = new TenIntsToStringsHandler();
        PromiseCheckerHandler promiseCheckerHandler = new PromiseCheckerHandler();
        NettyAdaptor nettyAdaptor = new NettyAdaptor(new CapitalizeWriteHandler(),
            promiseCheckerHandler,
            new WriteInMiddleHandler(),
            tenIntsToStringsHandler);
        byte[] bytes = "SHOULD_WRITE".getBytes(StandardCharsets.UTF_8);
        ByteBuffer message = ByteBuffer.wrap(bytes);
        ByteBuffer[] buffers = {message};
        assertNull(nettyAdaptor.pollOutboundOperation());
        nettyAdaptor.read(buffers);
        assertFalse(tenIntsToStringsHandler.wasCalled);
        FlushOperation flushOperation = nettyAdaptor.pollOutboundOperation();
        assertNotNull(flushOperation);
        assertEquals("FAILED", Unpooled.wrappedBuffer(flushOperation.getBuffersToWrite()).toString(StandardCharsets.UTF_8));
        assertFalse(promiseCheckerHandler.isCalled.get());
        flushOperation.getListener().accept(null, null);
        assertTrue(promiseCheckerHandler.isCalled.get());
    }

    public void testCloseListener() {
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        CloseChannelHandler handler = new CloseChannelHandler();
        NettyAdaptor nettyAdaptor = new NettyAdaptor(handler);
        byte[] bytes = "SHOULD_CLOSE".getBytes(StandardCharsets.UTF_8);
        ByteBuffer[] buffers = {ByteBuffer.wrap(bytes)};
        nettyAdaptor.addCloseListener((v, e) -> listenerCalled.set(true));
        assertFalse(listenerCalled.get());
        nettyAdaptor.read(buffers);
        assertTrue(listenerCalled.get());

    }

    private class TenIntsToStringsHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private String result;
        boolean wasCalled = false;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            wasCalled = true;
            if (msg.readableBytes() < 10 * 4) {
                throw new IllegalStateException("Must have ten ints");
            }
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < 10; ++i) {
                builder.append(msg.readInt());
            }
            result = builder.toString();
        }
    }

    private class WriteInMiddleHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf buffer = (ByteBuf) msg;
            String bufferString = buffer.toString(StandardCharsets.UTF_8);
            if (bufferString.equals("SHOULD_WRITE")) {
                ctx.writeAndFlush("Failed");
            } else {
                throw new IllegalArgumentException("Only accept SHOULD_WRITE message");
            }
        }
    }

    private class CapitalizeWriteHandler extends ChannelOutboundHandlerAdapter {

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            String string = (String) msg;
            assert string.equals("Failed") : "Should be the same was what we wrote.";
            super.write(ctx, Unpooled.wrappedBuffer(string.toUpperCase(Locale.ROOT).getBytes(StandardCharsets.UTF_8)), promise);
        }
    }

    private class PromiseCheckerHandler extends ChannelOutboundHandlerAdapter {

        private AtomicBoolean isCalled = new AtomicBoolean(false);

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            promise.addListener((f) -> isCalled.set(true));
            super.write(ctx, msg, promise);
        }
    }

    private class CloseChannelHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf buffer = (ByteBuf) msg;
            String bufferString = buffer.toString(StandardCharsets.UTF_8);
            if (bufferString.equals("SHOULD_CLOSE")) {
                ctx.close();
            } else {
                throw new IllegalArgumentException("Only accept SHOULD_CLOSE message");
            }
        }
    }
}
