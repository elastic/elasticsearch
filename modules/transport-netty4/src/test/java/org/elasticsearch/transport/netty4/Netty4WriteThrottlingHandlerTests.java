/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class Netty4WriteThrottlingHandlerTests extends ESTestCase {

    private SharedGroupFactory.SharedGroup transportGroup;

    @Before
    public void createGroup() {
        final SharedGroupFactory sharedGroupFactory = new SharedGroupFactory(Settings.EMPTY);
        transportGroup = sharedGroupFactory.getTransportGroup();
    }

    @After
    public void stopGroup() {
        transportGroup.shutdown();
    }

    public void testThrottlesLargeMessage() throws ExecutionException, InterruptedException {
        final List<ByteBuf> seen = new CopyOnWriteArrayList<>();
        final CapturingHandler capturingHandler = new CapturingHandler(seen);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            capturingHandler,
            new Netty4WriteThrottlingHandler(new ThreadContext(Settings.EMPTY))
        );
        // we assume that the channel outbound buffer is smaller than Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE
        final int writeableBytes = Math.toIntExact(embeddedChannel.bytesBeforeUnwritable());
        assertThat(writeableBytes, lessThan(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE));
        final int fullSizeChunks = randomIntBetween(2, 10);
        final int extraChunkSize = randomIntBetween(0, 10);
        final ByteBuf message = Unpooled.wrappedBuffer(
            randomByteArrayOfLength(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE * fullSizeChunks + extraChunkSize)
        );
        final ChannelPromise promise = embeddedChannel.newPromise();
        transportGroup.getLowLevelGroup().submit(() -> embeddedChannel.write(message, promise)).get();
        assertThat(seen, hasSize(1));
        assertEquals(message.slice(0, Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE), seen.get(0));
        assertFalse(promise.isDone());
        transportGroup.getLowLevelGroup().submit(embeddedChannel::flush).get();
        assertTrue(promise.isDone());
        assertThat(seen, hasSize(fullSizeChunks + (extraChunkSize == 0 ? 0 : 1)));
        assertTrue(capturingHandler.didWriteAfterThrottled);
        if (extraChunkSize != 0) {
            assertEquals(
                message.slice(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE * fullSizeChunks, extraChunkSize),
                seen.get(seen.size() - 1)
            );
        }
    }

    public void testPassesSmallMessageDirectly() throws ExecutionException, InterruptedException {
        final List<ByteBuf> seen = new CopyOnWriteArrayList<>();
        final CapturingHandler capturingHandler = new CapturingHandler(seen);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            capturingHandler,
            new Netty4WriteThrottlingHandler(new ThreadContext(Settings.EMPTY))
        );
        final int writeableBytes = Math.toIntExact(embeddedChannel.bytesBeforeUnwritable());
        assertThat(writeableBytes, lessThan(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE));
        final ByteBuf message = Unpooled.wrappedBuffer(
            randomByteArrayOfLength(randomIntBetween(0, Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE))
        );
        final ChannelPromise promise = embeddedChannel.newPromise();
        transportGroup.getLowLevelGroup().submit(() -> embeddedChannel.write(message, promise)).get();
        assertThat(seen, hasSize(1)); // first message should be passed through straight away
        assertSame(message, seen.get(0));
        assertFalse(promise.isDone());
        transportGroup.getLowLevelGroup().submit(embeddedChannel::flush).get();
        assertTrue(promise.isDone());
        assertThat(seen, hasSize(1));
        assertFalse(capturingHandler.didWriteAfterThrottled);
    }

    public void testThrottlesOnUnwritable() throws ExecutionException, InterruptedException {
        final List<ByteBuf> seen = new CopyOnWriteArrayList<>();
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            new CapturingHandler(seen),
            new Netty4WriteThrottlingHandler(new ThreadContext(Settings.EMPTY))
        );
        final int writeableBytes = Math.toIntExact(embeddedChannel.bytesBeforeUnwritable());
        assertThat(writeableBytes, lessThan(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE));
        final ByteBuf message = Unpooled.wrappedBuffer(randomByteArrayOfLength(writeableBytes + randomIntBetween(0, 10)));
        final ChannelPromise promise = embeddedChannel.newPromise();
        transportGroup.getLowLevelGroup().submit(() -> embeddedChannel.write(message, promise)).get();
        assertThat(seen, hasSize(1)); // first message should be passed through straight away
        assertSame(message, seen.get(0));
        assertFalse(promise.isDone());
        final ByteBuf messageToQueue = Unpooled.wrappedBuffer(
            randomByteArrayOfLength(randomIntBetween(0, Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE))
        );
        final ChannelPromise promiseForQueued = embeddedChannel.newPromise();
        transportGroup.getLowLevelGroup().submit(() -> embeddedChannel.write(messageToQueue, promiseForQueued)).get();
        assertThat(seen, hasSize(1));
        assertFalse(promiseForQueued.isDone());
        assertFalse(promise.isDone());
        transportGroup.getLowLevelGroup().submit(embeddedChannel::flush).get();
        assertTrue(promise.isDone());
        assertTrue(promiseForQueued.isDone());
    }

    private static class CapturingHandler extends ChannelOutboundHandlerAdapter {
        private final List<ByteBuf> seen;

        private boolean wasThrottled = false;

        private boolean didWriteAfterThrottled = false;

        CapturingHandler(List<ByteBuf> seen) {
            this.seen = seen;
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            assertTrue("should only write to writeable channel", ctx.channel().isWritable());
            assertThat(msg, instanceOf(ByteBuf.class));
            final ByteBuf buf = (ByteBuf) msg;
            assertThat(buf.readableBytes(), lessThanOrEqualTo(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE));
            seen.add(buf);
            if (wasThrottled) {
                didWriteAfterThrottled = true;
            }
            super.write(ctx, msg, promise);
            if (ctx.channel().isWritable() == false) {
                wasThrottled = true;
            }
        }
    }
}
