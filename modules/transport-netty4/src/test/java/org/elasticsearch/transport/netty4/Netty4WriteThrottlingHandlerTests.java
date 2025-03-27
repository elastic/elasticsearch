/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.network.ThreadWatchdog;
import org.elasticsearch.common.network.ThreadWatchdogHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transports;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;

public class Netty4WriteThrottlingHandlerTests extends ESTestCase {

    private ThreadWatchdog threadWatchdog = new ThreadWatchdog();

    @Before
    public void setFakeThreadName() {
        // These tests interact with EmbeddedChannel instances directly on the test thread, so we rename it temporarily to satisfy checks
        // that we're running on a transport thread
        Thread.currentThread().setName(Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX + Thread.currentThread().getName());
    }

    @After
    public void resetThreadName() {
        final var threadName = Thread.currentThread().getName();
        assertThat(threadName, startsWith(Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX));
        Thread.currentThread().setName(threadName.substring(Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX.length()));
    }

    public void testThrottlesLargeMessage() {
        final List<ByteBuf> seen = new CopyOnWriteArrayList<>();
        final CapturingHandler capturingHandler = new CapturingHandler(seen);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            capturingHandler,
            new Netty4WriteThrottlingHandler(new ThreadContext(Settings.EMPTY), threadWatchdog.getActivityTrackerForCurrentThread())
        );
        // we assume that the channel outbound buffer is smaller than Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE
        final int writeableBytes = Math.toIntExact(embeddedChannel.bytesBeforeUnwritable());
        assertThat(writeableBytes, lessThan(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE));
        final int fullSizeChunks = randomIntBetween(2, 10);
        final int extraChunkSize = randomIntBetween(0, 10);
        final byte[] messageBytes = randomByteArrayOfLength(
            Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE * fullSizeChunks + extraChunkSize
        );
        final Object message = wrapAsNettyOrEsBuffer(messageBytes);
        final ChannelPromise promise = embeddedChannel.newPromise();
        embeddedChannel.write(message, promise);
        assertThat(seen, hasSize(1));
        assertSliceEquals(seen.get(0), message, 0, Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE);
        assertFalse(promise.isDone());
        embeddedChannel.flush();
        assertTrue(promise.isDone());
        assertThat(seen, hasSize(fullSizeChunks + (extraChunkSize == 0 ? 0 : 1)));
        assertTrue(capturingHandler.didWriteAfterThrottled);
        if (extraChunkSize != 0) {
            assertSliceEquals(
                seen.get(seen.size() - 1),
                message,
                Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE * fullSizeChunks,
                extraChunkSize
            );
        }
    }

    public void testThrottleLargeCompositeMessage() {
        final List<ByteBuf> seen = new CopyOnWriteArrayList<>();
        final CapturingHandler capturingHandler = new CapturingHandler(seen);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            capturingHandler,
            new Netty4WriteThrottlingHandler(new ThreadContext(Settings.EMPTY), threadWatchdog.getActivityTrackerForCurrentThread())
        );
        // we assume that the channel outbound buffer is smaller than Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE
        final int writeableBytes = Math.toIntExact(embeddedChannel.bytesBeforeUnwritable());
        assertThat(writeableBytes, lessThan(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE));
        final int fullSizeChunks = randomIntBetween(2, 10);
        final int extraChunkSize = randomIntBetween(0, 10);
        final byte[] messageBytes = randomByteArrayOfLength(
            Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE * fullSizeChunks + extraChunkSize
        );
        int splitOffset = randomIntBetween(0, messageBytes.length);
        int lastChunkSizeOfTheFirstSplit = splitOffset % Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE;
        final BytesReference message = CompositeBytesReference.of(
            new BytesArray(messageBytes, 0, splitOffset),
            new BytesArray(messageBytes, splitOffset, messageBytes.length - splitOffset)
        );
        final ChannelPromise promise = embeddedChannel.newPromise();
        embeddedChannel.write(message, promise);
        assertThat(seen, hasSize(oneOf(1, 2)));
        assertSliceEquals(seen.get(0), message, 0, seen.get(0).readableBytes());
        assertFalse(promise.isDone());
        embeddedChannel.flush();
        assertTrue(promise.isDone());
        // If the extra chunk size is greater than the last chunk size for the first half of the split, it means we will need to send
        // (extraChunkSize - lastChunkSizeOfTheFirstSplit) bytes as the very last chunk of the entire message.
        assertThat(seen, hasSize(oneOf(fullSizeChunks, fullSizeChunks + 1 + (extraChunkSize > lastChunkSizeOfTheFirstSplit ? 1 : 0))));
        assertTrue(capturingHandler.didWriteAfterThrottled);
        assertBufferEquals(Unpooled.compositeBuffer().addComponents(true, seen), message);
    }

    public void testPassesSmallMessageDirectly() {
        final List<ByteBuf> seen = new CopyOnWriteArrayList<>();
        final CapturingHandler capturingHandler = new CapturingHandler(seen);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            capturingHandler,
            new Netty4WriteThrottlingHandler(new ThreadContext(Settings.EMPTY), threadWatchdog.getActivityTrackerForCurrentThread())
        );
        final int writeableBytes = Math.toIntExact(embeddedChannel.bytesBeforeUnwritable());
        assertThat(writeableBytes, lessThan(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE));
        final byte[] messageBytes = randomByteArrayOfLength(randomIntBetween(0, Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE));
        final Object message = wrapAsNettyOrEsBuffer(messageBytes);
        final ChannelPromise promise = embeddedChannel.newPromise();
        embeddedChannel.write(message, promise);
        assertThat(seen, hasSize(1)); // first message should be passed through straight away
        assertBufferEquals(seen.get(0), message);
        assertFalse(promise.isDone());
        embeddedChannel.flush();
        assertTrue(promise.isDone());
        assertThat(seen, hasSize(1));
        assertFalse(capturingHandler.didWriteAfterThrottled);
    }

    public void testThrottlesOnUnwritable() {
        final List<ByteBuf> seen = new CopyOnWriteArrayList<>();
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            new CapturingHandler(seen),
            new Netty4WriteThrottlingHandler(new ThreadContext(Settings.EMPTY), threadWatchdog.getActivityTrackerForCurrentThread())
        );
        final int writeableBytes = Math.toIntExact(embeddedChannel.bytesBeforeUnwritable());
        assertThat(writeableBytes, lessThan(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE));
        final byte[] messageBytes = randomByteArrayOfLength(writeableBytes + randomIntBetween(0, 10));
        final Object message = wrapAsNettyOrEsBuffer(messageBytes);
        final ChannelPromise promise = embeddedChannel.newPromise();
        embeddedChannel.write(message, promise);
        assertThat(seen, hasSize(1)); // first message should be passed through straight away
        assertBufferEquals(seen.get(0), message);
        assertFalse(promise.isDone());
        final Object messageToQueue = wrapAsNettyOrEsBuffer(
            randomByteArrayOfLength(randomIntBetween(0, Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE))
        );
        final ChannelPromise promiseForQueued = embeddedChannel.newPromise();
        embeddedChannel.write(messageToQueue, promiseForQueued);
        assertThat(seen, hasSize(1));
        assertFalse(promiseForQueued.isDone());
        assertFalse(promise.isDone());
        embeddedChannel.flush();
        assertTrue(promise.isDone());
        assertTrue(promiseForQueued.isDone());
    }

    private static void assertBufferEquals(ByteBuf expected, Object message) {
        if (message instanceof ByteBuf buf) {
            assertSame(expected, buf);
        } else {
            assertEquals(expected, Netty4Utils.toByteBuf(asInstanceOf(BytesReference.class, message)));
        }
    }

    private static void assertSliceEquals(ByteBuf expected, Object message, int index, int length) {
        assertEquals(
            (message instanceof ByteBuf buf ? buf : Netty4Utils.toByteBuf(asInstanceOf(BytesReference.class, message))).slice(
                index,
                length
            ),
            expected
        );
    }

    private static Object wrapAsNettyOrEsBuffer(byte[] messageBytes) {
        if (randomBoolean()) {
            return Unpooled.wrappedBuffer(messageBytes);
        }
        return new BytesArray(messageBytes);
    }

    private class CapturingHandler extends ChannelOutboundHandlerAdapter {
        private final List<ByteBuf> seen;

        private boolean wasThrottled = false;

        private boolean didWriteAfterThrottled = false;

        CapturingHandler(List<ByteBuf> seen) {
            this.seen = seen;
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            assertThat(
                ThreadWatchdogHelper.getStuckThreadNames(threadWatchdog),
                // writes are re-entrant so we might already be considered stuck due to an earlier check
                anyOf(emptyIterable(), hasItem(Thread.currentThread().getName()))
            );
            assertThat(ThreadWatchdogHelper.getStuckThreadNames(threadWatchdog), hasItem(Thread.currentThread().getName()));

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
