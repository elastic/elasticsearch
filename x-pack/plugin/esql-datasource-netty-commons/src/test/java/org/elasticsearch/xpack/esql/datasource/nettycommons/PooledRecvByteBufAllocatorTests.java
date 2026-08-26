/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.nettycommons;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelConfig;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.util.UncheckedBooleanSupplier;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PooledRecvByteBufAllocatorTests extends ESTestCase {

    public void testAllocateUsesPooledAllocatorAndIgnoresChannelAllocator() {
        ByteBuf buf = null;
        try {
            RecvByteBufAllocator.Handle handle = PooledRecvByteBufAllocator.DEFAULT.newHandle();
            // Pass an Unpooled allocator to mimic the AWS SDK's override; the handle should still
            // return a pooled buffer.
            buf = handle.allocate(UnpooledByteBufAllocator.DEFAULT);
            assertThat(buf, is(notNullValue()));
            assertThat(
                "expected a pooled buffer regardless of the channel allocator",
                buf.alloc(),
                sameInstance(PooledByteBufAllocator.DEFAULT)
            );
        } finally {
            if (buf != null) {
                buf.release();
            }
        }
    }

    public void testHandleSizeFollowsWrappedAllocatorGuess() {
        // PooledHandle.allocate calls delegate.guess() at allocate time and uses that as capacity.
        // Use a stub that returns a fixed guess so the assertion is deterministic regardless of the
        // adaptive allocator's internal heuristics. Pick a value that is not a Netty size-class
        // boundary so the assertion exercises the rounding behavior rather than passing accidentally.
        int fixedGuess = 300;
        RecvByteBufAllocator stubAllocator = () -> new RecordingExtendedHandle(new AtomicInteger(), new AtomicBoolean(true)) {
            @Override
            public int guess() {
                return fixedGuess;
            }
        };
        PooledRecvByteBufAllocator allocator = new PooledRecvByteBufAllocator(stubAllocator, PooledByteBufAllocator.DEFAULT);
        RecvByteBufAllocator.Handle handle = allocator.newHandle();
        ByteBuf buf = null;
        try {
            buf = handle.allocate(UnpooledByteBufAllocator.DEFAULT);
            // PooledByteBufAllocator rounds up to its own size class, so the actual capacity may be
            // larger than the requested guess. We only require the buffer can hold the requested size.
            assertThat(buf.capacity(), greaterThanOrEqualTo(fixedGuess));
        } finally {
            if (buf != null) {
                buf.release();
            }
        }
        // Sanity check on the live AdaptiveRecvByteBufAllocator wiring: a fresh handle should still
        // produce a non-trivial guess and a usable buffer.
        ByteBuf adaptiveBuf = null;
        try {
            RecvByteBufAllocator.Handle adaptiveHandle = PooledRecvByteBufAllocator.DEFAULT.newHandle();
            adaptiveBuf = adaptiveHandle.allocate(UnpooledByteBufAllocator.DEFAULT);
            assertThat(adaptiveBuf.capacity(), greaterThan(0));
            assertThat(adaptiveBuf.alloc(), sameInstance(PooledByteBufAllocator.DEFAULT));
        } finally {
            if (adaptiveBuf != null) {
                adaptiveBuf.release();
            }
        }
    }

    public void testAllocateUsesProvidedPooledAllocator() {
        AtomicInteger ioBufferCalls = new AtomicInteger();
        ByteBufAllocator recordingAllocator = new RecordingByteBufAllocator(ioBufferCalls);
        PooledRecvByteBufAllocator allocator = new PooledRecvByteBufAllocator(new AdaptiveRecvByteBufAllocator(), recordingAllocator);
        RecvByteBufAllocator.Handle handle = allocator.newHandle();
        ByteBuf buf = null;
        try {
            buf = handle.allocate(UnpooledByteBufAllocator.DEFAULT);
            // The recording allocator delegates to UnpooledByteBufAllocator for the actual buffer
            // (and so buf.alloc() points to that delegate), but the ioBuffer call goes through the
            // recording allocator first, which is what we are asserting here.
            assertThat("custom pooled allocator must receive the ioBuffer call", ioBufferCalls.get(), equalTo(1));
        } finally {
            if (buf != null) {
                buf.release();
            }
        }
    }

    public void testNewHandleReturnsIndependentInstances() {
        RecvByteBufAllocator.Handle h1 = PooledRecvByteBufAllocator.DEFAULT.newHandle();
        RecvByteBufAllocator.Handle h2 = PooledRecvByteBufAllocator.DEFAULT.newHandle();
        assertThat("each newHandle() must return a fresh instance", h1, not(sameInstance(h2)));
    }

    public void testForwardsContinueReadingAndBookkeepingToDelegate() {
        AtomicInteger incCount = new AtomicInteger();
        AtomicBoolean continueResult = new AtomicBoolean(true);
        RecordingExtendedHandle inner = new RecordingExtendedHandle(incCount, continueResult);
        RecvByteBufAllocator stubAllocator = () -> inner;

        PooledRecvByteBufAllocator allocator = new PooledRecvByteBufAllocator(stubAllocator, PooledByteBufAllocator.DEFAULT);
        RecvByteBufAllocator.Handle handle = allocator.newHandle();

        handle.incMessagesRead(3);
        assertThat("incMessagesRead must reach the delegate", incCount.get(), equalTo(3));

        handle.lastBytesRead(123);
        assertThat(handle.lastBytesRead(), equalTo(123));

        handle.attemptedBytesRead(456);
        assertThat(handle.attemptedBytesRead(), equalTo(456));

        assertThat(handle.continueReading(), is(true));
        continueResult.set(false);
        assertThat(handle.continueReading(), is(false));

        handle.reset(null);
        assertThat("reset must reach the delegate", inner.resetCount, equalTo(1));

        handle.readComplete();
        assertThat("readComplete must reach the delegate", inner.readCompleteCount, equalTo(1));
    }

    public void testContinueReadingWithSupplierForwardsToExtendedHandle() {
        AtomicBoolean supplierConsulted = new AtomicBoolean();
        UncheckedBooleanSupplier supplier = () -> {
            supplierConsulted.set(true);
            return true;
        };

        RecvByteBufAllocator stubAllocator = () -> new RecordingExtendedHandle(new AtomicInteger(), new AtomicBoolean(true)) {
            @Override
            public boolean continueReading(UncheckedBooleanSupplier maybeMoreDataSupplier) {
                // Calling the supplier here proves PooledHandle forwarded it through rather than
                // dropping it on the floor.
                return maybeMoreDataSupplier.get();
            }
        };

        PooledRecvByteBufAllocator allocator = new PooledRecvByteBufAllocator(stubAllocator, PooledByteBufAllocator.DEFAULT);
        RecvByteBufAllocator.ExtendedHandle handle = (RecvByteBufAllocator.ExtendedHandle) allocator.newHandle();
        assertThat(handle.continueReading(supplier), is(true));
        assertThat("supplier must have been consulted by the delegate", supplierConsulted.get(), is(true));
    }

    public void testRejectsDelegateThatDoesNotProduceExtendedHandle() {
        // Stub delegate that returns a plain Handle (not an ExtendedHandle); the wrapper must refuse
        // it so we never silently drop the supplier in continueReading(supplier).
        RecvByteBufAllocator stubAllocator = () -> new BasicHandle();
        PooledRecvByteBufAllocator allocator = new PooledRecvByteBufAllocator(stubAllocator, PooledByteBufAllocator.DEFAULT);
        IllegalStateException ex = expectThrows(IllegalStateException.class, allocator::newHandle);
        assertThat(ex.getMessage(), containsString("ExtendedHandle"));
    }

    private static class RecordingExtendedHandle implements RecvByteBufAllocator.ExtendedHandle {
        private final AtomicInteger incCount;
        private final AtomicBoolean continueResult;
        private int lastBytes;
        private int attemptedBytes;
        int resetCount;
        int readCompleteCount;

        RecordingExtendedHandle(AtomicInteger incCount, AtomicBoolean continueResult) {
            this.incCount = incCount;
            this.continueResult = continueResult;
        }

        @Override
        public ByteBuf allocate(ByteBufAllocator alloc) {
            // Only invoked if PooledHandle accidentally forwards the allocation; the wrapper should
            // never call this in production paths.
            return alloc.ioBuffer(64);
        }

        @Override
        public int guess() {
            return 64;
        }

        @Override
        public void reset(ChannelConfig config) {
            resetCount++;
        }

        @Override
        public void incMessagesRead(int numMessages) {
            incCount.addAndGet(numMessages);
        }

        @Override
        public void lastBytesRead(int bytes) {
            this.lastBytes = bytes;
        }

        @Override
        public int lastBytesRead() {
            return lastBytes;
        }

        @Override
        public void attemptedBytesRead(int bytes) {
            this.attemptedBytes = bytes;
        }

        @Override
        public int attemptedBytesRead() {
            return attemptedBytes;
        }

        @Override
        public boolean continueReading() {
            return continueResult.get();
        }

        @Override
        public boolean continueReading(UncheckedBooleanSupplier maybeMoreDataSupplier) {
            return continueResult.get();
        }

        @Override
        public void readComplete() {
            readCompleteCount++;
        }
    }

    /**
     * Records {@code ioBuffer} invocations and delegates to {@link UnpooledByteBufAllocator#DEFAULT} so
     * we can verify which {@link ByteBufAllocator} {@link PooledRecvByteBufAllocator} actually targets,
     * without relying on identity comparisons against a singleton.
     */
    private static final class RecordingByteBufAllocator extends io.netty.buffer.AbstractByteBufAllocator {
        private final AtomicInteger ioBufferCalls;

        RecordingByteBufAllocator(AtomicInteger ioBufferCalls) {
            super(false);
            this.ioBufferCalls = ioBufferCalls;
        }

        @Override
        public ByteBuf ioBuffer(int initialCapacity) {
            ioBufferCalls.incrementAndGet();
            return super.ioBuffer(initialCapacity);
        }

        @Override
        protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            return UnpooledByteBufAllocator.DEFAULT.heapBuffer(initialCapacity, maxCapacity);
        }

        @Override
        protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
            return UnpooledByteBufAllocator.DEFAULT.directBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public boolean isDirectBufferPooled() {
            return false;
        }
    }

    private static class BasicHandle implements RecvByteBufAllocator.Handle {
        @Override
        public ByteBuf allocate(ByteBufAllocator alloc) {
            return alloc.ioBuffer(64);
        }

        @Override
        public int guess() {
            return 64;
        }

        @Override
        public void reset(ChannelConfig config) {}

        @Override
        public void incMessagesRead(int numMessages) {}

        @Override
        public void lastBytesRead(int bytes) {}

        @Override
        public int lastBytesRead() {
            return 0;
        }

        @Override
        public void attemptedBytesRead(int bytes) {}

        @Override
        public int attemptedBytesRead() {
            return 0;
        }

        @Override
        public boolean continueReading() {
            return false;
        }

        @Override
        public void readComplete() {}
    }
}
