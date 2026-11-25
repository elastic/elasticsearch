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
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelConfig;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.util.UncheckedBooleanSupplier;

import java.util.Objects;

/**
 * A {@link RecvByteBufAllocator} that delegates sizing decisions to an inner allocator (an
 * {@link AdaptiveRecvByteBufAllocator} by default) but redirects the actual buffer allocation to a fixed
 * pooled {@link ByteBufAllocator} (defaulting to {@link PooledByteBufAllocator#DEFAULT}), regardless of
 * the {@link ByteBufAllocator} associated with the channel.
 *
 * <p>This is a workaround for the AWS SDK v2 Netty HTTP client, whose
 * {@code ChannelPipelineInitializer.channelCreated} unconditionally overrides
 * {@code ChannelOption.ALLOCATOR} to {@link io.netty.buffer.UnpooledByteBufAllocator#DEFAULT} on every
 * SSL channel that uses the JDK SSL provider. Because that override happens after bootstrap options are
 * applied, configuring {@code ChannelOption.ALLOCATOR} on the {@code NettyNioAsyncHttpClient.Builder}
 * has no effect for HTTPS connections, and every socket read ends up allocating a fresh
 * {@code UnpooledHeapByteBuf} (a JVM-zeroed {@code byte[]}). The zero-fill itself shows up as 5–11% of
 * CPU time on read-heavy S3 workloads, even though the bytes are immediately overwritten by the response
 * payload.
 *
 * <p>The AWS SDK does not touch {@link io.netty.channel.ChannelOption#RCVBUF_ALLOCATOR}, so installing
 * an instance of this class via
 * {@code NettyNioAsyncHttpClient.Builder.putChannelOption(ChannelOption.RCVBUF_ALLOCATOR, ...)} is enough
 * to redirect the receive path through the pooled allocator without depending on native TLS bindings.
 *
 * <p>Scope: only the receive path (socket read) is redirected. Any allocations made directly via
 * {@code channel.alloc()} elsewhere (write path, codec internals) still go through the channel-bound
 * allocator and are therefore unaffected by this wrapper.
 *
 * <p>This allocator is safe to share across channels and threads; each call to {@link #newHandle()}
 * returns an independent stateful handle, just like the wrapped allocator does.
 */
public final class PooledRecvByteBufAllocator implements RecvByteBufAllocator {

    /**
     * Default instance backed by {@link AdaptiveRecvByteBufAllocator} (Netty's default sizing strategy)
     * and {@link PooledByteBufAllocator#DEFAULT}.
     */
    public static final PooledRecvByteBufAllocator DEFAULT = new PooledRecvByteBufAllocator(
        new AdaptiveRecvByteBufAllocator(),
        PooledByteBufAllocator.DEFAULT
    );

    private final RecvByteBufAllocator delegate;
    private final ByteBufAllocator pooledAllocator;

    public PooledRecvByteBufAllocator(RecvByteBufAllocator delegate, ByteBufAllocator pooledAllocator) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.pooledAllocator = Objects.requireNonNull(pooledAllocator, "pooledAllocator");
    }

    @Override
    public Handle newHandle() {
        Handle inner = delegate.newHandle();
        if (inner instanceof ExtendedHandle extended) {
            return new PooledHandle(extended, pooledAllocator);
        }
        // Netty's read loop calls continueReading(UncheckedBooleanSupplier) on any handle that
        // implements ExtendedHandle. Since PooledHandle implements ExtendedHandle, we would receive
        // that call but, with a non-extended inner handle, would have to drop the supplier and fall
        // back to the no-arg overload — silently changing the more-data heuristic. Refuse this rather
        // than ship a subtle behavior change.
        throw new IllegalStateException("Wrapped RecvByteBufAllocator must produce an ExtendedHandle, got: " + inner.getClass().getName());
    }

    /**
     * Forwards every method to the wrapped handle except {@link #allocate(ByteBufAllocator)}, which
     * ignores the channel-supplied allocator and uses the pooled allocator instead. The buffer size is
     * still determined by the wrapped handle's adaptive logic via {@link Handle#guess()}.
     */
    private static final class PooledHandle implements ExtendedHandle {
        private final ExtendedHandle delegate;
        private final ByteBufAllocator pooledAllocator;

        PooledHandle(ExtendedHandle delegate, ByteBufAllocator pooledAllocator) {
            this.delegate = delegate;
            this.pooledAllocator = pooledAllocator;
        }

        @Override
        public ByteBuf allocate(ByteBufAllocator ignored) {
            return pooledAllocator.ioBuffer(delegate.guess());
        }

        @Override
        public int guess() {
            return delegate.guess();
        }

        @Override
        public void reset(ChannelConfig config) {
            delegate.reset(config);
        }

        @Override
        public void incMessagesRead(int numMessages) {
            delegate.incMessagesRead(numMessages);
        }

        @Override
        public void lastBytesRead(int bytes) {
            delegate.lastBytesRead(bytes);
        }

        @Override
        public int lastBytesRead() {
            return delegate.lastBytesRead();
        }

        @Override
        public void attemptedBytesRead(int bytes) {
            delegate.attemptedBytesRead(bytes);
        }

        @Override
        public int attemptedBytesRead() {
            return delegate.attemptedBytesRead();
        }

        @Override
        public boolean continueReading() {
            return delegate.continueReading();
        }

        @Override
        public boolean continueReading(UncheckedBooleanSupplier maybeMoreDataSupplier) {
            return delegate.continueReading(maybeMoreDataSupplier);
        }

        @Override
        public void readComplete() {
            delegate.readComplete();
        }
    }
}
