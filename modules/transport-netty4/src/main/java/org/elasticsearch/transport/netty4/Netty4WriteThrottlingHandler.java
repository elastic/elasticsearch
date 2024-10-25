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
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.PromiseCombiner;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.ThreadWatchdog;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Channel handler that queues up writes it receives and tries to only flush bytes as they can be written by the backing channel.
 * This is helpful in reducing heap usage with handlers like {@link io.netty.handler.ssl.SslHandler} that might otherwise themselves
 * buffer a large amount of data when the channel is not able to physically execute writes immediately.
 */
public final class Netty4WriteThrottlingHandler extends ChannelDuplexHandler {

    public static final int MAX_BYTES_PER_WRITE = 1 << 18;
    private final Queue<WriteOperation> queuedWrites = new LinkedList<>();

    private final ThreadContext threadContext;
    private final ThreadWatchdog.ActivityTracker threadWatchdogActivityTracker;
    private WriteOperation currentWrite;

    public Netty4WriteThrottlingHandler(ThreadContext threadContext, ThreadWatchdog.ActivityTracker threadWatchdogActivityTracker) {
        this.threadContext = threadContext;
        this.threadWatchdogActivityTracker = threadWatchdogActivityTracker;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws IOException {
        final boolean startedActivity = threadWatchdogActivityTracker.maybeStartActivity();
        try {
            if (msg instanceof BytesReference reference) {
                if (reference.hasArray()) {
                    writeSingleByteBuf(
                        ctx,
                        Unpooled.wrappedBuffer(reference.array(), reference.arrayOffset(), reference.length()),
                        promise
                    );
                } else {
                    BytesRefIterator iter = reference.iterator();
                    final PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
                    BytesRef next;
                    while ((next = iter.next()) != null) {
                        final ChannelPromise chunkPromise = ctx.newPromise();
                        combiner.add((Future<Void>) chunkPromise);
                        writeSingleByteBuf(ctx, Unpooled.wrappedBuffer(next.bytes, next.offset, next.length), chunkPromise);
                    }
                    combiner.finish(promise);
                }
            } else {
                assert msg instanceof ByteBuf;
                writeSingleByteBuf(ctx, (ByteBuf) msg, promise);
            }
        } finally {
            if (startedActivity) {
                threadWatchdogActivityTracker.stopActivity();
            }
        }
    }

    private void writeSingleByteBuf(ChannelHandlerContext ctx, ByteBuf buf, ChannelPromise promise) {
        assert Transports.assertDefaultThreadContext(threadContext);
        assert Transports.assertTransportThread();
        if (ctx.channel().isWritable() && currentWrite == null && queuedWrites.isEmpty()) {
            // nothing is queued for writing and the channel is writable, just pass the write down the pipeline directly
            if (buf.readableBytes() > MAX_BYTES_PER_WRITE) {
                writeInSlices(ctx, promise, buf);
            } else {
                ctx.write(buf, promise);
            }
        } else {
            queueWrite(buf, promise);
        }
    }

    /**
     * Writes slices of up to the max write size until the channel stops being writable or the message has been written in full.
     */
    private void writeInSlices(ChannelHandlerContext ctx, ChannelPromise promise, ByteBuf buf) {
        while (true) {
            final int readableBytes = buf.readableBytes();
            final int bufferSize = Math.min(readableBytes, MAX_BYTES_PER_WRITE);
            if (readableBytes == bufferSize) {
                // last write for this chunk we're done
                Netty4Utils.addListener(ctx.write(buf), forwardResultListener(promise));
                return;
            }
            final int readerIndex = buf.readerIndex();
            final ByteBuf writeBuffer = buf.retainedSlice(readerIndex, bufferSize);
            buf.readerIndex(readerIndex + bufferSize);
            Netty4Utils.addListener(ctx.write(writeBuffer), forwardFailureListener(promise));
            if (ctx.channel().isWritable() == false) {
                // channel isn't writable any longer -> move to queuing
                queueWrite(buf, promise);
                return;
            }
        }
    }

    private void queueWrite(ByteBuf buf, ChannelPromise promise) {
        final boolean queued = queuedWrites.offer(new WriteOperation(buf, promise));
        assert queued;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        final boolean startedActivity = threadWatchdogActivityTracker.maybeStartActivity();
        try {
            if (ctx.channel().isWritable()) {
                doFlush(ctx);
            }
            ctx.fireChannelWritabilityChanged();
        } finally {
            if (startedActivity) {
                threadWatchdogActivityTracker.stopActivity();
            }
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        final boolean startedActivity = threadWatchdogActivityTracker.maybeStartActivity();
        try {
            if (doFlush(ctx) == false) {
                ctx.flush();
            }
        } finally {
            if (startedActivity) {
                threadWatchdogActivityTracker.stopActivity();
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        final boolean startedActivity = threadWatchdogActivityTracker.maybeStartActivity();
        try {
            doFlush(ctx);
        } finally {
            if (startedActivity) {
                threadWatchdogActivityTracker.stopActivity();
            }
        }

        // super.channelInactive() can trigger reads which are tracked separately (and are not re-entrant) so no activity tracking here
        super.channelInactive(ctx);
    }

    private boolean doFlush(ChannelHandlerContext ctx) {
        assert ctx.executor().inEventLoop();
        final Channel channel = ctx.channel();
        if (channel.isActive() == false) {
            failQueuedWrites();
            return false;
        }
        while (channel.isWritable()) {
            if (currentWrite == null) {
                currentWrite = queuedWrites.poll();
            }
            if (currentWrite == null) {
                break;
            }
            final WriteOperation write = currentWrite;
            final int readableBytes = write.buf.readableBytes();
            final int bufferSize = Math.min(readableBytes, MAX_BYTES_PER_WRITE);
            final int readerIndex = write.buf.readerIndex();
            final boolean sliced = readableBytes != bufferSize;
            final ByteBuf writeBuffer;
            if (sliced) {
                writeBuffer = write.buf.retainedSlice(readerIndex, bufferSize);
                write.buf.readerIndex(readerIndex + bufferSize);
            } else {
                writeBuffer = write.buf;
            }
            final ChannelFuture writeFuture = ctx.write(writeBuffer);
            if (sliced == false) {
                currentWrite = null;
                Netty4Utils.addListener(writeFuture, forwardResultListener(write.promise));
            } else {
                Netty4Utils.addListener(writeFuture, forwardFailureListener(write.promise));
            }
        }
        ctx.flush();
        if (channel.isActive() == false) {
            failQueuedWrites();
        }
        return true;
    }

    private static ChannelFutureListener forwardFailureListener(ChannelPromise promise) {
        return future -> {
            assert future.channel().eventLoop().inEventLoop();
            if (future.isSuccess() == false) {
                promise.tryFailure(future.cause());
            }
        };
    }

    private static ChannelFutureListener forwardResultListener(ChannelPromise promise) {
        return future -> {
            assert future.channel().eventLoop().inEventLoop();
            if (future.isSuccess()) {
                promise.trySuccess();
            } else {
                promise.tryFailure(future.cause());
            }
        };
    }

    private void failQueuedWrites() {
        if (currentWrite != null) {
            final WriteOperation current = currentWrite;
            currentWrite = null;
            current.failAsClosedChannel();
        }
        WriteOperation queuedWrite;
        while ((queuedWrite = queuedWrites.poll()) != null) {
            queuedWrite.failAsClosedChannel();
        }
    }

    private record WriteOperation(ByteBuf buf, ChannelPromise promise) {

        void failAsClosedChannel() {
            promise.tryFailure(new ClosedChannelException());
            buf.release();
        }
    }

}
