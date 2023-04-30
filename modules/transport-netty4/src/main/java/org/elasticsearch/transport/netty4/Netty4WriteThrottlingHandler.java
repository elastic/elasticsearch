/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.Transports;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Channel handler that queues up writes it receives and tries to only flush bytes as they can be written by the backing channel.
 * This is helpful in reducing heap usage with handlers like {@link io.netty.handler.ssl.SslHandler} that might otherwise themselves
 * buffer a large amount of data when the channel is not able to physically execute writes immediately.
 */
public final class Netty4WriteThrottlingHandler extends ChannelDuplexHandler {

    public static final int MAX_BYTES_PER_WRITE = 1 << 18;
    private final Queue<WriteOperation> queuedWrites = new ArrayDeque<>();

    private final ThreadContext threadContext;
    private WriteOperation currentWrite;

    public Netty4WriteThrottlingHandler(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        assert msg instanceof ByteBuf;
        assert Transports.assertDefaultThreadContext(threadContext);
        assert Transports.assertTransportThread();
        final ByteBuf buf = (ByteBuf) msg;
        if (ctx.channel().isWritable() && currentWrite == null && queuedWrites.isEmpty()) {
            // nothing is queued for writing and the channel is writable, just pass the write down the pipeline directly
            if (buf.readableBytes() > MAX_BYTES_PER_WRITE) {
                writeInSlices(ctx, promise, buf);
            } else {
                ctx.write(msg, promise);
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
                ctx.write(buf).addListener(forwardResultListener(ctx, promise));
                return;
            }
            final int readerIndex = buf.readerIndex();
            final ByteBuf writeBuffer = buf.retainedSlice(readerIndex, bufferSize);
            buf.readerIndex(readerIndex + bufferSize);
            ctx.write(writeBuffer).addListener(forwardFailureListener(ctx, promise));
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
        if (ctx.channel().isWritable()) {
            doFlush(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        if (doFlush(ctx) == false) {
            ctx.flush();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        doFlush(ctx);
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
                writeFuture.addListener(forwardResultListener(ctx, write.promise));
            } else {
                writeFuture.addListener(forwardFailureListener(ctx, write.promise));
            }
        }
        ctx.flush();
        if (channel.isActive() == false) {
            failQueuedWrites();
        }
        return true;
    }

    private static GenericFutureListener<Future<Void>> forwardFailureListener(ChannelHandlerContext ctx, ChannelPromise promise) {
        return future -> {
            assert ctx.executor().inEventLoop();
            if (future.isSuccess() == false) {
                promise.tryFailure(future.cause());
            }
        };
    }

    private static GenericFutureListener<Future<Void>> forwardResultListener(ChannelHandlerContext ctx, ChannelPromise promise) {
        return future -> {
            assert ctx.executor().inEventLoop();
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
