/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.OutboundMessage;
import org.elasticsearch.transport.Transports;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.List;
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
        assert msg instanceof OutboundMessage.SerializedBytes || msg instanceof ByteBuf;
        assert Transports.assertDefaultThreadContext(threadContext);
        assert Transports.assertTransportThread();
        final ByteBuf buf;
        if (msg instanceof OutboundMessage.SerializedBytes serializedBytes) {
            List<ReleasableBytesReference> components = serializedBytes.components();
            serializedBytes.takeOwnership();
            if (components.size() > 1) {
                CompositeByteBuf composite = Unpooled.compositeBuffer(components.size());
                for (ReleasableBytesReference component : components) {
                    composite.addComponent(true, Netty4Utils.toReleasableByteBuf(component));
                }
                buf = composite;
            } else {
                ReleasableBytesReference component = components.get(0);
                buf = Netty4Utils.toReleasableByteBuf(component);
            }
        } else {
            buf = (ByteBuf) msg;
        }

        if (ctx.channel().isWritable() && currentWrite == null && queuedWrites.isEmpty()) {
            // nothing is queued for writing and the channel is writable, set to current write and write
            currentWrite = new WriteOperation(buf, promise);
            writeBytes(ctx);
        } else {
            final boolean queued = queuedWrites.offer(new WriteOperation(buf, promise));
            assert queued;
        }
    }

    private boolean writeBytes(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();

        boolean bytesWritten = false;
        while (channel.isWritable()) {
            if (currentWrite == null) {
                currentWrite = queuedWrites.poll();
                if (currentWrite == null) {
                    break;
                }
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
            bytesWritten = true;

            if (sliced == false) {
                currentWrite = null;
                writeFuture.addListener(forwardResultListener(ctx, write.promise));
            } else {
                writeFuture.addListener(forwardFailureListener(ctx, write.promise));
            }
        }
        attemptToReleaseBytes();
        return bytesWritten;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (ctx.channel().isWritable()) {
            if (writeBytes(ctx)) {
                ctx.flush();
                attemptToReleaseBytes();
            }

        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        super.flush(ctx);
        attemptToReleaseBytes();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        failQueuedWrites();
        super.channelInactive(ctx);
    }

    private void attemptToReleaseBytes() {
        if (currentWrite != null && currentWrite.buf.refCnt() == 1) {
            currentWrite.buf.discardSomeReadBytes();
        }
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
