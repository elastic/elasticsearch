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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.InboundPipeline;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.Transports;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
final class Netty4MessageChannelHandler extends ChannelDuplexHandler {

    private final Netty4Transport transport;

    private final Queue<WriteOperation> queuedWrites = new ArrayDeque<>();

    private WriteOperation currentWrite;
    private final InboundPipeline pipeline;

    Netty4MessageChannelHandler(PageCacheRecycler recycler, Netty4Transport transport) {
        this.transport = transport;
        final ThreadPool threadPool = transport.getThreadPool();
        final Transport.RequestHandlers requestHandlers = transport.getRequestHandlers();
        this.pipeline = new InboundPipeline(transport.getVersion(), transport.getStatsTracker(), recycler, threadPool::relativeTimeInMillis,
            transport.getInflightBreaker(), requestHandlers::getHandler, transport::inboundMessage);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        assert Transports.assertTransportThread();
        assert msg instanceof ByteBuf : "Expected message type ByteBuf, found: " + msg.getClass();

        final ByteBuf buffer = (ByteBuf) msg;
        Netty4TcpChannel channel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        final BytesReference wrapped = Netty4Utils.toBytesReference(buffer);
        try (ReleasableBytesReference reference = new ReleasableBytesReference(wrapped, buffer::release)) {
            pipeline.handleBytes(channel, reference);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        final Throwable unwrapped = ExceptionsHelper.unwrap(cause, ElasticsearchException.class);
        final Throwable newCause = unwrapped != null ? unwrapped : cause;
        Netty4TcpChannel tcpChannel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        if (newCause instanceof Error) {
            transport.onException(tcpChannel, new Exception(newCause));
        } else {
            transport.onException(tcpChannel, (Exception) newCause);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        assert msg instanceof ByteBuf;
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        final boolean queued = queuedWrites.offer(new WriteOperation((ByteBuf) msg, promise));
        assert queued;
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        if (ctx.channel().isWritable()) {
            doFlush(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        Channel channel = ctx.channel();
        if (channel.isWritable() || channel.isActive() == false) {
            doFlush(ctx);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        doFlush(ctx);
        Releasables.closeExpectNoException(pipeline);
        super.channelInactive(ctx);
    }

    private void doFlush(ChannelHandlerContext ctx) {
        assert ctx.executor().inEventLoop();
        final Channel channel = ctx.channel();
        if (channel.isActive() == false) {
            if (currentWrite != null) {
                currentWrite.promise.tryFailure(new ClosedChannelException());
            }
            failQueuedWrites();
            return;
        }
        while (channel.isWritable()) {
            if (currentWrite == null) {
                currentWrite = queuedWrites.poll();
            }
            if (currentWrite == null) {
                break;
            }
            final WriteOperation write = currentWrite;
            if (write.buf.readableBytes() == 0) {
                write.promise.trySuccess();
                currentWrite = null;
                continue;
            }
            final int readableBytes = write.buf.readableBytes();
            final int bufferSize = Math.min(readableBytes, 1 << 18);
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
            if (sliced == false || write.buf.readableBytes() == 0) {
                currentWrite = null;
                writeFuture.addListener(future -> {
                    assert ctx.executor().inEventLoop();
                    if (future.isSuccess()) {
                        write.promise.trySuccess();
                    } else {
                        write.promise.tryFailure(future.cause());
                    }
                });
            } else {
                writeFuture.addListener(future -> {
                    assert ctx.executor().inEventLoop();
                    if (future.isSuccess() == false) {
                        write.promise.tryFailure(future.cause());
                    }
                });
            }
            ctx.flush();
            if (channel.isActive() == false) {
                failQueuedWrites();
                return;
            }
        }
    }

    private void failQueuedWrites() {
        WriteOperation queuedWrite;
        while ((queuedWrite = queuedWrites.poll()) != null) {
            queuedWrite.promise.tryFailure(new ClosedChannelException());
        }
    }

    private static final class WriteOperation {

        private final ByteBuf buf;

        private final ChannelPromise promise;

        WriteOperation(ByteBuf buf, ChannelPromise promise) {
            this.buf = buf;
            this.promise = promise;
        }
    }
}
