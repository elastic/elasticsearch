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

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.transport.Transports;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
final class Netty4MessageChannelHandler extends ChannelDuplexHandler {

    private final Netty4Transport transport;

    private final ConcurrentLinkedQueue<WriteOperation> queuedWrites = new ConcurrentLinkedQueue<>();

    private WriteOperation currentWrite;

    Netty4MessageChannelHandler(Netty4Transport transport) {
        this.transport = transport;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Transports.assertTransportThread();
        assert msg instanceof ByteBuf : "Expected message type ByteBuf, found: " + msg.getClass();

        final ByteBuf buffer = (ByteBuf) msg;
        try {
            Channel channel = ctx.channel();
            Attribute<Netty4TcpChannel> channelAttribute = channel.attr(Netty4Transport.CHANNEL_KEY);
            transport.inboundMessage(channelAttribute.get(), Netty4Utils.toBytesReference(buffer));
        } finally {
            buffer.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
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
        final boolean queued = queuedWrites.offer(new WriteOperation((ByteBuf) msg, promise));
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
        Channel channel = ctx.channel();
        if (channel.isWritable() || channel.isActive() == false) {
            doFlush(ctx);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        doFlush(ctx);
        super.channelInactive(ctx);
    }

    private void doFlush(ChannelHandlerContext ctx) {
        final Channel channel = ctx.channel();
        if (channel.isActive() == false) {
            if (currentWrite != null) {
                currentWrite.promise.tryFailure(new ClosedChannelException());
            }
            WriteOperation queuedWrite;
            while ((queuedWrite = queuedWrites.poll()) != null) {
                queuedWrite.promise.tryFailure(new ClosedChannelException());
            }
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
            final ByteBuf writeBuffer;
            if (write.buf.readableBytes() == 0) {
                writeBuffer = Unpooled.EMPTY_BUFFER;
            } else {
                writeBuffer = ctx.alloc().directBuffer(Math.min(write.buf.readableBytes(), 1 << 18));
                writeBuffer.writeBytes(write.buf);
            }
            writeBuffer.retain();
            final ChannelFuture writeFuture;
            try {
                writeFuture = ctx.write(writeBuffer);
            } finally {
                writeBuffer.release();
            }
            if (write.buf.readableBytes() == 0) {
                currentWrite = null;
                writeFuture.addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        write.promise.trySuccess();
                    } else {
                        write.promise.tryFailure(future.cause());
                    }
                });
            } else {
                writeFuture.addListener(f -> {
                    if (f.isSuccess() == false) {
                        write.promise.tryFailure(f.cause());
                    }
                });
            }
            ctx.flush();
            if (channel.isActive() == false) {
                WriteOperation queuedWrite;
                while ((queuedWrite = queuedWrites.poll()) != null) {
                    queuedWrite.promise.tryFailure(new ClosedChannelException());
                }
                return;
            }
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
