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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TransportServiceAdapter;
import org.elasticsearch.transport.Transports;

import java.net.InetSocketAddress;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
final class Netty4MessageChannelHandler extends ChannelDuplexHandler {

    private final TransportServiceAdapter transportServiceAdapter;
    private final Netty4Transport transport;
    private final String profileName;

    Netty4MessageChannelHandler(Netty4Transport transport, String profileName) {
        this.transportServiceAdapter = transport.transportServiceAdapter();
        this.transport = transport;
        this.profileName = profileName;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf && transportServiceAdapter != null) {
            // record the number of bytes send on the channel
            promise.addListener(f -> transportServiceAdapter.addBytesSent(((ByteBuf) msg).readableBytes()));
        }
        ctx.write(msg, promise);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Transports.assertTransportThread();
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }
        final ByteBuf buffer = (ByteBuf) msg;
        final int remainingMessageSize = buffer.getInt(buffer.readerIndex() - TcpHeader.MESSAGE_LENGTH_SIZE);
        final int expectedReaderIndex = buffer.readerIndex() + remainingMessageSize;
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        try {
            // netty always copies a buffer, either in NioWorker in its read handler, where it copies to a fresh
            // buffer, or in the cumulation buffer, which is cleaned each time so it could be bigger than the actual size
            BytesReference reference = Netty4Utils.toBytesReference(buffer, remainingMessageSize);
            transport.messageReceived(reference, ctx.channel(), profileName, remoteAddress, remainingMessageSize);
        } finally {
            // Set the expected position of the buffer, no matter what happened
            buffer.readerIndex(expectedReaderIndex);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        transport.exceptionCaught(ctx, cause);
    }

}
