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
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.Transports;

import java.net.InetSocketAddress;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
final class Netty4MessageChannelHandler extends ChannelDuplexHandler {

    private final Netty4Transport transport;
    private final String profileName;

    Netty4MessageChannelHandler(Netty4Transport transport, String profileName) {
        this.transport = transport;
        this.profileName = profileName;
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
        try {
            Channel channel = ctx.channel();
            InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
            // netty always copies a buffer, either in NioWorker in its read handler, where it copies to a fresh
            // buffer, or in the cumulative buffer, which is cleaned each time so it could be bigger than the actual size
            BytesReference reference = Netty4Utils.toBytesReference(buffer, remainingMessageSize);
            Attribute<NettyTcpChannel> channelAttribute = channel.attr(Netty4Transport.CHANNEL_KEY);
            transport.messageReceived(reference, channelAttribute.get(), profileName, remoteAddress, remainingMessageSize);
        } finally {
            // Set the expected position of the buffer, no matter what happened
            buffer.readerIndex(expectedReaderIndex);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Netty4Utils.maybeDie(cause);
        transport.exceptionCaught(ctx, cause);
    }

}
