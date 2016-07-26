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

package org.elasticsearch.transport.netty3;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TransportServiceAdapter;
import org.elasticsearch.transport.Transports;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.WriteCompletionEvent;

import java.net.InetSocketAddress;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
class Netty3MessageChannelHandler extends SimpleChannelUpstreamHandler {

    protected final TransportServiceAdapter transportServiceAdapter;
    protected final Netty3Transport transport;
    protected final String profileName;

    Netty3MessageChannelHandler(Netty3Transport transport, String profileName) {
        this.transportServiceAdapter = transport.transportServiceAdapter();
        this.transport = transport;
        this.profileName = profileName;
    }

    @Override
    public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
        transportServiceAdapter.addBytesSent(e.getWrittenAmount());
        super.writeComplete(ctx, e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Transports.assertTransportThread();
        Object m = e.getMessage();
        if (!(m instanceof ChannelBuffer)) {
            ctx.sendUpstream(e);
            return;
        }
        final ChannelBuffer buffer = (ChannelBuffer) m;
        final int remainingMessageSize = buffer.getInt(buffer.readerIndex() - TcpHeader.MESSAGE_LENGTH_SIZE);
        final int expectedReaderIndex = buffer.readerIndex() + remainingMessageSize;
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.getChannel().getRemoteAddress();
        try {
            // netty always copies a buffer, either in NioWorker in its read handler, where it copies to a fresh
            // buffer, or in the cumulation buffer, which is cleaned each time so it could be bigger than the actual size
            BytesReference reference = Netty3Utils.toBytesReference(buffer, remainingMessageSize);
            transport.messageReceived(reference, ctx.getChannel(), profileName, remoteAddress, remainingMessageSize);
        } finally {
            // Set the expected position of the buffer, no matter what happened
            buffer.readerIndex(expectedReaderIndex);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        transport.exceptionCaught(ctx, e);
    }
}
