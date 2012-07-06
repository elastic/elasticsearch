/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.transport.netty;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.ThrowableObjectOutputStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.transport.NotSerializableTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponseOptions;
import org.elasticsearch.transport.support.TransportStreams;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import java.io.IOException;
import java.io.NotSerializableException;

/**
 *
 */
public class NettyTransportChannel implements TransportChannel {

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    private final NettyTransport transport;

    private final String action;

    private final Channel channel;

    private final long requestId;

    public NettyTransportChannel(NettyTransport transport, String action, Channel channel, long requestId) {
        this.transport = transport;
        this.action = action;
        this.channel = channel;
        this.requestId = requestId;
    }

    @Override
    public String action() {
        return this.action;
    }

    @Override
    public void sendResponse(Streamable message) throws IOException {
        sendResponse(message, TransportResponseOptions.EMPTY);
    }

    @Override
    public void sendResponse(Streamable message, TransportResponseOptions options) throws IOException {
        if (transport.compress) {
            options.withCompress(true);
        }
        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
        TransportStreams.buildResponse(cachedEntry, requestId, message, options);
        BytesReference bytes = cachedEntry.bytes().bytes();
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(bytes.array(), bytes.arrayOffset(), bytes.length());
        ChannelFuture future = channel.write(buffer);
        future.addListener(new NettyTransport.CacheFutureListener(cachedEntry));
    }

    @Override
    public void sendResponse(Throwable error) throws IOException {
        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
        BytesStreamOutput stream;
        try {
            stream = cachedEntry.bytes();
            writeResponseExceptionHeader(stream);
            RemoteTransportException tx = new RemoteTransportException(transport.nodeName(), transport.wrapAddress(channel.getLocalAddress()), action, error);
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(stream);
            too.writeObject(tx);
            too.close();
        } catch (NotSerializableException e) {
            cachedEntry.reset();
            stream = cachedEntry.bytes();
            writeResponseExceptionHeader(stream);
            RemoteTransportException tx = new RemoteTransportException(transport.nodeName(), transport.wrapAddress(channel.getLocalAddress()), action, new NotSerializableTransportException(error));
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(stream);
            too.writeObject(tx);
            too.close();
        }
        BytesReference bytes = stream.bytes();
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(bytes.array(), bytes.arrayOffset(), bytes.length());
        buffer.setInt(0, buffer.writerIndex() - 4); // update real size.
        ChannelFuture future = channel.write(buffer);
        future.addListener(new NettyTransport.CacheFutureListener(cachedEntry));
    }

    private void writeResponseExceptionHeader(BytesStreamOutput stream) throws IOException {
        stream.writeBytes(LENGTH_PLACEHOLDER);
        stream.writeLong(requestId);
        byte status = 0;
        status = TransportStreams.statusSetResponse(status);
        status = TransportStreams.statusSetError(status);
        stream.writeByte(status);
    }
}
