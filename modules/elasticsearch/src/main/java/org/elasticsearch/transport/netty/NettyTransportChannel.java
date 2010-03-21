/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.transport.NotSerializableTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.util.io.ThrowableObjectOutputStream;
import org.elasticsearch.util.io.stream.BytesStreamOutput;
import org.elasticsearch.util.io.stream.HandlesStreamOutput;
import org.elasticsearch.util.io.stream.Streamable;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

import java.io.IOException;
import java.io.NotSerializableException;

import static org.elasticsearch.transport.Transport.Helper.*;

/**
 * @author kimchy (Shay Banon)
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

    @Override public String action() {
        return this.action;
    }

    @Override public void sendResponse(Streamable message) throws IOException {
        HandlesStreamOutput stream = BytesStreamOutput.Cached.cachedHandles();
        stream.writeBytes(LENGTH_PLACEHOLDER); // fake size
        stream.writeLong(requestId);
        byte status = 0;
        status = setResponse(status);
        stream.writeByte(status); // 0 for request, 1 for response.
        message.writeTo(stream);
        byte[] data = ((BytesStreamOutput) stream.wrappedOut()).copiedByteArray();
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(data);
        buffer.setInt(0, buffer.writerIndex() - 4); // update real size.
        channel.write(buffer);
    }

    @Override public void sendResponse(Throwable error) throws IOException {
        BytesStreamOutput stream;
        try {
            stream = BytesStreamOutput.Cached.cached();
            writeResponseExceptionHeader(stream);
            RemoteTransportException tx = new RemoteTransportException(transport.nodeName(), transport.wrapAddress(channel.getLocalAddress()), action, error);
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(stream);
            too.writeObject(tx);
            too.close();
        } catch (NotSerializableException e) {
            stream = BytesStreamOutput.Cached.cached();
            writeResponseExceptionHeader(stream);
            RemoteTransportException tx = new RemoteTransportException(transport.nodeName(), transport.wrapAddress(channel.getLocalAddress()), action, new NotSerializableTransportException(error));
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(stream);
            too.writeObject(tx);
            too.close();
        }
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(stream.copiedByteArray());
        buffer.setInt(0, buffer.writerIndex() - 4); // update real size.
        channel.write(buffer);
    }

    private void writeResponseExceptionHeader(BytesStreamOutput stream) throws IOException {
        stream.writeBytes(LENGTH_PLACEHOLDER);
        stream.writeLong(requestId);
        byte status = 0;
        status = setResponse(status);
        status = setError(status);
        stream.writeByte(status);
    }
}
