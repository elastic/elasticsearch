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
import org.elasticsearch.util.io.ByteArrayDataOutputStream;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.io.ThrowableObjectOutputStream;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
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
        ByteArrayDataOutputStream stream = ByteArrayDataOutputStream.Cached.cached();
        stream.write(LENGTH_PLACEHOLDER); // fake size
        stream.writeLong(requestId);
        byte status = 0;
        status = setResponse(status);
        stream.writeByte(status); // 0 for request, 1 for response.
        message.writeTo(stream);
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(stream.copiedByteArray());
        buffer.setInt(0, buffer.writerIndex() - 4); // update real size.
        channel.write(buffer);
    }

    @Override public void sendResponse(Throwable error) throws IOException {
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        ChannelBufferOutputStream os = new ChannelBufferOutputStream(buffer);

        os.write(LENGTH_PLACEHOLDER);
        os.writeLong(requestId);

        byte status = 0;
        status = setResponse(status);
        status = setError(status);
        os.writeByte(status);

        // mark the buffer, so we can reset it when the exception is not serializable
        os.flush();
        buffer.markWriterIndex();
        try {
            RemoteTransportException tx = new RemoteTransportException(transport.settings().get("name"), transport.wrapAddress(channel.getLocalAddress()), action, error);
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(os);
            too.writeObject(tx);
            too.close();
        } catch (NotSerializableException e) {
            buffer.resetWriterIndex();
            RemoteTransportException tx = new RemoteTransportException(transport.settings().get("name"), transport.wrapAddress(channel.getLocalAddress()), action, new NotSerializableTransportException(error));
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(os);
            too.writeObject(tx);
            too.close();
        }

        buffer.setInt(0, buffer.writerIndex() - 4); // update real size.
        channel.write(buffer);
    }
}
