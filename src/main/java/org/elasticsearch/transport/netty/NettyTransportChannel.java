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

import org.elasticsearch.Version;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.ThrowableObjectOutputStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.support.TransportStatus;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import java.io.IOException;
import java.io.NotSerializableException;

/**
 *
 */
public class NettyTransportChannel implements TransportChannel {

    private final NettyTransport transport;

    private final Version version;

    private final String action;

    private final Channel channel;

    private final long requestId;

    public NettyTransportChannel(NettyTransport transport, String action, Channel channel, long requestId, Version version) {
        this.version = version;
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
    public void sendResponse(TransportResponse response) throws IOException {
        sendResponse(response, TransportResponseOptions.EMPTY);
    }

    @Override
    public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
        if (transport.compress) {
            options.withCompress(true);
        }
        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();

        byte status = 0;
        status = TransportStatus.setResponse(status);

        if (options.compress()) {
            status = TransportStatus.setCompress(status);
            cachedEntry.bytes().skip(NettyHeader.HEADER_SIZE);
            StreamOutput stream = cachedEntry.handles(CompressorFactory.defaultCompressor());
            stream.setVersion(version);
            response.writeTo(stream);
            stream.close();
        } else {
            StreamOutput stream = cachedEntry.handles();
            stream.setVersion(version);
            cachedEntry.bytes().skip(NettyHeader.HEADER_SIZE);
            response.writeTo(stream);
            stream.close();
        }
        ChannelBuffer buffer = cachedEntry.bytes().bytes().toChannelBuffer();
        NettyHeader.writeHeader(buffer, requestId, status, version);
        ChannelFuture future = channel.write(buffer);
        future.addListener(new NettyTransport.CacheFutureListener(cachedEntry));
    }

    @Override
    public void sendResponse(Throwable error) throws IOException {
        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
        BytesStreamOutput stream;
        try {
            stream = cachedEntry.bytes();
            cachedEntry.bytes().skip(NettyHeader.HEADER_SIZE);
            RemoteTransportException tx = new RemoteTransportException(transport.nodeName(), transport.wrapAddress(channel.getLocalAddress()), action, error);
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(stream);
            too.writeObject(tx);
            too.close();
        } catch (NotSerializableException e) {
            cachedEntry.reset();
            stream = cachedEntry.bytes();
            cachedEntry.bytes().skip(NettyHeader.HEADER_SIZE);
            RemoteTransportException tx = new RemoteTransportException(transport.nodeName(), transport.wrapAddress(channel.getLocalAddress()), action, new NotSerializableTransportException(error));
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(stream);
            too.writeObject(tx);
            too.close();
        }

        byte status = 0;
        status = TransportStatus.setResponse(status);
        status = TransportStatus.setError(status);

        ChannelBuffer buffer = cachedEntry.bytes().bytes().toChannelBuffer();
        NettyHeader.writeHeader(buffer, requestId, status, version);
        ChannelFuture future = channel.write(buffer);
        future.addListener(new NettyTransport.CacheFutureListener(cachedEntry));
    }
}
