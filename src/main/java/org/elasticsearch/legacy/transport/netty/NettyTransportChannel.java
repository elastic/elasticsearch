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

package org.elasticsearch.legacy.transport.netty;

import org.elasticsearch.legacy.Version;
import org.elasticsearch.legacy.common.bytes.BytesReference;
import org.elasticsearch.legacy.common.bytes.ReleasableBytesReference;
import org.elasticsearch.legacy.common.compress.CompressorFactory;
import org.elasticsearch.legacy.common.io.ThrowableObjectOutputStream;
import org.elasticsearch.legacy.common.io.stream.BytesStreamOutput;
import org.elasticsearch.legacy.common.io.stream.HandlesStreamOutput;
import org.elasticsearch.legacy.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.legacy.common.io.stream.StreamOutput;
import org.elasticsearch.legacy.common.lease.Releasables;
import org.elasticsearch.legacy.common.netty.ReleaseChannelFutureListener;
import org.elasticsearch.legacy.transport.*;
import org.elasticsearch.legacy.transport.support.TransportStatus;
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

        byte status = 0;
        status = TransportStatus.setResponse(status);

        ReleasableBytesStreamOutput bStream = new ReleasableBytesStreamOutput(transport.bigArrays);
        boolean addedReleaseListener = false;
        try {
            bStream.skip(NettyHeader.HEADER_SIZE);
            StreamOutput stream = bStream;
            if (options.compress()) {
                status = TransportStatus.setCompress(status);
                stream = CompressorFactory.defaultCompressor().streamOutput(stream);
            }
            stream = new HandlesStreamOutput(stream);
            stream.setVersion(version);
            response.writeTo(stream);
            stream.close();

            ReleasableBytesReference bytes = bStream.bytes();
            ChannelBuffer buffer = bytes.toChannelBuffer();
            NettyHeader.writeHeader(buffer, requestId, status, version);
            ChannelFuture future = channel.write(buffer);
            ReleaseChannelFutureListener listener = new ReleaseChannelFutureListener(bytes);
            future.addListener(listener);
            addedReleaseListener = true;
        } finally {
            if (!addedReleaseListener) {
                Releasables.close(bStream.bytes());
            }
        }
    }

    @Override
    public void sendResponse(Throwable error) throws IOException {
        BytesStreamOutput stream = new BytesStreamOutput();
        try {
            stream.skip(NettyHeader.HEADER_SIZE);
            RemoteTransportException tx = new RemoteTransportException(transport.nodeName(), transport.wrapAddress(channel.getLocalAddress()), action, error);
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(stream);
            too.writeObject(tx);
            too.close();
        } catch (NotSerializableException e) {
            stream.reset();
            stream.skip(NettyHeader.HEADER_SIZE);
            RemoteTransportException tx = new RemoteTransportException(transport.nodeName(), transport.wrapAddress(channel.getLocalAddress()), action, new NotSerializableTransportException(error));
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(stream);
            too.writeObject(tx);
            too.close();
        }

        byte status = 0;
        status = TransportStatus.setResponse(status);
        status = TransportStatus.setError(status);

        BytesReference bytes = stream.bytes();
        ChannelBuffer buffer = bytes.toChannelBuffer();
        NettyHeader.writeHeader(buffer, requestId, status, version);
        channel.write(buffer);
    }
}
