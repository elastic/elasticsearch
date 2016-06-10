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

package org.elasticsearch.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.netty.ReleaseChannelFutureListener;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseOptions;
import org.elasticsearch.transport.TransportServiceAdapter;
import org.elasticsearch.transport.support.TransportStatus;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class NettyTransportChannel implements TransportChannel {

    private final NettyTransport transport;
    private final TransportServiceAdapter transportServiceAdapter;
    private final Version version;
    private final String action;
    private final Channel channel;
    private final long requestId;
    private final String profileName;
    private final long reservedBytes;
    private final AtomicBoolean released = new AtomicBoolean();

    public NettyTransportChannel(NettyTransport transport, TransportServiceAdapter transportServiceAdapter, String action, Channel channel,
                                 long requestId, Version version, String profileName, long reservedBytes) {
        this.transportServiceAdapter = transportServiceAdapter;
        this.version = version;
        this.transport = transport;
        this.action = action;
        this.channel = channel;
        this.requestId = requestId;
        this.profileName = profileName;
        this.reservedBytes = reservedBytes;
    }

    @Override
    public String getProfileName() {
        return profileName;
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
        release();
        if (transport.compress) {
            options = TransportResponseOptions.builder(options).withCompress(transport.compress).build();
        }

        byte status = 0;
        status = TransportStatus.setResponse(status);

        ReleasableBytesStreamOutput bStream = null;
        boolean addedReleaseListener = false;
        try {
            bStream = new ReleasableBytesStreamOutput(transport.bigArrays);
            bStream.skip(NettyHeader.HEADER_SIZE);
            StreamOutput stream = bStream;
            if (options.compress()) {
                status = TransportStatus.setCompress(status);
                stream = CompressorFactory.defaultCompressor().streamOutput(stream);
            }
            stream.setVersion(version);
            response.writeTo(stream);
            stream.close();

            ReleasablePagedBytesReference bytes = bStream.bytes();
            ChannelBuffer buffer = bytes.toChannelBuffer();
            NettyHeader.writeHeader(buffer, requestId, status, version);
            ChannelFuture future = channel.write(buffer);
            ReleaseChannelFutureListener listener = new ReleaseChannelFutureListener(bytes);
            future.addListener(listener);
            addedReleaseListener = true;
            final TransportResponseOptions finalOptions = options;
            ChannelFutureListener onResponseSentListener =
                f -> transportServiceAdapter.onResponseSent(requestId, action, response, finalOptions);
            future.addListener(onResponseSentListener);
        } finally {
            if (!addedReleaseListener && bStream != null) {
                Releasables.close(bStream.bytes());
            }
        }
    }

    @Override
    public void sendResponse(Throwable error) throws IOException {
        release();
        BytesStreamOutput stream = new BytesStreamOutput();
        stream.skip(NettyHeader.HEADER_SIZE);
        RemoteTransportException tx = new RemoteTransportException(
            transport.nodeName(), transport.wrapAddress(channel.getLocalAddress()), action, error);
        stream.writeThrowable(tx);
        byte status = 0;
        status = TransportStatus.setResponse(status);
        status = TransportStatus.setError(status);

        BytesReference bytes = stream.bytes();
        ChannelBuffer buffer = bytes.toChannelBuffer();
        NettyHeader.writeHeader(buffer, requestId, status, version);
        ChannelFuture future = channel.write(buffer);
        ChannelFutureListener onResponseSentListener =
            f -> transportServiceAdapter.onResponseSent(requestId, action, error);
        future.addListener(onResponseSentListener);
    }

    private void release() {
        // attempt to release once atomically
        if (released.compareAndSet(false, true) == false) {
            throw new IllegalStateException("reserved bytes are already released");
        }
        transport.inFlightRequestsBreaker().addWithoutBreaking(-reservedBytes);
    }

    @Override
    public long getRequestId() {
        return requestId;
    }

    @Override
    public String getChannelType() {
        return "netty";
    }

    /**
     * Returns the underlying netty channel. This method is intended be used for access to netty to get additional
     * details when processing the request and may be used by plugins. Responses should be sent using the methods
     * defined in this class and not directly on the channel.
     * @return underlying netty channel
     */
    public Channel getChannel() {
        return channel;
    }

}
