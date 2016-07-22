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
package org.elasticsearch.transport;

import org.elasticsearch.Version;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TcpTransportChannel<Channel> implements TransportChannel {
    private final TcpTransport<Channel> transport;
    protected final Version version;
    protected final String action;
    protected final long requestId;
    private final String profileName;
    private final long reservedBytes;
    private final AtomicBoolean released = new AtomicBoolean();
    private final String channelType;
    private final Channel channel;

    public TcpTransportChannel(TcpTransport<Channel> transport, Channel channel, String channelType, String action,
                               long requestId, Version version, String profileName, long reservedBytes) {
        this.version = version;
        this.channel = channel;
        this.transport = transport;
        this.action = action;
        this.requestId = requestId;
        this.profileName = profileName;
        this.reservedBytes = reservedBytes;
        this.channelType = channelType;
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
        try {
            transport.sendResponse(version, channel, response, requestId, action, options);
        } finally {
            release();
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            transport.sendErrorResponse(version, channel, exception, requestId, action);
        } finally {
            release();
        }
    }
    private Exception releaseBy;

    private void release() {
        // attempt to release once atomically
        if (released.compareAndSet(false, true) == false) {
            throw new IllegalStateException("reserved bytes are already released", releaseBy);
        } else {
            assert (releaseBy = new Exception()) != null; // easier to debug if it's already closed
        }
        transport.getInFlightRequestBreaker().addWithoutBreaking(-reservedBytes);
    }

    @Override
    public long getRequestId() {
        return requestId;
    }

    @Override
    public String getChannelType() {
        return channelType;
    }

    public Channel getChannel() {
        return channel;
    }

}

