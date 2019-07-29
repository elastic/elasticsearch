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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TcpTransportChannel implements TransportChannel {

    private final AtomicBoolean released = new AtomicBoolean();
    private final OutboundHandler outboundHandler;
    private final TcpChannel channel;
    private final String action;
    private final long requestId;
    private final Version version;
    private final CircuitBreakerService breakerService;
    private final long reservedBytes;
    private final boolean compressResponse;

    TcpTransportChannel(OutboundHandler outboundHandler, TcpChannel channel, String action, long requestId, Version version,
                        CircuitBreakerService breakerService, long reservedBytes, boolean compressResponse) {
        this.version = version;
        this.channel = channel;
        this.outboundHandler = outboundHandler;
        this.action = action;
        this.requestId = requestId;
        this.breakerService = breakerService;
        this.reservedBytes = reservedBytes;
        this.compressResponse = compressResponse;
    }

    @Override
    public String getProfileName() {
        return channel.getProfile();
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        try {
            outboundHandler.sendResponse(version, channel, requestId, action, response, compressResponse, false);
        } finally {
            release(false);
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            outboundHandler.sendErrorResponse(version, channel, requestId, action, exception);
        } finally {
            release(true);
        }
    }

    private Exception releaseBy;

    private void release(boolean isExceptionResponse) {
        if (released.compareAndSet(false, true)) {
            assert (releaseBy = new Exception()) != null; // easier to debug if it's already closed
            breakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS).addWithoutBreaking(-reservedBytes);
        } else if (isExceptionResponse == false) {
            // only fail if we are not sending an error - we might send the error triggered by the previous
            // sendResponse call
            throw new IllegalStateException("reserved bytes are already released", releaseBy);
        }
    }

    @Override
    public String getChannelType() {
        return "transport";
    }

    @Override
    public Version getVersion() {
        return version;
    }

    public TcpChannel getChannel() {
        return channel;
    }

}

