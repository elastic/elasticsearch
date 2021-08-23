/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TcpTransportChannel implements TransportChannel {

    private final AtomicBoolean released = new AtomicBoolean();
    private final OutboundHandler outboundHandler;
    private final TcpChannel channel;
    private final String action;
    private final long requestId;
    private final Version version;
    private final Set<String> features;
    private final Compression.Scheme compressionScheme;
    private final boolean isHandshake;
    private final Releasable breakerRelease;

    TcpTransportChannel(OutboundHandler outboundHandler, TcpChannel channel, String action, long requestId, Version version,
                        Set<String> features, Compression.Scheme compressionScheme, boolean isHandshake, Releasable breakerRelease) {
        this.version = version;
        this.features = features;
        this.channel = channel;
        this.outboundHandler = outboundHandler;
        this.action = action;
        this.requestId = requestId;
        this.compressionScheme = compressionScheme;
        this.isHandshake = isHandshake;
        this.breakerRelease = breakerRelease;
    }

    @Override
    public String getProfileName() {
        return channel.getProfile();
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        try {
            outboundHandler.sendResponse(version, features, channel, requestId, action, response, compressionScheme, isHandshake);
        } finally {
            release(false);
        }
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            outboundHandler.sendErrorResponse(version, features, channel, requestId, action, exception);
        } finally {
            release(true);
        }
    }

    private Exception releaseBy;

    private void release(boolean isExceptionResponse) {
        if (released.compareAndSet(false, true)) {
            assert (releaseBy = new Exception()) != null; // easier to debug if it's already closed
            breakerRelease.close();
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

