/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Releasable;

public final class TcpTransportChannel implements TransportChannel {

    private final OutboundHandler outboundHandler;
    private final TcpChannel channel;
    private final String action;
    private final long requestId;
    private final TransportVersion version;
    private final Compression.Scheme compressionScheme;
    private final ResponseStatsConsumer responseStatsConsumer;
    private final boolean isHandshake;
    private final Releasable breakerRelease;

    TcpTransportChannel(
        OutboundHandler outboundHandler,
        TcpChannel channel,
        String action,
        long requestId,
        TransportVersion version,
        Compression.Scheme compressionScheme,
        ResponseStatsConsumer responseStatsConsumer,
        boolean isHandshake,
        Releasable breakerRelease
    ) {
        this.version = version;
        this.channel = channel;
        this.outboundHandler = outboundHandler;
        this.action = action;
        this.requestId = requestId;
        this.compressionScheme = compressionScheme;
        this.responseStatsConsumer = responseStatsConsumer;
        this.isHandshake = isHandshake;
        this.breakerRelease = breakerRelease;
    }

    @Override
    public String getProfileName() {
        return channel.getProfile();
    }

    @Override
    public void sendResponse(TransportResponse response) {
        try {
            outboundHandler.sendResponse(
                version,
                channel,
                requestId,
                action,
                response,
                compressionScheme,
                isHandshake,
                responseStatsConsumer
            );
        } finally {
            breakerRelease.close();
        }
    }

    @Override
    public void sendResponse(Exception exception) {
        try {
            outboundHandler.sendErrorResponse(version, channel, requestId, action, responseStatsConsumer, exception);
        } finally {
            breakerRelease.close();
        }
    }

    @Override
    public TransportVersion getVersion() {
        return version;
    }

    public TcpChannel getChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return Strings.format("TcpTransportChannel{req=%d}{%s}{%s}", requestId, action, channel);
    }
}
