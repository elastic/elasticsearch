/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.transport;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.transport.TcpChannel;

import java.util.function.BiConsumer;

public final class SecurityTransportExceptionHandler implements BiConsumer<TcpChannel, Exception> {

    private final Lifecycle lifecycle;
    private final Logger logger;
    private final BiConsumer<TcpChannel, Exception> fallback;

    public SecurityTransportExceptionHandler(Logger logger, Lifecycle lifecycle, BiConsumer<TcpChannel, Exception> fallback) {
        this.lifecycle = lifecycle;
        this.logger = logger;
        this.fallback = fallback;
    }

    public void accept(TcpChannel channel, Exception e) {
        if (!lifecycle.started()) {
            // just close and ignore - we are already stopped and just need to make sure we release all resources
            CloseableChannel.closeChannel(channel);
        } else if (SSLExceptionHelper.isNotSslRecordException(e)) {
            logger.warn("received plaintext traffic on an encrypted channel, closing connection {}", channel);
            CloseableChannel.closeChannel(channel);
        } else if (SSLExceptionHelper.isCloseDuringHandshakeException(e)) {
            logger.debug("connection {} closed during handshake", channel);
            CloseableChannel.closeChannel(channel);
        } else if (SSLExceptionHelper.isReceivedCertificateUnknownException(e)) {
            logger.warn("client did not trust this server's certificate, closing connection {}", channel);
            CloseableChannel.closeChannel(channel);
        } else {
            fallback.accept(channel, e);
        }
    }
}
