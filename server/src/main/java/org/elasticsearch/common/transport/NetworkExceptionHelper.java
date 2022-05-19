/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.transport;

import org.apache.logging.log4j.Level;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;

public class NetworkExceptionHelper {

    public static boolean isConnectException(Throwable e) {
        return e instanceof ConnectException;
    }

    /**
     * @return a log level indicating an approximate severity of the exception, or {@link Level#OFF} if the exception doesn't look to be
     *         network-related.
     */
    public static Level getCloseConnectionExceptionLevel(Throwable e, boolean rstOnClose) {
        if (e instanceof ClosedChannelException) {
            // The channel is already closed for some reason, no need to shout about it.
            return Level.DEBUG;
        }

        // There's no great (portable) way to work out what exactly happened. However we can do some ugly but effective heuristics based on
        // the message contents if we assume we're in the root locale.
        final String message = e.getMessage();
        if (message != null) {

            // Messages from libc

            if (message.contains("Connection timed out")) {
                // Either we were sending some data and retransmitted it `net.ipv4.tcp_retries2` times without ever receiving an ACK, or
                // else the connection is idle and we sent `net.ipv4.tcp_keepalive_probes` consecutive keepalives without receiving any ACK.
                // This is often because some broken middleware (a firewall device or similar) starts dropping packets on established
                // connections due to an ill-considered timeout. However sometimes when Docker containers shut down they just totally
                // vanish from the network leading to this message too. Since it could be benign, we report it at INFO level.
                return Level.INFO;
            }
            if (message.contains("Connection reset")) {
                // We received a packet with the RST flag set. This is often caused by some broken middleware (a firewall device or similar)
                // which injects a RST into an established connection due to an ill-considered timeout. However it can also happen with
                // older TLS versions even if the connection is closed cleanly. Since it could be benign, we report it at INFO level.
                // We expect connection resets in tests (because we set SO_LINGER to 0 to avoid having too many connections in TIME_WAIT
                // state which exhausts the available set of ports) so in this case we push it down to DEBUG instead.
                return rstOnClose ? Level.DEBUG : Level.INFO;
            }
            if (message.contains("Broken pipe")) {
                // The channel was previously closed and then we tried to send some more data. Believed to be similar to "Connection reset"
                // so we keep it at INFO level.
                return Level.INFO;
            }

            // Messages from Windows

            if (message.contains("connection was aborted") || message.contains("forcibly closed")) {
                // These are how connection resets are reported on Windows. We haven't verified whether they're benign or not so we err
                // on the safe side.
                return Level.INFO;
            }

            // Messages from the JDK

            if (message.equals("Socket is closed") || message.equals("Socket closed")) {
                // The JDK saw us trying to interact with a socket that had already been closed. Kept at DEBUG since we haven't verified
                // whether it is benign or not and we prefer to avoid introducing noise.
                return Level.DEBUG;
            }

            // Messages from Netty

            if (message.equals("SSLEngine closed already")) {
                // Netty's SSLHandler discovered that the underlying channel closed without encountering a more specific exception that
                // indicates why. Kept at DEBUG since we haven't verified whether it is benign or not and we prefer to avoid introducing
                // noise.
                return Level.DEBUG;
            }
        }
        return Level.OFF;
    }
}
