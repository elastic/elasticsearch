/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.transport;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;

public class NetworkExceptionHelper {

    public static boolean isConnectException(Throwable e) {
        if (e instanceof ConnectException) {
            return true;
        }
        return false;
    }

    public static boolean isCloseConnectionException(Throwable e) {
        if (e instanceof ClosedChannelException) {
            return true;
        }
        if (e.getMessage() != null) {
            // UGLY!, this exception messages seems to represent closed connection
            if (e.getMessage().contains("Connection reset")) {
                return true;
            }
            if (e.getMessage().contains("connection was aborted")) {
                return true;
            }
            if (e.getMessage().contains("forcibly closed")) {
                return true;
            }
            if (e.getMessage().contains("Broken pipe")) {
                return true;
            }
            if (e.getMessage().contains("Connection timed out")) {
                return true;
            }
            if (e.getMessage().equals("Socket is closed")) {
                return true;
            }
            if (e.getMessage().equals("Socket closed")) {
                return true;
            }
            if (e.getMessage().equals("SSLEngine closed already")) {
                return true;
            }
        }
        return false;
    }
}
