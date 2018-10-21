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
        }
        return false;
    }
}
