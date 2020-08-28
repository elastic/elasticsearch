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

package org.elasticsearch.nio;

import java.net.InetSocketAddress;

public abstract class Config {

    private final boolean tcpReuseAddress;

    public Config(boolean tcpReuseAddress) {
        this.tcpReuseAddress = tcpReuseAddress;
    }

    public boolean tcpReuseAddress() {
        return tcpReuseAddress;
    }

    public static class Socket extends Config {

        private final boolean tcpNoDelay;
        private final boolean tcpKeepAlive;
        private final int tcpKeepIdle;
        private final int tcpKeepInterval;
        private final int tcpKeepCount;
        private final int tcpSendBufferSize;
        private final int tcpReceiveBufferSize;
        private final InetSocketAddress remoteAddress;
        private final boolean isAccepted;

        public Socket(boolean tcpNoDelay, boolean tcpKeepAlive, int tcpKeepIdle, int tcpKeepInterval, int tcpKeepCount,
                      boolean tcpReuseAddress, int tcpSendBufferSize, int tcpReceiveBufferSize,
                      InetSocketAddress remoteAddress, boolean isAccepted) {
            super(tcpReuseAddress);
            this.tcpNoDelay = tcpNoDelay;
            this.tcpKeepAlive = tcpKeepAlive;
            this.tcpKeepIdle = tcpKeepIdle;
            this.tcpKeepInterval = tcpKeepInterval;
            this.tcpKeepCount = tcpKeepCount;
            this.tcpSendBufferSize = tcpSendBufferSize;
            this.tcpReceiveBufferSize = tcpReceiveBufferSize;
            this.remoteAddress = remoteAddress;
            this.isAccepted = isAccepted;
        }

        public boolean tcpNoDelay() {
            return tcpNoDelay;
        }

        public boolean tcpKeepAlive() {
            return tcpKeepAlive;
        }

        public int tcpKeepIdle() {
            return tcpKeepIdle;
        }

        public int tcpKeepInterval() {
            return tcpKeepInterval;
        }

        public int tcpKeepCount() {
            return tcpKeepCount;
        }

        public int tcpSendBufferSize() {
            return tcpSendBufferSize;
        }

        public int tcpReceiveBufferSize() {
            return tcpReceiveBufferSize;
        }

        public boolean isAccepted() {
            return isAccepted;
        }

        public InetSocketAddress getRemoteAddress() {
            return remoteAddress;
        }
    }

    public static class ServerSocket extends Config {

        private InetSocketAddress localAddress;

        public ServerSocket(boolean tcpReuseAddress, InetSocketAddress localAddress) {
            super(tcpReuseAddress);
            this.localAddress = localAddress;
        }

        public InetSocketAddress getLocalAddress() {
            return localAddress;
        }
    }
}
