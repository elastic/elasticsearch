/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */
package org.elasticsearch.util.http.client;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;

/**
 * Represents a proxy server.
 */
public class ProxyServer {
    public enum Protocol {
        HTTP("http"), HTTPS("https");

        private final String protocol;

        private Protocol(final String protocol) {
            this.protocol = protocol;
        }

        public String getProtocol() {
            return protocol;
        }

        @Override
        public String toString() {
            return getProtocol();
        }
    }

    private final Protocol protocol;
    private final String host;
    private int port;

    public ProxyServer(final Protocol protocol, final String host, final int port) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
    }

    public ProxyServer(final String host, final int port) {
        this(Protocol.HTTP, host, port);
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public String getProtocolAsString() {
        return protocol.toString();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    /**
     * Convert from Java java.net.Proxy object.
     *
     * @param proxy
     * @return A ProxyServer object or null if the proxy object can not converted.
     */
    public static final ProxyServer fromProxy(final Proxy proxy) {
        if (proxy == null || proxy.type() == Proxy.Type.DIRECT) {
            return null;
        }

        if (proxy.type() != Proxy.Type.HTTP) {
            throw new IllegalArgumentException("Only DIRECT and HTTP Proxies are supported!");
        }

        final SocketAddress sa = proxy.address();

        if (!(sa instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("Only Internet Address sockets are supported!");
        }

        InetSocketAddress isa = (InetSocketAddress) sa;

        if (isa.isUnresolved()) {
            return new ProxyServer(isa.getHostName(), isa.getPort());
        } else {
            return new ProxyServer(isa.getAddress().getHostAddress(), isa.getPort());
        }
    }

    @Override
    public String toString() {
        return String.format("%s://%s:%d", protocol.toString(), host, port);
    }
}

