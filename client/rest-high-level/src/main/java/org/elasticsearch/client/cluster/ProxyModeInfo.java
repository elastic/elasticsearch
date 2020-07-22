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

package org.elasticsearch.client.cluster;

import java.util.Objects;

public class ProxyModeInfo implements RemoteConnectionInfo.ModeInfo {
    static final String NAME = "proxy";
    static final String PROXY_ADDRESS = "proxy_address";
    static final String SERVER_NAME = "server_name";
    static final String NUM_PROXY_SOCKETS_CONNECTED = "num_proxy_sockets_connected";
    static final String MAX_PROXY_SOCKET_CONNECTIONS = "max_proxy_socket_connections";
    private final String address;
    private final String serverName;
    private final int maxSocketConnections;
    private final int numSocketsConnected;

    ProxyModeInfo(String address, String serverName, int maxSocketConnections, int numSocketsConnected) {
        this.address = address;
        this.serverName = serverName;
        this.maxSocketConnections = maxSocketConnections;
        this.numSocketsConnected = numSocketsConnected;
    }

    @Override
    public boolean isConnected() {
        return numSocketsConnected > 0;
    }

    @Override
    public String modeName() {
        return NAME;
    }

    public String getAddress() {
        return address;
    }

    public String getServerName() {
        return serverName;
    }

    public int getMaxSocketConnections() {
        return maxSocketConnections;
    }

    public int getNumSocketsConnected() {
        return numSocketsConnected;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProxyModeInfo otherProxy = (ProxyModeInfo) o;
        return maxSocketConnections == otherProxy.maxSocketConnections &&
                numSocketsConnected == otherProxy.numSocketsConnected &&
                Objects.equals(address, otherProxy.address) &&
                Objects.equals(serverName, otherProxy.serverName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, serverName, maxSocketConnections, numSocketsConnected);
    }
}
