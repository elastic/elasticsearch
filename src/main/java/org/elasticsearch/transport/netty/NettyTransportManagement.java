/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.transport.netty;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.jmx.MBean;
import org.elasticsearch.jmx.ManagedAttribute;
import org.elasticsearch.transport.Transport;

/**
 *
 */
@MBean(objectName = "service=transport,transportType=netty", description = "Netty Transport")
public class NettyTransportManagement {

    private final NettyTransport transport;

    @Inject
    public NettyTransportManagement(Transport transport) {
        this.transport = (NettyTransport) transport;
    }

    @ManagedAttribute(description = "Number of connections this node has to other nodes")
    public long getNumberOfOutboundConnections() {
        return transport.connectedNodes.size();
    }

    @ManagedAttribute(description = "Number if IO worker threads")
    public int getWorkerCount() {
        return transport.workerCount;
    }

    @ManagedAttribute(description = "Port(s) netty was configured to bind on")
    public String getPort() {
        return transport.port;
    }

    @ManagedAttribute(description = "Host to bind to")
    public String getBindHost() {
        return transport.bindHost;
    }

    @ManagedAttribute(description = "Host to publish")
    public String getPublishHost() {
        return transport.publishHost;
    }

    @ManagedAttribute(description = "Connect timeout")
    public String getConnectTimeout() {
        return transport.connectTimeout.toString();
    }

    @ManagedAttribute(description = "TcpNoDelay")
    public Boolean getTcpNoDelay() {
        return transport.tcpNoDelay;
    }

    @ManagedAttribute(description = "TcpKeepAlive")
    public Boolean getTcpKeepAlive() {
        return transport.tcpKeepAlive;
    }

    @ManagedAttribute(description = "ReuseAddress")
    public Boolean getReuseAddress() {
        return transport.reuseAddress;
    }

    @ManagedAttribute(description = "TcpSendBufferSize")
    public String getTcpSendBufferSize() {
        if (transport.tcpSendBufferSize == null) {
            return null;
        }
        return transport.tcpSendBufferSize.toString();
    }

    @ManagedAttribute(description = "TcpReceiveBufferSize")
    public String getTcpReceiveBufferSize() {
        if (transport.tcpReceiveBufferSize == null) {
            return null;
        }
        return transport.tcpReceiveBufferSize.toString();
    }
}
