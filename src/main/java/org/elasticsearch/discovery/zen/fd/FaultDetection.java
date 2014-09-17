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
package org.elasticsearch.discovery.zen.fd;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 * A base class for {@link org.elasticsearch.discovery.zen.fd.MasterFaultDetection} & {@link org.elasticsearch.discovery.zen.fd.NodesFaultDetection},
 * making sure both use the same setting.
 */
public abstract class FaultDetection extends AbstractComponent {

    public static final String SETTING_CONNECT_ON_NETWORK_DISCONNECT = "discovery.zen.fd.connect_on_network_disconnect";
    public static final String SETTING_PING_INTERVAL = "discovery.zen.fd.ping_interval";
    public static final String SETTING_PING_TIMEOUT = "discovery.zen.fd.ping_timeout";
    public static final String SETTING_PING_RETRIES = "discovery.zen.fd.ping_retries";
    public static final String SETTING_REGISTER_CONNECTION_LISTENER = "discovery.zen.fd.register_connection_listener";

    protected final ThreadPool threadPool;
    protected final ClusterName clusterName;
    protected final TransportService transportService;

    // used mainly for testing, should always be true
    protected final boolean registerConnectionListener;
    protected final FDConnectionListener connectionListener;
    protected final boolean connectOnNetworkDisconnect;

    protected final TimeValue pingInterval;
    protected final TimeValue pingRetryTimeout;
    protected final int pingRetryCount;

    public FaultDetection(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterName clusterName) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterName = clusterName;

        this.connectOnNetworkDisconnect = settings.getAsBoolean(SETTING_CONNECT_ON_NETWORK_DISCONNECT, false);
        this.pingInterval = settings.getAsTime(SETTING_PING_INTERVAL, timeValueSeconds(1));
        this.pingRetryTimeout = settings.getAsTime(SETTING_PING_TIMEOUT, timeValueSeconds(30));
        this.pingRetryCount = settings.getAsInt(SETTING_PING_RETRIES, 3);
        this.registerConnectionListener = settings.getAsBoolean(SETTING_REGISTER_CONNECTION_LISTENER, true);

        this.connectionListener = new FDConnectionListener();
        if (registerConnectionListener) {
            transportService.addConnectionListener(connectionListener);
        }
    }

    public void close() {
        transportService.removeConnectionListener(connectionListener);
    }

    /**
     * This method will be called when the {@link org.elasticsearch.transport.TransportService} raised a node disconnected event
     */
    abstract void handleTransportDisconnect(DiscoveryNode node);

    private class FDConnectionListener implements TransportConnectionListener {
        @Override
        public void onNodeConnected(DiscoveryNode node) {
        }

        @Override
        public void onNodeDisconnected(DiscoveryNode node) {
            handleTransportDisconnect(node);
        }
    }

}
