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

package org.elasticsearch.discovery.zen;

import java.io.Closeable;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 * A base class for {@link MasterFaultDetection} &amp; {@link NodesFaultDetection},
 * making sure both use the same setting.
 */
public abstract class FaultDetection extends AbstractComponent implements Closeable {

    public static final Setting<Boolean> CONNECT_ON_NETWORK_DISCONNECT_SETTING =
        Setting.boolSetting("discovery.zen.fd.connect_on_network_disconnect", false, Property.NodeScope);
    public static final Setting<TimeValue> PING_INTERVAL_SETTING =
        Setting.positiveTimeSetting("discovery.zen.fd.ping_interval", timeValueSeconds(1), Property.NodeScope);
    public static final Setting<TimeValue> PING_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen.fd.ping_timeout", timeValueSeconds(30), Property.NodeScope);
    public static final Setting<Integer> PING_RETRIES_SETTING =
        Setting.intSetting("discovery.zen.fd.ping_retries", 3, Property.NodeScope);
    public static final Setting<Boolean> REGISTER_CONNECTION_LISTENER_SETTING =
        Setting.boolSetting("discovery.zen.fd.register_connection_listener", true, Property.NodeScope);

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

        this.connectOnNetworkDisconnect = CONNECT_ON_NETWORK_DISCONNECT_SETTING.get(settings);
        this.pingInterval = PING_INTERVAL_SETTING.get(settings);
        this.pingRetryTimeout = PING_TIMEOUT_SETTING.get(settings);
        this.pingRetryCount = PING_RETRIES_SETTING.get(settings);
        this.registerConnectionListener = REGISTER_CONNECTION_LISTENER_SETTING.get(settings);

        this.connectionListener = new FDConnectionListener();
        if (registerConnectionListener) {
            transportService.addConnectionListener(connectionListener);
        }
    }

    @Override
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
