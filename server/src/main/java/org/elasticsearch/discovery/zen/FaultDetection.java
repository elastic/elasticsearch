/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;

/**
 * A base class for {@link MasterFaultDetection} &amp; {@link NodesFaultDetection},
 * making sure both use the same setting.
 */
public abstract class FaultDetection implements Closeable {

    private static final Logger logger = LogManager.getLogger(FaultDetection.class);

    public static final Setting<Boolean> CONNECT_ON_NETWORK_DISCONNECT_SETTING = Setting.boolSetting(
        "discovery.zen.fd.connect_on_network_disconnect",
        false,
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<TimeValue> PING_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "discovery.zen.fd.ping_interval",
        timeValueSeconds(1),
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<TimeValue> PING_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.zen.fd.ping_timeout",
        timeValueSeconds(30),
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<Integer> PING_RETRIES_SETTING = Setting.intSetting(
        "discovery.zen.fd.ping_retries",
        3,
        Property.NodeScope,
        Property.Deprecated
    );
    public static final Setting<Boolean> REGISTER_CONNECTION_LISTENER_SETTING = Setting.boolSetting(
        "discovery.zen.fd.register_connection_listener",
        true,
        Property.NodeScope,
        Property.Deprecated
    );

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
        public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
            AbstractRunnable runnable = new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to handle transport disconnect for node: {}", node);
                }

                @Override
                protected void doRun() {
                    handleTransportDisconnect(node);
                }
            };
            threadPool.generic().execute(runnable);
        }
    }

}
