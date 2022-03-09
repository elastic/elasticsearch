/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.readiness;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.transport.BindTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.node.Node.WRITE_PORTS_FILE_SETTING;
import static org.elasticsearch.node.Node.writePortsFile;

public class ReadinessService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(ReadinessService.class);

    private final Environment environment;

    private volatile boolean active = false;
    private volatile ServerSocketChannel serverChannel;
    final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();

    public static final Setting<String> PORT = new Setting<>(
        "readiness.port",
        "9400-9500",
        Function.identity(),
        Setting.Property.NodeScope
    );

    public ReadinessService(ClusterService clusterService, Environment environment) {
        this.serverChannel = null;
        this.environment = environment;
        clusterService.addListener(this);
    }

    // package private for testing
    boolean ready() {
        return this.serverChannel != null;
    }

    // package private for testing
    ServerSocketChannel serverChannel() {
        return serverChannel;
    }

    public BoundTransportAddress boundAddress() {
        TransportAddress publishAddress = new TransportAddress(boundSocket.get());
        return new BoundTransportAddress(new TransportAddress[] { publishAddress }, publishAddress);
    }

    ServerSocketChannel setupSocket() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            if (boundSocket.get() != null) {
                try {
                    serverChannel = ServerSocketChannel.open(StandardProtocolFamily.INET);
                    serverChannel.bind(boundSocket.get());
                } catch (Exception e) {
                    throw new BindTransportException("Failed to re-bind to " + boundSocket.get(), e);
                }

                return null;
            }

            final AtomicReference<Exception> lastException = new AtomicReference<>();

            PortsRange portsRange = new PortsRange(PORT.get(environment.settings()));
            InetAddress localhost = InetAddress.getLoopbackAddress();

            boolean success = portsRange.iterate(portNumber -> {
                try {
                    InetSocketAddress socketAddress = new InetSocketAddress(localhost, portNumber);
                    serverChannel = ServerSocketChannel.open(StandardProtocolFamily.INET);
                    serverChannel.bind(socketAddress);

                    boundSocket.set(socketAddress);
                } catch (Exception e) {
                    lastException.set(e);
                    return false;
                }
                return true;
            });

            if (success == false) {
                throw new BindTransportException("Failed to bind to " + NetworkAddress.format(localhost, portsRange), lastException.get());
            }

            return null;
        });

        return serverChannel;
    }

    @Override
    protected void doStart() {
        // Mark the service as active, we'll start the listener when ES is ready
        this.active = true;
    }

    // package private for testing
    synchronized void startListener() {
        if (this.serverChannel != null || this.active == false) {
            return;
        }

        this.serverChannel = setupSocket();

        new Thread(() -> {
            while (serverChannel != null && serverChannel.isOpen()) {
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    try (SocketChannel channel = serverChannel.accept()) {} catch (IOException e) {
                        logger.debug("encountered exception while responding to readiness check request", e);
                    } catch (Exception other) {
                        logger.warn("encountered unknown exception while responding to readiness check request", other);
                    }
                    return null;
                });
            }
        }, "elasticsearch[readiness-service]").start();

        logger.info("readiness service up and running on {}", boundAddress().publishAddress());

        if (WRITE_PORTS_FILE_SETTING.get(environment.settings())) {
            writePortsFile(environment, "readiness", boundAddress());
        }
    }

    @Override
    protected void doStop() {
        this.active = false;
        stopListener();
    }

    // package private for testing
    synchronized void stopListener() {
        try {
            if (this.serverChannel != null) {
                this.serverChannel.close();
            }
        } catch (IOException e) {
            logger.warn("error closing readiness service channel", e);
        } finally {
            this.serverChannel = null;
            logger.info("readiness service stopped");
        }
    }

    @Override
    protected void doClose() {}

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState clusterState = event.state();

        Set<String> shutdownNodeIds = PluginShutdownService.shutdownNodes(clusterState);
        if (shutdownNodeIds.contains(clusterState.nodes().getLocalNodeId())) {
            setReady(false);
            logger.info("marking node as not ready because it's shutting down");
        } else {
            setReady(clusterState.nodes().getMasterNodeId() != null);
        }
    }

    private void setReady(boolean ready) {
        if (ready) {
            startListener();
        } else {
            stopListener();
        }
    }
}
