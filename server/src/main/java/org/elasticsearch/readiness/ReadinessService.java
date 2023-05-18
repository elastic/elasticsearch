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
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.reservedstate.service.FileChangedListener;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.transport.BindTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class ReadinessService extends AbstractLifecycleComponent implements ClusterStateListener, FileChangedListener {
    private static final Logger logger = LogManager.getLogger(ReadinessService.class);

    private final Environment environment;

    private volatile boolean active; // false;
    private volatile ServerSocketChannel serverChannel;
    // package private for testing
    volatile CountDownLatch listenerThreadLatch = new CountDownLatch(0);
    final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
    private final Collection<BoundAddressListener> boundAddressListeners = new CopyOnWriteArrayList<>();
    private volatile boolean fileSettingsApplied = false;
    private volatile boolean masterElected = false;
    private volatile boolean shuttingDown = false;

    public static final Setting<Integer> PORT = Setting.intSetting("readiness.port", -1, Setting.Property.NodeScope);

    public ReadinessService(ClusterService clusterService, Environment environment) {
        this.serverChannel = null;
        this.environment = environment;
        clusterService.addListener(this);
    }

    // package private for testing
    boolean ready() {
        return this.serverChannel != null;
    }

    /**
     * Checks to see if the readiness service is enabled in the current environment
     * @param environment
     * @return
     */
    public static boolean enabled(Environment environment) {
        return PORT.get(environment.settings()) != -1;
    }

    // package private for testing
    ServerSocketChannel serverChannel() {
        return serverChannel;
    }

    /**
     * Returns the current bound address for the readiness service.
     * If Elasticsearch was never ready, this method will return null.
     * @return the bound address for the readiness service
     */
    public BoundTransportAddress boundAddress() {
        InetSocketAddress boundAddress = boundSocket.get();
        if (boundAddress == null) {
            return null;
        }
        TransportAddress publishAddress = new TransportAddress(boundAddress);
        return new BoundTransportAddress(new TransportAddress[] { publishAddress }, publishAddress);
    }

    // package private for testing
    InetSocketAddress socketAddress(InetAddress host, int portNumber) {
        // If we have previously bound to a specific port, we always rebind to the same one.
        var socketAddress = boundSocket.get();
        if (socketAddress == null) {
            socketAddress = new InetSocketAddress(host, portNumber);
        }

        return socketAddress;
    }

    // package private for testing
    ServerSocketChannel setupSocket() {
        var settings = environment.settings();
        int portNumber = PORT.get(settings);
        assert portNumber >= 0;

        var socketAddress = AccessController.doPrivileged((PrivilegedAction<InetSocketAddress>) () -> {
            try {
                return socketAddress(InetAddress.getByName("0"), portNumber);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to resolve readiness host address", e);
            }
        });

        try {
            serverChannel = ServerSocketChannel.open();

            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    serverChannel.bind(socketAddress);
                } catch (IOException e) {
                    throw new BindTransportException("Failed to bind to " + NetworkAddress.format(socketAddress), e);
                }
                return null;
            });

            // First time bounding the socket, we notify any listeners
            if (boundSocket.get() == null) {
                boundSocket.set((InetSocketAddress) serverChannel.getLocalAddress());

                // Address bound event is only sent on first bind.
                BoundTransportAddress boundAddress = boundAddress();
                for (BoundAddressListener listener : boundAddressListeners) {
                    listener.addressBound(boundAddress);
                }
            }
        } catch (Exception e) {
            throw new BindTransportException("Failed to open socket channel " + NetworkAddress.format(socketAddress), e);
        }

        return serverChannel;
    }

    @Override
    protected void doStart() {
        // Mark the service as active, we'll start the listener when ES is ready
        this.active = true;
    }

    // package private for testing
    synchronized void startListener() {
        assert enabled(environment);

        if (this.serverChannel != null || this.active == false) {
            return;
        }

        this.serverChannel = setupSocket();
        this.listenerThreadLatch = new CountDownLatch(1);

        new Thread(() -> {
            assert serverChannel != null;
            try {
                while (serverChannel.isOpen()) {
                    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                        try (SocketChannel channel = serverChannel.accept()) {} catch (IOException e) {
                            logger.debug("encountered exception while responding to readiness check request", e);
                        } catch (Exception other) {
                            logger.warn("encountered unknown exception while responding to readiness check request", other);
                        }
                        return null;
                    });
                }
            } finally {
                listenerThreadLatch.countDown();
            }
        }, "elasticsearch[readiness-service]").start();

        logger.info("readiness service up and running on {}", boundAddress().publishAddress());
    }

    @Override
    protected void doStop() {
        this.active = false;
        stopListener();
    }

    // package private for testing
    synchronized void stopListener() {
        assert enabled(environment);

        // Avoid unnecessary logging if stop is repeatedly called.
        // This can happen because we call stop listener on cluster state updates.
        if (ready() == false) {
            return;
        }

        try {
            logger.info(
                "stopping readiness service on channel {}",
                (this.serverChannel == null) ? "None" : this.serverChannel.getLocalAddress()
            );
            if (this.serverChannel != null) {
                this.serverChannel.close();
                listenerThreadLatch.await();
            }
        } catch (InterruptedException | IOException e) {
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

        this.masterElected = clusterState.nodes().getMasterNodeId() != null;
        this.shuttingDown = shutdownNodeIds.contains(clusterState.nodes().getLocalNodeId());

        if (shuttingDown) {
            setReady(false);
            logger.info("marking node as not ready because it's shutting down");
        } else {
            if (clusterState.nodes().getLocalNodeId().equals(clusterState.nodes().getMasterNodeId())) {
                setReady(fileSettingsApplied);
            } else {
                setReady(masterElected);
            }
        }
    }

    private void setReady(boolean ready) {
        if (ready) {
            startListener();
        } else {
            stopListener();
        }
    }

    /**
     * Add a listener for bound readiness service address.
     * @param listener
     */
    public synchronized void addBoundAddressListener(BoundAddressListener listener) {
        // this expects that setupSocket is called within a synchronized method
        var b = boundAddress();
        if (b != null) {
            listener.addressBound(b);
        }
        boundAddressListeners.add(listener);
    }

    @Override
    public void watchedFileChanged() {
        fileSettingsApplied = true;
        setReady(masterElected && (shuttingDown == false));
    }

    /**
     * A listener to be notified when the readiness service establishes the port it's listening on.
     * The {@link #addressBound(BoundTransportAddress)} method is called after the readiness service socket
     * is up and listening.
     */
    public interface BoundAddressListener {
        /**
         * This method is going to be called only the first time the address is bound. The readiness service
         * always binds to the same port it did initially. Subsequent changes to ready from not-ready states will
         * not send this notification.
         * @param address
         */
        void addressBound(BoundTransportAddress address);
    }
}
