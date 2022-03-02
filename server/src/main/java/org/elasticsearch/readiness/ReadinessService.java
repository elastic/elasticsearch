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
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.transport.BindTransportException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ReadinessService extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(ReadinessService.class);
    private static final int RESPONSE_TIMEOUT_MILLIS = 1_000;

    private final Environment environment;
    private final ExecutorService workerExecutor;

    private volatile ServerSocketChannel serverChannel;

    private volatile boolean ready = false;
    private volatile BoundTransportAddress boundAddress;

    public static final Setting<String> PORT = new Setting<>(
        "readiness.port",
        "9400-9500",
        Function.identity(),
        Setting.Property.NodeScope
    );

    private final HttpServerTransport httpTransport;

    public ReadinessService(ClusterService clusterService, Environment environment, HttpServerTransport httpTransport) {
        this.httpTransport = httpTransport;
        this.serverChannel = null;
        this.environment = environment;
        clusterService.addListener(this);

        this.workerExecutor = EsExecutors.newScaling(
            "readiness-worker",
            0,
            EsExecutors.allocatedProcessors(environment.settings()) * 2,
            60,
            TimeUnit.SECONDS,
            false,
            EsExecutors.daemonThreadFactory("elasticsearch[readiness-worker]"),
            new ThreadContext(environment.settings())
        );
    }

    boolean ready() {
        return ready;
    }

    ServerSocketChannel serverChannel() {
        return serverChannel;
    }

    public BoundTransportAddress boundAddress() {
        return boundAddress;
    }

    ServerSocketChannel setupSocket() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            final AtomicReference<Exception> lastException = new AtomicReference<>();
            final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();

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

            TransportAddress publishAddress = new TransportAddress(boundSocket.get());
            boundAddress = new BoundTransportAddress(new TransportAddress[] { publishAddress }, publishAddress);

            return null;
        });

        return serverChannel;
    }

    @Override
    protected void doStart() {
        this.serverChannel = setupSocket();

        new Thread(() -> {
            while (serverChannel.isOpen()) {
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    try (SocketChannel channel = serverChannel.accept()) {
                        List<Callable<Void>> responders = List.of(() -> {
                            sendStatus(channel);
                            return null;
                        });
                        workerExecutor.invokeAll(responders, RESPONSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                    } catch (IOException | InterruptedException e) {
                        logger.debug("encountered exception while responding to readiness check request", e);
                    } catch (Exception other) {
                        logger.warn("encountered unknown exception while responding to readiness check request", other);
                    }
                    return null;
                });
            }
        }, "elasticsearch[readiness-service]").start();

        logger.info("readiness service up and running on localhost:{}", boundAddress.publishAddress().getPort());
    }

    @Override
    protected void doStop() {
        try {
            if (this.serverChannel != null) {
                this.serverChannel.close();
            }
        } catch (IOException e) {
            logger.warn("error closing readiness service channel", e);
        } finally {
            this.ready = false;
            try {
                if (workerExecutor != null) {
                    workerExecutor.shutdown();
                }
            } catch (Exception other) {
                logger.info("error shutting down readiness service executor", other);
            }
            logger.info("readiness service stopped");
        }
    }

    @Override
    protected void doClose() {}

    void sendStatus(SocketChannel channel) {
        try {
            BoundTransportAddress boundAddress = httpTransport.boundAddress();
            StringBuilder sb = new StringBuilder(Boolean.toString(ready));

            if (boundAddress != null && boundAddress.publishAddress() != null) {
                sb.append(',').append(boundAddress.publishAddress().getPort());
            }

            Channels.writeToChannel(sb.toString().getBytes(StandardCharsets.UTF_8), channel);
        } catch (IOException e) {
            logger.warn("encountered I/O exception while responding to readiness client", e);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState clusterState = event.state();

        Set<String> shutdownNodeIds = PluginShutdownService.shutdownNodes(clusterState);
        if (shutdownNodeIds.contains(clusterState.nodes().getLocalNodeId())) {
            this.ready = false;
            logger.info("marking node as not ready because it's shutting down");
        } else {
            this.ready = clusterState.nodes().getMasterNodeId() != null;
        }
    }
}
