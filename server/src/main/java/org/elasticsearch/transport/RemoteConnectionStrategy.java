/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

public abstract class RemoteConnectionStrategy implements TransportConnectionListener, Closeable {

    enum ConnectionStrategy {
        SNIFF(SniffConnectionStrategy.CHANNELS_PER_CONNECTION, SniffConnectionStrategy::infoReader) {
            @Override
            public String toString() {
                return "sniff";
            }
        },
        PROXY(ProxyConnectionStrategy.CHANNELS_PER_CONNECTION, ProxyConnectionStrategy::infoReader) {
            @Override
            public String toString() {
                return "proxy";
            }
        };

        private final int numberOfChannels;
        private final Supplier<Writeable.Reader<RemoteConnectionInfo.ModeInfo>> reader;

        ConnectionStrategy(int numberOfChannels, Supplier<Writeable.Reader<RemoteConnectionInfo.ModeInfo>> reader) {
            this.numberOfChannels = numberOfChannels;
            this.reader = reader;
        }

        public int getNumberOfChannels() {
            return numberOfChannels;
        }

        public Writeable.Reader<RemoteConnectionInfo.ModeInfo> getReader() {
            return reader.get();
        }
    }

    enum ConnectionAttempt {
        initial,
        reconnect
    }

    private final int maxPendingConnectionListeners;

    protected final Logger logger = LogManager.getLogger(getClass());

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object mutex = new Object();
    private List<ActionListener<Void>> listeners = new ArrayList<>();
    private final AtomicBoolean initialConnectionAttempted = new AtomicBoolean(false);
    private final LongCounter connectionAttemptFailures;

    protected final TransportService transportService;
    protected final RemoteConnectionManager connectionManager;
    protected final ProjectId originProjectId;
    protected final ProjectId linkedProjectId;
    protected final String clusterAlias;

    RemoteConnectionStrategy(LinkedProjectConfig config, TransportService transportService, RemoteConnectionManager connectionManager) {
        this.originProjectId = config.originProjectId();
        this.linkedProjectId = config.linkedProjectId();
        this.clusterAlias = config.linkedProjectAlias();
        this.transportService = transportService;
        this.connectionManager = connectionManager;
        this.maxPendingConnectionListeners = config.maxPendingConnectionListeners();
        this.connectionAttemptFailures = lookupConnectionFailureMetric(transportService.getTelemetryProvider());
        connectionManager.addListener(this);
    }

    private LongCounter lookupConnectionFailureMetric(TelemetryProvider telemetryProvider) {
        final var meterRegistry = telemetryProvider == null ? null : telemetryProvider.getMeterRegistry();
        return meterRegistry == null ? null : meterRegistry.getLongCounter(RemoteClusterService.CONNECTION_ATTEMPT_FAILURES_COUNTER_NAME);
    }

    static ConnectionProfile buildConnectionProfile(LinkedProjectConfig config, String transportProfile) {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder().setConnectTimeout(config.transportConnectTimeout())
            .setHandshakeTimeout(config.transportConnectTimeout())
            .setCompressionEnabled(config.connectionCompression())
            .setCompressionScheme(config.connectionCompressionScheme())
            .setPingInterval(config.clusterPingSchedule())
            .addConnections(
                0,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.STATE,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.PING
            )
            .addConnections(config.connectionStrategy().getNumberOfChannels(), TransportRequestOptions.Type.REG)
            .setTransportProfile(transportProfile);
        return builder.build();
    }

    static InetSocketAddress parseConfiguredAddress(String configuredAddress) {
        final String host = parseHost(configuredAddress);
        final int port = parsePort(configuredAddress);
        InetAddress hostAddress;
        try {
            hostAddress = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("unknown host [" + host + "]", e);
        }
        return new InetSocketAddress(hostAddress, port);
    }

    static String parseHost(final String configuredAddress) {
        return configuredAddress.substring(0, indexOfPortSeparator(configuredAddress));
    }

    static int parsePort(String remoteHost) {
        try {
            int port = Integer.parseInt(remoteHost.substring(indexOfPortSeparator(remoteHost) + 1));
            if (port <= 0) {
                throw new IllegalArgumentException("port number must be > 0 but was: [" + port + "]");
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse port", e);
        }
    }

    private static int indexOfPortSeparator(String remoteHost) {
        int portSeparator = remoteHost.lastIndexOf(':'); // in case we have a IPv6 address ie. [::1]:9300
        if (portSeparator == -1 || portSeparator == remoteHost.length()) {
            throw new IllegalArgumentException("remote hosts need to be configured as [host:port], found [" + remoteHost + "] instead");
        }
        return portSeparator;
    }

    /**
     * Triggers a connect round unless there is one running already. If there is a connect round running, the listener will either
     * be queued or rejected and failed.
     */
    void connect(ActionListener<Void> connectListener) {
        boolean runConnect = false;
        final ActionListener<Void> listener = ContextPreservingActionListener.wrapPreservingContext(
            connectListener,
            transportService.getThreadPool().getThreadContext()
        );
        boolean isCurrentlyClosed;
        synchronized (mutex) {
            isCurrentlyClosed = this.closed.get();
            if (isCurrentlyClosed) {
                assert listeners.isEmpty();
            } else {
                if (listeners.size() >= maxPendingConnectionListeners) {
                    assert listeners.size() == maxPendingConnectionListeners;
                    listener.onFailure(new EsRejectedExecutionException("connect listener queue is full"));
                    return;
                } else {
                    listeners.add(listener);
                }
                runConnect = listeners.size() == 1;
            }
        }
        if (isCurrentlyClosed) {
            connectListener.onFailure(new AlreadyClosedException("connect handler is already closed"));
            return;
        }
        if (runConnect) {
            ExecutorService executor = transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT);
            executor.submit(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    ActionListener.onFailure(getAndClearListeners(), e);
                }

                @Override
                protected void doRun() {
                    connectImpl(new ActionListener<>() {
                        @Override
                        public void onResponse(Void aVoid) {
                            connectionAttemptCompleted(null);
                            ActionListener.onResponse(getAndClearListeners(), aVoid);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            connectionAttemptCompleted(e);
                            ActionListener.onFailure(getAndClearListeners(), e);
                        }
                    });
                }
            });
        }
    }

    private void connectionAttemptCompleted(@Nullable Exception e) {
        final boolean isInitialAttempt = initialConnectionAttempted.compareAndSet(false, true);
        final org.apache.logging.log4j.util.Supplier<String> msgSupplier = () -> format(
            "Origin project [%s] %s linked project [%s] with alias [%s] on %s attempt",
            originProjectId,
            e == null ? "successfully connected to" : "failed to connect to",
            linkedProjectId,
            clusterAlias,
            isInitialAttempt ? "the initial connection" : "a reconnection"
        );
        if (e == null) {
            logger.debug(msgSupplier);
        } else {
            logger.warn(msgSupplier, e);
            if (connectionAttemptFailures != null) {
                connectionAttemptFailures.incrementBy(
                    1,
                    Map.of(
                        "linked_project_id",
                        linkedProjectId.toString(),
                        "linked_project_alias",
                        clusterAlias,
                        "attempt",
                        (isInitialAttempt ? ConnectionAttempt.initial : ConnectionAttempt.reconnect).toString(),
                        "strategy",
                        strategyType().toString()
                    )
                );
            }
        }
    }

    boolean shouldRebuildConnection(LinkedProjectConfig config) {
        return config.connectionStrategy().equals(strategyType()) == false
            || connectionProfileChanged(config)
            || strategyMustBeRebuilt(config);
    }

    protected abstract boolean strategyMustBeRebuilt(LinkedProjectConfig config);

    protected abstract ConnectionStrategy strategyType();

    @Override
    public void onNodeDisconnected(DiscoveryNode node, @Nullable Exception closeException) {
        if (shouldOpenMoreConnections()) {
            // try to reconnect and fill up the slot of the disconnected node
            connect(
                ActionListener.wrap(
                    ignore -> logger.trace("[{}] successfully connected after disconnect of {}", clusterAlias, node),
                    e -> logger.debug(() -> format("[%s] failed to connect after disconnect of %s", clusterAlias, node), e)
                )
            );
        }
    }

    @Override
    public void close() {
        final List<ActionListener<Void>> toNotify;
        synchronized (mutex) {
            if (closed.compareAndSet(false, true)) {
                connectionManager.removeListener(this);
                toNotify = listeners;
                listeners = Collections.emptyList();
            } else {
                toNotify = Collections.emptyList();
            }
        }
        ActionListener.onFailure(toNotify, new AlreadyClosedException("connect handler is already closed"));
    }

    public boolean isClosed() {
        return closed.get();
    }

    // for testing only
    boolean assertNoRunningConnections() {
        synchronized (mutex) {
            assert listeners.isEmpty();
        }
        return true;
    }

    protected abstract boolean shouldOpenMoreConnections();

    protected abstract void connectImpl(ActionListener<Void> listener);

    protected abstract RemoteConnectionInfo.ModeInfo getModeInfo();

    protected static boolean isRetryableException(Exception e) {
        // ISE if we fail the handshake with a version incompatible node
        return e instanceof ConnectTransportException || e instanceof IOException || e instanceof IllegalStateException;
    }

    private List<ActionListener<Void>> getAndClearListeners() {
        final List<ActionListener<Void>> result;
        synchronized (mutex) {
            if (listeners.isEmpty()) {
                result = Collections.emptyList();
            } else {
                result = listeners;
                listeners = new ArrayList<>();
            }
        }
        return result;
    }

    private boolean connectionProfileChanged(LinkedProjectConfig config) {
        final var oldProfile = connectionManager.getConnectionProfile();
        final var newProfile = new ConnectionProfile.Builder(oldProfile).setCompressionEnabled(config.connectionCompression())
            .setCompressionScheme(config.connectionCompressionScheme())
            .setPingInterval(config.clusterPingSchedule())
            .build();
        return Objects.equals(oldProfile.getCompressionEnabled(), newProfile.getCompressionEnabled()) == false
            || Objects.equals(oldProfile.getPingInterval(), newProfile.getPingInterval()) == false
            || Objects.equals(oldProfile.getCompressionScheme(), newProfile.getCompressionScheme()) == false;
    }
}
