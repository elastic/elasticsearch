/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.transport.RemoteConnectionStrategy.ConnectionStrategy;

/**
 * <p>Configuration for initializing {@link RemoteClusterConnection}s to linked projects.</p>
 *
 * <p>A {@link LinkedProjectConfig.Builder} instance is used to build up the configuration,
 * with a concrete configuration type generated via {@link LinkedProjectConfig.Builder#build}
 * based on the {@link RemoteConnectionStrategy.ConnectionStrategy} that was specified,
 * or by constructing a specific configuration type via
 * {@link LinkedProjectConfig.Builder#buildProxyConnectionStrategyConfig()} or
 * {@link LinkedProjectConfig.Builder#buildSniffConnectionStrategyConfig()}.</p>
 *
 * <p>The {@link RemoteClusterSettings#toConfig(String, Settings)} and {@link RemoteClusterSettings#toConfig(ProjectId, String, Settings)}
 * methods can be used to read {@link RemoteClusterSettings} to build a concrete {@link LinkedProjectConfig} from {@link Settings}.</p>
 */
public sealed interface LinkedProjectConfig {
    ProjectId originProjectId();

    ProjectId linkedProjectId();

    String linkedProjectAlias();

    TimeValue transportConnectTimeout();

    Compression.Enabled connectionCompression();

    Compression.Scheme connectionCompressionScheme();

    TimeValue clusterPingSchedule();

    TimeValue initialConnectionTimeout();

    boolean skipUnavailable();

    int maxPendingConnectionListeners();

    ConnectionStrategy connectionStrategy();

    boolean isConnectionEnabled();

    RemoteConnectionStrategy buildRemoteConnectionStrategy(TransportService transportService, RemoteConnectionManager connectionManager);

    /**
     * Configuration for initializing {@link RemoteClusterConnection}s to linked projects using the {@link ProxyConnectionStrategy}.
     */
    record ProxyLinkedProjectConfig(
        ProjectId originProjectId,
        ProjectId linkedProjectId,
        String linkedProjectAlias,
        TimeValue transportConnectTimeout,
        Compression.Enabled connectionCompression,
        Compression.Scheme connectionCompressionScheme,
        TimeValue clusterPingSchedule,
        TimeValue initialConnectionTimeout,
        boolean skipUnavailable,
        int maxPendingConnectionListeners,
        int maxNumConnections,
        String proxyAddress,
        String serverName
    ) implements LinkedProjectConfig {

        @Override
        public ConnectionStrategy connectionStrategy() {
            return ConnectionStrategy.PROXY;
        }

        @Override
        public boolean isConnectionEnabled() {
            return Strings.isEmpty(proxyAddress) == false;
        }

        @Override
        public RemoteConnectionStrategy buildRemoteConnectionStrategy(
            TransportService transportService,
            RemoteConnectionManager connectionManager
        ) {
            return new ProxyConnectionStrategy(this, transportService, connectionManager);
        }
    }

    /**
     * Configuration for initializing {@link RemoteClusterConnection}s to linked projects using the {@link SniffConnectionStrategy}.
     */
    record SniffLinkedProjectConfig(
        ProjectId originProjectId,
        ProjectId linkedProjectId,
        String linkedProjectAlias,
        TimeValue transportConnectTimeout,
        Compression.Enabled connectionCompression,
        Compression.Scheme connectionCompressionScheme,
        TimeValue clusterPingSchedule,
        TimeValue initialConnectionTimeout,
        boolean skipUnavailable,
        int maxPendingConnectionListeners,
        int maxNumConnections,
        Predicate<DiscoveryNode> nodePredicate,
        List<String> seedNodes,
        String proxyAddress
    ) implements LinkedProjectConfig {

        @Override
        public ConnectionStrategy connectionStrategy() {
            return ConnectionStrategy.SNIFF;
        }

        @Override
        public boolean isConnectionEnabled() {
            return seedNodes.isEmpty() == false;
        }

        @Override
        public RemoteConnectionStrategy buildRemoteConnectionStrategy(
            TransportService transportService,
            RemoteConnectionManager connectionManager
        ) {
            return new SniffConnectionStrategy(this, transportService, connectionManager);
        }
    }

    TimeValue DEFAULT_TRANSPORT_CONNECT_TIMEOUT = TimeValue.timeValueSeconds(30);
    Compression.Enabled DEFAULT_CONNECTION_COMPRESSION = Compression.Enabled.INDEXING_DATA;
    Compression.Scheme DEFAULT_CONNECTION_COMPRESSION_SCHEME = Compression.Scheme.LZ4;
    TimeValue DEFAULT_CLUSTER_PING_SCHEDULE = TimeValue.MINUS_ONE;
    TimeValue DEFAULT_INITIAL_CONNECTION_TIMEOUT = TimeValue.timeValueSeconds(30);
    boolean DEFAULT_SKIP_UNAVAILABLE = true;
    int DEFAULT_REMOTE_MAX_PENDING_CONNECTION_LISTENERS = 1000;
    int DEFAULT_PROXY_NUM_SOCKET_CONNECTIONS = 18;
    int DEFAULT_SNIFF_MAX_NUM_CONNECTIONS = 3;
    List<String> DEFAULT_SNIFF_SEED_NODES = Collections.emptyList();
    Predicate<DiscoveryNode> DEFAULT_SNIFF_NODE_PREDICATE = (node) -> Version.CURRENT.isCompatible(node.getVersion())
        && (node.isMasterNode() == false || node.canContainData() || node.isIngestNode());

    static Builder buildForAlias(String linkedProjectAlias) {
        return buildForLinkedProject(ProjectId.DEFAULT, ProjectId.DEFAULT, linkedProjectAlias);
    }

    static Builder buildForLinkedProject(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
        return new Builder(originProjectId, linkedProjectId, linkedProjectAlias);
    }

    class Builder {
        private ProjectId originProjectId;
        private ProjectId linkedProjectId;
        private String linkedProjectAlias;
        private TimeValue transportConnectTimeout = DEFAULT_TRANSPORT_CONNECT_TIMEOUT;
        private Compression.Enabled connectionCompression = DEFAULT_CONNECTION_COMPRESSION;
        private Compression.Scheme connectionCompressionScheme = DEFAULT_CONNECTION_COMPRESSION_SCHEME;
        private TimeValue clusterPingSchedule = DEFAULT_CLUSTER_PING_SCHEDULE;
        private TimeValue initialConnectionTimeout = DEFAULT_INITIAL_CONNECTION_TIMEOUT;
        private boolean skipUnavailable = DEFAULT_SKIP_UNAVAILABLE;
        private ConnectionStrategy connectionStrategy;
        private int proxyNumSocketConnections = DEFAULT_PROXY_NUM_SOCKET_CONNECTIONS;
        private String proxyAddress = "";
        private String proxyServerName = "";
        private int sniffMaxNumConnections = DEFAULT_SNIFF_MAX_NUM_CONNECTIONS;
        private Predicate<DiscoveryNode> sniffNodePredicate = DEFAULT_SNIFF_NODE_PREDICATE;
        private List<String> sniffSeedNodes = DEFAULT_SNIFF_SEED_NODES;
        private int maxPendingConnectionListeners = DEFAULT_REMOTE_MAX_PENDING_CONNECTION_LISTENERS;

        private Builder(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
            originProjectId(originProjectId);
            linkedProjectId(linkedProjectId);
            linkedProjectAlias(linkedProjectAlias);
        }

        public Builder originProjectId(ProjectId originProjectId) {
            this.originProjectId = Objects.requireNonNull(originProjectId);
            return this;
        }

        public Builder linkedProjectId(ProjectId linkedProjectId) {
            this.linkedProjectId = Objects.requireNonNull(linkedProjectId);
            return this;
        }

        public Builder linkedProjectAlias(String linkedProjectAlias) {
            this.linkedProjectAlias = requireNonEmpty(linkedProjectAlias, "linkedProjectAlias");
            return this;
        }

        public Builder transportConnectTimeout(TimeValue transportConnectTimeout) {
            this.transportConnectTimeout = Objects.requireNonNull(transportConnectTimeout);
            return this;
        }

        public Builder connectionCompression(Compression.Enabled connectionCompression) {
            this.connectionCompression = Objects.requireNonNull(connectionCompression);
            return this;
        }

        public Builder connectionCompressionScheme(Compression.Scheme connectionCompressionScheme) {
            this.connectionCompressionScheme = Objects.requireNonNull(connectionCompressionScheme);
            return this;
        }

        public Builder clusterPingSchedule(TimeValue clusterPingSchedule) {
            this.clusterPingSchedule = Objects.requireNonNull(clusterPingSchedule);
            return this;
        }

        public Builder initialConnectionTimeout(TimeValue initialConnectionTimeout) {
            this.initialConnectionTimeout = Objects.requireNonNull(initialConnectionTimeout);
            return this;
        }

        public Builder skipUnavailable(boolean skipUnavailable) {
            this.skipUnavailable = skipUnavailable;
            return this;
        }

        public Builder connectionStrategy(ConnectionStrategy connectionStrategy) {
            this.connectionStrategy = Objects.requireNonNull(connectionStrategy);
            return this;
        }

        public Builder proxyNumSocketConnections(int proxyNumSocketConnections) {
            this.proxyNumSocketConnections = requireGreaterThanZero(proxyNumSocketConnections, "proxyNumSocketConnections");
            return this;
        }

        public Builder proxyAddress(String proxyAddress) {
            if (Strings.hasLength(proxyAddress)) {
                RemoteConnectionStrategy.parsePort(proxyAddress);
            }
            this.proxyAddress = proxyAddress;
            return this;
        }

        public Builder proxyServerName(String proxyServerName) {
            this.proxyServerName = proxyServerName;
            return this;
        }

        public Builder sniffMaxNumConnections(int sniffMaxNumConnections) {
            this.sniffMaxNumConnections = requireGreaterThanZero(sniffMaxNumConnections, "sniffMaxNumConnections");
            return this;
        }

        public Builder sniffNodePredicate(Predicate<DiscoveryNode> sniffNodePredicate) {
            this.sniffNodePredicate = Objects.requireNonNull(sniffNodePredicate);
            return this;
        }

        public Builder sniffSeedNodes(List<String> sniffSeedNodes) {
            Objects.requireNonNull(sniffSeedNodes).forEach(RemoteConnectionStrategy::parsePort);
            this.sniffSeedNodes = sniffSeedNodes;
            return this;
        }

        public Builder maxPendingConnectionListeners(int maxPendingConnectionListeners) {
            this.maxPendingConnectionListeners = requireGreaterThanZero(maxPendingConnectionListeners, "maxPendingConnectionListeners");
            return this;
        }

        public LinkedProjectConfig build() {
            if (connectionStrategy == null) {
                throw new IllegalArgumentException("[connectionStrategy] must be set before calling build()");
            }
            return switch (connectionStrategy) {
                case PROXY -> buildProxyConnectionStrategyConfig();
                case SNIFF -> buildSniffConnectionStrategyConfig();
            };
        }

        public ProxyLinkedProjectConfig buildProxyConnectionStrategyConfig() {
            if (connectionStrategy != null && ConnectionStrategy.PROXY.equals(connectionStrategy) == false) {
                throw new IllegalArgumentException("ConnectionStrategy must be PROXY");
            }
            return new ProxyLinkedProjectConfig(
                originProjectId,
                linkedProjectId,
                linkedProjectAlias,
                transportConnectTimeout,
                connectionCompression,
                connectionCompressionScheme,
                clusterPingSchedule,
                initialConnectionTimeout,
                skipUnavailable,
                maxPendingConnectionListeners,
                proxyNumSocketConnections,
                proxyAddress,
                proxyServerName
            );
        }

        public SniffLinkedProjectConfig buildSniffConnectionStrategyConfig() {
            if (connectionStrategy != null && ConnectionStrategy.SNIFF.equals(connectionStrategy) == false) {
                throw new IllegalArgumentException("ConnectionStrategy must be SNIFF");
            }
            return new SniffLinkedProjectConfig(
                originProjectId,
                linkedProjectId,
                linkedProjectAlias,
                transportConnectTimeout,
                connectionCompression,
                connectionCompressionScheme,
                clusterPingSchedule,
                initialConnectionTimeout,
                skipUnavailable,
                maxPendingConnectionListeners,
                sniffMaxNumConnections,
                sniffNodePredicate,
                sniffSeedNodes,
                proxyAddress
            );
        }

        private static int requireGreaterThanZero(int value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException("[" + name + "] must be greater than 0");
            }
            return value;
        }

        private String requireNonEmpty(String value, String name) {
            if (Objects.requireNonNull(value).isBlank()) {
                throw new IllegalArgumentException("[" + name + "] cannot be empty");
            }
            return value;
        }
    }
}
