/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.transport.RemoteConnectionStrategy.ConnectionStrategy;

/**
 * <p>Configuration for initializing {@link RemoteClusterConnection}s to linked projects.</p>
 *
 * <p>The {@link ProxyLinkedProjectConfigBuilder} and {@link SniffLinkedProjectConfigBuilder} classes can be used to build concrete
 * implementations of {@link LinkedProjectConfig}.</p>
 *
 * <p>The {@link RemoteClusterSettings#toConfig(String, Settings)} and
 * {@link RemoteClusterSettings#toConfig(ProjectId, ProjectId, String, Settings)} methods
 * can be used to read {@link RemoteClusterSettings} to build a concrete {@link LinkedProjectConfig} from {@link Settings}.</p>
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

    int maxNumConnections();

    String proxyAddress();

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

    abstract class Builder<B extends Builder<B>> {
        protected final ProjectId originProjectId;
        protected final ProjectId linkedProjectId;
        protected final String linkedProjectAlias;
        protected final ConnectionStrategy connectionStrategy;
        private final B concreteBuilder;
        protected TimeValue transportConnectTimeout = TransportSettings.DEFAULT_CONNECT_TIMEOUT;
        protected Compression.Enabled connectionCompression = TransportSettings.DEFAULT_TRANSPORT_COMPRESS;
        protected Compression.Scheme connectionCompressionScheme = TransportSettings.DEFAULT_TRANSPORT_COMPRESSION_SCHEME;
        protected TimeValue clusterPingSchedule = TransportSettings.DEFAULT_PING_SCHEDULE;
        protected TimeValue initialConnectionTimeout = RemoteClusterSettings.DEFAULT_INITIAL_CONNECTION_TIMEOUT;
        protected boolean skipUnavailable = RemoteClusterSettings.DEFAULT_SKIP_UNAVAILABLE;
        protected String proxyAddress = "";
        protected int maxNumConnections;
        protected int maxPendingConnectionListeners = RemoteClusterSettings.DEFAULT_MAX_PENDING_CONNECTION_LISTENERS;

        private Builder(
            ProjectId originProjectId,
            ProjectId linkedProjectId,
            String linkedProjectAlias,
            ConnectionStrategy connectionStrategy
        ) {
            this.originProjectId = Objects.requireNonNull(originProjectId);
            this.linkedProjectId = Objects.requireNonNull(linkedProjectId);
            this.linkedProjectAlias = requireNonEmpty(linkedProjectAlias, "linkedProjectAlias");
            this.connectionStrategy = Objects.requireNonNull(connectionStrategy);
            this.concreteBuilder = self();
            this.maxNumConnections = switch (connectionStrategy) {
                case PROXY -> RemoteClusterSettings.ProxyConnectionStrategySettings.DEFAULT_REMOTE_SOCKET_CONNECTIONS;
                case SNIFF -> RemoteClusterSettings.SniffConnectionStrategySettings.DEFAULT_REMOTE_CONNECTIONS_PER_CLUSTER;
            };
        }

        public B transportConnectTimeout(TimeValue transportConnectTimeout) {
            this.transportConnectTimeout = Objects.requireNonNull(transportConnectTimeout);
            return concreteBuilder;
        }

        public B connectionCompression(Compression.Enabled connectionCompression) {
            this.connectionCompression = Objects.requireNonNull(connectionCompression);
            return concreteBuilder;
        }

        public B connectionCompressionScheme(Compression.Scheme connectionCompressionScheme) {
            this.connectionCompressionScheme = Objects.requireNonNull(connectionCompressionScheme);
            return concreteBuilder;
        }

        public B clusterPingSchedule(TimeValue clusterPingSchedule) {
            this.clusterPingSchedule = Objects.requireNonNull(clusterPingSchedule);
            return concreteBuilder;
        }

        public B initialConnectionTimeout(TimeValue initialConnectionTimeout) {
            this.initialConnectionTimeout = Objects.requireNonNull(initialConnectionTimeout);
            return concreteBuilder;
        }

        public B skipUnavailable(boolean skipUnavailable) {
            this.skipUnavailable = skipUnavailable;
            return concreteBuilder;
        }

        public B proxyAddress(String proxyAddress) {
            // TODO: Eliminate leniency here allowing an empty proxy address, ES-12737.
            if (Strings.hasLength(proxyAddress)) {
                RemoteConnectionStrategy.parsePort(proxyAddress);
            }
            this.proxyAddress = proxyAddress;
            return concreteBuilder;
        }

        public B maxNumConnections(int maxNumConnections) {
            this.maxNumConnections = requireGreaterThanZero(maxNumConnections, "maxNumConnections");
            return concreteBuilder;
        }

        public B maxPendingConnectionListeners(int maxPendingConnectionListeners) {
            this.maxPendingConnectionListeners = requireGreaterThanZero(maxPendingConnectionListeners, "maxPendingConnectionListeners");
            return concreteBuilder;
        }

        public abstract LinkedProjectConfig build();

        protected abstract B self();

        protected static int requireGreaterThanZero(int value, String name) {
            if (value <= 0) {
                throw new IllegalArgumentException("[" + name + "] must be greater than 0");
            }
            return value;
        }

        protected static String requireNonEmpty(String value, String name) {
            if (Objects.requireNonNull(value).isBlank()) {
                throw new IllegalArgumentException("[" + name + "] cannot be empty");
            }
            return value;
        }
    }

    class ProxyLinkedProjectConfigBuilder extends Builder<ProxyLinkedProjectConfigBuilder> {
        private String serverName = "";

        public ProxyLinkedProjectConfigBuilder(String linkedProjectAlias) {
            super(ProjectId.DEFAULT, ProjectId.DEFAULT, linkedProjectAlias, ConnectionStrategy.PROXY);
        }

        public ProxyLinkedProjectConfigBuilder(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
            super(originProjectId, linkedProjectId, linkedProjectAlias, ConnectionStrategy.PROXY);
        }

        public ProxyLinkedProjectConfigBuilder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }

        @Override
        public ProxyLinkedProjectConfig build() {
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
                maxNumConnections,
                proxyAddress,
                serverName
            );
        }

        @Override
        protected ProxyLinkedProjectConfigBuilder self() {
            return this;
        }
    }

    class SniffLinkedProjectConfigBuilder extends Builder<SniffLinkedProjectConfigBuilder> {
        private Predicate<DiscoveryNode> nodePredicate = RemoteClusterSettings.SniffConnectionStrategySettings.DEFAULT_NODE_PREDICATE;
        private List<String> seedNodes = RemoteClusterSettings.SniffConnectionStrategySettings.DEFAULT_SEED_NODES;

        public SniffLinkedProjectConfigBuilder(String linkedProjectAlias) {
            super(ProjectId.DEFAULT, ProjectId.DEFAULT, linkedProjectAlias, ConnectionStrategy.SNIFF);
        }

        public SniffLinkedProjectConfigBuilder(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
            super(originProjectId, linkedProjectId, linkedProjectAlias, ConnectionStrategy.SNIFF);
        }

        public SniffLinkedProjectConfigBuilder nodePredicate(Predicate<DiscoveryNode> nodePredicate) {
            this.nodePredicate = Objects.requireNonNull(nodePredicate);
            return this;
        }

        public SniffLinkedProjectConfigBuilder seedNodes(List<String> seedNodes) {
            // TODO: Eliminate leniency here allowing an empty set of seed nodes, ES-12737.
            Objects.requireNonNull(seedNodes).forEach(RemoteConnectionStrategy::parsePort);
            this.seedNodes = seedNodes;
            return this;
        }

        @Override
        public SniffLinkedProjectConfig build() {
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
                maxNumConnections,
                nodePredicate,
                seedNodes,
                proxyAddress
            );
        }

        @Override
        protected SniffLinkedProjectConfigBuilder self() {
            return this;
        }
    }
}
