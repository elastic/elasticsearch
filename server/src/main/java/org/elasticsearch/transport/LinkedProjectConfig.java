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
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.transport.RemoteConnectionStrategy.ConnectionStrategy;

public interface LinkedProjectConfig {
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

    RemoteConnectionStrategy buildConnectionStrategy(TransportService transportService, RemoteConnectionManager connectionManager);

    record ProxyConnectionStrategyConfig(
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
        public RemoteConnectionStrategy buildConnectionStrategy(
            TransportService transportService,
            RemoteConnectionManager connectionManager
        ) {
            return new ProxyConnectionStrategy(this, transportService, connectionManager);
        }
    }

    record SniffConnectionStrategyConfig(
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
        public RemoteConnectionStrategy buildConnectionStrategy(
            TransportService transportService,
            RemoteConnectionManager connectionManager
        ) {
            return new SniffConnectionStrategy(this, transportService, connectionManager);
        }
    }

    static Builder builder() {
        return new Builder();
    }

    static Builder buildForAlias(String linkedProjectAlias) {
        return new Builder().linkedProjectAlias(linkedProjectAlias);
    }

    static Builder buildForLinkedProject(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
        return new Builder(originProjectId, linkedProjectId, linkedProjectAlias);
    }

    class Builder {
        private ProjectId originProjectId;
        private ProjectId linkedProjectId;
        private String linkedProjectAlias;
        private TimeValue transportConnectTimeout;
        private Compression.Enabled connectionCompression;
        private Compression.Scheme connectionCompressionScheme;
        private TimeValue clusterPingSchedule;
        private TimeValue initialConnectionTimeout;
        private boolean skipUnavailable;
        private ConnectionStrategy connectionStrategy;
        private int maxNumConnections;
        private String proxyAddress;
        private String proxyServerName;
        private Predicate<DiscoveryNode> sniffNodePredicate;
        private List<String> sniffSeedNodes;
        private int maxPendingConnectionListeners = 1000;

        private Builder() {}

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
            if (linkedProjectAlias == null || linkedProjectAlias.isBlank()) {
                throw new IllegalArgumentException("linkedProjectAlias cannot be null or empty");
            }
            this.linkedProjectAlias = linkedProjectAlias;
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

        public Builder maxNumConnections(int maxNumConnections) {
            this.maxNumConnections = maxNumConnections;
            return this;
        }

        public Builder proxyAddress(String proxyAddress) {
            this.proxyAddress = proxyAddress;
            return this;
        }

        public Builder proxyServerName(String proxyServerName) {
            this.proxyServerName = proxyServerName;
            return this;
        }

        public Builder sniffNodePredicate(Predicate<DiscoveryNode> sniffNodePredicate) {
            this.sniffNodePredicate = Objects.requireNonNull(sniffNodePredicate);
            return this;
        }

        public Builder sniffSeedNodes(List<String> sniffSeedNodes) {
            this.sniffSeedNodes = Objects.requireNonNull(sniffSeedNodes);
            return this;
        }

        public Builder maxPendingConnectionListeners(int maxPendingConnectionListeners) {
            this.maxPendingConnectionListeners = maxPendingConnectionListeners;
            return this;
        }

        public LinkedProjectConfig build() {
            assert connectionStrategy != null : "ConnectionStrategy must be set";
            return switch (connectionStrategy) {
                case PROXY -> buildProxyConnectionStrategyConfig();
                case SNIFF -> buildSniffConnectionStrategyConfig();
            };
        }

        public ProxyConnectionStrategyConfig buildProxyConnectionStrategyConfig() {
            assert ConnectionStrategy.PROXY.equals(connectionStrategy) : "ConnectionStrategy must be PROXY";
            return new ProxyConnectionStrategyConfig(
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
                proxyServerName
            );
        }

        public SniffConnectionStrategyConfig buildSniffConnectionStrategyConfig() {
            assert ConnectionStrategy.SNIFF.equals(connectionStrategy) : "ConnectionStrategy must be SNIFF";
            return new SniffConnectionStrategyConfig(
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
                sniffNodePredicate,
                sniffSeedNodes,
                proxyAddress
            );
        }
    }
}
