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
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.transport.RemoteConnectionStrategy.ConnectionStrategy;

// FIXME: Refactor into a class heirarchy with subclasses for each connection strategy
public record LinkedProjectConfig(
    ProjectId originProjectId,
    ProjectId linkedProjectId,
    String linkedProjectAlias,
    TimeValue transportConnectTimeout,
    Compression.Enabled connectionCompression,
    Compression.Scheme connectionCompressionScheme,
    TimeValue clusterPingSchedule,
    TimeValue initialConnectionTimeout,
    boolean skipUnavailable,
    ConnectionStrategy connectionStrategy,
    int maxNumConnections,
    String proxyAddress,
    String proxyServerName,
    Predicate<DiscoveryNode> sniffNodePredicate,
    List<String> sniffSeedNodes,
    int maxPendingConnectionListeners // only used in tests
) {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
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

        public Builder originProjectId(ProjectId originProjectId) {
            this.originProjectId = originProjectId;
            return this;
        }

        public Builder linkedProjectId(ProjectId linkedProjectId) {
            this.linkedProjectId = linkedProjectId;
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
            this.transportConnectTimeout = transportConnectTimeout;
            return this;
        }

        public Builder connectionCompression(Compression.Enabled connectionCompression) {
            this.connectionCompression = connectionCompression;
            return this;
        }

        public Builder connectionCompressionScheme(Compression.Scheme connectionCompressionScheme) {
            this.connectionCompressionScheme = connectionCompressionScheme;
            return this;
        }

        public Builder clusterPingSchedule(TimeValue clusterPingSchedule) {
            this.clusterPingSchedule = clusterPingSchedule;
            return this;
        }

        public Builder initialConnectionTimeout(TimeValue initialConnectionTimeout) {
            this.initialConnectionTimeout = initialConnectionTimeout;
            return this;
        }

        public Builder skipUnavailable(boolean skipUnavailable) {
            this.skipUnavailable = skipUnavailable;
            return this;
        }

        public Builder connectionStrategy(ConnectionStrategy connectionStrategy) {
            this.connectionStrategy = connectionStrategy;
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
            this.sniffNodePredicate = sniffNodePredicate;
            return this;
        }

        public Builder sniffSeedNodes(List<String> sniffSeedNodes) {
            this.sniffSeedNodes = sniffSeedNodes;
            return this;
        }

        public Builder maxPendingConnectionListeners(int maxPendingConnectionListeners) {
            this.maxPendingConnectionListeners = maxPendingConnectionListeners;
            return this;
        }

        public LinkedProjectConfig build() {
            return new LinkedProjectConfig(
                originProjectId,
                linkedProjectId,
                linkedProjectAlias,
                transportConnectTimeout,
                connectionCompression,
                connectionCompressionScheme,
                clusterPingSchedule,
                initialConnectionTimeout,
                skipUnavailable,
                connectionStrategy,
                maxNumConnections,
                proxyAddress,
                proxyServerName,
                sniffNodePredicate,
                sniffSeedNodes,
                maxPendingConnectionListeners
            );
        }
    }
}
