/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A connection profile describes how many connection are established to specific node for each of the available request types.
 * ({@link org.elasticsearch.transport.TransportRequestOptions.Type}). This allows to tailor a connection towards a specific usage.
 */
public final class ConnectionProfile {

    /**
     * takes a {@link ConnectionProfile} resolves it to a fully specified (i.e., no nulls) profile
     */
    public static ConnectionProfile resolveConnectionProfile(@Nullable ConnectionProfile profile, ConnectionProfile fallbackProfile) {
        Objects.requireNonNull(fallbackProfile);
        if (profile == null) {
            return fallbackProfile;
        } else if (profile.getConnectTimeout() != null && profile.getHandshakeTimeout() != null
            && profile.getPingInterval() != null && profile.getCompressionEnabled() != null
            && profile.getCompressionScheme() != null) {
            return profile;
        } else {
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder(profile);
            if (profile.getConnectTimeout() == null) {
                builder.setConnectTimeout(fallbackProfile.getConnectTimeout());
            }
            if (profile.getHandshakeTimeout() == null) {
                builder.setHandshakeTimeout(fallbackProfile.getHandshakeTimeout());
            }
            if (profile.getPingInterval() == null) {
                builder.setPingInterval(fallbackProfile.getPingInterval());
            }
            if (profile.getCompressionEnabled() == null) {
                builder.setCompressionEnabled(fallbackProfile.getCompressionEnabled());
            }
            if (profile.getCompressionScheme() == null) {
                builder.setCompressionScheme(fallbackProfile.getCompressionScheme());
            }
            return builder.build();
        }
    }

    /**
     * Builds a default connection profile based on the provided settings.
     *
     * @param settings to build the connection profile from
     * @return the connection profile
     */
    public static ConnectionProfile buildDefaultConnectionProfile(Settings settings) {
        int connectionsPerNodeRecovery = TransportSettings.CONNECTIONS_PER_NODE_RECOVERY.get(settings);
        int connectionsPerNodeBulk = TransportSettings.CONNECTIONS_PER_NODE_BULK.get(settings);
        int connectionsPerNodeReg = TransportSettings.CONNECTIONS_PER_NODE_REG.get(settings);
        int connectionsPerNodeState = TransportSettings.CONNECTIONS_PER_NODE_STATE.get(settings);
        int connectionsPerNodePing = TransportSettings.CONNECTIONS_PER_NODE_PING.get(settings);
        Builder builder = new Builder();
        builder.setConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings));
        builder.setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings));
        builder.setPingInterval(TransportSettings.PING_SCHEDULE.get(settings));
        builder.setCompressionEnabled(TransportSettings.TRANSPORT_COMPRESS.get(settings));
        builder.setCompressionScheme(TransportSettings.TRANSPORT_COMPRESSION_SCHEME.get(settings));
        builder.addConnections(connectionsPerNodeBulk, TransportRequestOptions.Type.BULK);
        builder.addConnections(connectionsPerNodePing, TransportRequestOptions.Type.PING);
        // if we are not master eligible we don't need a dedicated channel to publish the state
        builder.addConnections(DiscoveryNode.isMasterNode(settings) ? connectionsPerNodeState : 0, TransportRequestOptions.Type.STATE);
        // if we are not a data-node we don't need any dedicated channels for recovery
        builder.addConnections(
            DiscoveryNode.canContainData(settings) ? connectionsPerNodeRecovery : 0, TransportRequestOptions.Type.RECOVERY);
        builder.addConnections(connectionsPerNodeReg, TransportRequestOptions.Type.REG);
        return builder.build();
    }

    /**
     * Builds a connection profile that is dedicated to a single channel type. Allows passing connection and
     * handshake timeouts and compression settings.
     */
    public static ConnectionProfile buildSingleChannelProfile(TransportRequestOptions.Type channelType, @Nullable TimeValue connectTimeout,
                                                              @Nullable TimeValue handshakeTimeout, @Nullable TimeValue pingInterval,
                                                              @Nullable Compression.Enabled compressionEnabled,
                                                              @Nullable Compression.Scheme compressionScheme) {
        Builder builder = new Builder();
        builder.addConnections(1, channelType);
        final EnumSet<TransportRequestOptions.Type> otherTypes = EnumSet.allOf(TransportRequestOptions.Type.class);
        otherTypes.remove(channelType);
        builder.addConnections(0, otherTypes.toArray(new TransportRequestOptions.Type[0]));
        if (connectTimeout != null) {
            builder.setConnectTimeout(connectTimeout);
        }
        if (handshakeTimeout != null) {
            builder.setHandshakeTimeout(handshakeTimeout);
        }
        if (pingInterval != null) {
            builder.setPingInterval(pingInterval);
        }
        if (compressionEnabled != null) {
            builder.setCompressionEnabled(compressionEnabled);
        }
        if (compressionScheme != null) {
            builder.setCompressionScheme(compressionScheme);
        }
        return builder.build();
    }

    private final List<ConnectionTypeHandle> handles;
    private final int numConnections;
    private final TimeValue connectTimeout;
    private final TimeValue handshakeTimeout;
    private final TimeValue pingInterval;
    private final Compression.Enabled compressionEnabled;
    private final Compression.Scheme compressionScheme;

    private ConnectionProfile(List<ConnectionTypeHandle> handles, int numConnections, TimeValue connectTimeout,
                              TimeValue handshakeTimeout, TimeValue pingInterval, Compression.Enabled compressionEnabled,
                              Compression.Scheme compressionScheme) {
        this.handles = handles;
        this.numConnections = numConnections;
        this.connectTimeout = connectTimeout;
        this.handshakeTimeout = handshakeTimeout;
        this.pingInterval = pingInterval;
        this.compressionEnabled = compressionEnabled;
        this.compressionScheme = compressionScheme;
    }

    /**
     * A builder to build a new {@link ConnectionProfile}
     */
    public static class Builder {
        private final List<ConnectionTypeHandle> handles = new ArrayList<>();
        private final Set<TransportRequestOptions.Type> addedTypes = EnumSet.noneOf(TransportRequestOptions.Type.class);
        private int numConnections = 0;
        private TimeValue connectTimeout;
        private TimeValue handshakeTimeout;
        private Compression.Enabled compressionEnabled;
        private Compression.Scheme compressionScheme;
        private TimeValue pingInterval;

        /** create an empty builder */
        public Builder() {
        }

        /** copy constructor, using another profile as a base */
        public Builder(ConnectionProfile source) {
            handles.addAll(source.getHandles());
            numConnections = source.getNumConnections();
            handles.forEach(th -> addedTypes.addAll(th.types));
            connectTimeout = source.getConnectTimeout();
            handshakeTimeout = source.getHandshakeTimeout();
            compressionEnabled = source.getCompressionEnabled();
            compressionScheme = source.getCompressionScheme();
            pingInterval = source.getPingInterval();
        }
        /**
         * Sets a connect timeout for this connection profile
         */
        public Builder setConnectTimeout(TimeValue connectTimeout) {
            if (connectTimeout.millis() < 0) {
                throw new IllegalArgumentException("connectTimeout must be non-negative but was: " + connectTimeout);
            }
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Sets a handshake timeout for this connection profile
         */
        public Builder setHandshakeTimeout(TimeValue handshakeTimeout) {
            if (handshakeTimeout.millis() < 0) {
                throw new IllegalArgumentException("handshakeTimeout must be non-negative but was: " + handshakeTimeout);
            }
            this.handshakeTimeout = handshakeTimeout;
            return this;
        }

        /**
         * Sets a ping interval for this connection profile
         */
        public Builder setPingInterval(TimeValue pingInterval) {
            this.pingInterval = pingInterval;
            return this;
        }

        /**
         * Sets compression enabled configuration for this connection profile
         */
        public Builder setCompressionEnabled(Compression.Enabled compressionEnabled) {
            this.compressionEnabled = compressionEnabled;
            return this;
        }

        /**
         * Sets compression scheme for this connection profile
         */
        public Builder setCompressionScheme(Compression.Scheme compressionScheme) {
            this.compressionScheme = compressionScheme;
            return this;
        }

        /**
         * Adds a number of connections for one or more types. Each type can only be added once.
         * @param numConnections the number of connections to use in the pool for the given connection types
         * @param types a set of types that should share the given number of connections
         */
        public Builder addConnections(int numConnections, TransportRequestOptions.Type... types) {
            if (types == null || types.length == 0) {
                throw new IllegalArgumentException("types must not be null");
            }
            for (TransportRequestOptions.Type type : types) {
                if (addedTypes.contains(type)) {
                    throw new IllegalArgumentException("type [" + type + "] is already registered");
                }
            }
            addedTypes.addAll(Arrays.asList(types));
            handles.add(new ConnectionTypeHandle(this.numConnections, numConnections, EnumSet.copyOf(Arrays.asList(types))));
            this.numConnections += numConnections;
            return this;
        }

        /**
         * Creates a new {@link ConnectionProfile} based on the added connections.
         * @throws IllegalStateException if any of the {@link org.elasticsearch.transport.TransportRequestOptions.Type} enum is missing
         */
        public ConnectionProfile build() {
            EnumSet<TransportRequestOptions.Type> types = EnumSet.allOf(TransportRequestOptions.Type.class);
            types.removeAll(addedTypes);
            if (types.isEmpty() == false) {
                throw new IllegalStateException("not all types are added for this connection profile - missing types: " + types);
            }
            return new ConnectionProfile(Collections.unmodifiableList(handles), numConnections, connectTimeout, handshakeTimeout,
                pingInterval, compressionEnabled, compressionScheme);
        }

    }

    /**
     * Returns the connect timeout or <code>null</code> if no explicit timeout is set on this profile.
     */
    public TimeValue getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Returns the handshake timeout or <code>null</code> if no explicit timeout is set on this profile.
     */
    public TimeValue getHandshakeTimeout() {
        return handshakeTimeout;
    }

    /**
     * Returns the ping interval or <code>null</code> if no explicit ping interval is set on this profile.
     */
    public TimeValue getPingInterval() {
        return pingInterval;
    }

    /**
     * Returns the compression enabled configuration or <code>null</code> if no explicit compression configuration
     * is set on this profile.
     */
    public Compression.Enabled getCompressionEnabled() {
        return compressionEnabled;
    }

    /**
     * Returns the configured compression scheme or <code>null</code> if no explicit
     * compression scheme is set on this profile.
     */
    public Compression.Scheme getCompressionScheme() {
        return compressionScheme;
    }

    /**
     * Returns the total number of connections for this profile
     */
    public int getNumConnections() {
        return numConnections;
    }

    /**
     * Returns the number of connections per type for this profile. This might return a count that is shared with other types such
     * that the sum of all connections per type might be higher than {@link #getNumConnections()}. For instance if
     * {@link org.elasticsearch.transport.TransportRequestOptions.Type#BULK} shares connections with
     * {@link org.elasticsearch.transport.TransportRequestOptions.Type#REG} they will return both the same number of connections from
     * this method but the connections are not distinct.
     */
    public int getNumConnectionsPerType(TransportRequestOptions.Type type) {
        for (ConnectionTypeHandle handle : handles) {
            if (handle.getTypes().contains(type)) {
                return handle.length;
            }
        }
        throw new AssertionError("no handle found for type: "  + type);
    }

    /**
     * Returns the type handles for this connection profile
     */
    List<ConnectionTypeHandle> getHandles() {
        return Collections.unmodifiableList(handles);
    }

    /**
     * Connection type handle encapsulates the logic which connection
     */
    static final class ConnectionTypeHandle {
        public final int length;
        public final int offset;
        private final Set<TransportRequestOptions.Type> types;
        private final AtomicInteger counter = new AtomicInteger();

        private ConnectionTypeHandle(int offset, int length, Set<TransportRequestOptions.Type> types) {
            this.length = length;
            this.offset = offset;
            this.types = types;
        }

        /**
         * Returns one of the channels out configured for this handle. The channel is selected in a round-robin
         * fashion.
         */
        <T> T getChannel(List<T> channels) {
            if (length == 0) {
                throw new IllegalStateException("can't select channel size is 0 for types: " + types);
            }
            assert channels.size() >= offset + length : "illegal size: " + channels.size() + " expected >= " + (offset + length);
            return channels.get(offset + Math.floorMod(counter.incrementAndGet(), length));
        }

        /**
         * Returns all types for this handle
         */
        Set<TransportRequestOptions.Type> getTypes() {
            return types;
        }
    }
}
