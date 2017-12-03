/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A connection profile describes how many connection are established to specific node for each of the available request types.
 * ({@link org.elasticsearch.transport.TransportRequestOptions.Type}). This allows to tailor a connection towards a specific usage.
 */
public final class ConnectionProfile {

    /**
     * Builds a connection profile that is dedicated to a single channel type. Use this
     * when opening single use connections
     */
    public static ConnectionProfile buildSingleChannelProfile(TransportRequestOptions.Type channelType,
                                                              @Nullable TimeValue connectTimeout,
                                                               @Nullable TimeValue handshakeTimeout) {
        Builder builder = new Builder();
        builder.addConnections(1, channelType);
        final EnumSet<TransportRequestOptions.Type> otherTypes = EnumSet.allOf(TransportRequestOptions.Type.class);
        otherTypes.remove(channelType);
        builder.addConnections(0, otherTypes.stream().toArray(TransportRequestOptions.Type[]::new));
        if (connectTimeout != null) {
            builder.setConnectTimeout(connectTimeout);
        }
        if (handshakeTimeout != null) {
            builder.setHandshakeTimeout(handshakeTimeout);
        }
        return builder.build();
    }

    private final List<ConnectionTypeHandle> handles;
    private final int numConnections;
    private final TimeValue connectTimeout;
    private final TimeValue handshakeTimeout;

    private ConnectionProfile(List<ConnectionTypeHandle> handles, int numConnections, TimeValue connectTimeout, TimeValue handshakeTimeout)
    {
        this.handles = handles;
        this.numConnections = numConnections;
        this.connectTimeout = connectTimeout;
        this.handshakeTimeout = handshakeTimeout;
    }

    /**
     * A builder to build a new {@link ConnectionProfile}
     */
    public static class Builder {
        private final List<ConnectionTypeHandle> handles = new ArrayList<>();
        private final Set<TransportRequestOptions.Type> addedTypes = EnumSet.noneOf(TransportRequestOptions.Type.class);
        private int offset = 0;
        private TimeValue connectTimeout;
        private TimeValue handshakeTimeout;

        /** create an empty builder */
        public Builder() {
        }

        /** copy constructor, using another profile as a base */
        public Builder(ConnectionProfile source) {
            handles.addAll(source.getHandles());
            offset = source.getNumConnections();
            handles.forEach(th -> addedTypes.addAll(th.types));
            connectTimeout = source.getConnectTimeout();
            handshakeTimeout = source.getHandshakeTimeout();
        }
        /**
         * Sets a connect timeout for this connection profile
         */
        public void setConnectTimeout(TimeValue connectTimeout) {
            if (connectTimeout.millis() < 0) {
                throw new IllegalArgumentException("connectTimeout must be non-negative but was: " + connectTimeout);
            }
            this.connectTimeout = connectTimeout;
        }

        /**
         * Sets a handshake timeout for this connection profile
         */
        public void setHandshakeTimeout(TimeValue handshakeTimeout) {
            if (handshakeTimeout.millis() < 0) {
                throw new IllegalArgumentException("handshakeTimeout must be non-negative but was: " + handshakeTimeout);
            }
            this.handshakeTimeout = handshakeTimeout;
        }

        /**
         * Adds a number of connections for one or more types. Each type can only be added once.
         * @param numConnections the number of connections to use in the pool for the given connection types
         * @param types a set of types that should share the given number of connections
         */
        public void addConnections(int numConnections, TransportRequestOptions.Type... types) {
            if (types == null || types.length == 0) {
                throw new IllegalArgumentException("types must not be null");
            }
            for (TransportRequestOptions.Type type : types) {
                if (addedTypes.contains(type)) {
                    throw new IllegalArgumentException("type [" + type + "] is already registered");
                }
            }
            addedTypes.addAll(Arrays.asList(types));
            handles.add(new ConnectionTypeHandle(offset, numConnections, EnumSet.copyOf(Arrays.asList(types))));
            offset += numConnections;
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
            return new ConnectionProfile(Collections.unmodifiableList(handles), offset, connectTimeout, handshakeTimeout);
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
