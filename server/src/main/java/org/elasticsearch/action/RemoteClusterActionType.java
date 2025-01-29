/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportResponse;

/**
 * An action which can be invoked by {@link RemoteClusterClient#execute}. The implementation must be registered with the transport service.
 * <p>
 * Typically, every {@link RemoteClusterActionType} instance is a global constant (i.e. a public static final field) called {@code
 * REMOTE_TYPE}.
 */
public final class RemoteClusterActionType<Response extends TransportResponse> {

    private final String name;
    private final Writeable.Reader<Response> responseReader;

    public static RemoteClusterActionType<ActionResponse.Empty> emptyResponse(String name) {
        return new RemoteClusterActionType<>(name, in -> ActionResponse.Empty.INSTANCE);
    }

    /**
     * Construct an {@link ActionType} which callers can execute on a remote cluster using a {@link RemoteClusterClient}, typically obtained
     * from {@link Client#getRemoteClusterClient}).
     *
     * @param name           The name of the action, which must be unique across actions. This is the ID of the transport action which is
     *                       sent to the handling node in the remote cluster.
     * @param responseReader Defines how to deserialize responses received from executions of this action.
     */
    public RemoteClusterActionType(String name, Writeable.Reader<Response> responseReader) {
        this.name = name;
        this.responseReader = responseReader;
    }

    /**
     * The name of the action. Must be unique across actions.
     */
    public String name() {
        return this.name;
    }

    /**
     * Get a reader that can read a response from a {@link org.elasticsearch.common.io.stream.StreamInput}.
     */
    public Writeable.Reader<Response> getResponseReader() {
        return responseReader;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RemoteClusterActionType<?> actionType && name.equals(actionType.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return name;
    }
}
