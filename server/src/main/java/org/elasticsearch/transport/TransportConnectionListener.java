/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 * A listener interface that allows to react on transport events. All methods may be
 * executed on network threads. Consumers must fork in the case of long running or blocking
 * operations.
 */
public interface TransportConnectionListener {

    /**
     * Called once a connection was opened
     * @param connection the connection
     */
    default void onConnectionOpened(Transport.Connection connection) {}

    /**
     * Called once a connection ws closed.
     * @param connection the closed connection
     */
    default void onConnectionClosed(Transport.Connection connection) {}

    /**
     * Called once a node connection is opened and registered.
     */
    default void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {}

    /**
     * Called once a node connection is closed and unregistered.
     */
    default void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {}
}
