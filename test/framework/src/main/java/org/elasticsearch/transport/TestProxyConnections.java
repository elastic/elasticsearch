/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 * Builds the connections a remote cluster gives for nodes it reaches through another node, whose constructor is package-private.
 */
public class TestProxyConnections {

    private TestProxyConnections() {}

    /**
     * A connection to {@code targetNode} through the node of {@code gateway}, as a remote cluster connection gives for a node it isn't
     * connected to.
     */
    public static Transport.Connection newProxyConnection(Transport.Connection gateway, DiscoveryNode targetNode) {
        return new RemoteConnectionManager.ProxyConnection(gateway, targetNode);
    }
}
