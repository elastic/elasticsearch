/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.transport.Transport;

import java.util.Optional;

public class RemoteClusterAliasResolver {

    private final ClusterService clusterService;
    private final Client client;

    public RemoteClusterAliasResolver(final ClusterService clusterService, final Client client) {
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * This method attempts to resolve a remote cluster alias for the given connection
     * in case the connection is targeting a node of a remote cluster.
     * @param connection the transport connection for which to resolve a remote cluster alias
     * @return an empty result if the given connection targets a node in the local cluster, otherwise a remote cluster alias
     */
    public Optional<String> resolve(final Transport.Connection connection) {
        final DiscoveryNode targetNode = connection.getNode();
        // First check if the node belongs to the local cluster and skip invoking remote cluster service.
        if (clusterService.state().nodes().nodeExists(targetNode)) {
            return Optional.empty();
        }
        // TODO: Handle the race condition case when a remote cluster connection gets disconnected and we don't get a cluster alias.
        final String remoteClusterAlias = client.getRemoteClusterAliasForConnection(connection);
        return Optional.ofNullable(remoteClusterAlias);
    }

}
