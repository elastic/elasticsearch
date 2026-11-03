/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Objects;

/**
 * A node that holds fetch contexts, named the way the origin of a document names it: by the alias of its cluster and its
 * id. It means the same node on any coordinator, also one of another cluster, which a {@code DiscoveryNode} of the local
 * cluster state can't name.
 *
 * @param clusterAlias {@link RemoteClusterAware#LOCAL_CLUSTER_GROUP_KEY} for the local cluster
 */
public record FetchNode(String clusterAlias, String nodeId) {
    public FetchNode {
        Objects.requireNonNull(clusterAlias, "clusterAlias");
        Objects.requireNonNull(nodeId, "nodeId");
    }

    /**
     * The node {@code nodeId} of the local cluster.
     */
    public static FetchNode local(String nodeId) {
        return new FetchNode(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, nodeId);
    }

    public boolean isLocal() {
        return RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias);
    }
}
