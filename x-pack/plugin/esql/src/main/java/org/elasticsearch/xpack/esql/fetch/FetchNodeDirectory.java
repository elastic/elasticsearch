/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.util.function.Supplier;

/**
 * Finds the node that a {@link FetchNode} names, for a coordinator that fetches documents or frees their contexts. A node
 * of the local cluster comes from the cluster state.
 * <p>
 * The fetch phase reads the local cluster only, so a node of a remote cluster fails the lookup. The lookup takes a
 * listener anyway: the nodes of a remote cluster have to be collected first, which the directory will do without a change
 * for its callers.
 * <p>
 * Thread-safe. The listener runs on the calling thread.
 */
public final class FetchNodeDirectory {
    private final Supplier<DiscoveryNodes> localNodes;

    /**
     * @param localNodes the nodes of the local cluster, as of now
     */
    public FetchNodeDirectory(Supplier<DiscoveryNodes> localNodes) {
        this.localNodes = localNodes;
    }

    /**
     * Finds the node {@code node} names. Fails with {@link NoNodeAvailableException} when the cluster doesn't have it
     * anymore, and with {@link IllegalArgumentException} for a node of a remote cluster.
     */
    public void find(FetchNode node, ActionListener<DiscoveryNode> listener) {
        if (node.isLocal() == false) {
            listener.onFailure(
                new IllegalArgumentException("[" + node + "] is a node of a remote cluster, which the fetch phase can't reach")
            );
            return;
        }
        DiscoveryNode found = localNodes.get().get(node.nodeId());
        if (found == null) {
            listener.onFailure(new NoNodeAvailableException("node [" + node.nodeId() + "] left the cluster"));
        } else {
            listener.onResponse(found);
        }
    }
}
