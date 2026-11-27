/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Set;

/**
 * Keeps a node out of the cluster unless the join attributes it sent pass a check on the elected master, like the node feature barrier.
 * Unlike a join validator, the check runs in the cluster state update that adds the node, so the cluster state can't change between the
 * check and the node joining. Unlike the node feature barrier, it isn't a ratchet: it admits any node that passes the check against the
 * current state.
 */
@FunctionalInterface
public interface JoinAttributeBarrier {

    /**
     * Returns this node's join attributes, sent with each join request together with those of every other barrier. Called each time this
     * node tries to join, possibly concurrently and sometimes under the coordinator's mutex, so it must be cheap, thread-safe and must not
     * throw. A change starts a new join attempt even if one is already pending, so the attributes should only change when the node's
     * configuration does. Attributes must not be secret, since join requests are logged.
     */
    default Set<String> joinAttributes() {
        return Set.of();
    }

    /**
     * Throws an {@link IllegalStateException} if {@code node} must not join a cluster with {@code state}. {@code joinAttributes} is what
     * the joining node reports about itself. Not called for nodes on versions that don't send join attributes, so a missing attribute
     * means the node doesn't have it.
     */
    void check(DiscoveryNode node, Set<String> joinAttributes, ClusterState state);
}
