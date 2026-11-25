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

import java.util.Map;

/**
 * Checks a node that joins against the cluster state it is added to, using the join attributes it sent with its join request. Unlike a
 * join validator, it runs on the elected master in the cluster state update that adds the node, so a concurrent update can't make it
 * pass against an outdated state, and it sees facts about the node that can change while the node runs, such as its secure settings.
 */
@FunctionalInterface
public interface JoinAttributesValidator {

    /**
     * Throws an {@link IllegalStateException} if {@code node} must not join a cluster with {@code state}. {@code joinAttributes} is
     * empty if the node is on a version that doesn't send them.
     */
    void validate(DiscoveryNode node, Map<String, String> joinAttributes, ClusterState state);
}
