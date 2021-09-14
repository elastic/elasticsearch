/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.function.Predicate;

public final class MasterNodeChangePredicate {

    private MasterNodeChangePredicate() {

    }

    /**
     * builds a predicate that will accept a cluster state only if it was generated after the current has
     * (re-)joined the master
     */
    public static Predicate<ClusterState> build(ClusterState currentState) {
        final long currentVersion = currentState.version();
        final DiscoveryNode masterNode = currentState.nodes().getMasterNode();
        final String currentMasterId = masterNode == null ? null : masterNode.getEphemeralId();
        return newState -> {
            final DiscoveryNode newMaster = newState.nodes().getMasterNode();
            final boolean accept;
            if (newMaster == null) {
                accept = false;
            } else if (newMaster.getEphemeralId().equals(currentMasterId) == false) {
                accept = true;
            } else {
                accept = newState.version() > currentVersion;
            }
            return accept;
        };
    }
}
