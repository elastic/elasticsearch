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
