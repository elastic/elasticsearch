/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

/// Tests the callbacks made by the default implementation of [LocalNodeMasterListener#clusterChanged].
public class LocalNodeMasterListenerTests extends ESTestCase {

    private static final String LOCAL_NODE_ID = "local";
    private static final String OTHER_NODE_ID = "other";

    private final List<String> callbacks = new ArrayList<>();

    private final LocalNodeMasterListener listener = new LocalNodeMasterListener() {
        @Override
        public void onMaster() {
            callbacks.add("onMaster");
        }

        @Override
        public void offMaster() {
            callbacks.add("offMaster");
        }
    };

    private static ClusterState state(String masterNodeId, long term) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create(LOCAL_NODE_ID))
                    .add(DiscoveryNodeUtils.create(OTHER_NODE_ID))
                    .localNodeId(LOCAL_NODE_ID)
                    .masterNodeId(masterNodeId)
            )
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(term).build()))
            .build();
    }

    private void applyChange(ClusterState previousState, ClusterState newState) {
        listener.clusterChanged(new ClusterChangedEvent("test", newState, previousState));
    }

    public void testNoMasterToLocalMasterCallsOnMaster() {
        applyChange(state(null, 1), state(LOCAL_NODE_ID, 2));
        assertThat(callbacks, contains("onMaster"));
    }

    public void testOtherMasterToLocalMasterCallsOnMaster() {
        applyChange(state(OTHER_NODE_ID, 1), state(LOCAL_NODE_ID, randomFrom(1L, 2L)));
        assertThat(callbacks, contains("onMaster"));
    }

    public void testLocalMasterToNoMasterCallsOffMaster() {
        applyChange(state(LOCAL_NODE_ID, 1), state(null, randomFrom(1L, 2L)));
        assertThat(callbacks, contains("offMaster"));
    }

    /// The previous state's master can be replaced directly by another node, without the local node applying a no-master state.
    public void testLocalMasterToOtherMasterCallsOffMaster() {
        applyChange(state(LOCAL_NODE_ID, 1), state(OTHER_NODE_ID, 2));
        assertThat(callbacks, contains("offMaster"));
    }

    public void testUnchangedLocalMasterCallsNothing() {
        applyChange(state(LOCAL_NODE_ID, 1), state(LOCAL_NODE_ID, 1));
        assertThat(callbacks, empty());
    }

    public void testUnchangedOtherMasterCallsNothing() {
        applyChange(state(OTHER_NODE_ID, 1), state(OTHER_NODE_ID, randomFrom(1L, 2L)));
        assertThat(callbacks, empty());
    }

    public void testNoMasterToNoMasterCallsNothing() {
        applyChange(state(null, 1), state(null, randomFrom(1L, 2L)));
        assertThat(callbacks, empty());
    }

    /// The local node can lose mastership and be re-elected in a new term without ever applying a state in which it was not the master. We
    /// report that as losing and then regaining mastership, in that order, so that any work that was only triggered by the previous
    /// election gets triggered again.
    public void testReElectionInNewTermCallsOffMasterThenOnMaster() {
        applyChange(state(LOCAL_NODE_ID, 1), state(LOCAL_NODE_ID, randomLongBetween(2, 10)));
        assertThat(callbacks, contains("offMaster", "onMaster"));
    }
}
