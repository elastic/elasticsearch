/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StatelessPersistedStateTests extends ESTestCase {
    public void testClusterStateIsWrittenIntoTheStore() throws Exception {
        var clusterStateWriter = new InMemoryClusterStateStore();

        var localNode = newDiscoveryNode("local-node");
        var clusterState = createClusterState(0, 0, localNode);

        try (var persistedState = new StatelessPersistedState(clusterState, 0, clusterStateWriter)) {
            persistedState.setCurrentTerm(1);

            var lastAcceptedState = createClusterState(1, 1, localNode);
            persistedState.setLastAcceptedState(lastAcceptedState);

            assertThat(persistedState.getLastAcceptedState(), is(lastAcceptedState));

            Optional<PersistedClusterState> latestStoredState = PlainActionFuture.get(f -> clusterStateWriter.read(1, f));

            assertThat(latestStoredState.isPresent(), is(true));
            PersistedClusterState persistedClusterState = latestStoredState.get();
            assertThat(persistedClusterState.term(), is(equalTo(lastAcceptedState.term())));
            assertThat(persistedClusterState.version(), is(equalTo(lastAcceptedState.version())));
            assertThat(persistedClusterState.clusterUUID(), is(equalTo(lastAcceptedState.metadata().clusterUUID())));
        }
    }

    public void testClusterStateIsWrittenIntoTheStoreOnlyInTheElectedMaster() throws Exception {
        var clusterStateWriter = new InMemoryClusterStateStore() {
            @Override
            public synchronized void write(ClusterState clusterState) {
                assert false;
            }
        };

        var localNode = newDiscoveryNode("local-node");
        var clusterState = createClusterState(0, 0, localNode);

        try (var persistedState = new StatelessPersistedState(clusterState, 0, clusterStateWriter)) {
            persistedState.setCurrentTerm(1);

            var lastAcceptedState = createClusterState(1, 1, localNode, false);
            persistedState.setLastAcceptedState(lastAcceptedState);

            assertThat(persistedState.getLastAcceptedState(), is(lastAcceptedState));

            Optional<PersistedClusterState> latestStoredState = PlainActionFuture.get(f -> clusterStateWriter.read(1, f));
            assertThat(latestStoredState.isPresent(), is(false));
        }
    }

    public void testGetLatestStoredStateReturnsTheLatestStateWhenLocalStateIsStale() throws Exception {
        var clusterStateWriter = new InMemoryClusterStateStore();

        var localNode = newDiscoveryNode("local-node");
        var clusterState = createClusterState(0, 0, localNode);

        var newState = createClusterState(2, 2, localNode);
        clusterStateWriter.write(newState);

        try (var persistedState = new StatelessPersistedState(clusterState, 0, clusterStateWriter)) {
            persistedState.setCurrentTerm(3);

            ClusterState latestStoredState = PlainActionFuture.get(f -> persistedState.getLatestStoredState(3, f));
            assertThat(latestStoredState, is(notNullValue()));
            assertThat(latestStoredState.term(), is(equalTo(2L)));
            assertThat(latestStoredState.version(), is(equalTo(2L)));
        }
    }

    public void testGetLatestStoredStateUnderFailures() throws Exception {
        boolean failListingClusterState = randomBoolean();
        boolean failReadingClusterState = failListingClusterState == false || randomBoolean();
        var clusterStateWriter = new InMemoryClusterStateStore() {
            @Override
            public void read(long term, ActionListener<Optional<PersistedClusterState>> listener) {
                if (failReadingClusterState) {
                    if (randomBoolean()) {
                        listener.onFailure(new IOException("Failed to read the cluster state"));
                    } else {
                        listener.onResponse(Optional.empty());
                    }
                } else {
                    super.read(term, listener);
                }
            }

            @Override
            public void getLatestStoredClusterStateMetadataForTerm(
                long targetTerm,
                ActionListener<Optional<PersistedClusterStateMetadata>> listener
            ) {
                if (failListingClusterState) {
                    listener.onFailure(new IOException("Failed to list cluster state"));
                } else {
                    super.getLatestStoredClusterStateMetadataForTerm(targetTerm, listener);
                }
            }
        };

        var localNode = newDiscoveryNode("local-node");
        var clusterState = createClusterState(0, 0, localNode);

        var newState = createClusterState(2, 2, localNode);
        clusterStateWriter.write(newState);

        try (var persistedState = new StatelessPersistedState(clusterState, 0, clusterStateWriter)) {
            persistedState.setCurrentTerm(3);

            expectThrows(
                Exception.class,
                () -> PlainActionFuture.<ClusterState, Exception>get(f -> persistedState.getLatestStoredState(3, f))
            );
        }
    }

    public void testGetLatestStoredStateReturnsNullWhenAppliedStateIsFresh() throws Exception {
        var currentTermSupplier = new AtomicLong();
        var clusterStateWriter = new InMemoryClusterStateStore() {
            @Override
            public void read(long targetTerm, ActionListener<Optional<PersistedClusterState>> listener) {
                assert false : "Unexpected call";
            }
        };

        var localNode = newDiscoveryNode("local-node");
        var clusterState = createClusterState(1, 1, localNode);

        clusterStateWriter.write(clusterState);

        try (var persistedState = new StatelessPersistedState(clusterState, 1, clusterStateWriter)) {
            persistedState.setCurrentTerm(1);
            currentTermSupplier.set(1);

            ClusterState latestStoredState = PlainActionFuture.get(f -> persistedState.getLatestStoredState(1, f));
            assertThat(latestStoredState, is(nullValue()));
        }
    }

    private static DiscoveryNode newDiscoveryNode(String id) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), emptyMap(), Set.of(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
    }

    private ClusterState createClusterState(long term, long version, DiscoveryNode localNode) {
        return createClusterState(term, version, localNode, true);
    }

    private ClusterState createClusterState(long term, long version, DiscoveryNode localNode, boolean localNodeMaster) {
        var discoveryNodes = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode);
        if (localNodeMaster) {
            discoveryNodes.masterNodeId(localNode.getId());
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .version(version)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(term).build()))
            .build();
    }
}
