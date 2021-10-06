/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.cluster.coordination.votingonly;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;

public class VotingOnlyNodeCoordinatorTests extends AbstractCoordinatorTestCase {

    @Override
    protected TransportInterceptor getTransportInterceptor(DiscoveryNode localNode, ThreadPool threadPool) {
        if (VotingOnlyNodePlugin.isVotingOnlyNode(localNode)) {
            return new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return new VotingOnlyNodePlugin.VotingOnlyNodeAsyncSender(sender, () -> threadPool);
                }
            };
        } else {
            return super.getTransportInterceptor(localNode, threadPool);
        }
    }

    @Override
    protected ElectionStrategy getElectionStrategy() {
        return new VotingOnlyNodePlugin.VotingOnlyNodeElectionStrategy();
    }

    public void testDoesNotElectVotingOnlyMasterNode() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5), false, Settings.EMPTY)) {
            cluster.runRandomly();
            cluster.stabilise();

            final Cluster.ClusterNode leader = cluster.getAnyLeader();
            assertTrue(leader.getLocalNode().isMasterNode());
            assertFalse(leader.getLocalNode().toString(), VotingOnlyNodePlugin.isVotingOnlyNode(leader.getLocalNode()));
        }
    }

    @Override
    protected DiscoveryNode createDiscoveryNode(int nodeIndex, boolean masterEligible) {
        final Set<DiscoveryNodeRole> allExceptVotingOnlyRole = DiscoveryNodeRole.roles()
            .stream()
            .filter(r -> r.equals(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE) == false)
            .collect(Collectors.toUnmodifiableSet());
        final TransportAddress address = buildNewFakeTransportAddress();
        return new DiscoveryNode(
            "",
            "node" + nodeIndex,
            UUIDs.randomBase64UUID(random()), // generated deterministically for repeatable tests
            address.address().getHostString(),
            address.getAddress(),
            address,
            Collections.emptyMap(),
            masterEligible ? allExceptVotingOnlyRole
                : randomBoolean() ? emptySet()
                : Set.of(
                    DiscoveryNodeRole.DATA_ROLE,
                    DiscoveryNodeRole.INGEST_ROLE,
                    DiscoveryNodeRole.MASTER_ROLE,
                    DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE
                ),
            Version.CURRENT
        );
    }

}
