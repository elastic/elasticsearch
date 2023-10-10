/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.cluster.coordination.votingonly;

import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;

import java.util.Set;

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
    protected CoordinatorStrategy createCoordinatorStrategy() {
        return new DefaultCoordinatorStrategy(new VotingOnlyNodePlugin.VotingOnlyNodeElectionStrategy());
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
        return DiscoveryNodeUtils.builder("node" + nodeIndex)
            .ephemeralId(UUIDs.randomBase64UUID(random()))  // generated deterministically for repeatable tests
            .roles(
                masterEligible ? ALL_ROLES_EXCEPT_VOTING_ONLY
                    : randomBoolean() ? emptySet()
                    : Set.of(
                        DiscoveryNodeRole.DATA_ROLE,
                        DiscoveryNodeRole.INGEST_ROLE,
                        DiscoveryNodeRole.MASTER_ROLE,
                        DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE
                    )
            )
            .build();
    }

}
