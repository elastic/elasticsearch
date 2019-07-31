/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.Set;

import static java.util.Collections.emptySet;

public class VotingOnlyNodeCoordinatorTests extends AbstractCoordinatorTestCase {

    @BeforeClass
    public static void setPossibleRolesWithVotingOnly() {
        DiscoveryNode.setPossibleRoles(
            Sets.union(DiscoveryNodeRole.BUILT_IN_ROLES, Sets.newHashSet(VotingOnlyNodePlugin.VOTING_ONLY_NODE_ROLE)));
    }

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
        final TransportAddress address = buildNewFakeTransportAddress();
        return new DiscoveryNode("", "node" + nodeIndex,
            UUIDs.randomBase64UUID(random()), // generated deterministically for repeatable tests
            address.address().getHostString(), address.getAddress(), address, Collections.emptyMap(),
            masterEligible ? DiscoveryNodeRole.BUILT_IN_ROLES :
                randomBoolean() ? emptySet() : Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE,
                    DiscoveryNodeRole.MASTER_ROLE, VotingOnlyNodePlugin.VOTING_ONLY_NODE_ROLE), Version.CURRENT);
    }

}
