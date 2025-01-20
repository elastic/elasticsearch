/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.cluster.coordination.votingonly;

import org.elasticsearch.cluster.coordination.CoordinationStateTestCluster;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VotingOnlyNodeCoordinationStateTests extends ESTestCase {

    public void testSafety() {
        new CoordinationStateTestCluster(
            IntStream.range(0, randomIntBetween(1, 5))
                .mapToObj(
                    i -> DiscoveryNodeUtils.create(
                        "node_" + i,
                        buildNewFakeTransportAddress(),
                        Map.of(),
                        randomBoolean()
                            ? DiscoveryNodeRole.roles()
                            : Set.of(
                                DiscoveryNodeRole.DATA_ROLE,
                                DiscoveryNodeRole.INGEST_ROLE,
                                DiscoveryNodeRole.MASTER_ROLE,
                                DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE
                            )
                    )
                )
                .collect(Collectors.toList()),
            new VotingOnlyNodePlugin.VotingOnlyNodeElectionStrategy()
        ).runRandomly();
    }

}
