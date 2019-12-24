/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VotingOnlyNodeCoordinationStateTests extends ESTestCase {

    public void testSafety() {
        new CoordinationStateTestCluster(IntStream.range(0, randomIntBetween(1, 5))
            .mapToObj(i -> new DiscoveryNode("node_" + i, buildNewFakeTransportAddress(), Map.of(),
                randomBoolean() ? DiscoveryNodeRole.BUILT_IN_ROLES :
                Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.MASTER_ROLE,
                    VotingOnlyNodePlugin.VOTING_ONLY_NODE_ROLE), Version.CURRENT))
            .collect(Collectors.toList()), new VotingOnlyNodePlugin.VotingOnlyNodeElectionStrategy())
            .runRandomly();
    }

}
