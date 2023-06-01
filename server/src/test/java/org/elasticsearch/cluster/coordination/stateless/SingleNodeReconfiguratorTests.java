/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class SingleNodeReconfiguratorTests extends ESTestCase {
    public void testReconfigureIsANoOp() {
        final var reconfigurator = new SingleNodeReconfigurator(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        final var currentLeader = DiscoveryNodeUtils.create("current-leader");
        final var currentVotingConfig = new CoordinationMetadata.VotingConfiguration(Set.of(currentLeader.getId()));

        assertThat(
            reconfigurator.reconfigure(Set.of(), Set.of(), currentLeader, currentVotingConfig),
            is(sameInstance(currentVotingConfig))
        );
    }

    public void testReconfigureAddsCurrentLeaderAsSingleNodeInVotingConfiguration() {
        final var reconfigurator = new SingleNodeReconfigurator(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        final var oldLeader = DiscoveryNodeUtils.create("old-leader");
        final var currentLeader = DiscoveryNodeUtils.create("current-leader");
        final var currentVotingConfig = new CoordinationMetadata.VotingConfiguration(Set.of(oldLeader.getId()));

        final var updatedState = reconfigurator.maybeReconfigureAfterNewMasterIsElected(
            ClusterState.builder(ClusterName.DEFAULT)
                .nodes(
                    DiscoveryNodes.builder()
                        .masterNodeId(currentLeader.getId())
                        .localNodeId(currentLeader.getId())
                        .add(currentLeader)
                        .add(oldLeader)
                        .build()
                )
                .metadata(
                    Metadata.builder()
                        .coordinationMetadata(
                            CoordinationMetadata.builder()
                                .lastCommittedConfiguration(currentVotingConfig)
                                .lastAcceptedConfiguration(currentVotingConfig)
                                .term(2)
                                .build()
                        )
                        .build()
                )
                .build()
        );

        final var expectedAcceptedVotingConfiguration = new CoordinationMetadata.VotingConfiguration(Set.of(currentLeader.getId()));
        assertThat(
            updatedState.metadata().coordinationMetadata().getLastAcceptedConfiguration(),
            is(equalTo(expectedAcceptedVotingConfiguration))
        );
    }

}
