/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.indices.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState.Stage;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class RecoveryStateTests extends ESTestCase {

    public void testCannotTransitionToCreatedStage() {
        final var state = createRecoveryState();
        state.setStage(Stage.INIT);

        final AssertionError error = expectThrows(AssertionError.class, () -> state.setStage(Stage.CREATED));
        assertThat(error.getMessage(), equalTo("can't move recovery to stage [CREATED] from [INIT]"));
        assertThat(state.getStage(), equalTo(Stage.INIT));
    }

    public void testCreatedStageIsDowngradedToInitForOldNodes() throws IOException {
        final var createdStageVersion = TransportVersion.fromName("recovery_stage_created");
        final var state = createRecoveryState();

        assertThat(
            serializeDeserialize(state, TransportVersionUtils.getPreviousVersion(createdStageVersion)).getStage(),
            equalTo(Stage.INIT)
        );
        assertThat(
            serializeDeserialize(state, TransportVersionUtils.randomVersionSupporting(createdStageVersion)).getStage(),
            equalTo(Stage.CREATED)
        );
    }

    private static RecoveryState createRecoveryState() {
        final var discoveryNode = DiscoveryNodeUtils.builder(randomUUID()).roles(emptySet()).build();
        final var shardRouting = TestShardRouting.newShardRouting(
            new ShardId(randomIndexName(), randomUUID(), 0),
            discoveryNode.getId(),
            randomBoolean(),
            ShardRoutingState.INITIALIZING
        );
        final var state = new RecoveryState(
            shardRouting,
            discoveryNode,
            shardRouting.recoverySource().getType() == RecoverySource.Type.PEER ? discoveryNode : null
        );
        assertThat(state.getStage(), equalTo(Stage.CREATED));
        return state;
    }

    private RecoveryState serializeDeserialize(RecoveryState state, TransportVersion version) throws IOException {
        return copyWriteable(state, writableRegistry(), RecoveryState::readRecoveryState, version);
    }
}
