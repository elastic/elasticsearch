/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static java.util.Collections.emptySet;

public class ShardRecoveryInfoTests extends ESTestCase {

    public void testGateForAllRecoveryStages() throws IOException {
        final RecoveryState recoveryState = createRecoveryState();
        final String gate = randomIdentifier();
        final long blockedForMillis = randomNonNegativeLong();
        final ShardRecoveryInfo recoveryInfo = new ShardRecoveryInfo(recoveryState, gate, blockedForMillis);

        assertSame(recoveryState, recoveryInfo.recoveryState());

        for (RecoveryState.Stage stage : RecoveryState.Stage.values()) {
            advanceToStage(recoveryState, stage);
            final boolean isBlocked = stage == RecoveryState.Stage.CREATED;
            assertEquals(isBlocked ? gate : null, recoveryInfo.blockedByGate());

            final ShardRecoveryInfo copy = serializeDeserialize(
                recoveryInfo,
                TransportVersionUtils.randomVersionSupporting(ShardRecoveryInfo.GATE_IN_RECOVERY_RESPONSE)
            );
            assertEquals(stage, copy.recoveryState().getStage());
            assertEquals(isBlocked ? gate : null, copy.blockedByGate());
            assertEquals(isBlocked ? blockedForMillis : ShardRecoveryInfo.NOT_BLOCKED_MILLIS, copy.blockedForMillis());
        }
    }

    public void testNotBlockedSentinel() {
        final ShardRecoveryInfo recoveryInfo = new ShardRecoveryInfo(createRecoveryState(), null, ShardRecoveryInfo.NOT_BLOCKED_MILLIS);

        assertNull(recoveryInfo.blockedByGate());
        assertEquals(ShardRecoveryInfo.NOT_BLOCKED_MILLIS, recoveryInfo.blockedForMillis());
    }

    public void testAssertsConsistentGateAndDuration() {
        final RecoveryState recoveryState = createRecoveryState();
        expectThrows(AssertionError.class, () -> new ShardRecoveryInfo(recoveryState, null, 0L));
        expectThrows(
            AssertionError.class,
            () -> new ShardRecoveryInfo(recoveryState, randomIdentifier(), ShardRecoveryInfo.NOT_BLOCKED_MILLIS)
        );
    }

    public void testGateOmittedForUnsupportedTransportVersion() throws IOException {
        final ShardRecoveryInfo copy = serializeDeserialize(
            new ShardRecoveryInfo(createRecoveryState(), randomIdentifier(), randomNonNegativeLong()),
            TransportVersionUtils.getPreviousVersion(ShardRecoveryInfo.GATE_IN_RECOVERY_RESPONSE)
        );
        assertNull(copy.blockedByGate());
        assertEquals(ShardRecoveryInfo.NOT_BLOCKED_MILLIS, copy.blockedForMillis());
    }

    private static void advanceToStage(RecoveryState recoveryState, RecoveryState.Stage stage) {
        if (stage != RecoveryState.Stage.CREATED) {
            recoveryState.setStage(stage);
            if (stage == RecoveryState.Stage.INDEX) {
                recoveryState.getIndex().setFileDetailsComplete();
            }
        }
        assertEquals(stage, recoveryState.getStage());
    }

    private ShardRecoveryInfo serializeDeserialize(ShardRecoveryInfo recoveryInfo, TransportVersion version) throws IOException {
        return copyWriteable(recoveryInfo, writableRegistry(), ShardRecoveryInfo::new, version);
    }

    private static RecoveryState createRecoveryState() {
        final DiscoveryNode sourceNode = DiscoveryNodeUtils.builder(randomIdentifier()).roles(emptySet()).build();
        final DiscoveryNode targetNode = DiscoveryNodeUtils.builder(randomIdentifier()).roles(emptySet()).build();
        return new RecoveryState(
            ShardRouting.newUnassigned(
                new ShardId(randomIndexName(), randomUUID(), randomIntBetween(0, 10)),
                false,
                RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
                ShardRouting.Role.DEFAULT,
                ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED
            ).initialize(sourceNode.getId(), null, randomNonNegativeLong()),
            sourceNode,
            targetNode
        );
    }
}
