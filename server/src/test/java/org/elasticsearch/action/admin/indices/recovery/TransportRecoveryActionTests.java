/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.ThrottlingRecoveryService.BlockedState;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TransportRecoveryActionTests extends ESTestCase {

    public void testGateIsOnlyReportedForQueuedCreatedRecoveries() {
        final String allocationId = randomIdentifier();
        final String gate = randomIdentifier();
        final var blockedRecoveries = new TransportRecoveryAction.BlockedRecoveries(
            new BlockedState(gate, randomNonNegativeLong()),
            Set.of(allocationId)
        );

        assertThat(blockedRecoveries.gateFor(allocationId, RecoveryState.Stage.CREATED), equalTo(gate));
        assertNull(blockedRecoveries.gateFor(randomIdentifier(), RecoveryState.Stage.CREATED));
        for (RecoveryState.Stage stage : RecoveryState.Stage.values()) {
            if (stage != RecoveryState.Stage.CREATED) {
                assertNull(blockedRecoveries.gateFor(allocationId, stage));
            }
        }
    }

    public void testNoGateIsReportedWhenNodeIsNotBlocked() {
        final String allocationId = randomIdentifier();
        final var blockedRecoveries = new TransportRecoveryAction.BlockedRecoveries(null, Set.of(allocationId));

        assertNull(blockedRecoveries.gateFor(allocationId, RecoveryState.Stage.CREATED));
    }
}
