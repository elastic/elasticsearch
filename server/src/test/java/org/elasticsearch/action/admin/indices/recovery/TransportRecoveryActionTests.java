/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.indices.recovery.ThrottlingRecoveryService.BlockedState;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TransportRecoveryActionTests extends ESTestCase {

    public void testBlockedForMillis() {
        final String allocationId = randomIdentifier();
        final String gate = randomIdentifier();
        final long sinceRelativeMillis = randomLongBetween(0L, 1_000_000L);
        final long blockedForMillis = randomLongBetween(0L, 1_000_000L);
        final BlockedState blockedState = new BlockedState(gate, sinceRelativeMillis);
        final var blockedRecoveries = new TransportRecoveryAction.BlockedRecoveries(
            blockedState,
            Set.of(allocationId),
            sinceRelativeMillis + blockedForMillis
        );

        assertThat(blockedRecoveries.blockedState(), equalTo(blockedState));
        assertThat(blockedRecoveries.allocationIds(), equalTo(Set.of(allocationId)));
        assertThat(blockedRecoveries.blockedForMillis(), equalTo(blockedForMillis));
    }
}
