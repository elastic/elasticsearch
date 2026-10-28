/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptySet;

public class MoveDecisionWireTests extends AbstractWireSerializingTestCase<MoveDecision> {

    @Override
    protected Writeable.Reader<MoveDecision> instanceReader() {
        return MoveDecision::new;
    }

    @Override
    protected MoveDecision createTestInstance() {
        final var nodeDecisions = randomBoolean() ? null : List.<NodeAllocationResult>of();
        if (randomBoolean()) {
            return MoveDecision.rebalance(
                randomLabelledDecision(Decision.Type.NO, Decision.Type.NOT_PREFERRED),
                randomLabelledDecision(Decision.Type.YES, Decision.Type.NO),
                randomFrom(AllocationDecision.values()),
                randomBoolean() ? null : randomNode(),
                randomIntBetween(0, 10),
                nodeDecisions == null ? List.of() : nodeDecisions
            );
        }
        return switch (randomIntBetween(0, 3)) {
            case 0 -> MoveDecision.move(Decision.NO, AllocationDecision.YES, randomNode(), nodeDecisions, null);
            // a not-preferred move has a target (canRemain NO) and a labelled canAllocate decision
            case 1 -> MoveDecision.move(
                Decision.NO,
                AllocationDecision.NOT_PREFERRED,
                randomNode(),
                nodeDecisions,
                randomLabelledDecision(Decision.Type.NO, Decision.Type.NOT_PREFERRED)
            );
            // ... or no target (canRemain NOT_PREFERRED)
            case 2 -> MoveDecision.move(Decision.NOT_PREFERRED, AllocationDecision.NOT_PREFERRED, null, nodeDecisions, null);
            case 3 -> MoveDecision.move(Decision.NO, AllocationDecision.THROTTLED, null, nodeDecisions, null);
            default -> throw new AssertionError("unreachable");
        };
    }

    @Override
    protected MoveDecision mutateInstance(MoveDecision instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    /**
     * The {@code canAllocateDecision} is internal to the node that made the move decision, so is not sent to older nodes.
     */
    public void testCanAllocateDecisionIsNotSentToOlderVersions() throws IOException {
        final var oldVersion = TransportVersion.minimumCompatible();
        assertFalse(oldVersion.supports(MoveDecision.MOVE_DECISION_CAN_ALLOCATE_DECISION));

        final var original = MoveDecision.move(
            Decision.NO,
            AllocationDecision.NOT_PREFERRED,
            randomNode(),
            null,
            randomLabelledDecision(Decision.Type.NO, Decision.Type.NOT_PREFERRED)
        );
        assertNotNull(original.getCanAllocateDecision());
        assertNotNull(copyInstance(original).getCanAllocateDecision());
        assertNull(copyInstance(original, oldVersion).getCanAllocateDecision());
    }

    private static DiscoveryNode randomNode() {
        return DiscoveryNodeUtils.builder(randomIdentifier()).roles(emptySet()).build();
    }

    private static Decision.Single randomLabelledDecision(Decision.Type... types) {
        return new Decision.Single(randomFrom(types), randomIdentifier(), null);
    }
}
