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
        if (instance.getClusterRebalanceDecision() != null) {
            return switch (between(0, 4)) {
                case 0 -> MoveDecision.rebalance(
                    instance.getCanRemainDecision(),
                    instance.getClusterRebalanceDecision(),
                    randomValueOtherThan(instance.getAllocationDecision(), () -> randomFrom(AllocationDecision.values())),
                    instance.getTargetNode(),
                    instance.getCurrentNodeRanking(),
                    instance.getNodeDecisions()
                );
                case 1 -> MoveDecision.rebalance(
                    randomValueOtherThan(
                        instance.getCanRemainDecision(),
                        () -> randomLabelledDecision(Decision.Type.YES, Decision.Type.NO, Decision.Type.NOT_PREFERRED)
                    ),
                    instance.getClusterRebalanceDecision(),
                    instance.getAllocationDecision(),
                    instance.getTargetNode(),
                    instance.getCurrentNodeRanking(),
                    instance.getNodeDecisions()
                );
                case 2 -> MoveDecision.rebalance(
                    instance.getCanRemainDecision(),
                    randomValueOtherThan(
                        instance.getClusterRebalanceDecision(),
                        () -> randomLabelledDecision(Decision.Type.YES, Decision.Type.NO)
                    ),
                    instance.getAllocationDecision(),
                    instance.getTargetNode(),
                    instance.getCurrentNodeRanking(),
                    instance.getNodeDecisions()
                );
                case 3 -> MoveDecision.rebalance(
                    instance.getCanRemainDecision(),
                    instance.getClusterRebalanceDecision(),
                    instance.getAllocationDecision(),
                    instance.getTargetNode(),
                    randomValueOtherThan(instance.getCurrentNodeRanking(), () -> randomIntBetween(0, 10)),
                    instance.getNodeDecisions()
                );
                default -> MoveDecision.rebalance(
                    instance.getCanRemainDecision(),
                    instance.getClusterRebalanceDecision(),
                    instance.getAllocationDecision(),
                    instance.getTargetNode(),
                    instance.getCurrentNodeRanking(),
                    randomValueOtherThan(instance.getNodeDecisions(), () -> randomBoolean() ? null : List.of())
                );
            };
        }
        // move() asserts allocationDecision/targetNode consistency — only mutate fields that stay valid alone.
        return switch (between(0, 2)) {
            case 0 -> MoveDecision.move(
                instance.getCanRemainDecision(),
                instance.getAllocationDecision(),
                instance.getTargetNode(),
                // Flip null <-> empty list so we never hang when the instance already has List.of().
                randomValueOtherThan(instance.getNodeDecisions(), () -> instance.getNodeDecisions() == null ? List.of() : null),
                instance.getCanAllocateDecision()
            );
            case 1 -> MoveDecision.move(
                instance.getCanRemainDecision(),
                instance.getAllocationDecision(),
                instance.getTargetNode(),
                instance.getNodeDecisions(),
                randomValueOtherThan(
                    instance.getCanAllocateDecision(),
                    () -> instance.getCanAllocateDecision() == null
                        ? randomLabelledDecision(Decision.Type.NO, Decision.Type.NOT_PREFERRED)
                        : null
                )
            );
            default -> {
                // Change only canMoveDecision, staying consistent with the existing targetNode/canRemain.
                DiscoveryNode target = instance.getTargetNode();
                Decision.Type remainType = instance.getCanRemainDecision().type();
                List<AllocationDecision> candidates = new java.util.ArrayList<>();
                if (target != null) {
                    candidates.add(AllocationDecision.YES);
                    candidates.add(AllocationDecision.NOT_PREFERRED);
                } else if (remainType == Decision.Type.NOT_PREFERRED) {
                    candidates.add(AllocationDecision.NO);
                    candidates.add(AllocationDecision.THROTTLED);
                    candidates.add(AllocationDecision.NOT_PREFERRED);
                } else {
                    candidates.add(AllocationDecision.NO);
                    candidates.add(AllocationDecision.THROTTLED);
                }
                candidates.removeIf(d -> d.equals(instance.getAllocationDecision()));
                // null nodeDecisions + NO returns a cached singleton — skip that candidate.
                if (instance.getNodeDecisions() == null) {
                    candidates.removeIf(d -> d == AllocationDecision.NO);
                }
                if (candidates.isEmpty()) {
                    yield MoveDecision.move(
                        instance.getCanRemainDecision(),
                        instance.getAllocationDecision(),
                        instance.getTargetNode(),
                        randomValueOtherThan(instance.getNodeDecisions(), () -> instance.getNodeDecisions() == null ? List.of() : null),
                        instance.getCanAllocateDecision()
                    );
                }
                yield MoveDecision.move(
                    instance.getCanRemainDecision(),
                    randomFrom(candidates),
                    target,
                    instance.getNodeDecisions(),
                    instance.getCanAllocateDecision()
                );
            }
        };
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
