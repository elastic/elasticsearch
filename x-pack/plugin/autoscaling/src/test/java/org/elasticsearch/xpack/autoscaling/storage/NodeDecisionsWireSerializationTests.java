/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;

public class NodeDecisionsWireSerializationTests extends AbstractWireSerializingTestCase<NodeDecisions> {

    @Override
    protected NodeDecisions mutateInstance(NodeDecisions instance) throws IOException {
        List<NodeDecision> canAllocateDecisions = instance.canAllocateDecisions();
        NodeDecision canRemainDecision = instance.canRemainDecision();
        switch (between(0, 1)) {
            case 0 -> canAllocateDecisions = randomValueOtherThan(
                canAllocateDecisions,
                NodeDecisionsWireSerializationTests::randomNodeDecisions
            );
            case 1 -> canRemainDecision = randomValueOtherThan(
                canRemainDecision,
                () -> randomBoolean() ? NodeDecisionTestUtils.randomNodeDecision() : null
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new NodeDecisions(canAllocateDecisions, canRemainDecision);
    }

    @Override
    protected Writeable.Reader<NodeDecisions> instanceReader() {
        return NodeDecisions::new;
    }

    @Override
    protected NodeDecisions createTestInstance() {
        return new NodeDecisions(randomNodeDecisions(), randomBoolean() ? NodeDecisionTestUtils.randomNodeDecision() : null);
    }

    private static List<NodeDecision> randomNodeDecisions() {
        return randomList(8, NodeDecisionTestUtils::randomNodeDecision);
    }

}
