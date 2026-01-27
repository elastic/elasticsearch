/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class NodeDecisionWireSerializationTests extends AbstractWireSerializingTestCase<NodeDecision> {

    @Override
    protected NodeDecision mutateInstance(NodeDecision instance) throws IOException {
        DiscoveryNode node = instance.node();
        Decision decision = instance.decision();
        switch (between(0, 1)) {
            case 0 -> node = randomValueOtherThan(node, () -> NodeDecisionTestUtils.randomDiscoveryNode());
            case 1 -> decision = randomValueOtherThan(decision, () -> NodeDecisionTestUtils.randomDecision());
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new NodeDecision(node, decision);
    }

    @Override
    protected Writeable.Reader<NodeDecision> instanceReader() {
        return NodeDecision::new;
    }

    @Override
    protected NodeDecision createTestInstance() {
        return NodeDecisionTestUtils.randomNodeDecision();
    }
}
