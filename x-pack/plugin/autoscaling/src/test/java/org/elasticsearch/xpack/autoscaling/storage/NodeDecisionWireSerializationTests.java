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

public class NodeDecisionWireSerializationTests extends AbstractWireSerializingTestCase<NodeDecision> {

    @Override
    protected NodeDecision mutateInstance(NodeDecision instance) throws IOException {
        if (randomBoolean()) {
            return new NodeDecision(NodeDecisionTestUtils.randomDiscoveryNode(), instance.decision());
        } else if (randomBoolean()) {
            return new NodeDecision(instance.node(), randomValueOtherThan(instance.decision(), NodeDecisionTestUtils::randomDecision));
        } else {
            return randomValueOtherThan(instance, this::createTestInstance);
        }
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
