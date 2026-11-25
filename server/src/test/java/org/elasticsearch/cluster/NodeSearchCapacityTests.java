/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;

public class NodeSearchCapacityTests extends AbstractWireSerializingTestCase<NodeSearchCapacity> {

    @Override
    protected Writeable.Reader<NodeSearchCapacity> instanceReader() {
        return NodeSearchCapacity::new;
    }

    @Override
    protected NodeSearchCapacity createTestInstance() {
        return randomNodeSearchCapacity();
    }

    @Override
    protected NodeSearchCapacity mutateInstance(NodeSearchCapacity instance) {
        if (randomBoolean()) {
            return new NodeSearchCapacity(
                randomValueOtherThan(instance.nodeCompute(), NodeSearchCapacityTests::randomPositiveDouble),
                instance.threadBudget()
            );
        } else {
            return new NodeSearchCapacity(
                instance.nodeCompute(),
                randomValueOtherThan(instance.threadBudget(), NodeSearchCapacityTests::randomPositiveDouble)
            );
        }
    }

    public void testRejectsZero() {
        final double positive = randomPositiveDouble();
        final AssertionError nodeComputeError = expectThrows(AssertionError.class, () -> new NodeSearchCapacity(0.0d, positive));
        assertThat(nodeComputeError.getMessage(), containsString("nodeCompute must be finite and positive"));

        final AssertionError threadBudgetError = expectThrows(AssertionError.class, () -> new NodeSearchCapacity(positive, 0.0d));
        assertThat(threadBudgetError.getMessage(), containsString("threadBudget must be finite and positive"));
    }

    private static NodeSearchCapacity randomNodeSearchCapacity() {
        return new NodeSearchCapacity(randomPositiveDouble(), randomPositiveDouble());
    }

    private static double randomPositiveDouble() {
        return randomDoubleBetween(Math.nextUp(0.0d), 64.0d, true);
    }
}
