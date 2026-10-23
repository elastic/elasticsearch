/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class QuantileStatesTests extends ComputeTestCase {

    /**
     * Serializing a group with no observations builds a temporary digest. Tripping the breaker while appending it
     * must still release that digest. Every limit up to the first that succeeds is tried, so the trip lands on the
     * append at least once; {@link #allBreakersEmpty} then checks nothing leaked.
     */
    public void testToIntermediateReleasesEmptyGroupDigestWhenBreaking() {
        for (long limit = 0; limit < ByteSizeValue.ofMb(1).getBytes(); limit++) {
            var blockFactory = blockFactory(ByteSizeValue.ofBytes(limit));
            var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
            Block[] intermediate = new Block[1];
            try (
                var state = new QuantileStates.GroupingState(blockFactory.breaker(), blockFactory.bigArrays(), 50.0);
                IntVector selected = blockFactory.newIntArrayVector(new int[] { 0, 1 }, 2)
            ) {
                state.add(1, 2.0);
                state.toIntermediate(intermediate, 0, selected, driverContext);
                intermediate[0].close();
                return;
            } catch (CircuitBreakingException e) {
                assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
            }
        }
        fail("toIntermediate never succeeded");
    }
}
