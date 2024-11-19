/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class TDigestStateReleasingTests extends ESTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(TDigestState.Type.values()).map(type -> new Object[] { type }).toList();
    }

    private final TDigestState.Type digestType;

    public TDigestStateReleasingTests(TDigestState.Type digestType) {
        this.digestType = digestType;
    }

    /**
     * Tests that a circuit breaker trip leaves no unreleased memory.
     */
    public void testCircuitBreakerTrip() {
        for (int bytes = randomIntBetween(0, 16); bytes < 50_000; bytes += 17) {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(bytes));

            try (TDigestState state = TDigestState.create(breaker, digestType, 100)) {
                // Add some data to make it trip. It won't work in all digest types
                for (int i = 0; i < 100; i++) {
                    state.add(randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true));
                }

                // Testing with more memory shouldn't change anything, we finished the test
                return;
            } catch (CircuitBreakingException e) {
                // Expected
            } finally {
                assertThat("unreleased bytes with a " + bytes + " bytes limit", breaker.getUsed(), equalTo(0L));
            }
        }

        fail("Test case didn't reach a non-tripping breaker limit");
    }
}
