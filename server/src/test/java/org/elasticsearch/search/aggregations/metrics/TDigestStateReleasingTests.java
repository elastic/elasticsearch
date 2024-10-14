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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
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

    public void testCreateOfType() {
        testCircuitBreakerTrip(circuitBreaker -> TDigestState.createOfType(circuitBreaker, digestType, 100));
    }

    public void testCreateUsingParamsFrom() {
        testCircuitBreakerTrip(circuitBreaker -> {
            try (TDigestState example = TDigestState.createOfType(newLimitedBreaker(ByteSizeValue.ofMb(100)), digestType, 100)) {
                return TDigestState.createUsingParamsFrom(example);
            }
        });
    }

    /**
     * This test doesn't use the {@code digestType} param.
     */
    public void testCreate() {
        testCircuitBreakerTrip(circuitBreaker -> TDigestState.create(circuitBreaker, 100));
    }

    /**
     * This test doesn't use the {@code digestType} param.
     */
    public void testCreateOptimizedForAccuracy() {
        testCircuitBreakerTrip(circuitBreaker -> TDigestState.createOptimizedForAccuracy(circuitBreaker, 100));
    }

    public void testRead() throws IOException {
        try (
            TDigestState state = TDigestState.createOfType(newLimitedBreaker(ByteSizeValue.ofMb(100)), digestType, 100);
            BytesStreamOutput output = new BytesStreamOutput()
        ) {
            TDigestState.write(state, output);

            testCircuitBreakerTrip(circuitBreaker -> {
                try (StreamInput input = output.bytes().streamInput()) {
                    return TDigestState.read(circuitBreaker, input);
                }
            });
        }
    }

    public void testReadWithData() throws IOException {
        try (
            TDigestState state = TDigestState.createOfType(newLimitedBreaker(ByteSizeValue.ofMb(100)), digestType, 100);
            BytesStreamOutput output = new BytesStreamOutput()
        ) {
            for (int i = 0; i < 1000; i++) {
                state.add(randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true));
            }

            TDigestState.write(state, output);

            testCircuitBreakerTrip(circuitBreaker -> {
                try (StreamInput input = output.bytes().streamInput()) {
                    return TDigestState.read(circuitBreaker, input);
                }
            });
        }
    }

    /**
     * Tests that a circuit breaker trip leaves no unreleased memory.
     */
    public <E extends Exception> void testCircuitBreakerTrip(CheckedFunction<CircuitBreaker, TDigestState, E> tDigestStateFactory)
        throws E {
        try (CrankyCircuitBreakerService circuitBreakerService = new CrankyCircuitBreakerService()) {
            CircuitBreaker breaker = circuitBreakerService.getBreaker("test");

            try (TDigestState state = tDigestStateFactory.apply(breaker)) {
                // Add some data to make it trip. It won't work in all digest types
                for (int i = 0; i < 10; i++) {
                    state.add(randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true));
                }
            } catch (CircuitBreakingException e) {
                // Expected
            } finally {
                assertThat("unreleased bytes", breaker.getUsed(), equalTo(0L));
            }
        }
    }
}
