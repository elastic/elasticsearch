/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tdigest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.search.aggregations.metrics.MemoryTrackingTDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class TDigestReleasingTests extends ESTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(
            makeTDigestParams("Hybrid", (arrays) -> TDigest.createHybridDigest(arrays, 100)),
            makeTDigestParams("Merging", (arrays) -> TDigest.createMergingDigest(arrays, 100)),
            makeTDigestParams("Sorting", TDigest::createSortingDigest),
            makeTDigestParams("AvlTree", (arrays) -> TDigest.createAvlTreeDigest(arrays, 100))
        );
    }

    public record TestCase(String name, CircuitBreaker breaker, Supplier<TDigest> tDigestSupplier) {
        @Override
        public String toString() {
            return name;
        }
    }

    private static Object[] makeTDigestParams(String name, Function<TDigestArrays, TDigest> tDigestSupplier) {
        var breaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        return new Object[] { new TestCase(name, breaker, () -> tDigestSupplier.apply(new MemoryTrackingTDigestArrays(breaker))) };
    }

    private final TestCase testCase;

    public TDigestReleasingTests(TestCase testCase) {
        this.testCase = testCase;
    }

    public void testRelease() {
        var breaker = testCase.breaker;
        assertThat(breaker.getUsed(), equalTo(0L));

        var tDigest = testCase.tDigestSupplier.get();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertThat(breaker.getUsed(), equalTo(tDigest.ramBytesUsed()));

        for (int i = 0; i < 10_000; i++) {
            tDigest.add(randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true));
        }
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertThat(breaker.getUsed(), equalTo(tDigest.ramBytesUsed()));

        tDigest.close();
        assertThat("close() must release all memory", breaker.getUsed(), equalTo(0L));

        tDigest.close();
        assertThat("close() must be idempotent", breaker.getUsed(), equalTo(0L));
    }

}
