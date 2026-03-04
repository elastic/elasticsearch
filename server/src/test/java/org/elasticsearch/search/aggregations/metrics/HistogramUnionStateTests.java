/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class HistogramUnionStateTests extends ESTestCase {

    record RandomState(HistogramUnionState unionState, TDigestState reference) implements Releasable {
        @Override
        public void close() {
            Releasables.close(unionState, reference);
        }
    }

    private RandomState randomState() {
        return randomState(true);
    }

    private RandomState randomState(boolean allowExponentialHistograms) {

        int numInputDatasets;
        if (randomDouble() < 0.1) {
            // 10% chance of an empty state
            numInputDatasets = 0;
        } else {
            numInputDatasets = randomIntBetween(1, 100);
        }

        HistogramUnionState resultState;
        if (randomBoolean()) {
            resultState = HistogramUnionState.create(
                breaker(),
                randomFrom(TDigestState.Type.values()),
                randomDoubleBetween(200, 300, true)
            );
        } else {
            resultState = HistogramUnionState.create(
                breaker(),
                randomFrom(TDigestExecutionHint.values()),
                randomDoubleBetween(200, 300, true)
            );
        }

        boolean useTDigests = allowExponentialHistograms == false || randomBoolean();
        boolean useExponentialHistograms = useTDigests == false || (allowExponentialHistograms && randomBoolean());

        TDigestState referenceState = TDigestState.create(breaker(), 1000.0);

        for (int i = 0; i < numInputDatasets; i++) {
            List<Double> values = IntStream.range(0, randomIntBetween(0, 100))
                .mapToDouble(j -> randomDouble() * 10_000 - 5_000)
                .boxed()
                .toList();

            values.forEach(referenceState::add);
            if (useTDigests && (useExponentialHistograms == false || randomBoolean())) {
                try (TDigestState toMerge = TDigestState.create(breaker(), randomDoubleBetween(100, 200, true))) {
                    values.forEach(toMerge::add);
                    resultState.add(toMerge);
                }
            } else {
                ReleasableExponentialHistogram hist = ExponentialHistogram.create(
                    200,
                    ExponentialHistogramCircuitBreaker.noop(),
                    values.stream().mapToDouble(Double::doubleValue).toArray()
                );
                resultState.add(hist);
            }
        }
        return new RandomState(resultState, referenceState);
    }

    private static CircuitBreaker breaker() {
        return newLimitedBreaker(ByteSizeValue.ofMb(100));
    }

    public void testEmptyBehavior() {
        try (HistogramUnionState state = HistogramUnionState.create(breaker(), randomFrom(TDigestState.Type.values()), 100)) {
            // No data at all -> contract says NaN/empty/0
            assertThat(state.size(), equalTo(0L));
            assertThat(state.cdf(0.0), equalTo(Double.NaN));
            assertThat(state.quantile(0.0), equalTo(Double.NaN));
            assertThat(state.quantile(0.5), equalTo(Double.NaN));
            assertThat(state.quantile(1.0), equalTo(Double.NaN));
            assertThat(state.centroids().isEmpty(), equalTo(true));
            assertThat(state.getMin(), equalTo(Double.POSITIVE_INFINITY));
            assertThat(state.getMax(), equalTo(Double.NEGATIVE_INFINITY));
        }
    }

    public void testRandomStateMatchesReference() {
        try (RandomState state = randomState()) {
            HistogramUnionState union = state.unionState();
            TDigestState ref = state.reference();

            assertThat(union.size(), equalTo(ref.size()));

            long centroidSum = union.centroids().stream().mapToLong(Centroid::count).sum();
            assertThat(centroidSum, equalTo(union.size()));

            if (ref.size() == 0) {
                assertThat(union.quantile(0.5), equalTo(Double.NaN));
                assertThat(union.cdf(0.0), equalTo(Double.NaN));
                assertThat(union.getMin(), equalTo(Double.POSITIVE_INFINITY));
                assertThat(union.getMax(), equalTo(Double.NEGATIVE_INFINITY));

                // only test quantiles if we have enough values to ignore the noise
            } else if (union.size() > 1000) {
                for (double q : new double[] { 0.0, 0.01, 0.1, 0.9, 0.99, 1.0 }) {
                    double expected = ref.quantile(q);
                    double actual = union.quantile(q);

                    double absoluteError = Math.abs(expected - actual);
                    double relativeError = absoluteError / Math.max(1, Math.abs(actual));
                    // In mixed-mode we may be converting TDigest->E-Histo; allow some tolerance.
                    assertThat("q=" + q, relativeError, lessThan(0.1));
                }

                for (int i = 0; i < 10; i++) {
                    double x = randomDoubleBetween(-6_000, 6_000, true);
                    double actual = union.cdf(x);
                    double expected_lower = ref.cdf(x - 1000);
                    double expected_upper = ref.cdf(x + 1000);
                    assertThat(actual, greaterThanOrEqualTo(expected_lower));
                    assertThat(actual, lessThanOrEqualTo(expected_upper));
                }

                // Verify min/max with relative error tolerance
                double refMin = ref.getMin();
                double refMax = ref.getMax();
                double actualMin = union.getMin();
                double actualMax = union.getMax();

                double minRelativeError = Math.abs(refMin - actualMin) / Math.max(1, Math.abs(refMin));
                double maxRelativeError = Math.abs(refMax - actualMax) / Math.max(1, Math.abs(refMax));

                assertThat("min relative error", minRelativeError, lessThan(0.1));
                assertThat("max relative error", maxRelativeError, lessThan(0.1));
            }
        }
    }

    public void testWriteToReadRoundTripPreservesBehavior() throws IOException {
        for (int iter = 0; iter < 30; iter++) {
            try (RandomState state = randomState()) {
                HistogramUnionState before = state.unionState();

                BytesStreamOutput out = new BytesStreamOutput();
                before.writeTo(out);

                try (HistogramUnionState after = HistogramUnionState.read(breaker(), out.bytes().streamInput())) {
                    assertThat(after, equalTo(before));
                }
            }
        }
    }

    public void testPureTDigestSerializationCompatibility() throws IOException {
        try (RandomState state = randomState(false)) {

            // Round-trip via pure-tdigest methods
            BytesStreamOutput out = new BytesStreamOutput();
            state.unionState.writeAsPureTDigestTo(out);
            try (HistogramUnionState readBack = HistogramUnionState.readAsPureTDigest(breaker(), out.bytes().streamInput())) {
                assertThat(readBack, equalTo(state.unionState));
            }

            // Compatibility: bytes written by TDigestState.write can be read by readAsPureTDigest
            BytesStreamOutput out2 = new BytesStreamOutput();
            TDigestState.write(state.reference, out2);
            try (HistogramUnionState compat = HistogramUnionState.readAsPureTDigest(breaker(), out2.bytes().streamInput())) {
                assertThat(compat.size(), equalTo(state.reference.size()));
            }
        }
    }

    public void testWriteAsPureTDigestThrowsIfContainsExponentialHistogramData() {
        try (HistogramUnionState union = HistogramUnionState.create(breaker(), randomFrom(TDigestState.Type.values()), 50)) {
            double[] values = IntStream.range(0, 10).mapToDouble(i -> i - 5.0).toArray();
            ReleasableExponentialHistogram hist = ExponentialHistogram.create(200, ExponentialHistogramCircuitBreaker.noop(), values);
            union.add(hist);

            expectThrows(IllegalStateException.class, () -> {
                BytesStreamOutput out = new BytesStreamOutput();
                union.writeAsPureTDigestTo(out);
            });
        }
    }

    public void testCacheInvalidatedAfterAdd() {
        try (
            HistogramUnionState union = HistogramUnionState.create(breaker(), randomFrom(TDigestState.Type.values()), 50);
            TDigestState td = TDigestState.create(breaker(), 50)
        ) {
            // Force t-digest + exponential histogram present
            for (int i = 0; i < 20; i++) {
                td.add(i);
            }
            union.add(td);

            double[] values = IntStream.range(0, 20).mapToDouble(i -> -1000 + i).toArray();
            ReleasableExponentialHistogram hist = ExponentialHistogram.create(200, ExponentialHistogramCircuitBreaker.noop(), values);
            union.add(hist);

            double before = union.quantile(0.5); // trigger generation of cache

            // Add more data and ensure quantile moves (very likely)
            try (TDigestState td2 = TDigestState.create(breaker(), 50)) {
                for (int i = 0; i < 200; i++) {
                    td2.add(10_000 + i);
                }
                union.add(td2);
            }

            double afterTDigest = union.quantile(0.5);
            assertThat(afterTDigest, not(equalTo(before)));

            // And now add even more data as exponential histograms and move it to the right even more
            values = IntStream.range(0, 1000).mapToDouble(i -> 100_00 + i).toArray();
            hist = ExponentialHistogram.create(200, ExponentialHistogramCircuitBreaker.noop(), values);
            union.add(hist);

            double afterExponential = union.quantile(0.5);
            assertThat(afterExponential, not(equalTo(afterTDigest)));
        }
    }

    public void testFailedDeserializationDoesNotLeak() throws IOException {
        CircuitBreaker b = breaker();

        try (RandomState state = randomState()) {
            BytesStreamOutput out = new BytesStreamOutput();
            state.unionState().writeTo(out);
            BytesReference bytes = out.bytes();
            if (bytes.length() == 0) {
                return;
            }
            BytesReference truncated = bytes.slice(0, bytes.length() - 1);

            long usedBefore = b.getUsed();
            try {
                expectThrows(EOFException.class, () -> HistogramUnionState.read(b, truncated.streamInput()));
            } finally {
                assertThat("breaker should not leak on failed read", b.getUsed(), equalTo(usedBefore));
            }
        }
    }

    public void testCrankyBreakerTripDoesNotLeak() throws IOException {
        CrankyCircuitBreakerService svc = new CrankyCircuitBreakerService();
        CircuitBreaker cranky = svc.getBreaker("test");

        BytesStreamOutput out = new BytesStreamOutput();
        try (RandomState state = randomState()) {
            state.unionState().writeTo(out);
        }

        try {
            HistogramUnionState.read(cranky, out.bytes().streamInput()).close();
        } catch (Exception e) {
            // could be CircuitBreakingException or any IO/runtime if partially read
        } finally {
            assertThat("unreleased bytes", cranky.getUsed(), equalTo(0L));
        }
    }

    public void testEqualsHashCodeDifferentCompressions() {
        try (
            HistogramUnionState empty1 = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 100);
            HistogramUnionState empty2 = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 100);
            HistogramUnionState a = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 200);
            HistogramUnionState b = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 100);
            HistogramUnionState c = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 100);
        ) {

            // Empty states with same parameters should be equal
            assertThat(empty1, equalTo(empty2));
            assertThat(empty1.hashCode(), equalTo(empty2.hashCode()));

            // Different compression values should not be equal
            assertThat(a, not(equalTo(b)));
            assertThat(a.hashCode(), not(equalTo(b.hashCode())));

            assertThat(a, not(equalTo(c)));
            assertThat(a.hashCode(), not(equalTo(c.hashCode())));

            // Same compression values should be equal
            assertThat(b, equalTo(c));
            assertThat(b.hashCode(), equalTo(c.hashCode()));

            // Add same data to all states
            for (int i = 0; i < 100; i++) {
                double value = randomDouble();
                try (TDigestState td = TDigestState.create(breaker(), 100)) {
                    td.add(value);
                    a.add(td);
                }
            }

            // After adding same data, compression difference still matters
            assertThat(a, not(equalTo(b)));
            assertThat(a.hashCode(), not(equalTo(b.hashCode())));

            assertThat(a, not(equalTo(c)));
            assertThat(a.hashCode(), not(equalTo(c.hashCode())));

            // Same compression with same data should still be equal
            assertThat(b, equalTo(c));
            assertThat(b.hashCode(), equalTo(c.hashCode()));

            // Add different data to b and c
            try (TDigestState td = TDigestState.create(breaker(), 100)) {
                td.add(randomDouble());
                b.add(td);
            }
            try (TDigestState td = TDigestState.create(breaker(), 100)) {
                td.add(randomDouble());
                c.add(td);
            }

            // Different data should not be equal
            assertThat(b, not(equalTo(c)));
            assertThat(b.hashCode(), not(equalTo(c.hashCode())));
        }
    }

    public void testEqualsHashCodeWithDifferentTypes() {
        try (
            HistogramUnionState hybrid = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 100);
            HistogramUnionState avltree = HistogramUnionState.create(breaker(), TDigestState.Type.AVL_TREE, 100);
            HistogramUnionState sorting = HistogramUnionState.create(breaker(), TDigestState.Type.SORTING, 100);
        ) {

            // Different types should not be equal even with same compression
            assertThat(hybrid, not(equalTo(avltree)));
            assertThat(hybrid.hashCode(), not(equalTo(avltree.hashCode())));

            assertThat(hybrid, not(equalTo(sorting)));
            assertThat(hybrid.hashCode(), not(equalTo(sorting.hashCode())));

            assertThat(avltree, not(equalTo(sorting)));
            assertThat(avltree.hashCode(), not(equalTo(sorting.hashCode())));

            // Add same data to all
            for (int i = 0; i < 50; i++) {
                double value = i;
                try (TDigestState td = TDigestState.create(breaker(), 100)) {
                    td.add(value);
                    hybrid.add(td);
                }
            }

            // Still not equal due to different types
            assertThat(hybrid, not(equalTo(avltree)));
            assertThat(hybrid.hashCode(), not(equalTo(avltree.hashCode())));

            assertThat(hybrid, not(equalTo(sorting)));
            assertThat(hybrid.hashCode(), not(equalTo(sorting.hashCode())));

            assertThat(avltree, not(equalTo(sorting)));
            assertThat(avltree.hashCode(), not(equalTo(sorting.hashCode())));
        }
    }

    public void testEqualsHashCodeWithExponentialHistogram() {
        try (
            HistogramUnionState a = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 100);
            HistogramUnionState b = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 100);
            HistogramUnionState c = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 100);
        ) {

            // Initially equal
            assertThat(a, equalTo(b));
            assertThat(a.hashCode(), equalTo(b.hashCode()));

            // Add exponential histogram to a and b with same data
            double[] values = new double[] { 1.0, 2.0, 3.0, 4.0, 5.0 };
            ReleasableExponentialHistogram histA = ExponentialHistogram.create(200, ExponentialHistogramCircuitBreaker.noop(), values);
            a.add(histA);
            b.add(histA);

            // Should still be equal with same exponential histogram data
            assertThat(a, equalTo(b));
            assertThat(a.hashCode(), equalTo(b.hashCode()));

            // c has no exponential histogram data
            assertThat(a, not(equalTo(c)));
            assertThat(a.hashCode(), not(equalTo(c.hashCode())));

            // Add different exponential histogram data to c
            double[] differentValues = new double[] { 10.0, 20.0, 30.0 };
            ReleasableExponentialHistogram histC = ExponentialHistogram.create(
                200,
                ExponentialHistogramCircuitBreaker.noop(),
                differentValues
            );
            c.add(histC);

            // Should not be equal with different data
            assertThat(a, not(equalTo(c)));
            assertThat(a.hashCode(), not(equalTo(c.hashCode())));
        }
    }

    public void testEqualsHashCodeWithMixedData() {
        try (
            HistogramUnionState a = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 100);
            HistogramUnionState b = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 100);
        ) {

            // Add TDigest data
            for (int i = 0; i < 50; i++) {
                try (TDigestState td = TDigestState.create(breaker(), 100)) {
                    td.add(i);
                    a.add(td);
                    b.add(td);
                }
            }

            assertThat(a, equalTo(b));
            assertThat(a.hashCode(), equalTo(b.hashCode()));

            // Add exponential histogram data to both
            double[] values = new double[] { 100.0, 200.0, 300.0 };
            ReleasableExponentialHistogram histA = ExponentialHistogram.create(200, ExponentialHistogramCircuitBreaker.noop(), values);
            a.add(histA);

            ReleasableExponentialHistogram histB = ExponentialHistogram.create(200, ExponentialHistogramCircuitBreaker.noop(), values);
            b.add(histB);

            // Mixed data with same content should be equal
            assertThat(a, equalTo(b));
            assertThat(a.hashCode(), equalTo(b.hashCode()));

            // Add more data to one of them
            try (TDigestState td = TDigestState.create(breaker(), 100)) {
                td.add(1000.0);
                a.add(td);
            }

            // Should no longer be equal
            assertThat(a, not(equalTo(b)));
            assertThat(a.hashCode(), not(equalTo(b.hashCode())));
        }
    }

    public void testCreateUsingParamsFromEmptyState() {
        try (HistogramUnionState original = HistogramUnionState.create(breaker(), TDigestState.Type.HYBRID, 42)) {
            try (HistogramUnionState copy = HistogramUnionState.createUsingParamsFrom(original)) {

                assertThat(copy.compression(), equalTo(42.0));

                // Mutate the copy to ensure that the original is unaffected
                try (RandomState randomState = randomState()) {
                    copy.add(randomState.unionState());
                }

                assertThat(original.size(), equalTo(0L));
            }
        }
    }

    public void testCreateUsingParamsFromPopulatedState() {
        try (RandomState randomState = randomState()) {
            HistogramUnionState original = randomState.unionState();
            try (HistogramUnionState copy = HistogramUnionState.createUsingParamsFrom(original)) {

                assertThat(copy.size(), equalTo(0L));
                assertThat(copy.compression(), equalTo(original.compression()));

                // Verify original is unchanged
                assertThat(original.size(), equalTo(randomState.reference().size()));
            }
        }
    }

}
