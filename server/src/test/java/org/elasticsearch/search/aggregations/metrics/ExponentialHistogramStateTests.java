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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramTestUtils;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramStateTests extends ESTestCase {

    public void testEmptySerializationDeserialization() throws IOException {
        ExponentialHistogramState empty = ExponentialHistogramState.create(breaker());
        BytesStreamOutput out = new BytesStreamOutput();
        empty.write(out);

        ExponentialHistogramState deserialized = ExponentialHistogramState.read(breaker(), out.bytes().streamInput());

        assertThat(deserialized, equalTo(empty));
        assertThat(deserialized.histogram(), equalTo(ExponentialHistogram.empty()));

        Releasables.close(empty, deserialized);
    }

    public void testRandomHistogramSerializationAndDeserialization() throws IOException {
        ReleasableExponentialHistogram histogram = randomHistogram(randomIntBetween(4, ExponentialHistogramState.MAX_HISTOGRAM_BUCKETS));

        ExponentialHistogramState state = ExponentialHistogramState.create(breaker(), histogram);
        assertThat(state.histogram(), equalTo(histogram));

        BytesStreamOutput out = new BytesStreamOutput();
        state.write(out);
        ExponentialHistogramState deserialized = ExponentialHistogramState.read(breaker(), out.bytes().streamInput());

        assertThat(deserialized, equalTo(state));
        assertThat(deserialized.histogram(), equalTo(histogram));

        Releasables.close(state, deserialized);
    }

    public void testFailedDeserializationDoesNotLeak() throws IOException {
        ExponentialHistogram histogram = ExponentialHistogram.create(
            4,
            ExponentialHistogramCircuitBreaker.noop(),
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            7.0
        );

        ExponentialHistogramState state = ExponentialHistogramState.create(breaker());
        state.add(histogram);
        BytesStreamOutput out = new BytesStreamOutput();
        state.write(out);
        Releasables.close(state);

        BytesReference bytes = out.bytes();
        BytesReference invalidBytes = bytes.slice(0, bytes.length() - 1);

        expectThrows(EOFException.class, () -> ExponentialHistogramState.read(breaker(), invalidBytes.streamInput()));
    }

    public void testAdd() throws IOException {
        List<ReleasableExponentialHistogram> inputHistos = IntStream.range(0, randomIntBetween(1, 20))
            .mapToObj(i -> randomHistogram(randomIntBetween(4, ExponentialHistogramState.MAX_HISTOGRAM_BUCKETS)))
            .toList();

        ExponentialHistogramState state = ExponentialHistogramState.create(breaker());

        for (ExponentialHistogram histogram : inputHistos) {
            // randomly serialize and deserialize the current state
            if (randomBoolean()) {
                BytesStreamOutput out = new BytesStreamOutput();
                state.write(out);
                ExponentialHistogramState deserialized = ExponentialHistogramState.read(breaker(), out.bytes().streamInput());
                Releasables.close(state);
                state = deserialized;
            }
            state.add(histogram);
        }

        ExponentialHistogram expectedResult = ExponentialHistogram.merge(
            ExponentialHistogramState.MAX_HISTOGRAM_BUCKETS,
            ExponentialHistogramCircuitBreaker.noop(),
            inputHistos.iterator()
        );
        assertThat(state.histogram(), equalTo(expectedResult));

        Releasables.close(state);
    }

    public void testEmpty() {
        try (ExponentialHistogramState state = ExponentialHistogramState.create(breaker())) {
            assertThat(state.isEmpty(), equalTo(true));
            assertThat(state.centroids(), empty());
            assertThat(state.centroidCount(), equalTo(0));
            assertThat(state.size(), equalTo(0L));
            assertThat(state.quantile(0.0), equalTo(Double.NaN));
            assertThat(state.quantile(0.5), equalTo(Double.NaN));
            assertThat(state.quantile(1.0), equalTo(Double.NaN));
            assertThat(state.getMin(), equalTo(Double.POSITIVE_INFINITY));
            assertThat(state.getMax(), equalTo(Double.NEGATIVE_INFINITY));
        }
    }

    public void testQuantiles() {
        try (ExponentialHistogramState state = ExponentialHistogramState.create(breaker())) {
            ExponentialHistogram sample = ExponentialHistogram.create(
                100,
                ExponentialHistogramCircuitBreaker.noop(),
                new double[] { 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0 }
            );
            state.add(sample);

            assertThat(state.quantile(0.0), closeTo(10.0, 0.0000001));
            assertThat(state.quantile(0.1), closeTo(11.0, 0.0000001));
            assertThat(state.quantile(0.5), closeTo(15.0, 0.0000001));
            assertThat(state.quantile(0.9), closeTo(19.0, 0.0000001));
            assertThat(state.quantile(1.0), closeTo(20.0, 0.0000001));
            assertThat(state.getMin(), equalTo(10.0));
            assertThat(state.getMax(), equalTo(20.0));
        }
    }

    public void testCDF() {
        try (ExponentialHistogramState state = ExponentialHistogramState.create(breaker())) {
            ExponentialHistogram sample = ExponentialHistogram.create(
                100,
                ExponentialHistogramCircuitBreaker.noop(),
                IntStream.range(0, 100).mapToDouble(i -> i).toArray()
            );
            state.add(sample);

            assertThat(state.cdf(0.0 - 0.001), closeTo(0.0, 0.0000001));
            assertThat(state.cdf(10.0 - 0.001), closeTo(0.1, 0.0000001));
            assertThat(state.cdf(50.0 - 0.001), closeTo(0.5, 0.0000001));
            assertThat(state.cdf(90.0 - 0.001), closeTo(0.9, 0.0000001));
            assertThat(state.cdf(100.0 - 0.001), closeTo(1.0, 0.0000001));
        }
    }

    public void testSizeMinMax() {
        try (ExponentialHistogramState state = ExponentialHistogramState.create(breaker())) {
            ExponentialHistogram sample;
            do {
                sample = randomHistogram(randomIntBetween(4, 100));
            } while (sample.valueCount() == 0);
            state.add(sample);

            assertThat(state.size(), equalTo(sample.valueCount()));
            assertThat(state.getMin(), equalTo(sample.min()));
            assertThat(state.getMax(), equalTo(sample.max()));
        }
    }

    public void testCentroids() {
        try (ExponentialHistogramState state = ExponentialHistogramState.create(breaker())) {
            List<Centroid> expectedCentroids = new ArrayList<>();
            ExponentialHistogramBuilder builder = ExponentialHistogram.builder(0, ExponentialHistogramCircuitBreaker.noop());

            if (randomBoolean()) {
                builder.setNegativeBucket(-1, 11).setNegativeBucket(2, 22);
                expectedCentroids.add(new Centroid(-ExponentialScaleUtils.getPointOfLeastRelativeError(2, 0), 22));
                expectedCentroids.add(new Centroid(-ExponentialScaleUtils.getPointOfLeastRelativeError(-1, 0), 11));
            }
            if (randomBoolean()) {
                builder.zeroBucket(ZeroBucket.create(0.0, 123));
                expectedCentroids.add(new Centroid(0.0, 123));
            }
            if (randomBoolean()) {
                builder.setPositiveBucket(-11, 40).setPositiveBucket(12, 41);
                expectedCentroids.add(new Centroid(ExponentialScaleUtils.getPointOfLeastRelativeError(-11, 0), 40));
                expectedCentroids.add(new Centroid(ExponentialScaleUtils.getPointOfLeastRelativeError(12, 0), 41));
            }

            state.add(builder.build());

            Collection<Centroid> centroids = state.centroids();
            assertThat(centroids.size(), equalTo(expectedCentroids.size()));
            assertThat(state.centroidCount(), equalTo(expectedCentroids.size()));

            Iterator<Centroid> actualIt = centroids.iterator();
            Iterator<Centroid> expectedIt = expectedCentroids.iterator();
            for (int i = 0; i < expectedCentroids.size(); i++) {
                Centroid actual = actualIt.next();
                Centroid expected = expectedIt.next();
                assertThat(actual.mean(), closeTo(expected.mean(), 0.00001));
                assertThat(actual.count(), equalTo(expected.count()));
            }
        }
    }

    public void testCentroidSorted() {
        try (ExponentialHistogramState state = ExponentialHistogramState.create(breaker())) {
            state.add(randomHistogram(randomIntBetween(4, 500)));

            List<Double> actualCentroidMeans = state.centroids().stream().map(Centroid::mean).toList();
            List<Double> sortedMeans = new ArrayList<>(actualCentroidMeans);
            Collections.sort(sortedMeans);
            assertThat(actualCentroidMeans, equalTo(sortedMeans));
        }
    }

    private static ReleasableExponentialHistogram randomHistogram(int maxBuckets) {
        return ExponentialHistogramTestUtils.randomHistogram(maxBuckets, ExponentialHistogramCircuitBreaker.noop());
    }

    private CircuitBreaker breaker() {
        return newLimitedBreaker(ByteSizeValue.ofMb(100));
    }

}
