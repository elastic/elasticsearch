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
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
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

    private static ReleasableExponentialHistogram randomHistogram(int maxBuckets) {
        int numPositiveValues = randomBoolean() ? 0 : randomIntBetween(1, 1000);
        int numNegativeValues = randomBoolean() ? 0 : randomIntBetween(1, 1000);

        double[] values = IntStream.concat(
            IntStream.range(0, numPositiveValues).map(i -> 1),
            IntStream.range(0, numNegativeValues).map(i -> -1)
        ).mapToDouble(sign -> sign * Math.pow(1_000_000_000, randomDouble())).toArray();
        ReleasableExponentialHistogram histogram = ExponentialHistogram.create(
            maxBuckets,
            ExponentialHistogramCircuitBreaker.noop(),
            values
        );

        if (randomBoolean()) {
            double zeroThreshold = Arrays.stream(values).map(Math::abs).min().orElse(1.0) / 2.0;
            long zeroBucketCount = randomIntBetween(0, 100);
            ZeroBucket zeroBucket;
            if (randomBoolean()) {
                zeroBucket = ZeroBucket.create(zeroThreshold, zeroBucketCount);
            } else {
                // define the zero bucket using index and scale to verify serialization is exact
                int scale = randomIntBetween(0, MAX_SCALE);
                long index = ExponentialScaleUtils.computeIndex(zeroThreshold, scale) - 1;
                zeroBucket = ZeroBucket.create(index, scale, zeroBucketCount);
            }
            histogram = ExponentialHistogram.builder(histogram, ExponentialHistogramCircuitBreaker.noop()).zeroBucket(zeroBucket).build();
        }
        return histogram;
    }

    private CircuitBreaker breaker() {
        return newLimitedBreaker(ByteSizeValue.ofMb(100));
    }

}
