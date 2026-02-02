/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class TDigestToExponentialHistogramConverterTests extends ExponentialHistogramTests {

    private ReleasableExponentialHistogram doConvert(List<Double> centroids, List<Long> counts) {
        ReleasableExponentialHistogram converted = TDigestToExponentialHistogramConverter.convert(
            new TDigestToExponentialHistogramConverter.ArrayBasedCentroidIterator(centroids, counts),
            breaker()
        );
        autoReleaseOnTestEnd(converted);
        return converted;
    }

    public void testToExponentialHistogramConversionWithCloseCentroids() {
        // build a t-digest with two centroids very close to each other
        List<Double> centroids = List.of(1.0, Math.nextAfter(1.0, 2));
        List<Long> counts = List.of(1L, 2L);

        ReleasableExponentialHistogram converted = doConvert(centroids, counts);

        assertThat(converted.zeroBucket().count(), equalTo(0L));
        assertThat(converted.zeroBucket().zeroThreshold(), equalTo(0.0d));
        assertBucketCentersCloseTo(converted.positiveBuckets().iterator(), List.of(1.0, 1.0), List.of(1L, 2L));
        assertBucketCentersCloseTo(converted.negativeBuckets().iterator(), List.of(), List.of());
    }

    public void testToExponentialHistogramConversionWithZeroCounts() {
        // build a t-digest with two centroids very close to each other
        List<Double> centroids = List.of(1.0, 2.0, 3.0);
        List<Long> counts = List.of(1L, 0L, 2L);

        ReleasableExponentialHistogram converted = doConvert(centroids, counts);

        assertThat(converted.zeroBucket().count(), equalTo(0L));
        assertBucketCentersCloseTo(converted.positiveBuckets().iterator(), List.of(1.0, 3.0), List.of(1L, 2L));
        assertBucketCentersCloseTo(converted.negativeBuckets().iterator(), List.of(), List.of());
    }

    public void testRandomTDigest() {
        List<Double> negativeCentroids = randomDoubles().map(val -> val * -10_000)
            .limit(randomBoolean() ? 0 : randomIntBetween(1, 100))
            .sorted()
            .boxed()
            .toList();
        List<Long> negativeCounts = negativeCentroids.stream().mapToLong(c -> randomIntBetween(1, 10)).boxed().toList();

        List<Double> positiveCentroids = randomDoubles().map(val -> val * 10_000)
            .limit(randomBoolean() ? 0 : randomIntBetween(1, 100))
            .sorted()
            .boxed()
            .toList();
        List<Long> positiveCounts = positiveCentroids.stream().mapToLong(c -> randomIntBetween(1, 10)).boxed().toList();

        int numZeroCentroids = randomIntBetween(0, 5);
        List<Long> zeroCentroidsCounts = IntStream.range(0, numZeroCentroids).mapToLong(i -> randomIntBetween(0, 10)).boxed().toList();

        List<Double> inputCentroids = new ArrayList<>();
        List<Long> inputCounts = new ArrayList<>();
        splitAndAddCentroids(negativeCentroids, negativeCounts, inputCentroids, inputCounts);
        for (int i = 0; i < numZeroCentroids; i++) {
            inputCentroids.add(0.0);
            inputCounts.add(zeroCentroidsCounts.get(i));
        }
        splitAndAddCentroids(positiveCentroids, positiveCounts, inputCentroids, inputCounts);

        ReleasableExponentialHistogram result = doConvert(inputCentroids, inputCounts);

        long expectedZeroCount = zeroCentroidsCounts.stream().mapToLong(Long::longValue).sum();
        assertThat(result.zeroBucket().count(), equalTo(expectedZeroCount));
        assertThat(result.zeroBucket().zeroThreshold(), equalTo(0.0d));
        assertBucketCentersCloseTo(result.positiveBuckets().iterator(), positiveCentroids, positiveCounts);

        // for the assertion we need to reverse the negative centroids and counts to be in the order of increasing bucket index
        assertBucketCentersCloseTo(result.negativeBuckets().iterator(), negativeCentroids.reversed(), negativeCounts.reversed());
    }

    private static void splitAndAddCentroids(
        List<Double> inputCentroids,
        List<Long> inputCounts,
        List<Double> outputCentroids,
        List<Long> outputCounts
    ) {
        for (int i = 0; i < inputCentroids.size(); i++) {
            // split the counts across n centroids
            int numSplit = randomIntBetween(1, 3);
            long totalCount = inputCounts.get(i);
            double centroidValue = inputCentroids.get(i);
            long consumedCount = 0;
            for (int j = 0; j < numSplit - 1; j++) {
                long count = randomLongBetween(0, totalCount - consumedCount);
                outputCentroids.add(centroidValue);
                outputCounts.add(count);
                consumedCount += count;
            }
            outputCentroids.add(centroidValue);
            outputCounts.add(totalCount - consumedCount);
        }
    }

    private static void assertBucketCentersCloseTo(BucketIterator buckets, List<Double> expectedCenters, List<Long> expectedCounts) {
        List<Double> actualCenters = new ArrayList<>();
        List<Long> actualCounts = new ArrayList<>();
        while (buckets.hasNext()) {
            actualCenters.add(ExponentialScaleUtils.getPointOfLeastRelativeError(buckets.peekIndex(), buckets.scale()));
            actualCounts.add(buckets.peekCount());
            buckets.advance();
        }
        assertThat(actualCenters.size(), equalTo(expectedCenters.size()));
        for (int i = 0; i < actualCenters.size(); i++) {
            double absoluteError = Math.abs(actualCenters.get(i) - Math.abs(expectedCenters.get(i)));
            double relativeError = absoluteError / Math.abs(expectedCenters.get(i));
            assertThat("Bucket center at index " + i + " differs more than allowed", relativeError, lessThan(0.00001));
        }
        assertThat(actualCounts, equalTo(expectedCounts));

    }
}
