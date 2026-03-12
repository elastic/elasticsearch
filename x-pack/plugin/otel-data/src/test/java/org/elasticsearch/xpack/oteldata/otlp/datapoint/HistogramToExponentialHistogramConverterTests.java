/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Types;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.exponentialhistogram.ExponentialScaleUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * @see <a href="https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.132.0/exporter/elasticsearchexporter/internal/exphistogram/exphistogram_test.go">
 *     OpenTelemetry Collector Exponential Histogram Tests
 * </a>
 */
public class HistogramToExponentialHistogramConverterTests extends ESTestCase {

    private record TestCase(
        HistogramDataPoint dataPoint,
        List<Long> expectedBucketCounts,
        List<Double> expectedBucketCenters,
        Double expectedSum,
        Double expectedMin,
        Double expectedMax
    ) {};

    private final HistogramDataPoint dataPoint;
    private final List<Double> expectedBucketCenters;
    private final List<Long> expectedBucketCounts;
    private final Double expectedSum;
    private final Double expectedMin;
    private final Double expectedMax;

    private final ExponentialHistogramConverter.BucketBuffer scratchBuffer = new ExponentialHistogramConverter.BucketBuffer();

    public HistogramToExponentialHistogramConverterTests(String name, Supplier<TestCase> testCaseSupplier) throws IOException {
        TestCase testCase = testCaseSupplier.get();
        this.dataPoint = testCase.dataPoint;
        this.expectedBucketCounts = testCase.expectedBucketCounts;
        this.expectedBucketCenters = testCase.expectedBucketCenters;
        this.expectedSum = testCase.expectedSum;
        this.expectedMin = testCase.expectedMin;
        this.expectedMax = testCase.expectedMax;

    }

    public void testExponentialHistograms() throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            // add a value to the scratch buffer to ensure it is properly cleared
            scratchBuffer.append(-42, 1L);
            scratchBuffer.append(42, 1L);

            ExponentialHistogramConverter.buildExponentialHistogram(dataPoint, builder, scratchBuffer);
            String json = Strings.toString(builder);
            ExponentialHistogram parsed;
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
                parsed = ExponentialHistogramXContent.parseForTesting(parser);
            }
            assertBucketsSorted(json);
            assertBucketsCloseTo(parsed, expectedBucketCenters, expectedBucketCounts);
            if (expectedSum != null) {
                assertThat(parsed.sum(), equalTo(expectedSum));
            } else {
                assertThat(json, not(containsString("sum")));
            }
            if (expectedMin != null) {
                assertThat(parsed.min(), equalTo(expectedMin));
            } else {
                assertThat(json, not(containsString("min")));
            }
            if (expectedMax != null) {
                assertThat(parsed.max(), equalTo(expectedMax));
            } else {
                assertThat(json, not(containsString("max")));
            }
        }
    }

    private void assertBucketsSorted(String json) {
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            Map<String, Object> parsedJson = parser.map();
            Map<String, Object> positive = Types.forciblyCast(parsedJson.getOrDefault("positive", Collections.emptyMap()));
            List<Long> positiveIndices = Types.forciblyCast(positive.getOrDefault("indices", Collections.emptyList()));
            Map<String, Object> negative = Types.forciblyCast(parsedJson.getOrDefault("negative", Collections.emptyMap()));
            List<Long> negativeIndices = Types.forciblyCast(negative.getOrDefault("indices", Collections.emptyList()));

            List<Long> positiveSorted = new ArrayList<>(positiveIndices);
            Collections.sort(positiveSorted);
            assertThat(positiveIndices, equalTo(positiveSorted));

            List<Long> negativeSorted = new ArrayList<>(negativeIndices);
            Collections.sort(negativeSorted);
            assertThat(negativeIndices, equalTo(negativeSorted));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    record CenterWithCount(double center, long count) {}

    private void assertBucketsCloseTo(ExponentialHistogram histo, List<Double> expectedBucketCenters, List<Long> expectedBucketCounts) {
        List<CenterWithCount> actualCenters = new ArrayList<>();
        BucketIterator neg = histo.negativeBuckets().iterator();
        while (neg.hasNext()) {
            actualCenters.add(
                new CenterWithCount(-ExponentialScaleUtils.getPointOfLeastRelativeError(neg.peekIndex(), neg.scale()), neg.peekCount())
            );
            neg.advance();
        }
        Collections.reverse(actualCenters);
        if (histo.zeroBucket().count() > 0) {
            actualCenters.add(new CenterWithCount(0, histo.zeroBucket().count()));
        }
        BucketIterator pos = histo.positiveBuckets().iterator();
        while (pos.hasNext()) {
            actualCenters.add(
                new CenterWithCount(ExponentialScaleUtils.getPointOfLeastRelativeError(pos.peekIndex(), pos.scale()), pos.peekCount())
            );
            pos.advance();
        }

        assertThat(actualCenters.size(), equalTo(expectedBucketCenters.size()));
        for (int i = 0; i < actualCenters.size(); i++) {
            assertThat(actualCenters.get(i).center(), closeTo(expectedBucketCenters.get(i), 0.0001));
            assertThat(actualCenters.get(i).count(), equalTo(expectedBucketCounts.get(i)));
        }
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> testCases() {
        List<Object[]> fixedTestCases = List.of(
            new Object[] {
                "empty",
                (Supplier<TestCase>) () -> new TestCase(HistogramDataPoint.newBuilder().build(), List.of(), List.of(), null, null, null) },
            new Object[] {
                "single bucket",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder().addBucketCounts(10L).addExplicitBounds(5.0).setSum(25.0).build(),
                    List.of(10L),
                    List.of(2.5),
                    25.0,
                    null,
                    null
                ) },
            new Object[] {
                "two buckets",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder().addAllBucketCounts(List.of(5L, 10L)).addExplicitBounds(5.0).setSum(65.0).build(),
                    List.of(5L, 10L),
                    List.of(2.5, 5.0),
                    65.0,
                    null,
                    null
                ) },
            new Object[] {
                "three buckets",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder()
                        .addAllBucketCounts(List.of(5L, 10L, 15L))
                        .addAllExplicitBounds(List.of(5.0, 10.0))
                        .setSum(200.0)
                        .build(),
                    List.of(5L, 10L, 15L),
                    List.of(2.5, 7.5, 10.0),
                    200.0,
                    null,
                    null
                ) },
            new Object[] {
                "zero count buckets",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder()
                        .addAllBucketCounts(List.of(5L, 0L, 15L))
                        .addAllExplicitBounds(List.of(5.0, 10.0))
                        .build(),
                    List.of(5L, 15L),
                    List.of(2.5, 10.0),
                    null,
                    null,
                    null
                ) },
            new Object[] {
                "negative bounds",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder()
                        .addAllBucketCounts(List.of(5L, 10L, 15L))
                        .addAllExplicitBounds(List.of(-10.0, 10.0))
                        .build(),
                    List.of(5L, 10L, 15L),
                    List.of(-10.0, 0.0, 10.0),
                    null,
                    null,
                    null
                ) },
            new Object[] {
                "all negative bounds",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder().addAllBucketCounts(List.of(5L, 10L)).addExplicitBounds(-5.0).build(),
                    List.of(15L),
                    List.of(-5.0),
                    null,
                    null,
                    null
                ) },
            new Object[] {
                "multiple buckets with varying distances",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder()
                        .addAllBucketCounts(List.of(5L, 10L, 15L, 20L))
                        .addAllExplicitBounds(List.of(1.0, 5.0, 20.0))
                        .build(),
                    List.of(5L, 10L, 15L, 20L),
                    List.of(0.5, 3.0, 12.5, 20.0),
                    null,
                    null,
                    null
                ) },
            new Object[] {
                "single bucket clamp to negative min",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder().addBucketCounts(1L).addExplicitBounds(5.0).setSum(25.0).setMin(-42).build(),
                    List.of(1L),
                    List.of(-42.0),
                    25.0,
                    -42.0,
                    null
                ) },
            new Object[] {
                "single bucket clamp to negative max",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder().addBucketCounts(1L).addExplicitBounds(5.0).setSum(25.0).setMax(-42).build(),
                    List.of(1L),
                    List.of(-42.0),
                    25.0,
                    null,
                    -42.0
                ) },
            new Object[] {
                "single bucket clamp to negative min and max",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder()
                        .addBucketCounts(10L)
                        .addExplicitBounds(5.0)
                        .setSum(25.0)
                        .setMax(-42)
                        .setMin(-42.0)
                        .build(),
                    List.of(10L),
                    List.of(-42.0),
                    25.0,
                    -42.0,
                    -42.0
                ) },
            new Object[] {
                "multiple buckets with varying distances with separate min/max buckets",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder()
                        .addAllBucketCounts(List.of(5L, 10L, 15L, 20L))
                        .addAllExplicitBounds(List.of(1.0, 5.0, 20.0))
                        .setMin(0.25)
                        .setMax(100)
                        .build(),
                    List.of(1L, 4L, 10L, 15L, 19L, 1L),
                    List.of(0.25, 0.5, 3.0, 12.5, 20.0, 100.0),
                    null,
                    0.25,
                    100.0
                ) },
            new Object[] {
                "zero bucket and non-zero positive min/max values",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder()
                        .addAllBucketCounts(List.of(0L, 2L, 0L))
                        .addAllExplicitBounds(List.of(-1.0, 1.0))
                        .setMin(0.25)
                        .setMax(0.5)
                        .build(),
                    List.of(1L, 1L),
                    List.of(0.25, 0.5),
                    null,
                    0.25,
                    0.5
                ) },
            new Object[] {
                "zero bucket and non-zero negative min/max values",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder()
                        .addAllBucketCounts(List.of(0L, 2L, 0L))
                        .addAllExplicitBounds(List.of(-1.0, 1.0))
                        .setMin(-0.5)
                        .setMax(-0.25)
                        .build(),
                    List.of(1L, 1L),
                    List.of(-0.5, -0.25),
                    null,
                    -0.5,
                    -0.25
                ) },
            new Object[] {
                "large zero bucket and non-zero min/max values",
                (Supplier<TestCase>) () -> new TestCase(
                    HistogramDataPoint.newBuilder()
                        .addAllBucketCounts(List.of(0L, 10L, 0L))
                        .addAllExplicitBounds(List.of(-1.0, 1.0))
                        .setMin(-0.42)
                        .setMax(0.5)
                        .build(),
                    List.of(1L, 8L, 1L),
                    List.of(-0.42, 0.0, 0.5),
                    null,
                    -0.42,
                    0.5
                ) }
        );

        return Stream.concat(
            fixedTestCases.stream(),
            IntStream.range(0, 100)
                .mapToObj(
                    i -> new Object[] { "random-" + i, (Supplier<TestCase>) HistogramToExponentialHistogramConverterTests::randomTestCase }
                )
        ).toList();
    }

    private static final double RANDOM_HISTOGRAM_RANGE = 1e6;

    private static TestCase randomTestCase() {
        int boundaryCount = randomIntBetween(1, 100);
        Set<Double> boundariesSet = new HashSet<>();
        for (int i = 0; i < boundaryCount; i++) {
            boundariesSet.add(randomDoubleBetween(-1e6, 1e6, true));
        }
        // random chance to have a bucket with its center at exactly zero
        if (randomBoolean()) {
            double closestToZero = boundariesSet.stream().min((a, b) -> Double.compare(Math.abs(a), Math.abs(b))).get();
            boundariesSet.add(-closestToZero);
        }

        List<Double> boundaries = boundariesSet.stream().sorted().toList();

        List<Long> bucketCounts = new ArrayList<>();
        for (int i = 0; i <= boundaries.size(); i++) {
            if (randomDouble() > 0.2) { // 20% chance for empty bucket
                bucketCounts.add((long) randomIntBetween(0, 1000));
            } else {
                bucketCounts.add(0L);
            }
        }

        HistogramDataPoint.Builder dataPointBuilder = HistogramDataPoint.newBuilder();
        bucketCounts.forEach(dataPointBuilder::addBucketCounts);
        boundaries.forEach(dataPointBuilder::addExplicitBounds);

        if (randomBoolean()) {
            dataPointBuilder.setSum(randomDoubleBetween(-RANDOM_HISTOGRAM_RANGE, RANDOM_HISTOGRAM_RANGE, true));
        }
        if (randomBoolean()) {
            // choose a min from the smallest, populated bucket
            int firstPopulatedBucket = -1;
            for (int i = 0; i < bucketCounts.size(); i++) {
                if (bucketCounts.get(i) > 0) {
                    firstPopulatedBucket = i;
                    break;
                }
            }
            if (firstPopulatedBucket != -1) {
                dataPointBuilder.setMin(getRandomValueInBucket(firstPopulatedBucket, boundaries));
            }
        }
        if (randomBoolean()) {
            int lastPopulatedBucket = -1;
            for (int i = bucketCounts.size() - 1; i >= 0; i--) {
                if (bucketCounts.get(i) > 0) {
                    lastPopulatedBucket = i;
                    break;
                }
            }
            if (lastPopulatedBucket != -1) {
                double randomMax = getRandomValueInBucket(lastPopulatedBucket, boundaries);
                if (dataPointBuilder.hasMin()) {
                    randomMax = Math.max(randomMax, dataPointBuilder.getMin());
                }
                dataPointBuilder.setMax(randomMax);
            }
        }

        HistogramDataPoint dataPoint = dataPointBuilder.build();

        List<Double> expectedBucketCenters = new ArrayList<>();
        List<Long> expectedBucketCounts = new ArrayList<>();
        for (int i = 0; i < bucketCounts.size(); i++) {
            if (bucketCounts.get(i) > 0) {
                expectedBucketCenters.add(TDigestConverter.getCentroid(dataPoint, i));
                expectedBucketCounts.add(bucketCounts.get(i));
            }
        }

        // add the expected min/max adjustments if required
        if (dataPoint.hasMin()) {
            Double min = dataPoint.getMin();
            if (expectedBucketCenters.get(0) <= min) {
                expectedBucketCenters.set(0, min);
            } else {
                expectedBucketCounts.set(0, expectedBucketCounts.get(0) - 1);
                if (expectedBucketCounts.get(0) == 0) {
                    expectedBucketCounts.remove(0);
                    expectedBucketCenters.remove(0);
                }
                expectedBucketCenters.add(0, min);
                expectedBucketCounts.add(0, 1L);
            }
        }
        if (dataPoint.hasMax()) {
            Double max = dataPoint.getMax();
            int lastIndex = expectedBucketCenters.size() - 1;
            if (expectedBucketCenters.get(lastIndex) >= max) {
                expectedBucketCenters.set(lastIndex, max);
            } else {
                expectedBucketCounts.set(lastIndex, expectedBucketCounts.get(lastIndex) - 1);
                if (expectedBucketCounts.get(lastIndex) == 0) {
                    expectedBucketCounts.remove(lastIndex);
                    expectedBucketCenters.remove(lastIndex);
                }
                expectedBucketCenters.add(max);
                expectedBucketCounts.add(1L);
            }
        }
        // combine duplicate bucket centers that may have arisen from min/max adjustments
        List<Double> finalBucketCenters = new ArrayList<>();
        List<Long> finalBucketCounts = new ArrayList<>();
        for (int i = 0; i < expectedBucketCenters.size(); i++) {
            if (finalBucketCenters.isEmpty() || finalBucketCenters.getLast().doubleValue() != expectedBucketCenters.get(i).doubleValue()) {
                finalBucketCenters.add(expectedBucketCenters.get(i));
                finalBucketCounts.add(expectedBucketCounts.get(i));
            } else {
                int lastIndex = finalBucketCounts.size() - 1;
                finalBucketCounts.set(lastIndex, finalBucketCounts.get(lastIndex) + expectedBucketCounts.get(i));
            }
        }
        return new TestCase(
            dataPoint,
            finalBucketCounts,
            finalBucketCenters,
            dataPoint.hasSum() ? dataPoint.getSum() : null,
            dataPoint.hasMin() ? dataPoint.getMin() : null,
            dataPoint.hasMax() ? dataPoint.getMax() : null
        );
    }

    private static double getRandomValueInBucket(int bucketIndex, List<Double> boundaries) {
        if (bucketIndex == 0) {
            return randomDoubleBetween(-RANDOM_HISTOGRAM_RANGE, boundaries.get(0), true);
        }
        if (bucketIndex == boundaries.size()) {
            return randomDoubleBetween(boundaries.get(bucketIndex - 1), RANDOM_HISTOGRAM_RANGE, true);
        }
        return randomDoubleBetween(boundaries.get(bucketIndex - 1), boundaries.get(bucketIndex), true);
    }
}
