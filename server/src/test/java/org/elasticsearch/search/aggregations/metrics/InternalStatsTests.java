/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyMap;

public class InternalStatsTests extends InternalAggregationTestCase<InternalStats> {

    @Override
    protected InternalStats createTestInstance(String name, Map<String, Object> metadata) {
        long count = frequently() ? randomIntBetween(1, Integer.MAX_VALUE) : 0;
        double min = randomDoubleBetween(-1000000, 1000000, true);
        double max = randomDoubleBetween(-1000000, 1000000, true);
        double sum = randomDoubleBetween(-1000000, 1000000, true);
        DocValueFormat format = randomNumericDocValueFormat();
        return createInstance(name, count, sum, min, max, format, metadata);
    }

    protected InternalStats createInstance(
        String name,
        long count,
        double sum,
        double min,
        double max,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        return new InternalStats(name, count, sum, min, max, formatter, metadata);
    }

    @Override
    protected void assertReduced(InternalStats reduced, List<InternalStats> inputs) {
        long expectedCount = 0;
        double expectedSum = 0;
        double expectedMin = Double.POSITIVE_INFINITY;
        double expectedMax = Double.NEGATIVE_INFINITY;
        for (InternalStats stats : inputs) {
            expectedCount += stats.getCount();
            if (Double.compare(stats.getMin(), expectedMin) < 0) {
                expectedMin = stats.getMin();
            }
            if (Double.compare(stats.getMax(), expectedMax) > 0) {
                expectedMax = stats.getMax();
            }
            expectedSum += stats.getSum();
        }
        assertEquals(expectedCount, reduced.getCount());
        assertEquals(expectedSum, reduced.getSum(), 1e-7);
        assertEquals(expectedMin, reduced.getMin(), 0d);
        assertEquals(expectedMax, reduced.getMax(), 0d);
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalStats sampled, InternalStats reduced, SamplingContext samplingContext) {
        assertEquals(sampled.getCount(), samplingContext.scaleUp(reduced.getCount()));
        assertEquals(sampled.getSum(), samplingContext.scaleUp(reduced.getSum()), 1e-7);
        assertEquals(sampled.getMin(), reduced.getMin(), 0d);
        assertEquals(sampled.getMax(), reduced.getMax(), 0d);
    }

    public void testSummationAccuracy() {
        double[] values = new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7 };
        verifyStatsOfDoubles(values, 13.5, 0.9, 0d);

        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        verifyStatsOfDoubles(values, sum, sum / n, TOLERANCE);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifyStatsOfDoubles(largeValues, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifyStatsOfDoubles(largeValues, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, 0d);
    }

    private void verifyStatsOfDoubles(double[] values, double expectedSum, double expectedAvg, double delta) {
        List<InternalAggregation> aggregations = new ArrayList<>(values.length);
        double max = Double.NEGATIVE_INFINITY;
        double min = Double.POSITIVE_INFINITY;
        for (double value : values) {
            max = Math.max(max, value);
            min = Math.min(min, value);
            aggregations.add(new InternalStats("dummy1", 1, value, value, value, null, null));
        }
        InternalStats internalStats = new InternalStats("dummy2", 0, 0.0, 2.0, 0.0, null, null);
        InternalStats reduced = internalStats.reduce(aggregations, null);
        assertEquals("dummy2", reduced.getName());
        assertEquals(values.length, reduced.getCount());
        assertEquals(expectedSum, reduced.getSum(), delta);
        assertEquals(expectedAvg, reduced.getAvg(), delta);
        assertEquals(min, reduced.getMin(), 0d);
        assertEquals(max, reduced.getMax(), 0d);
    }

    @Override
    protected void assertFromXContent(InternalStats aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedStats);
        ParsedStats parsed = (ParsedStats) parsedAggregation;
        assertStats(aggregation, parsed);
    }

    static void assertStats(InternalStats aggregation, ParsedStats parsed) {
        long count = aggregation.getCount();
        assertEquals(count, parsed.getCount());
        // for count == 0, fields are rendered as `null`, so we test that we parse to default values used also in the reduce phase
        assertEquals(count > 0 ? aggregation.getMin() : Double.POSITIVE_INFINITY, parsed.getMin(), 0);
        assertEquals(count > 0 ? aggregation.getMax() : Double.NEGATIVE_INFINITY, parsed.getMax(), 0);
        assertEquals(count > 0 ? aggregation.getSum() : 0, parsed.getSum(), 0);
        assertEquals(count > 0 ? aggregation.getAvg() : 0, parsed.getAvg(), 0);
        // also as_string values are only rendered for count != 0
        if (count > 0) {
            assertEquals(aggregation.getMinAsString(), parsed.getMinAsString());
            assertEquals(aggregation.getMaxAsString(), parsed.getMaxAsString());
            assertEquals(aggregation.getSumAsString(), parsed.getSumAsString());
            assertEquals(aggregation.getAvgAsString(), parsed.getAvgAsString());
        }
    }

    @Override
    protected InternalStats mutateInstance(InternalStats instance) {
        String name = instance.getName();
        long count = instance.getCount();
        double sum = instance.getSum();
        double min = instance.getMin();
        double max = instance.getMax();
        DocValueFormat formatter = instance.format;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 5)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                if (Double.isFinite(count)) {
                    count += between(1, 100);
                } else {
                    count = between(1, 100);
                }
                break;
            case 2:
                if (Double.isFinite(sum)) {
                    sum += between(1, 100);
                } else {
                    sum = between(1, 100);
                }
                break;
            case 3:
                if (Double.isFinite(min)) {
                    min += between(1, 100);
                } else {
                    min = between(1, 100);
                }
                break;
            case 4:
                if (Double.isFinite(max)) {
                    max += between(1, 100);
                } else {
                    max = between(1, 100);
                }
                break;
            case 5:
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalStats(name, count, sum, min, max, formatter, metadata);
    }

    public void testDoXContentBody() throws IOException {
        // count is greater than zero
        double min = randomDoubleBetween(-1000000, 1000000, true);
        double max = randomDoubleBetween(-1000000, 1000000, true);
        double sum = randomDoubleBetween(-1000000, 1000000, true);
        int count = randomIntBetween(1, 10);
        DocValueFormat format = randomNumericDocValueFormat();
        InternalStats internalStats = createInstance("stats", count, sum, min, max, format, null);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        internalStats.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Object[] args = new Object[] {
            count,
            min,
            max,
            internalStats.getAvg(),
            sum,
            format != DocValueFormat.RAW
                ? Strings.format(
                    """
                        ,
                        "min_as_string" : "%s",
                        "max_as_string" : "%s",
                        "avg_as_string" : "%s",
                        "sum_as_string" : "%s"
                        """,
                    format.format(internalStats.getMin()),
                    format.format(internalStats.getMax()),
                    format.format(internalStats.getAvg()),
                    format.format(internalStats.getSum())
                )
                : "" };
        String expected = Strings.format("""
            {
              "count" : %s,
              "min" : %s,
              "max" : %s,
              "avg" : %s,
              "sum" : %s
              %s
            }""", args);
        assertEquals(XContentHelper.stripWhitespace(expected), Strings.toString(builder));

        // count is zero
        format = randomNumericDocValueFormat();
        min = 0.0;
        max = 0.0;
        sum = 0.0;
        count = 0;
        internalStats = createInstance("stats", count, sum, min, max, format, null);
        builder = JsonXContent.contentBuilder();
        builder.startObject();
        internalStats.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertEquals(XContentHelper.stripWhitespace("""
            {
              "count" : 0,
              "min" : null,
              "max" : null,
              "avg" : null,
              "sum" : 0.0
            }"""), Strings.toString(builder));
    }

    public void testIterator() {
        InternalStats aggregation = createTestInstance("test", emptyMap());
        List<String> names = StreamSupport.stream(aggregation.valueNames().spliterator(), false).toList();

        assertEquals(5, names.size());
        assertTrue(names.contains("min"));
        assertTrue(names.contains("max"));
        assertTrue(names.contains("count"));
        assertTrue(names.contains("avg"));
        assertTrue(names.contains("sum"));
    }
}
