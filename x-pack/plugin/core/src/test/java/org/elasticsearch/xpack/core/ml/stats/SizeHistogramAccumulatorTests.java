/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SizeHistogramAccumulatorTests extends ESTestCase {

    public void testGivenNoValues() {
        SizeHistogramAccumulator accumulator = new SizeHistogramAccumulator();
        Map<String, Object> map = accumulator.asMap();
        assertThat(map.get(SizeHistogramAccumulator.COUNT), equalTo(0L));
        assertThat(map.get(StatsAccumulator.Fields.MIN), equalTo(0.0));
        assertThat(map.get(StatsAccumulator.Fields.MAX), equalTo(0.0));
        assertThat(map.get(SizeHistogramAccumulator.FAILURES), equalTo(0L));
        @SuppressWarnings("unchecked")
        Map<String, Long> buckets = (Map<String, Long>) map.get(SizeHistogramAccumulator.BUCKETS);
        buckets.values().forEach(count -> assertThat(count, equalTo(0L)));
    }

    public void testBucketBoundaries() {
        assertThat(SizeHistogramAccumulator.bucketFor(0), equalTo(SizeHistogramAccumulator.BUCKET_0_256));
        assertThat(SizeHistogramAccumulator.bucketFor(255), equalTo(SizeHistogramAccumulator.BUCKET_0_256));
        assertThat(SizeHistogramAccumulator.bucketFor(256), equalTo(SizeHistogramAccumulator.BUCKET_256_1K));
        assertThat(SizeHistogramAccumulator.bucketFor(1023), equalTo(SizeHistogramAccumulator.BUCKET_256_1K));
        assertThat(SizeHistogramAccumulator.bucketFor(1024), equalTo(SizeHistogramAccumulator.BUCKET_1K_4K));
        assertThat(SizeHistogramAccumulator.bucketFor(65536), equalTo(SizeHistogramAccumulator.BUCKET_64K_PLUS));
    }

    public void testAddAndAsMap() {
        SizeHistogramAccumulator accumulator = new SizeHistogramAccumulator();
        accumulator.add(10);
        accumulator.add(300);
        accumulator.add(2000);

        Map<String, Object> map = accumulator.asMap();
        assertThat(map.get(SizeHistogramAccumulator.COUNT), equalTo(3L));
        assertThat(map.get(StatsAccumulator.Fields.MIN), equalTo(10.0));
        assertThat(map.get(StatsAccumulator.Fields.MAX), equalTo(2000.0));
        assertThat(map.get(StatsAccumulator.Fields.TOTAL), equalTo(2310.0));
        assertThat(map.get(StatsAccumulator.Fields.AVG), equalTo(770.0));

        @SuppressWarnings("unchecked")
        Map<String, Long> buckets = (Map<String, Long>) map.get(SizeHistogramAccumulator.BUCKETS);
        assertThat(buckets.get(SizeHistogramAccumulator.BUCKET_0_256), equalTo(1L));
        assertThat(buckets.get(SizeHistogramAccumulator.BUCKET_256_1K), equalTo(1L));
        assertThat(buckets.get(SizeHistogramAccumulator.BUCKET_1K_4K), equalTo(1L));
    }

    public void testNegativeValueShouldIncrementFailures() {
        SizeHistogramAccumulator accumulator = new SizeHistogramAccumulator();
        accumulator.add(-1);

        Map<String, Object> map = accumulator.asMap();
        assertThat(map.get(SizeHistogramAccumulator.COUNT), equalTo(0L));
        assertThat(map.get(SizeHistogramAccumulator.FAILURES), equalTo(1L));
    }

    public void testMerge() {
        SizeHistogramAccumulator left = new SizeHistogramAccumulator();
        left.add(100);
        SizeHistogramAccumulator right = new SizeHistogramAccumulator();
        right.add(5000);
        left.merge(right);

        assertThat(left.asMap().get(SizeHistogramAccumulator.COUNT), equalTo(2L));
        assertThat(left.asMap().get(StatsAccumulator.Fields.MIN), equalTo(100.0));
        assertThat(left.asMap().get(StatsAccumulator.Fields.MAX), equalTo(5000.0));
    }
}
