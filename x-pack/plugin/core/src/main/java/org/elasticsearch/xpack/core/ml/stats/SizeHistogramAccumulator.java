/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.stats;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Collects min, max, avg, total, count and fixed-size bucket counts for config field sizes.
 */
public class SizeHistogramAccumulator {

    public static final String BUCKETS = "buckets";
    public static final String COUNT = "count";
    public static final String FAILURES = "failures";

    public static final String BUCKET_0_256 = "0-256";
    public static final String BUCKET_256_1K = "256-1K";
    public static final String BUCKET_1K_4K = "1K-4K";
    public static final String BUCKET_4K_16K = "4K-16K";
    public static final String BUCKET_16K_64K = "16K-64K";
    public static final String BUCKET_64K_PLUS = "64K+";

    private static final long BOUNDARY_256 = 256L;
    private static final long BOUNDARY_1K = 1024L;
    private static final long BOUNDARY_4K = 4096L;
    private static final long BOUNDARY_16K = 16384L;
    private static final long BOUNDARY_64K = 65536L;

    private final Map<String, Long> buckets = new LinkedHashMap<>();
    private long count;
    private long total;
    private long failures;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;

    public SizeHistogramAccumulator() {
        buckets.put(BUCKET_0_256, 0L);
        buckets.put(BUCKET_256_1K, 0L);
        buckets.put(BUCKET_1K_4K, 0L);
        buckets.put(BUCKET_4K_16K, 0L);
        buckets.put(BUCKET_16K_64K, 0L);
        buckets.put(BUCKET_64K_PLUS, 0L);
    }

    public void add(long value) {
        if (value < 0) {
            failures++;
            return;
        }
        count++;
        total += value;
        min = Math.min(min, value);
        max = Math.max(max, value);
        buckets.compute(bucketFor(value), (k, v) -> v + 1);
    }

    static String bucketFor(long value) {
        if (value < BOUNDARY_256) {
            return BUCKET_0_256;
        }
        if (value < BOUNDARY_1K) {
            return BUCKET_256_1K;
        }
        if (value < BOUNDARY_4K) {
            return BUCKET_1K_4K;
        }
        if (value < BOUNDARY_16K) {
            return BUCKET_4K_16K;
        }
        if (value < BOUNDARY_64K) {
            return BUCKET_16K_64K;
        }
        return BUCKET_64K_PLUS;
    }

    public void merge(SizeHistogramAccumulator other) {
        if (other.count > 0) {
            if (count == 0) {
                min = other.min;
                max = other.max;
            } else {
                min = Math.min(min, other.min);
                max = Math.max(max, other.max);
            }
        }
        count += other.count;
        total += other.total;
        failures += other.failures;
        for (Map.Entry<String, Long> entry : other.buckets.entrySet()) {
            buckets.merge(entry.getKey(), entry.getValue(), Long::sum);
        }
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(BUCKETS, new LinkedHashMap<>(buckets));
        map.put(StatsAccumulator.Fields.MIN, count == 0 ? 0.0 : (double) min);
        map.put(StatsAccumulator.Fields.MAX, count == 0 ? 0.0 : (double) max);
        map.put(StatsAccumulator.Fields.AVG, count == 0 ? 0.0 : (double) total / count);
        map.put(StatsAccumulator.Fields.TOTAL, (double) total);
        map.put(COUNT, count);
        map.put(FAILURES, failures);
        return map;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SizeHistogramAccumulator other = (SizeHistogramAccumulator) obj;
        return count == other.count
            && total == other.total
            && failures == other.failures
            && min == other.min
            && max == other.max
            && Objects.equals(buckets, other.buckets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, total, failures, min, max, buckets);
    }
}
