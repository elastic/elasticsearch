/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Collects min, max, avg, total, count and fixed-size bucket counts for config field sizes.
 */
public class SizeHistogramAccumulator implements Writeable {

    public static final String BUCKETS = "buckets";
    public static final String COUNT = "count";

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
    private Long min;
    private Long max;

    public SizeHistogramAccumulator() {
        buckets.put(BUCKET_0_256, 0L);
        buckets.put(BUCKET_256_1K, 0L);
        buckets.put(BUCKET_1K_4K, 0L);
        buckets.put(BUCKET_4K_16K, 0L);
        buckets.put(BUCKET_16K_64K, 0L);
        buckets.put(BUCKET_64K_PLUS, 0L);
    }

    public SizeHistogramAccumulator(StreamInput in) throws IOException {
        this();
        count = in.readLong();
        total = in.readLong();
        min = in.readOptionalLong();
        max = in.readOptionalLong();
        for (String bucket : buckets.keySet()) {
            buckets.put(bucket, in.readLong());
        }
    }

    public void add(long value) {
        if (value < 0) {
            return;
        }
        count++;
        total += value;
        min = min == null ? value : Math.min(min, value);
        max = max == null ? value : Math.max(max, value);
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
        count += other.count;
        total += other.total;
        // note: not using Math.min/max as some internal prefetch optimization causes an NPE
        min = min == null ? other.min : (other.min == null ? min : other.min < min ? other.min : min);
        max = max == null ? other.max : (other.max == null ? max : other.max > max ? other.max : max);
        for (Map.Entry<String, Long> entry : other.buckets.entrySet()) {
            buckets.merge(entry.getKey(), entry.getValue(), Long::sum);
        }
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(BUCKETS, new LinkedHashMap<>(buckets));
        map.put(StatsAccumulator.Fields.MIN, min == null ? 0.0 : min.doubleValue());
        map.put(StatsAccumulator.Fields.MAX, max == null ? 0.0 : max.doubleValue());
        map.put(StatsAccumulator.Fields.AVG, count == 0 ? 0.0 : (double) total / count);
        map.put(StatsAccumulator.Fields.TOTAL, (double) total);
        map.put(COUNT, count);
        return map;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(count);
        out.writeLong(total);
        out.writeOptionalLong(min);
        out.writeOptionalLong(max);
        for (Long bucketCount : buckets.values()) {
            out.writeLong(bucketCount);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SizeHistogramAccumulator other = (SizeHistogramAccumulator) obj;
        return count == other.count
            && total == other.total
            && Objects.equals(min, other.min)
            && Objects.equals(max, other.max)
            && Objects.equals(buckets, other.buckets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, total, min, max, buckets);
    }
}
