/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.EnumSet;

public class BucketGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);

    public void testIncludeEmptyBuckets_dateRange() {
        runGoldenTest("""
            FROM employees
            | STATS c = COUNT(*)
              BY b = BUCKET(hire_date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z", {"include_empty_buckets": true})
            """, STAGES, TransportVersionUtils.randomVersionSupporting(TransportVersion.fromName("esql_bucket_include_empty_buckets")));
    }

    public void testIncludeEmptyBuckets_numbers() {
        runGoldenTest("""
            FROM employees | STATS c = AVG(salary) by gender, b = BUCKET(salary, 10, 0, 100000, {"include_empty_buckets": true})
            """, STAGES, TransportVersionUtils.randomVersionSupporting(TransportVersion.fromName("esql_bucket_include_empty_buckets")));
    }

    public void testIncludeEmptyBuckets_timeSeries() {
        runGoldenTest("""
            TS k8s | STATS SUM(RATE(network.total_bytes_in))
                     BY TBUCKET(6, "2024-05-10T00:00:00Z", "2024-05-10T00:30:00Z", {"include_empty_buckets": true})
            """, STAGES, TransportVersionUtils.randomVersionSupporting(TransportVersion.fromName("esql_bucket_include_empty_buckets")));
    }

    public void testIncludeEmptyBuckets_timeSeriesAndDimension() {
        runGoldenTest("""
            TS k8s | STATS COUNT(2*network.bytes_in+1)
                     BY cluster, TBUCKET(6, "2024-05-10T00:00:00Z", "2024-05-10T00:30:00Z", {"include_empty_buckets": true})
            """, STAGES, TransportVersionUtils.randomVersionSupporting(TransportVersion.fromName("esql_bucket_include_empty_buckets")));
    }
}
