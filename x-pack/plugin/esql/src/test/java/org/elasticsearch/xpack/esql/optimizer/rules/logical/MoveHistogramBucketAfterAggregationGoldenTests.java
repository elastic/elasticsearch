/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

/**
 * Golden tests for moving histogram bucket generation and expansion after histogram merging.
 */
public class MoveHistogramBucketAfterAggregationGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOGICAL_OPTIMIZATION);
    private static final String HISTOGRAM_BUCKET_CAPABILITY = "esql_count_histogram_bucket";

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public MoveHistogramBucketAfterAggregationGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testHistogramBucketAfterPerSeriesAggregation() {
        run("""
            TS exp_histo_sample
            | STATS count = COUNT(responseTime, bucket) BY bucket = BUCKET(responseTime, 42)
            """);
    }

    public void testHistogramBucketAfterExplicitLastOverTime() {
        run("""
            TS exp_histo_sample
            | STATS count = COUNT(LAST_OVER_TIME(responseTime), bucket) BY bucket = BUCKET(responseTime, 42)
            """);
    }

    public void testTDigestCastHistogramBucketAfterPerSeriesAggregation() {
        run("""
            TS exp_histo_sample
            | STATS count = COUNT(responseTime::tdigest, bucket) BY bucket = BUCKET(responseTime::tdigest, 42)
            """);
    }

    public void testTDigestCastHistogramBucketAfterExplicitLastOverTime() {
        run("""
            TS exp_histo_sample
            | STATS count = COUNT(LAST_OVER_TIME(responseTime::tdigest), bucket) BY bucket = BUCKET(responseTime::tdigest, 42)
            """);
    }

    public void testPreservesOtherGroupingKeys() {
        run("""
            TS exp_histo_sample
            | STATS count = COUNT(responseTime, bucket) BY instance, bucket = BUCKET(responseTime, 42)
            """);
    }

    public void testDoesNotRewriteWithAnotherAggregation() {
        run("""
            TS exp_histo_sample
            | STATS count = COUNT(responseTime, bucket), latest = MAX(@timestamp) BY bucket = BUCKET(responseTime, 42)
            """);
    }

    public void testFromHistogramBucketAfterAggregation() {
        run("""
            FROM exp_histo_sample
            | STATS count = COUNT(responseTime, bucket) BY bucket = BUCKET(responseTime, 42)
            """);
    }

    public void testFromDoesNotRewriteWithCountAll() {
        run("""
            FROM exp_histo_sample
            | STATS count = COUNT(responseTime, bucket), total = COUNT(*) BY bucket = BUCKET(responseTime, 42)
            """);
    }

    private void run(String query) {
        builder(query).stages(STAGES).since(HISTOGRAM_BUCKET_CAPABILITY).run();
    }
}
