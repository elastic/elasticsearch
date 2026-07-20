/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.aggregation.QuantileStates;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ApproximationPlanTests extends ApproximationTestCase {

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    /**
     * The placeholder will always be substituted with a concrete non-null double before execution.
     * It must not be treated as nullable by optimizer rules (e.g. FoldNull, PropagateNullable).
     */
    public void testSampleProbabilityPlaceHolderIsNotNullable() {
        var placeholder = new ApproximationPlan.SampleProbabilityPlaceHolder(Source.EMPTY, randomInt());
        assertThat(placeholder.nullable(), equalTo(Nullability.FALSE));
    }

    public void testApproximationPlan_createsConfidenceInterval_withoutGrouping() {
        LogicalPlan originalPlan = ApproximationTests.getLogicalPlan("FROM test | STATS COUNT(), SUM(emp_no)");
        LogicalPlan approximationPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );

        assertThat(approximationPlan, hasPlan(SampledAggregate.class));
        assertThat(approximationPlan, hasPlan(Eval.class, withField("_approximation_confidence_interval(COUNT())")));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(COUNT())"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(SUM(emp_no))"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(SUM(emp_no))"))));
    }

    public void testApproximationPlan_createsConfidenceInterval_withGrouping() {
        LogicalPlan originalPlan = ApproximationTests.getLogicalPlan("FROM test | STATS COUNT(), SUM(emp_no) BY emp_no");
        LogicalPlan approximationPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );

        assertThat(approximationPlan, hasPlan(SampledAggregate.class));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(COUNT())"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(COUNT())"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(SUM(emp_no))"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(SUM(emp_no))"))));
    }

    public void testApproximationPlan_dependentConfidenceIntervals() {
        LogicalPlan originalPlan = ApproximationTests.getLogicalPlan(
            "FROM test | STATS x=SUM(emp_no) | EVAL a=x*x, b=7, c=TO_STRING(x), d=MV_APPEND(x, 1::LONG), e=a+POW(b, 2)"
        );
        LogicalPlan approximationPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );

        assertThat(approximationPlan, hasPlan(SampledAggregate.class));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(x)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(x)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(a)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(a)"))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_confidence_interval(b)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_certified(b)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_confidence_interval(c)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_certified(c)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_confidence_interval(d)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_certified(d)")))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(e)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(e)"))));
    }

    public void testApproximationPlan_withFork() {
        assumeTrue("needs approximation fork", EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled());
        LogicalPlan originalPlan = ApproximationTests.getLogicalPlan(
            "FROM test | FORK (STATS sum=SUM(emp_no)) (KEEP emp_no) (WHERE emp_no < 10 | STATS max=MAX(emp_no))"
        );
        LogicalPlan approximationPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );
        assertThat(
            approximationPlan.output().stream().map(Attribute::name).toList(),
            contains("sum", "_fork", "emp_no", "max", "_approximation_confidence_interval(sum)", "_approximation_certified(sum)")
        );
    }

    public void testApproximationPlan_withNonApproximableSubqueries() {
        assumeTrue("needs approximation fork", EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled());
        LogicalPlan originalPlan = ApproximationTests.getLogicalPlan(
            "FROM (FROM test | LIMIT 1 | STATS bad = COUNT(*)), (FROM test | STATS good = COUNT(*))"
        );
        LogicalPlan approximationPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );
        assertThat(
            approximationPlan.output().stream().map(Attribute::name).toList(),
            contains("bad", "good", "_approximation_confidence_interval(good)", "_approximation_certified(good)")
        );
    }

    public void testApproximationPlan_percentileBucketUsesReducedCompression() {
        assertBucketUsesReducedCompression("FROM test | STATS PERCENTILE(emp_no, 95)");
    }

    /**
     * MEDIAN is substituted by PERCENTILE(50) during optimization. Bucket columns therefore appear as
     * Percentile with reduced compression while the main aggregate retains full precision.
     */
    public void testApproximationPlan_medianBucketUsesReducedCompression() {
        assertBucketUsesReducedCompression("FROM test | STATS MEDIAN(emp_no)");
    }

    private void assertBucketUsesReducedCompression(String query) {
        LogicalPlan originalPlan = ApproximationTests.getLogicalPlan(query);
        LogicalPlan approximationPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );

        List<SampledAggregate> sampledAggs = approximationPlan.collect(SampledAggregate.class);
        assertThat(sampledAggs, hasSize(1));
        SampledAggregate sampledAgg = sampledAggs.getFirst();

        // Bucket aggregates use reduced compression for memory efficiency.
        List<Percentile> bucketPercentiles = sampledAgg.aggregates()
            .stream()
            .filter(
                ne -> ne instanceof Alias a && a.child() instanceof Percentile && ne.name().contains(ApproximationPlan.BUCKET_NAME_PART)
            )
            .map(ne -> (Percentile) ((Alias) ne).child())
            .toList();
        assertThat(bucketPercentiles, hasSize(ApproximationPlan.TRIAL_COUNT * ApproximationPlan.BUCKET_COUNT));
        for (Percentile bucketPercentile : bucketPercentiles) {
            assertThat(bucketPercentile.tDigestStateCompression(), equalTo(ApproximationPlan.PERCENTILE_BUCKET_TDIGEST_STATE_COMPRESSION));
        }

        // Main aggregate retains full compression for accurate results.
        List<Percentile> mainPercentiles = sampledAgg.aggregates()
            .stream()
            .filter(
                ne -> ne instanceof Alias a
                    && a.child() instanceof Percentile
                    && ne.name().contains(ApproximationPlan.BUCKET_NAME_PART) == false
            )
            .map(ne -> (Percentile) ((Alias) ne).child())
            .toList();
        assertThat(mainPercentiles, hasSize(1));
        assertThat(mainPercentiles.getFirst().tDigestStateCompression(), equalTo(QuantileStates.DEFAULT_COMPRESSION));
    }

    /**
     * Shows that reducing t-digest compression from DEFAULT_COMPRESSION (1000) to
     * PERCENTILE_BUCKET_TDIGEST_STATE_COMPRESSION (100) produces bucket percentile estimates
     * that are within 1% of each other. Since the BCa CI is computed from the mean, stddev, and
     * skewness of the bucket values, near-identical bucket values produce near-identical CIs.
     * The dominant source of error in BCa is sampling variance between buckets — far larger than
     * the quantization difference between compression=100 and compression=1000.
     */
    public void testBucketCompressionDoesNotMateriallyAffectPercentileEstimates() {
        var breaker = new NoopCircuitBreaker("test");
        int totalDataPoints = 100_000;
        int bucketCount = ApproximationPlan.BUCKET_COUNT;
        double percentileRank = 95.0;

        // Generate a fixed dataset (exponential distribution — similar to real latency data).
        Random rng = new Random(42);
        List<Double> data = new ArrayList<>(totalDataPoints);
        for (int i = 0; i < totalDataPoints; i++) {
            data.add(-Math.log(1.0 - rng.nextDouble()) * 100.0);
        }

        double maxRelativeDiff = 0.0;

        for (int bucket = 0; bucket < bucketCount; bucket++) {
            try (
                TDigestState full = TDigestState.create(breaker, QuantileStates.DEFAULT_COMPRESSION);
                TDigestState reduced = TDigestState.create(breaker, ApproximationPlan.PERCENTILE_BUCKET_TDIGEST_STATE_COMPRESSION)
            ) {
                // Each bucket gets every 16th data point starting at its index (simulates bucket split).
                for (int i = bucket; i < totalDataPoints; i += bucketCount) {
                    full.add(data.get(i));
                    reduced.add(data.get(i));
                }

                double fullP = full.quantile(percentileRank / 100.0);
                double reducedP = reduced.quantile(percentileRank / 100.0);
                double relativeDiff = Math.abs(fullP - reducedP) / Math.abs(fullP);
                maxRelativeDiff = Math.max(maxRelativeDiff, relativeDiff);
            }
        }

        logger.info(
            "Max relative difference in p{} bucket estimates between compression={} and compression={}: {}%",
            (int) percentileRank,
            (long) QuantileStates.DEFAULT_COMPRESSION,
            (long) ApproximationPlan.PERCENTILE_BUCKET_TDIGEST_STATE_COMPRESSION,
            String.format("%.4f", maxRelativeDiff * 100)
        );

        // Bucket percentile estimates are within 1% — negligible compared to BCa sampling variance.
        assertTrue("Expected max relative difference < 1% but got " + (maxRelativeDiff * 100) + "%", maxRelativeDiff < 0.01);
    }

    /**
     * Proves that PERCENTILE_BUCKET_TDIGEST_STATE_COMPRESSION reduces memory usage relative to DEFAULT_COMPRESSION.
     * Each approximation plan creates TRIAL_COUNT * BUCKET_COUNT = 32 bucket copies per percentile aggregation,
     * so the per-state saving is amplified significantly under high-cardinality group-by fields.
     */
    public void testBucketCompressionReducesMemoryFootprint() {
        var breaker = new NoopCircuitBreaker("test");
        int dataPoints = 10_000;

        try (
            TDigestState fullDigest = TDigestState.create(breaker, QuantileStates.DEFAULT_COMPRESSION);
            TDigestState reducedDigest = TDigestState.create(breaker, ApproximationPlan.PERCENTILE_BUCKET_TDIGEST_STATE_COMPRESSION)
        ) {
            for (int i = 0; i < dataPoints; i++) {
                fullDigest.add(i);
                reducedDigest.add(i);
            }

            long fullMemory = fullDigest.ramBytesUsed();
            long reducedMemory = reducedDigest.ramBytesUsed();

            // Reduced compression must use meaningfully less memory than full compression.
            assertThat(fullMemory, greaterThan(reducedMemory));

            // Across all 32 bucket copies the saving compounds significantly.
            int bucketCopies = ApproximationPlan.TRIAL_COUNT * ApproximationPlan.BUCKET_COUNT;
            long totalSavingPerGroup = (fullMemory - reducedMemory) * bucketCopies;
            assertThat(totalSavingPerGroup, greaterThan(0L));

            logger.info(
                "TDigestState memory — full compression ({}): {} bytes, reduced compression ({}): {} bytes; "
                    + "saving per group across {} bucket copies: {} bytes",
                (long) QuantileStates.DEFAULT_COMPRESSION,
                fullMemory,
                (long) ApproximationPlan.PERCENTILE_BUCKET_TDIGEST_STATE_COMPRESSION,
                reducedMemory,
                bucketCopies,
                totalSavingPerGroup
            );
        }
    }

    public void testColumnMetadata() {
        LogicalPlan originalPlan = ApproximationTests.getLogicalPlan("FROM test | STATS count=COUNT(), sum=SUM(emp_no)");
        LogicalPlan approximationPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );

        for (Attribute attr : approximationPlan.output()) {
            Map<String, Object> metadata = ApproximationPlan.createColumnMetadata(attr);
            switch (attr.name()) {
                case "count", "sum":
                    assertThat(attr.synthetic(), equalTo(false));
                    assertThat(metadata, nullValue());
                    break;
                case "_approximation_confidence_interval(count)":
                    assertThat(attr.synthetic(), equalTo(true));
                    assertThat(metadata, equalTo(Map.of("approximation", Map.of("type", "confidence_interval", "column", "count"))));
                    break;
                case "_approximation_certified(count)":
                    assertThat(attr.synthetic(), equalTo(true));
                    assertThat(metadata, equalTo(Map.of("approximation", Map.of("type", "certified", "column", "count"))));
                    break;
                case "_approximation_confidence_interval(sum)":
                    assertThat(attr.synthetic(), equalTo(true));
                    assertThat(metadata, equalTo(Map.of("approximation", Map.of("type", "confidence_interval", "column", "sum"))));
                    break;
                case "_approximation_certified(sum)":
                    assertThat(attr.synthetic(), equalTo(true));
                    assertThat(metadata, equalTo(Map.of("approximation", Map.of("type", "certified", "column", "sum"))));
                    break;
                default:
                    fail("Unexpected attribute: " + attr);
            }
        }
    }
}
