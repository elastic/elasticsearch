/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.promql.function.HashOffset;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.junit.Before;

import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class PromqlPlanLimitRatioTests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanLimitRatioTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void assumeLimitRatioEnabled() {
        assumeTrue("Requires PROMQL_LIMIT_RATIO capability", EsqlCapabilities.Cap.PROMQL_LIMIT_RATIO.isEnabled());
    }

    /**
     * {@code limit_ratio} over an aggregate samples result series, not raw series: the input rows are
     * groups carrying no {@code _timeseries}, so the sampling key is the concrete group columns from
     * below (here {@code pod}) -- no synthesized key, no extra plan node beyond the sampling filter.
     */
    public void testLimitRatioOverAggregateKeysOnGroupColumns() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, sum by (pod) (network.total_bytes_in{cluster=\"prod\"})))", false)
        );

        var offset = hashOffset(plan);
        assertThat(samplingKeyNames(offset), equalTo(List.of("pod")));
        var comparison = as(hashOffsetComparison(plan), LessThan.class);
        assertThat(comparison.right().fold(null), equalTo(0.5));
    }

    /**
     * At series grain the sampling key is the {@code _timeseries} blob.
     */
    public void testLimitRatioBareKeysOnTimeseries() {
        var offset = hashOffset();
        assertThat(offset.children(), hasSize(1));
        Attribute key = as(offset.children().get(0), Attribute.class);
        assertThat(MetadataAttribute.isTimeSeriesAttribute(key), equalTo(true));
    }

    public void testLimitRatioProducesHashOffsetFilter() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in))", false)
        );

        var comparison = as(hashOffsetComparison(plan), LessThan.class);
        assertThat(comparison.left(), instanceOf(HashOffset.class));
        assertThat(((Number) ((Literal) comparison.right()).value()).doubleValue(), closeTo(0.5, 1e-10));
    }

    /**
     * Unlike aggregations, {@code limit_ratio} keeps the full label identity of each selected series.
     */
    public void testLimitRatioBareKeepsFullSeriesIdentity() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.3, network.bytes_in))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));
    }

    /**
     * {@code limit_ratio(...) by (...)} is membership-neutral like Prometheus: the outer partitions
     * neither join the sampling key nor change which series are kept. The key stays the input
     * identity ({@code _timeseries} at series grain) and full series identity is preserved.
     */
    public void testLimitRatioByGroupingPartitionsByLabelAndKeepsFullIdentity() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in) by (pod))", false)
        );

        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));

        var offset = hashOffset(plan);
        // Membership-neutral: only the series identity, no outer partition label.
        assertThat(samplingKeyNames(offset), equalTo(List.of(MetadataAttribute.TIMESERIES)));
    }

    /**
     * Bare and outer-grouped {@code limit_ratio} sample on identical keys, so they select
     * identical series: the outer {@code by} must not append partition labels to the key.
     */
    public void testLimitRatioOuterGroupingIsMembershipNeutral() {
        var bare = hashOffset("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in))");
        var grouped = hashOffset("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in) by (pod))");

        assertThat(samplingKeyNames(grouped), equalTo(samplingKeyNames(bare)));
        assertThat(samplingKeyNames(grouped), equalTo(List.of(MetadataAttribute.TIMESERIES)));
    }

    /**
     * A missing outer partition label must not materialize an extra null key column: it ranks
     * as one partition for order-statistic reductions, but {@code limit_ratio} ignores it entirely.
     */
    public void testLimitRatioMissingOuterLabelAddsNoNullKey() {
        var bare = hashOffset("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in))");
        var missing = hashOffset("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in) by (does_not_exist))");

        assertThat(samplingKeyNames(missing), equalTo(samplingKeyNames(bare)));
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in) by (does_not_exist))", false)
        );
        // Exactly one sampling filter: no extra null-materializing plan node for the missing label.
        assertThat(hashOffsetFilters(plan), hasSize(1));
    }

    /**
     * Reordered outer grouping labels sample identically: the key derives solely from the input
     * identity, so label order in the outer {@code by} cannot change the hash.
     */
    public void testLimitRatioReorderedOuterGroupingIdentical() {
        var bare = hashOffset("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in))");
        var ordered = hashOffset("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in) by (pod, cluster))");
        var reordered = hashOffset("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in) by (cluster, pod))");

        assertThat(samplingKeyNames(ordered), equalTo(samplingKeyNames(bare)));
        assertThat(samplingKeyNames(reordered), equalTo(samplingKeyNames(bare)));
    }

    /**
     * Over an aggregate the sampling key is the aggregated identity (here {@code pod, cluster}),
     * regardless of any outer {@code by}: outer partitions must not narrow or widen the hashed set.
     */
    public void testLimitRatioOverSumByIgnoresOuterGrouping() {
        var bare = hashOffset("PROMQL index=k8s step=1h result=(limit_ratio(0.5, sum by (pod, cluster) (network.bytes_in)))");
        var outer = hashOffset("PROMQL index=k8s step=1h result=(limit_ratio(0.5, sum by (pod, cluster) (network.bytes_in)) by (pod))");
        var reorderedOuter = hashOffset(
            "PROMQL index=k8s step=1h result=(limit_ratio(0.5, sum by (pod, cluster) (network.bytes_in)) by (cluster, pod))"
        );
        var missingOuter = hashOffset(
            "PROMQL index=k8s step=1h result=(limit_ratio(0.5, sum by (pod, cluster) (network.bytes_in)) by (does_not_exist))"
        );

        assertThat(samplingKeyNames(bare).stream().sorted().toList(), equalTo(List.of("cluster", "pod")));
        assertThat(samplingKeyNames(outer).stream().sorted().toList(), equalTo(List.of("cluster", "pod")));
        assertThat(samplingKeyNames(reorderedOuter).stream().sorted().toList(), equalTo(List.of("cluster", "pod")));
        assertThat(samplingKeyNames(missingOuter).stream().sorted().toList(), equalTo(List.of("cluster", "pod")));
    }

    /**
     * A constant vector's empty label set is a valid sampling identity: the sampler applies
     * directly with an empty key set, without introducing an aggregation.
     * Ratio zero keeps nothing, so the instant query yields zero rows.
     */
    public void testLimitRatioOverConstantInstantRatioZeroKeepsNothing() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" result=(limit_ratio(0, vector(1)))", false, false)
        );

        assertThat(plan.collect(Aggregate.class).isEmpty(), equalTo(true));
        assertThat(plan.collect(TimeSeriesAggregate.class).isEmpty(), equalTo(true));
        // Ratio zero keeps nothing: the sampling filter is either a constant FALSE (possibly ANDed
        // with the step filter) or already folded to an empty relation -- either way zero rows pass.
        boolean hasFalseFilter = plan.collect(Filter.class)
            .stream()
            .anyMatch(f -> f.condition().anyMatch(e -> e instanceof Literal literal && Boolean.FALSE.equals(literal.value())));
        boolean hasEmptyRelation = plan.collect(LocalRelation.class).stream().anyMatch(lr -> lr.supplier() == EmptyLocalSupplier.EMPTY);
        assertThat(hasFalseFilter || hasEmptyRelation, equalTo(true));
    }

    /**
     * Ratio one over a constant keeps the single empty identity: no sampling filter at all.
     */
    public void testLimitRatioOverConstantInstantRatioOneKeepsEverything() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" result=(limit_ratio(1, vector(1)))", false, false)
        );

        assertThat(hashOffsetFilters(plan).isEmpty(), equalTo(true));
        assertThat(plan.collect(Aggregate.class).isEmpty(), equalTo(true));
    }

    /**
     * Fractional complementary ratios over the same empty identity keep complementary subsets:
     * {@code 0.3} keeps offsets below 0.3 while {@code -0.7} keeps offsets at or above 0.3,
     * so exactly one keeps the row without asserting which one (the hash is stable but the
     * test pins structure, not the subset).
     */
    public void testLimitRatioOverConstantComplementaryRatios() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" result=(limit_ratio(0.3, vector(1)))", false, false)
        );
        var positive = as(hashOffsetComparison(plan), LessThan.class);
        assertThat(((Number) ((Literal) positive.right()).value()).doubleValue(), closeTo(0.3, 1e-12));
        assertThat(as(positive.left(), HashOffset.class).children(), hasSize(0));

        var negPlan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" result=(limit_ratio(-0.7, vector(1)))", false, false)
        );
        var negative = as(hashOffsetComparison(negPlan), GreaterThanOrEqual.class);
        assertThat(((Number) ((Literal) negative.right()).value()).doubleValue(), closeTo(0.3, 1e-12));
        assertThat(as(negative.left(), HashOffset.class).children(), hasSize(0));
    }

    /**
     * A range query over a constant shares one empty identity across steps, so a fractional
     * ratio keeps either all steps or none -- never a strict subset -- consistently.
     */
    public void testLimitRatioOverConstantRangeConsistentAcrossSteps() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql(
                "PROMQL index=empty_index start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m result=(limit_ratio(0.3, vector(1)))",
                false,
                false
            )
        );

        var offset = hashOffset(plan);
        assertThat(offset.children(), hasSize(0));
        // The constant range still fans out to one row per step; the sampling filter sits above it.
        assertThat(plan.collect(MvExpand.class).isEmpty(), equalTo(false));
    }

    public void testLimitRatioWithoutGroupingNotYetSupported() {
        var e = expectThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in) without (pod))", true)
        );
        assertThat(e.getMessage(), containsString("limit_ratio"));
    }

    /**
     * Like Prometheus, a negative ratio is accepted and keeps the complement subset
     * (offsets at or above {@code 1 + r}).
     */
    public void testLimitRatioNegativeAcceptedAsComplement() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(-0.5, network.bytes_in))", false)
        );

        var comparison = as(hashOffsetComparison(plan), GreaterThanOrEqual.class);
        assertThat(comparison.left(), instanceOf(HashOffset.class));
        assertThat(((Number) ((Literal) comparison.right()).value()).doubleValue(), closeTo(0.5, 1e-10));
    }

    /**
     * Like Prometheus, NaN ratios are rejected; infinite ratios clamp naturally (+Inf keeps everything).
     */
    public void testLimitRatioNaNRejected() {
        var e = expectThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=1h result=(limit_ratio(nan, network.bytes_in))", true)
        );
        assertThat(e.getMessage(), containsString("must not be NaN"));
    }

    /**
     * Like Prometheus, infinite ratios are accepted and clamp naturally (+Inf keeps everything:
     * no sampling filter at all).
     */
    public void testLimitRatioInfiniteAccepted() {
        var plan = logicalOptimizerWithLatestVersion.optimize(
            planPromql("PROMQL index=k8s step=1h result=(limit_ratio(Inf, network.bytes_in))", false)
        );

        assertThat(hashOffsetFilters(plan).isEmpty(), equalTo(true));
    }

    public void testLimitRatioStringRejected() {
        var e = expectThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=1h result=(limit_ratio(\"0.5\", network.bytes_in))", true)
        );
        assertThat(e.getMessage(), containsString("numeric ratio"));
    }

    private HashOffset hashOffset() {
        return hashOffset("PROMQL index=k8s step=1h result=(limit_ratio(0.5, network.bytes_in))", false);
    }

    private HashOffset hashOffset(String query) {
        return hashOffset(query, false);
    }

    private HashOffset hashOffset(String query, boolean allowEmptyReferences) {
        var plan = logicalOptimizerWithLatestVersion.optimize(planPromql(query, allowEmptyReferences, false));
        return as(hashOffsetFilter(plan).condition().collect(HashOffset.class).get(0), HashOffset.class);
    }

    private HashOffset hashOffset(LogicalPlan plan) {
        var offsets = plan.collect(Filter.class).stream().flatMap(f -> f.condition().collect(HashOffset.class).stream()).toList();
        assertThat(offsets, hasSize(1));
        return offsets.get(0);
    }

    private Filter hashOffsetFilter(LogicalPlan plan) {
        var filters = hashOffsetFilters(plan);
        assertThat(filters, hasSize(1));
        return filters.get(0);
    }

    private List<Filter> hashOffsetFilters(LogicalPlan plan) {
        return plan.collect(Filter.class).stream().filter(f -> f.condition().anyMatch(HashOffset.class::isInstance)).toList();
    }

    /** The sampling comparison ({@code offset < r} or {@code offset >= 1 + r}) carrying the offset. */
    private Expression hashOffsetComparison(LogicalPlan plan) {
        var comparisons = plan.collect(Filter.class)
            .stream()
            .flatMap(
                f -> Stream.concat(f.condition().collect(LessThan.class).stream(), f.condition().collect(GreaterThanOrEqual.class).stream())
            )
            .filter(c -> c.anyMatch(HashOffset.class::isInstance))
            .toList();
        assertThat(comparisons, hasSize(1));
        return comparisons.get(0);
    }

    /** The sampling key names carried by the offset function. */
    private static List<String> samplingKeyNames(HashOffset offset) {
        return offset.children().stream().map(c -> {
            assertThat(c, instanceOf(Attribute.class));
            return ((Attribute) c).name();
        }).toList();
    }
}
