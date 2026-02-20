/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PromqlPlanVectorMatchingTests extends AbstractPromqlPlanOptimizerTests {

    // --- one-to-one matching, same groupings (on/ignoring with compatible dimensions) ---

    public void testOnClauseSameGroupings() {
        var plan = planPromql(
            "PROMQL index=k8s step=1m ratio=(sum by (cluster)(network.total_bytes_in) / on(cluster) sum by (cluster)(network.bytes_in))"
        );
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("ratio", "step", "cluster")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat(outerAggs, hasSize(1));
        var agg = outerAggs.getFirst();
        assertThat(agg.aggregates().stream().filter(e -> e.anyMatch(Sum.class::isInstance)).count(), equalTo(2L));
    }

    public void testOnEmptyListGlobalJoin() {
        var plan = planPromql("PROMQL index=k8s step=1m ratio=(sum(network.total_bytes_in) / on() sum(network.bytes_in))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("ratio", "step")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat(outerAggs, hasSize(1));
        assertThat(outerAggs.getFirst().aggregates().stream().filter(e -> e.anyMatch(Sum.class::isInstance)).count(), equalTo(2L));
    }

    public void testIgnoringEmptyListMatchAll() {
        var plan = planPromql("PROMQL index=k8s step=1m ratio=(sum(network.total_bytes_in) / ignoring() sum(network.bytes_in))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("ratio", "step")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat(outerAggs, hasSize(1));
        assertThat(outerAggs.getFirst().aggregates().stream().filter(e -> e.anyMatch(Sum.class::isInstance)).count(), equalTo(2L));
    }

    public void testOnClauseWithSubtraction() {
        var plan = planPromql(
            "PROMQL index=k8s step=1m diff=(max by (cluster)(network.bytes_in) - on(cluster) min by (cluster)(network.bytes_in))"
        );
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("diff", "step", "cluster")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat(outerAggs, hasSize(1));
        var agg = outerAggs.getFirst();
        assertThat(agg.aggregates().stream().filter(e -> e.anyMatch(Max.class::isInstance)).count(), equalTo(1L));
        assertThat(agg.aggregates().stream().filter(e -> e.anyMatch(Min.class::isInstance)).count(), equalTo(1L));
    }

    public void testOnClauseWithMultiplication() {
        var plan = planPromql(
            "PROMQL index=k8s step=1m prod=(sum by (cluster)(network.bytes_in) * on(cluster) sum by (cluster)(network.bytes_in))"
        );
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("prod", "step", "cluster")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat(outerAggs, hasSize(1));
        // same field deduplicates to a single Sum in the merged aggregate
        assertThat(outerAggs.getFirst().aggregates().stream().filter(e -> e.anyMatch(Sum.class::isInstance)).count(), equalTo(1L));
    }

    public void testOnClauseDifferentFieldsSameGroupings() {
        var plan = planPromql(
            "PROMQL index=k8s step=1m prod=(sum by (cluster)(network.bytes_in) * on(cluster) sum by (cluster)(network.total_bytes_in))"
        );
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("prod", "step", "cluster")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat(outerAggs, hasSize(1));
        assertThat(outerAggs.getFirst().aggregates().stream().filter(e -> e.anyMatch(Sum.class::isInstance)).count(), equalTo(2L));
    }

    // instant selectors with on/ignoring require full series-level vector matching (future phase)
    public void testBlocked_onClauseInstantSelectors() {
        expectThrows(
            IllegalStateException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(network.bytes_in / on(cluster) network.bytes_in)")
        );
    }

    // --- one-to-one matching, different groupings (not yet supported) ---

    public void testBlocked_onClauseArithmeticDifferentGroupings() {
        var e = expectThrows(
            QlIllegalArgumentException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m ratio=(sum by (cluster)(network.total_bytes_in) / on(cluster) sum(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("different grouping keys"));
    }

    public void testBlocked_ignoringClauseArithmeticDifferentGroupings() {
        var e = expectThrows(
            QlIllegalArgumentException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m ratio=(sum by (cluster, pod)(network.bytes_in) / ignoring(pod) sum by (cluster)(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("different grouping keys"));
    }

    public void testBlocked_ignoringMultipleLabels() {
        var e = expectThrows(
            QlIllegalArgumentException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m r=(sum by (cluster, pod, region)(network.bytes_in)"
                    + " / ignoring(pod, region) sum by (cluster)(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("different grouping keys"));
    }

    // --- many-to-one / one-to-many (group_left / group_right) ---

    public void testBlocked_groupLeftBasic() {
        var e = expectThrows(
            VerificationException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m r=(sum by (cluster, pod)(network.bytes_in)"
                    + " / ignoring(pod) group_left sum by (cluster)(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("group modifiers are not supported at this time"));
    }

    public void testBlocked_groupRightBasic() {
        var e = expectThrows(
            VerificationException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m r=(sum by (cluster)(network.bytes_in)"
                    + " / on(cluster) group_right sum by (cluster, pod)(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("group modifiers are not supported at this time"));
    }

    public void testBlocked_groupLeftWithExtraLabels() {
        var e = expectThrows(
            VerificationException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m r=(sum by (cluster, pod)(network.bytes_in)"
                    + " / ignoring(pod) group_left(region) sum by (cluster)(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("group modifiers are not supported at this time"));
    }

    // --- parser-level validations ---

    public void testParserRejectsModifierWithScalarLeft() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(1 / on(cluster) sum(network.bytes_in))")
        );
        assertThat(e.getMessage(), containsString("Vector matching allowed only between instant vectors"));
    }

    public void testParserRejectsModifierWithScalarRight() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(sum(network.bytes_in) / on(cluster) 1)")
        );
        assertThat(e.getMessage(), containsString("Vector matching allowed only between instant vectors"));
    }

    public void testParserRejectsModifierWithScalarFunctionLeft() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(pi() / on(cluster) sum(network.bytes_in))")
        );
        assertThat(e.getMessage(), containsString("Vector matching allowed only between instant vectors"));
    }

    public void testParserRejectsModifierWithRangeVectors() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(network.bytes_in[1m] + on(cluster) network.bytes_in[1m])")
        );
        assertThat(e.getMessage(), containsString("Vector matching allowed only between instant vectors"));
    }

    public void testParserRejectsLabelInBothOnAndGroupClauses() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m r=(sum by (cluster, pod)(network.bytes_in)"
                    + " / on(cluster, pod) group_left(cluster, region) sum by (pod)(network.bytes_in))"
            )
        );
        assertThat(e.getMessage(), containsString("must not occur in ON and GROUP clause at once"));
    }

    public void testParserRejectsGroupModifierWithSetOperators() {
        var e = expectThrows(
            ParsingException.class,
            () -> planPromql("PROMQL index=k8s step=1m r=(network.bytes_in and on(cluster) group_left network.bytes_in)")
        );
        assertThat(e.getMessage(), containsString("No grouping"));
    }
}
