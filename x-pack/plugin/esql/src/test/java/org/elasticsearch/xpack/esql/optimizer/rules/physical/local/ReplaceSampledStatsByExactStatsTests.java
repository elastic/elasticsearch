/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.approximation.ApproximationPlan;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampledAggregateExec;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ReplaceSampledStatsByExactStatsTests extends ESTestCase {

    /**
     * COUNT(*) is pushable to Lucene, so SampledAggregateExec should be replaced
     * by AggregateExec wrapped in EvalExec that replicates original values to buckets.
     *
     * Plan: SampledAggregateExec(INITIAL) -> EvalExec($bucket_id) -> EsQueryExec
     * Expected: EvalExec(replicated buckets) -> AggregateExec(INITIAL) -> EsQueryExec
     */
    public void testReplace_countStar() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(esQueryExec()), List.of(count), List.of());

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(EvalExec.class));
        EvalExec evalExec = (EvalExec) result;
        assertBucketsReplicateOriginal(evalExec, sampledAgg.originalIntermediateAttributes());
        assertThat(evalExec.child(), instanceOf(AggregateExec.class));
        AggregateExec aggExec = (AggregateExec) evalExec.child();
        assertThat(aggExec.getMode(), is(AggregatorMode.INITIAL));
        assertThat(aggExec.groupings().size(), is(0));
        assertThat(aggExec.aggregates().size(), is(1));
        assertThat(aggExec.aggregates().getFirst(), is(count));
        assertThat(aggExec.child(), instanceOf(EsQueryExec.class));
    }

    /**
     * COUNT(field) on a single-valued field is pushable, so the same replacement should happen.
     */
    public void testReplace_countFieldSingleValue() {
        FieldAttribute field = fieldAttribute("emp_no", DataType.INTEGER);
        Alias count = countAlias(field);
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(esQueryExec()), List.of(count), List.of());

        PhysicalPlan result = applyRule(sampledAgg, new TestSingleValueSearchStats());

        assertThat(result, instanceOf(EvalExec.class));
        EvalExec evalExec = (EvalExec) result;
        assertBucketsReplicateOriginal(evalExec, sampledAgg.originalIntermediateAttributes());
        assertThat(evalExec.child(), instanceOf(AggregateExec.class));
        AggregateExec aggExec = (AggregateExec) evalExec.child();
        assertThat(aggExec.getMode(), is(AggregatorMode.INITIAL));
        assertThat(aggExec.groupings().size(), is(0));
        assertThat(aggExec.aggregates().size(), is(1));
        assertThat(aggExec.aggregates().getFirst(), is(count));
        assertThat(aggExec.child(), instanceOf(EsQueryExec.class));
    }

    /**
     * Non-INITIAL mode should not be transformed.
     */
    public void testDontReplace_nonInitialMode() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(esQueryExec()), List.of(count), List.of(), AggregatorMode.FINAL);

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    /**
     * If the child is not EvalExec (missing bucket_id eval), the rule should not apply.
     */
    public void testDontReplace_noBucketIdEval() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        SampledAggregateExec sampledAgg = sampledAggregate(esQueryExec(), List.of(count), List.of());

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    /**
     * If the EvalExec alias name is not $bucket_id, the rule should not apply.
     */
    public void testDontReplace_wrongAliasName() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        Alias wrongAlias = new Alias(Source.EMPTY, "not_bucket_id", new Literal(Source.EMPTY, 0, DataType.INTEGER));
        EvalExec evalExec = new EvalExec(Source.EMPTY, esQueryExec(), List.of(wrongAlias));
        SampledAggregateExec sampledAgg = sampledAggregate(evalExec, List.of(count), List.of());

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    /**
     * Non-pushable aggregate (e.g. SUM) should not be transformed.
     */
    public void testDontReplace_nonPushableAggregate() {
        FieldAttribute field = fieldAttribute("salary", DataType.INTEGER);
        Alias sumAlias = new Alias(Source.EMPTY, "sum", new Sum(Source.EMPTY, field));
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(esQueryExec()), List.of(sumAlias), List.of());

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    /**
     * Multiple original aggregates are not supported
     */
    public void testDontReplace_multipleOriginalAggregates() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        Alias count2 = countAlias(Literal.keyword(Source.EMPTY, "*"));
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(esQueryExec()), List.of(count, count2), List.of());

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    /**
     * COUNT(field) on a multi-value field is not pushable (isSingleValue returns false),
     * so the rule should not transform the plan.
     */
    public void testDontReplace_countFieldMultiValue() {
        FieldAttribute field = fieldAttribute("tags", DataType.KEYWORD);
        Alias count = countAlias(field);
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(esQueryExec()), List.of(count), List.of());

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    /**
     * If the EvalExec has multiple expressions (not just the single bucket_id alias),
     * the rule should not apply.
     */
    public void testDontReplace_multipleEvalExpressions() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        Alias bucketAlias = new Alias(
            Source.EMPTY,
            ApproximationPlan.BUCKET_ID_COLUMN_NAME,
            new Literal(Source.EMPTY, 0, DataType.INTEGER)
        );
        Alias extraAlias = new Alias(Source.EMPTY, "extra", new Literal(Source.EMPTY, 1, DataType.INTEGER));
        EvalExec evalExec = new EvalExec(Source.EMPTY, esQueryExec(), List.of(bucketAlias, extraAlias));
        SampledAggregateExec sampledAgg = sampledAggregate(evalExec, List.of(count), List.of());

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    /**
     * If the EvalExec child is not EsQueryExec, the rule should not apply.
     */
    public void testDontReplace_evalChildNotEsQueryExec() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        EvalExec innerEvalExec = new EvalExec(
            Source.EMPTY,
            esQueryExec(),
            List.of(new Alias(Source.EMPTY, "x", new Literal(Source.EMPTY, 1, DataType.INTEGER)))
        );
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(innerEvalExec), List.of(count), List.of());

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    /**
     * Grouped aggregates whose grouping is not a pushable RoundTo (e.g. grouping on a keyword field) cannot be executed exactly via
     * query-and-tags, so the rule should not transform the plan.
     */
    public void testDontReplace_groupedAggregate() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        FieldAttribute groupField = fieldAttribute("dept", DataType.KEYWORD);
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(esQueryExec()), List.of(count), List.of(groupField));

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    /**
     * COUNT(*) BY BUCKET(date, ...) is rewritten to COUNT(*) BY RoundTo(date, ...) by {@code ReplaceDateTruncBucketWithRoundTo}. When the
     * RoundTo can be pushed down to Lucene as query-and-tags, sampling should be skipped: the SampledAggregateExec is turned back into a
     * regular grouped AggregateExec on top of the RoundTo eval (with the now-unused bucket id dropped), and the exact intermediate values
     * are replicated to the bucket intermediates.
     */
    public void testReplace_groupedCountByPushableRoundTo() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        Alias roundToAlias = new Alias(Source.EMPTY, "x", roundToDate(fieldAttribute("date", DataType.DATETIME)));
        Attribute groupingKey = roundToAlias.toAttribute();
        EvalExec eval = new EvalExec(Source.EMPTY, esQueryExec(), List.of(roundToAlias, bucketIdAlias()));
        SampledAggregateExec sampledAgg = sampledAggregate(eval, List.of(count), List.of(groupingKey));

        PhysicalPlan result = applyRule(sampledAgg, searchStatsWithDateMinMax());

        assertThat(result, instanceOf(EvalExec.class));
        EvalExec bucketReplication = (EvalExec) result;
        assertBucketsReplicateOriginal(bucketReplication, sampledAgg.originalIntermediateAttributes());
        assertThat(bucketReplication.child(), instanceOf(AggregateExec.class));
        AggregateExec aggExec = (AggregateExec) bucketReplication.child();
        assertThat(aggExec.getMode(), is(AggregatorMode.INITIAL));
        assertThat(aggExec.groupings().size(), is(1));
        assertThat(aggExec.aggregates(), is(List.of(count, groupingKey)));
        // The grouping eval keeps only the RoundTo; the random bucket id is no longer needed once sampling is skipped.
        assertThat(aggExec.child(), instanceOf(EvalExec.class));
        EvalExec groupingEval = (EvalExec) aggExec.child();
        assertThat(groupingEval.fields(), is(List.of(roundToAlias)));
        assertThat(groupingEval.child(), instanceOf(EsQueryExec.class));
    }

    /**
     * If the RoundTo grouping's field is not pushable to Lucene (here it is not indexed), no query-and-tags can be generated, so running
     * the exact aggregation would be a full scan, which is worse than sampling. The rule should therefore leave the plan untouched.
     */
    public void testDontReplace_groupedCountByNonPushableRoundTo() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        FieldAttribute nonAggregatableDate = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "date",
            new EsField("date", DataType.DATETIME, new HashMap<>(), false, EsField.TimeSeriesFieldType.NONE)
        );
        Alias roundToAlias = new Alias(Source.EMPTY, "x", roundToDate(nonAggregatableDate));
        Attribute groupingKey = roundToAlias.toAttribute();
        EvalExec eval = new EvalExec(Source.EMPTY, esQueryExec(), List.of(roundToAlias, bucketIdAlias()));
        SampledAggregateExec sampledAgg = sampledAggregate(eval, List.of(count), List.of(groupingKey));

        SearchStats notIndexed = new EsqlTestUtils.TestConfigurableSearchStats().exclude(
            EsqlTestUtils.TestConfigurableSearchStats.Config.INDEXED,
            "date"
        ).exclude(EsqlTestUtils.TestConfigurableSearchStats.Config.DOC_VALUES, "date");
        PhysicalPlan result = applyRule(sampledAgg, notIndexed);

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    /**
     * A grouped non-count aggregate (e.g. SUM) by a pushable RoundTo is not pushable as query-and-tags, so the rule should not apply.
     */
    public void testDontReplace_groupedSumByRoundTo() {
        FieldAttribute salary = fieldAttribute("salary", DataType.INTEGER);
        Alias sumAlias = new Alias(Source.EMPTY, "sum", new Sum(Source.EMPTY, salary));
        Alias roundToAlias = new Alias(Source.EMPTY, "x", roundToDate(fieldAttribute("date", DataType.DATETIME)));
        Attribute groupingKey = roundToAlias.toAttribute();
        EvalExec eval = new EvalExec(Source.EMPTY, esQueryExec(), List.of(roundToAlias, bucketIdAlias()));
        SampledAggregateExec sampledAgg = sampledAggregate(eval, List.of(sumAlias), List.of(groupingKey));

        PhysicalPlan result = applyRule(sampledAgg, searchStatsWithDateMinMax());

        assertThat(result, instanceOf(SampledAggregateExec.class));
    }

    private static PhysicalPlan applyRule(SampledAggregateExec sampledAgg, SearchStats searchStats) {
        LocalPhysicalOptimizerContext context = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            TEST_CFG,
            FoldContext.small(),
            searchStats
        );
        return new ReplaceSampledStatsByExactStats().rule(sampledAgg, context);
    }

    private static SampledAggregateExec sampledAggregate(
        PhysicalPlan child,
        List<NamedExpression> originalAggregates,
        List<NamedExpression> groupings
    ) {
        return sampledAggregate(child, originalAggregates, groupings, AggregatorMode.INITIAL);
    }

    private static SampledAggregateExec sampledAggregate(
        PhysicalPlan child,
        List<NamedExpression> originalAggregates,
        List<NamedExpression> groupings,
        AggregatorMode mode
    ) {
        originalAggregates = new ArrayList<>(originalAggregates);
        ArrayList<NamedExpression> allAggregates = new ArrayList<>(originalAggregates);
        for (NamedExpression agg : originalAggregates) {
            allAggregates.add(new Alias(Source.EMPTY, agg.name() + "_bucket", agg.toAttribute()));
        }

        originalAggregates.addAll(groupings);
        allAggregates.addAll(groupings);

        List<Attribute> originalIntermediateAttrs = intermediateAttributes(originalAggregates);
        List<Attribute> allIntermediateAttrs = new ArrayList<>(originalIntermediateAttrs);
        allIntermediateAttrs.add(bucketAttribute());

        return new SampledAggregateExec(
            Source.EMPTY,
            child,
            groupings,
            allAggregates,
            originalAggregates,
            new Literal(Source.EMPTY, 0.5, DataType.DOUBLE),
            mode,
            allIntermediateAttrs,
            originalIntermediateAttrs,
            null
        );
    }

    private static EvalExec bucketIdEval(PhysicalPlan child) {
        return new EvalExec(Source.EMPTY, child, List.of(bucketIdAlias()));
    }

    private static Alias bucketIdAlias() {
        return new Alias(Source.EMPTY, ApproximationPlan.BUCKET_ID_COLUMN_NAME, new Literal(Source.EMPTY, 0, DataType.INTEGER));
    }

    private static RoundTo roundToDate(FieldAttribute dateField) {
        // Day boundaries within the [2023-10-20, 2023-10-23] min/max range of searchStatsWithDateMinMax().
        List<Expression> points = List.of(
            new Literal(Source.EMPTY, 1697760000000L, DataType.DATETIME),
            new Literal(Source.EMPTY, 1697846400000L, DataType.DATETIME),
            new Literal(Source.EMPTY, 1697932800000L, DataType.DATETIME),
            new Literal(Source.EMPTY, 1698019200000L, DataType.DATETIME)
        );
        return new RoundTo(Source.EMPTY, dateField, points);
    }

    private static SearchStats searchStatsWithDateMinMax() {
        return new EsqlTestUtils.TestSearchStatsWithMinMax(
            java.util.Map.of("date", 1697804103360L), // 2023-10-20T12:15:03.360Z
            java.util.Map.of("date", 1698069301543L)  // 2023-10-23T13:55:01.543Z
        );
    }

    private static EsQueryExec esQueryExec() {
        return new EsQueryExec(Source.EMPTY, "test", IndexMode.STANDARD, List.of(), null, null, null, List.of());
    }

    private static Alias countAlias(Expression field) {
        return new Alias(Source.EMPTY, "count", new Count(Source.EMPTY, field));
    }

    private static FieldAttribute fieldAttribute(String name, DataType type) {
        return new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            name,
            new EsField(name, type, new HashMap<>(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static ReferenceAttribute bucketAttribute() {
        return new ReferenceAttribute(Source.EMPTY, null, ApproximationPlan.BUCKET_ID_COLUMN_NAME, DataType.INTEGER);
    }

    private static List<Attribute> intermediateAttributes(List<? extends NamedExpression> aggregates) {
        return AbstractPhysicalOperationProviders.intermediateAttributes(aggregates, List.of());
    }

    /**
     * Asserts that all bucket fields in the EvalExec replicate original
     * intermediate values. Each bucket field should reference an original
     * intermediate attribute (not be null or a computed expression).
     */
    private static void assertBucketsReplicateOriginal(EvalExec evalExec, List<Attribute> originalAttributes) {
        assertThat("at least one bucket field expected", evalExec.fields().isEmpty(), is(false));
        for (Alias field : evalExec.fields()) {
            assertThat(
                "bucket field '" + field.name() + "' should alias an original attribute",
                field.child(),
                instanceOf(Attribute.class)
            );
            assertThat(
                "bucket field '" + field.name() + "' should alias an original attribute",
                (Attribute) field.child(),
                in(originalAttributes)
            );
        }
    }

    /**
     * SearchStats that reports all fields as single-valued, enabling count-field pushdown.
     */
    private static class TestSingleValueSearchStats extends EsqlTestUtils.TestSearchStats {
        @Override
        public boolean isSingleValue(FieldName field) {
            return true;
        }
    }
}
