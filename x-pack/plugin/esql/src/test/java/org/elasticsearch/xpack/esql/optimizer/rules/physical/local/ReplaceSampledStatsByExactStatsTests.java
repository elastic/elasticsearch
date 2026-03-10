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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ReplaceSampledStatsByExactStatsTests extends ESTestCase {

    /**
     * COUNT(*) is pushable to Lucene, so SampledAggregateExec should be replaced
     * by AggregateExec wrapped in EvalExec that nullifies bucket columns.
     *
     * Plan: SampledAggregateExec(INITIAL) -> EvalExec($bucket_id) -> EsQueryExec
     * Expected: EvalExec(null buckets) -> AggregateExec(INITIAL) -> EsQueryExec
     */
    public void testReplace_countStar() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(esQueryExec()), List.of(count), List.of());

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

        assertThat(result, instanceOf(EvalExec.class));
        EvalExec evalExec = (EvalExec) result;
        assertAllFieldsNull(evalExec);

        assertThat(evalExec.child(), instanceOf(AggregateExec.class));
        AggregateExec aggExec = (AggregateExec) evalExec.child();
        assertThat(aggExec.getMode(), is(AggregatorMode.INITIAL));
        assertThat(aggExec.groupings().size(), is(0));
        assertThat(aggExec.aggregates().size(), is(1));
        assertThat(aggExec.aggregates().getFirst(), is(count));
        assertThat(aggExec.child(), instanceOf(EsQueryExec.class));
    }

    /**
     * COUNT(field) on a single-value field is pushable, so the same replacement should happen.
     */
    public void testReplace_countFieldSingleValue() {
        FieldAttribute field = fieldAttribute("emp_no", DataType.INTEGER);
        Alias count = countAlias(field);
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(esQueryExec()), List.of(count), List.of());

        PhysicalPlan result = applyRule(sampledAgg, new TestSingleValueSearchStats());

        assertThat(result, instanceOf(EvalExec.class));
        EvalExec evalExec = (EvalExec) result;
        assertAllFieldsNull(evalExec);

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
     * Grouped aggregates (with BY clause) are not pushable by PushStatsToSource,
     * so the rule should not transform the plan.
     */
    public void testDontReplace_groupedAggregate() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        FieldAttribute groupField = fieldAttribute("dept", DataType.KEYWORD);
        SampledAggregateExec sampledAgg = sampledAggregate(bucketIdEval(esQueryExec()), List.of(count), List.of(groupField));

        PhysicalPlan result = applyRule(sampledAgg, EsqlTestUtils.TEST_SEARCH_STATS);

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
        List<? extends NamedExpression> originalAggregates,
        List<? extends Expression> groupings
    ) {
        return sampledAggregate(child, originalAggregates, groupings, AggregatorMode.INITIAL);
    }

    private static SampledAggregateExec sampledAggregate(
        PhysicalPlan child,
        List<? extends NamedExpression> originalAggregates,
        List<? extends Expression> groupings,
        AggregatorMode mode
    ) {
        ArrayList<NamedExpression> allAggregates = new ArrayList<>(originalAggregates);
        for (NamedExpression agg : originalAggregates) {
            allAggregates.add(new Alias(Source.EMPTY, agg.name() + "_bucket", agg.toAttribute()));
        }
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
        Alias bucketIdAlias = new Alias(
            Source.EMPTY,
            ApproximationPlan.BUCKET_ID_COLUMN_NAME,
            new Literal(Source.EMPTY, 0, DataType.INTEGER)
        );
        return new EvalExec(Source.EMPTY, child, List.of(bucketIdAlias));
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

    private static void assertAllFieldsNull(EvalExec evalExec) {
        assertThat("at least one null bucket field expected", evalExec.fields().isEmpty(), is(false));
        for (Alias field : evalExec.fields()) {
            assertThat(
                "bucket field '" + field.name() + "' should be set to null",
                field.child() instanceof Literal lit && lit.value() == null,
                is(true)
            );
            assertThat(field.child().fold(FoldContext.small()), nullValue());
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
