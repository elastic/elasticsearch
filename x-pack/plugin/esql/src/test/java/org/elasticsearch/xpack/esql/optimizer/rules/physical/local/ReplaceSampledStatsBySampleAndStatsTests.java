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
import org.elasticsearch.xpack.esql.approximation.ApproximationPlan;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.plan.physical.SampledAggregateExec;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ReplaceSampledStatsBySampleAndStatsTests extends ESTestCase {

    /**
     * COUNT(*), no groupings, INITIAL mode, prob=0.5.
     * SampleExec should wrap the leaf.
     */
    public void testReplace_countStarWithSampling() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        SampledAggregateExec sampledAgg = sampledAggregate(esQueryExec(), List.of(count), List.of(), AggregatorMode.INITIAL, 0.5);

        PhysicalPlan result = applyRule(sampledAgg);
        AggregateExec aggExec = assertAggregate(result, sampledAgg);

        assertThat(aggExec.child(), instanceOf(SampleExec.class));
        SampleExec sampleExec = (SampleExec) aggExec.child();
        assertThat(sampleExec.probability(), is(sampledAgg.sampleProbability()));
        assertThat(sampleExec.child(), instanceOf(EsQueryExec.class));
    }

    /**
     * SUM(salary), BY dept, INITIAL mode, prob=1.0.
     * No SampleExec when probability is 1.0.
     */
    public void testReplace_sumWithGroupingNoSampling() {
        FieldAttribute salary = fieldAttribute("salary", DataType.INTEGER);
        Alias sum = new Alias(Source.EMPTY, "sum", new Sum(Source.EMPTY, salary));
        FieldAttribute dept = fieldAttribute("dept", DataType.KEYWORD);
        SampledAggregateExec sampledAgg = sampledAggregate(esQueryExec(), List.of(sum), List.of(dept), AggregatorMode.INITIAL, 1.0);

        PhysicalPlan result = applyRule(sampledAgg);
        AggregateExec aggExec = assertAggregate(result, sampledAgg);

        assertThat(aggExec.child(), instanceOf(EsQueryExec.class));
    }

    /**
     * COUNT(emp_no), no groupings, FINAL mode, prob=0.3.
     * Verifies non-INITIAL mode is preserved.
     */
    public void testReplace_countFieldFinalMode() {
        FieldAttribute empNo = fieldAttribute("emp_no", DataType.INTEGER);
        Alias count = countAlias(empNo);
        SampledAggregateExec sampledAgg = sampledAggregate(esQueryExec(), List.of(count), List.of(), AggregatorMode.FINAL, 0.3);

        PhysicalPlan result = applyRule(sampledAgg);
        AggregateExec aggExec = assertAggregate(result, sampledAgg);

        assertThat(aggExec.child(), instanceOf(SampleExec.class));
        SampleExec sampleExec = (SampleExec) aggExec.child();
        assertThat(sampleExec.probability(), is(sampledAgg.sampleProbability()));
    }

    /**
     * COUNT(*) + SUM(salary), BY dept, INITIAL mode, prob=0.5.
     * SampleExec should wrap the leaf, not the intermediate EvalExec.
     */
    public void testReplace_multipleAggsWithEvalChild() {
        Alias count = countAlias(Literal.keyword(Source.EMPTY, "*"));
        FieldAttribute salary = fieldAttribute("salary", DataType.INTEGER);
        Alias sum = new Alias(Source.EMPTY, "sum", new Sum(Source.EMPTY, salary));
        FieldAttribute dept = fieldAttribute("dept", DataType.KEYWORD);
        EvalExec evalExec = new EvalExec(
            Source.EMPTY,
            esQueryExec(),
            List.of(new Alias(Source.EMPTY, "x", new Literal(Source.EMPTY, 1, DataType.INTEGER)))
        );
        SampledAggregateExec sampledAgg = sampledAggregate(evalExec, List.of(count, sum), List.of(dept), AggregatorMode.INITIAL, 0.5);

        PhysicalPlan result = applyRule(sampledAgg);
        AggregateExec aggExec = assertAggregate(result, sampledAgg);

        assertThat(aggExec.child(), instanceOf(EvalExec.class));
        EvalExec resultEval = (EvalExec) aggExec.child();
        assertThat(resultEval.child(), instanceOf(SampleExec.class));
        SampleExec sampleExec = (SampleExec) resultEval.child();
        assertThat(sampleExec.probability(), is(sampledAgg.sampleProbability()));
        assertThat(sampleExec.child(), instanceOf(EsQueryExec.class));
    }

    private static PhysicalPlan applyRule(SampledAggregateExec sampledAgg) {
        return new ReplaceSampledStatsBySampleAndStats().apply(sampledAgg);
    }

    private static AggregateExec assertAggregate(PhysicalPlan result, SampledAggregateExec sampledAgg) {
        assertThat(result, instanceOf(AggregateExec.class));
        AggregateExec aggExec = (AggregateExec) result;
        assertThat(aggExec.aggregates(), is(sampledAgg.aggregates()));
        assertThat(aggExec.groupings(), is(sampledAgg.groupings()));
        assertThat(aggExec.getMode(), is(sampledAgg.getMode()));
        assertThat(aggExec.intermediateAttributes(), is(sampledAgg.intermediateAttributes()));
        return aggExec;
    }

    private static SampledAggregateExec sampledAggregate(
        PhysicalPlan child,
        List<? extends NamedExpression> originalAggregates,
        List<? extends Expression> groupings,
        AggregatorMode mode,
        double probability
    ) {
        ArrayList<NamedExpression> allAggregates = new ArrayList<>(originalAggregates);
        for (NamedExpression agg : originalAggregates) {
            allAggregates.add(new Alias(Source.EMPTY, agg.name() + "_bucket", agg.toAttribute()));
        }

        return new SampledAggregateExec(
            Source.EMPTY,
            child,
            groupings,
            allAggregates,
            originalAggregates,
            new Literal(Source.EMPTY, probability, DataType.DOUBLE),
            mode,
            AbstractPhysicalOperationProviders.intermediateAttributes(allAggregates, groupings),
            AbstractPhysicalOperationProviders.intermediateAttributes(originalAggregates, groupings),
            null
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
}
