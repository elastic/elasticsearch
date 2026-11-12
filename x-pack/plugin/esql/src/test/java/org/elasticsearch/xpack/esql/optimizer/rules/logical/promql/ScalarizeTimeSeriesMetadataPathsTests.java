/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.AbstractTimeSeriesMetadataFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.ToTimeSeriesMetadata;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Splitting the label container of a translated PromQL plan, on plans built the way translation builds them: a collapse keyed by the
 * source's labels, and operations over its container above it.
 */
public class ScalarizeTimeSeriesMetadataPathsTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    private final FieldAttribute pod = dimension("pod");
    private final FieldAttribute value = new FieldAttribute(SOURCE, "value", new EsField("value", DataType.DOUBLE, Map.of(), true, null));
    private final EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(pod, value));

    /** A collapse whose series only read a label above it keys by the series id and the label: the source reads no view. */
    public void testLabelReadAboveCollapseIsAColumn() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate collapse = collapse(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Alias read = new Alias(SOURCE, "pod", new TimeSeriesMetadataPath(SOURCE, labels.toAttribute(), Literal.keyword(SOURCE, "pod")));
        LogicalPlan plan = ScalarizeTimeSeriesMetadataPaths.pushDown(new Eval(SOURCE, collapse, List.of(read)), EsqlTestUtils.TEST_CFG);

        TimeSeriesAggregate pushed = plan.collect(TimeSeriesAggregate.class).getFirst();
        // the keyword view of every label keys the collapse
        var views = pushed.groupings().stream().map(Alias::unwrap).filter(TimeSeriesWithout.class::isInstance).toList();
        assertThat(views, hasSize(1));
        assertThat(views.getFirst().children(), empty());
        assertThat(expressions(plan, ToTimeSeriesMetadata.class), empty());
        assertThat(expressions(plan, AbstractTimeSeriesMetadataFunction.class), empty());
        Eval eval = as(plan, Eval.class);
        assertThat(Alias.unwrap(eval.fields().getFirst()), instanceOf(Attribute.class));
    }

    /** A collapse whose container is rendered without a label carries the view that leaves it out. */
    public void testRenderedContainerIsAView() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate collapse = collapse(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Expression without = new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), true);
        Alias rendered = new Alias(SOURCE, MetadataAttribute.TIMESERIES, new ToString(SOURCE, without, EsqlTestUtils.TEST_CFG));
        LogicalPlan plan = ScalarizeTimeSeriesMetadataPaths.pushDown(new Eval(SOURCE, collapse, List.of(rendered)), EsqlTestUtils.TEST_CFG);

        List<Expression> views = expressions(plan, TimeSeriesWithout.class);
        assertThat(views, hasSize(1));
        assertEquals(List.of(pod), views.getFirst().children());
        assertThat(expressions(plan, AbstractTimeSeriesMetadataFunction.class), empty());
    }

    /** A label the source has no field for is in no series: the view leaves out no field for it. */
    public void testLabelWithoutFieldIsNotLeftOutOfTheView() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate collapse = collapse(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Expression without = new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "__name__")), true);
        Alias rendered = new Alias(SOURCE, MetadataAttribute.TIMESERIES, new ToString(SOURCE, without, EsqlTestUtils.TEST_CFG));
        LogicalPlan plan = ScalarizeTimeSeriesMetadataPaths.pushDown(new Eval(SOURCE, collapse, List.of(rendered)), EsqlTestUtils.TEST_CFG);

        List<Expression> views = expressions(plan, TimeSeriesWithout.class);
        assertThat(views, hasSize(1));
        assertThat(views.getFirst().children(), empty());
    }

    /** An aggregate keyed by labels packs them: a label of several values keys as a whole. */
    public void testAggregateKeysByPackedLabels() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate collapse = collapse(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Alias key = new Alias(
            SOURCE,
            MetadataAttribute.TIMESERIES,
            new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), false)
        );
        Alias sum = new Alias(SOURCE, "value", new Values(SOURCE, collapse.aggregates().getFirst().toAttribute()));
        Aggregate aggregate = new Aggregate(SOURCE, collapse, List.of(key), List.of(sum, key.toAttribute()));
        LogicalPlan plan = ScalarizeTimeSeriesMetadataPaths.pushDown(aggregate, EsqlTestUtils.TEST_CFG);

        Aggregate pushed = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).findFirst().get();
        assertThat(pushed.child(), instanceOf(PackDims.class));
        assertEquals(List.of("pod"), ((PackDims) pushed.child()).dims().stream().map(Attribute::name).toList());
        assertThat(expressions(plan, AbstractTimeSeriesMetadataFunction.class), empty());
    }

    /** A label container a user's time-series query computes is theirs: nothing is split. */
    public void testUserContainerIsKept() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate collapse = collapse(labels, TimeSeriesAggregate.Origin.TS_COMMAND);
        Alias read = new Alias(SOURCE, "pod", new TimeSeriesMetadataPath(SOURCE, labels.toAttribute(), Literal.keyword(SOURCE, "pod")));
        LogicalPlan plan = new Eval(SOURCE, collapse, List.of(read));

        assertThat(ScalarizeTimeSeriesMetadataPaths.pushDown(plan, EsqlTestUtils.TEST_CFG), sameInstance(plan));
    }

    /** Where a node before 9.7 takes part, the view a collapse carries is the source's keyword column, rendered as it is. */
    public void testKeywordViewIsTheSourceColumn() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate collapse = collapse(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Expression without = new TimeSeriesMetadataFilter(SOURCE, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), true);
        Alias rendered = new Alias(SOURCE, MetadataAttribute.TIMESERIES, new ToString(SOURCE, without, EsqlTestUtils.TEST_CFG));
        LogicalPlan plan = ScalarizeTimeSeriesMetadataPaths.pushDown(new Eval(SOURCE, collapse, List.of(rendered)), EsqlTestUtils.TEST_CFG, false);

        assertThat(expressions(plan, TimeSeriesWithout.class), hasSize(1));
        assertThat(expressions(plan, ToTimeSeriesMetadata.class), empty());
        assertThat(expressions(plan, ToString.class), empty());
        assertThat(as(plan, Eval.class).fields().getFirst().dataType(), sameInstance(DataType.KEYWORD));
    }

    /**
     * A typed view read after the collapse is narrowed from the carried one; a keyword view cannot be, so a narrower reader reads
     * the carried view, as the keyword translation did.
     */
    public void testKeywordNarrowerViewReadsTheCarriedOne() {
        Alias labels = sourceLabels();
        TimeSeriesAggregate collapse = collapse(labels, TimeSeriesAggregate.Origin.PROMQL_COMMAND);
        Source withoutPod = new Source(1, 1, "without (pod)");
        Expression without = new TimeSeriesMetadataFilter(withoutPod, labels.toAttribute(), List.of(Literal.keyword(SOURCE, "pod")), true);
        Alias narrow = new Alias(SOURCE, "narrow", new ToString(SOURCE, without, EsqlTestUtils.TEST_CFG));
        Alias whole = new Alias(SOURCE, "whole", new ToString(SOURCE, labels.toAttribute(), EsqlTestUtils.TEST_CFG));
        LogicalPlan plan = new Eval(SOURCE, collapse, List.of(narrow, whole));

        assertThat(
            expressions(ScalarizeTimeSeriesMetadataPaths.pushDown(plan, EsqlTestUtils.TEST_CFG, true), TimeSeriesMetadataFilter.class),
            hasSize(1)
        );
        Eval keyword = as(ScalarizeTimeSeriesMetadataPaths.pushDown(plan, EsqlTestUtils.TEST_CFG, false), Eval.class);
        assertThat(expressions(keyword, TimeSeriesMetadataFilter.class), empty());
        assertThat(keyword.fields().get(0).child(), equalTo(keyword.fields().get(1).child()));
    }

    private static Alias sourceLabels() {
        return new Alias(SOURCE, MetadataAttribute.TIMESERIES, new ToTimeSeriesMetadata(SOURCE, new TimeSeriesWithout(SOURCE)));
    }

    private TimeSeriesAggregate collapse(Alias labels, TimeSeriesAggregate.Origin origin) {
        Alias last = new Alias(SOURCE, "value", new Values(SOURCE, value));
        return new TimeSeriesAggregate(SOURCE, relation, List.of(labels), List.of(last, labels.toAttribute()), null, null, origin, false);
    }

    private static FieldAttribute dimension(String name) {
        return new FieldAttribute(SOURCE, name, new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION));
    }

    private static List<Expression> expressions(LogicalPlan plan, Class<? extends Expression> type) {
        return plan.collect(node -> true)
            .stream()
            .flatMap(node -> node.expressions().stream())
            .flatMap(expression -> expression.collect(type::isInstance).stream())
            .toList();
    }

    private static List<String> names(List<? extends Expression> expressions) {
        return expressions.stream().map(e -> e instanceof NamedExpression named ? named.name() : e.toString()).toList();
    }
}
