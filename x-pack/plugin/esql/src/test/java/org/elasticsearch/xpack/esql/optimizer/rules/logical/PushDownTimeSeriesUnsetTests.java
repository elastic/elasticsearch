/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.sameInstance;

public class PushDownTimeSeriesUnsetTests extends AbstractLogicalPlanOptimizerTests {

    public PushDownTimeSeriesUnsetTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testMovesIntoTheAggregateGroupingByTheTimeSeries() {
        TimeSeriesAggregate aggregate = perSeriesAggregate();
        Attribute timeseries = timeseries(aggregate);
        Alias unset = unset(timeseries);

        Eval eval = as(new PushDownTimeSeriesUnset().apply(new Eval(Source.EMPTY, aggregate, List.of(unset))), Eval.class);

        TimeSeriesAggregate moved = as(eval.child(), TimeSeriesAggregate.class);
        Alias grouping = as(moved.groupings().getLast(), Alias.class);
        TimeSeriesUnset computed = as(grouping.child(), TimeSeriesUnset.class);
        assertThat(computed.timeseries(), sameInstance(timeseries(aggregate)));
        assertThat(computed.dimensionNames(), equalTo(List.of("pod")));
        assertThat(Expressions.names(moved.aggregates()), hasItem(grouping.name()));
        // the eval keeps the name and id the plan above reads, now aliasing the grouping
        Alias field = eval.fields().getFirst();
        assertThat(field.id(), equalTo(unset.id()));
        assertThat(field.name(), equalTo(MetadataAttribute.TIMESERIES));
        assertThat(as(field.child(), Attribute.class).id(), equalTo(grouping.id()));
    }

    public void testMovesThroughNodesCarryingTheTimeSeries() {
        TimeSeriesAggregate aggregate = perSeriesAggregate();
        Attribute timeseries = timeseries(aggregate);
        Attribute value = Expressions.attribute(aggregate.aggregates().getFirst());
        LogicalPlan carrying = new Filter(Source.EMPTY, aggregate, Literal.TRUE);
        carrying = new TopNBy(
            Source.EMPTY,
            carrying,
            List.of(new Order(Source.EMPTY, value, Order.OrderDirection.DESC, Order.NullsPosition.LAST)),
            new Literal(Source.EMPTY, 1, DataType.INTEGER),
            List.of()
        );
        carrying = new Project(Source.EMPTY, carrying, List.of(value, timeseries));

        Eval eval = as(new PushDownTimeSeriesUnset().apply(new Eval(Source.EMPTY, carrying, List.of(unset(timeseries)))), Eval.class);

        Project project = as(eval.child(), Project.class);
        Attribute grouping = as(eval.fields().getFirst().child(), Attribute.class);
        assertThat(project.output(), hasItem(grouping));
        TimeSeriesAggregate moved = as(as(as(project.child(), TopNBy.class).child(), Filter.class).child(), TimeSeriesAggregate.class);
        assertThat(Expressions.attribute(moved.groupings().getLast()).id(), equalTo(grouping.id()));
    }

    public void testStaysWhereNoPerSeriesAggregateGroupsByIt() {
        ReferenceAttribute timeseries = new ReferenceAttribute(Source.EMPTY, MetadataAttribute.TIMESERIES, DataType.KEYWORD);
        LogicalPlan regroup = new Aggregate(
            Source.EMPTY,
            EsqlTestUtils.relation(IndexMode.TIME_SERIES),
            List.of(timeseries),
            List.<NamedExpression>of(timeseries)
        );
        Eval eval = new Eval(Source.EMPTY, regroup, List.of(unset(timeseries)));

        assertThat(new PushDownTimeSeriesUnset().apply(eval), sameInstance(eval));
    }

    /** A per-series aggregate grouping by its series' whole {@code _timeseries}, lowered as the analyzer leaves it. */
    private TimeSeriesAggregate perSeriesAggregate() {
        FieldAttribute pod = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "pod",
            new EsField("pod", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION)
        );
        FieldAttribute bytes = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "bytes",
            new EsField("bytes", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC)
        );
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(pod, bytes));
        var timeseries = new Alias(Source.EMPTY, MetadataAttribute.TIMESERIES, new TimeSeriesWithout(Source.EMPTY, List.<Expression>of()));
        Alias value = new Alias(Source.EMPTY, "value", bytes);
        TimeSeriesAggregate aggregate = new TimeSeriesAggregate(
            Source.EMPTY,
            relation,
            List.of(timeseries),
            List.of(value, timeseries.toAttribute()),
            null,
            null,
            TimeSeriesAggregate.Origin.PROMQL_COMMAND
        );
        return as(new TranslateTimeSeriesWithout().apply(aggregate, metricsAnalyzer().buildContext()), TimeSeriesAggregate.class);
    }

    private static Attribute timeseries(TimeSeriesAggregate aggregate) {
        return Expressions.attribute(aggregate.groupings().getFirst());
    }

    private static Alias unset(Attribute timeseries) {
        TimeSeriesUnset unset = new TimeSeriesUnset(Source.EMPTY, timeseries, List.of(Literal.keyword(Source.EMPTY, "pod")));
        return new Alias(Source.EMPTY, MetadataAttribute.TIMESERIES, unset);
    }
}
