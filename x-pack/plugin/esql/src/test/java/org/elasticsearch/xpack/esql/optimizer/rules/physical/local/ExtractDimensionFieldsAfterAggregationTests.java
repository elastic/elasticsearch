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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.ReadDimsExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ExtractDimensionFieldsAfterAggregationTests extends ESTestCase {

    private final Attribute doc = new FieldAttribute(Source.EMPTY, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
    private final Attribute tsid = new MetadataAttribute(Source.EMPTY, MetadataAttribute.TSID_FIELD, DataType.TSID_DATA_TYPE, false);
    private final Attribute timeseries = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
    private final EsQueryExec query = new EsQueryExec(
        Source.EMPTY,
        "test",
        IndexMode.TIME_SERIES,
        List.of(doc, tsid, timeseries),
        null,
        List.of(),
        null,
        List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
    );

    public void testUnsetsOncePerSeries() {
        Alias unset = unset();
        Alias values = new Alias(Source.EMPTY, MetadataAttribute.TIMESERIES, new Values(Source.EMPTY, unset.toAttribute()));
        TimeSeriesAggregateExec aggregate = aggregate(new EvalExec(Source.EMPTY, query, List.of(unset)), values);

        ProjectExec project = as(rule(aggregate), ProjectExec.class);
        assertThat(project.projections(), equalTo(aggregate.intermediateAttributes()));
        EvalExec after = as(project.child(), EvalExec.class);
        Alias moved = after.fields().getFirst();
        assertThat(moved.id(), equalTo(aggregate.intermediateAttributes().get(1).id()));
        TimeSeriesUnset applied = as(moved.child(), TimeSeriesUnset.class);
        assertThat(applied.dimensionNames(), contains("pod"));
        ReadDimsExec read = as(after.child(), ReadDimsExec.class);
        assertThat(read.dims(), contains(applied.timeseries()));
        // the eval computing the unset for every document is gone
        TimeSeriesAggregateExec moveFrom = as(read.child(), TimeSeriesAggregateExec.class);
        assertThat(moveFrom.child(), sameInstance(query));
    }

    public void testKeepsAnUnsetReadBelowTheAggregate() {
        Alias unset = unset();
        Alias reader = new Alias(Source.EMPTY, "reader", unset.toAttribute());
        Alias values = new Alias(Source.EMPTY, MetadataAttribute.TIMESERIES, new Values(Source.EMPTY, unset.toAttribute()));
        Alias readerValues = new Alias(Source.EMPTY, "reader", new Values(Source.EMPTY, reader.toAttribute()));
        TimeSeriesAggregateExec aggregate = aggregate(new EvalExec(Source.EMPTY, query, List.of(unset, reader)), values, readerValues);

        assertThat(rule(aggregate), sameInstance(aggregate));
    }

    private Alias unset() {
        var unset = new TimeSeriesUnset(Source.EMPTY, timeseries, List.of(Literal.keyword(Source.EMPTY, "pod")));
        return new Alias(Source.EMPTY, "$$TimeSeriesUnset$1", unset);
    }

    private TimeSeriesAggregateExec aggregate(PhysicalPlan child, Alias... aggregates) {
        List<NamedExpression> outputs = new ArrayList<>(List.of(aggregates));
        outputs.add(tsid);
        return new TimeSeriesAggregateExec(
            Source.EMPTY,
            child,
            List.of(tsid),
            outputs,
            AggregatorMode.INITIAL,
            AbstractPhysicalOperationProviders.intermediateAttributes(outputs, List.of(tsid)),
            null,
            null
        );
    }

    private static PhysicalPlan rule(TimeSeriesAggregateExec aggregate) {
        var context = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            EsqlTestUtils.TEST_CFG,
            FoldContext.small(),
            SearchStats.EMPTY
        );
        return new ExtractDimensionFieldsAfterAggregation().rule(aggregate, context);
    }
}
