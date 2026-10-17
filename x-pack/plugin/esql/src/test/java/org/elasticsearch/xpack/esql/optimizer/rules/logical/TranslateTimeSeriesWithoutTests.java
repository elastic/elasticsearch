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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.sameInstance;

public class TranslateTimeSeriesWithoutTests extends AbstractLogicalPlanOptimizerTests {

    public TranslateTimeSeriesWithoutTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testLoweringPreservesGroupingAliasAndOutputReferences() {
        assertLowering(TimeSeriesAggregate.Origin.PROMQL_COMMAND, false);
    }

    public void testLoweringPreservesDistinctMetadataProjectionNames() {
        assertLowering(TimeSeriesAggregate.Origin.PROMQL_COMMAND, true);
    }

    public void testLoweringPreservesNativeGroupingAlias() {
        assertLowering(TimeSeriesAggregate.Origin.TS_COMMAND, false);
    }

    private void assertLowering(TimeSeriesAggregate.Origin origin, boolean includeFullIdentity) {
        FieldAttribute cluster = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "cluster",
            new EsField("cluster", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION)
        );
        var fullIdentity = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES)
            .withAttributes(includeFullIdentity ? List.of(cluster, fullIdentity) : List.of(cluster));
        var fn = new TimeSeriesWithout(Source.EMPTY, new ArrayList<>(List.<Expression>of(cluster)));
        boolean promql = origin == TimeSeriesAggregate.Origin.PROMQL_COMMAND;
        String groupingName = promql ? MetadataAttribute.TIMESERIES + "$1" : "WITHOUT(cluster)";
        var timeSeriesGrouping = (NamedExpression) new Alias(fn.source(), groupingName, fn, null, randomBoolean());
        TimeSeriesAggregate aggregate = new TimeSeriesAggregate(
            Source.EMPTY,
            relation,
            includeFullIdentity ? List.of(timeSeriesGrouping, fullIdentity) : List.of(timeSeriesGrouping),
            List.of(timeSeriesGrouping.toAttribute()),
            null,
            null,
            origin
        );

        TimeSeriesAggregate lowered = as(
            new TranslateTimeSeriesWithout().apply(aggregate, metricsAnalyzer().buildContext()),
            TimeSeriesAggregate.class
        );

        Alias loweredAlias = as(lowered.groupings().getFirst(), Alias.class);
        assertThat(loweredAlias.name(), equalTo(groupingName));
        assertThat(loweredAlias.id(), equalTo(timeSeriesGrouping.id()));
        assertEquals(timeSeriesGrouping.synthetic(), loweredAlias.synthetic());
        TimeSeriesMetadataAttribute loweredGrouping = as(loweredAlias.child(), TimeSeriesMetadataAttribute.class);
        assertNotEquals(loweredGrouping.id(), loweredAlias.id());
        assertThat(lowered.aggregates().getFirst().name(), equalTo(groupingName));
        assertThat(lowered.aggregates().getFirst().id(), equalTo(timeSeriesGrouping.id()));
        assertThat(lowered.aggregates().getFirst(), sameInstance(aggregate.aggregates().getFirst()));
        assertThat(loweredGrouping.name(), equalTo(MetadataAttribute.TIMESERIES));
        assertThat(loweredGrouping.excludedFields(), equalTo(Set.of("cluster")));

        EsRelation loweredRelation = as(lowered.child(), EsRelation.class);
        assertThat(loweredRelation.output(), hasItem(loweredGrouping));
        assertSame(lowered, new TranslateTimeSeriesWithout().apply(lowered, metricsAnalyzer().buildContext()));
    }
}
