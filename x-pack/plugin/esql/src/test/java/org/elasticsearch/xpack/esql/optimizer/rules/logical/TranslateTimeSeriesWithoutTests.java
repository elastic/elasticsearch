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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class TranslateTimeSeriesWithoutTests extends AbstractLogicalPlanOptimizerTests {

    public void testLoweringReplacesGroupingAndOutputReferences() {
        FieldAttribute cluster = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "cluster",
            new EsField("cluster", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION)
        );
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(cluster));
        Alias timeSeriesGrouping = new TimeSeriesWithout(Source.EMPTY, List.of(cluster)).toAttribute();
        TimeSeriesAggregate aggregate = new TimeSeriesAggregate(
            Source.EMPTY,
            relation,
            List.of(timeSeriesGrouping),
            List.of(timeSeriesGrouping.toAttribute()),
            null,
            null
        );

        TimeSeriesAggregate lowered = as(
            new TranslateTimeSeriesWithout().apply(aggregate, unboundLogicalOptimizerContext()),
            TimeSeriesAggregate.class
        );

        assertThat(lowered.groupings().getFirst(), instanceOf(TimeSeriesMetadataAttribute.class));
        TimeSeriesMetadataAttribute loweredGrouping = (TimeSeriesMetadataAttribute) lowered.groupings().getFirst();
        assertThat(loweredGrouping.name(), equalTo(MetadataAttribute.TIMESERIES));
        assertThat(loweredGrouping.id(), equalTo(timeSeriesGrouping.id()));
        assertThat(loweredGrouping.withoutFields(), equalTo(Set.of("cluster")));

        assertThat(lowered.aggregates().getFirst(), sameInstance(loweredGrouping));

        EsRelation loweredRelation = as(lowered.child(), EsRelation.class);
        assertThat(loweredRelation.output(), hasItem(loweredGrouping));
    }
}
