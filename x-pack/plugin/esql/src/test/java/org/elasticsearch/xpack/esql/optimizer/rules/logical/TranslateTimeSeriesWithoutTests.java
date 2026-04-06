/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;

public class TranslateTimeSeriesWithoutTests extends AbstractLogicalPlanOptimizerTests {

    // public void testLoweringReplacesGroupingAndOutputReferences() {
    // FieldAttribute cluster = new FieldAttribute(
    // Source.EMPTY,
    // null,
    // null,
    // "cluster",
    // new EsField("cluster", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION)
    // );
    // EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(cluster));
    // Alias timeSeriesGrouping = new TimeSeriesWithout(Source.EMPTY, List.of(cluster)).toAttribute();
    // TimeSeriesAggregate aggregate = new TimeSeriesAggregate(
    // Source.EMPTY,
    // relation,
    // List.of(timeSeriesGrouping),
    // List.of(timeSeriesGrouping.toAttribute()),
    // null,
    // null
    // );
    //
    // TimeSeriesAggregate lowered = as(
    // new TranslateTimeSeriesWithout().apply(aggregate, unboundLogicalOptimizerContext()),
    // TimeSeriesAggregate.class
    // );
    //
    // assertThat(lowered.groupings().getFirst(), instanceOf(TimeSeriesMetadataAttribute.class));
    // TimeSeriesMetadataAttribute loweredGrouping = (TimeSeriesMetadataAttribute) lowered.groupings().getFirst();
    // assertThat(loweredGrouping.name(), equalTo(MetadataAttribute.TIMESERIES));
    // assertThat(loweredGrouping.id(), equalTo(timeSeriesGrouping.id()));
    // assertThat(loweredGrouping.withoutFields(), equalTo(Set.of("cluster")));
    //
    // assertThat(lowered.aggregates().getFirst(), sameInstance(loweredGrouping));
    //
    // EsRelation loweredRelation = as(lowered.child(), EsRelation.class);
    // assertThat(loweredRelation.output(), hasItem(loweredGrouping));
    // }
}
