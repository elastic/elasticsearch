/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

public class RemoveRedundantTimeSeriesAliasesTests extends ESTestCase {
    public void testSingleProjectionKeepsIdentityAndUpdatesParentReferences() {
        for (var origin : TimeSeriesAggregate.Origin.values()) {
            var field = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
            var alias = new Alias(Source.EMPTY, randomFrom("WITHOUT(cluster)", "_timeseries$1"), field);
            var aggregate = aggregate(List.of(field), List.of(alias), origin);
            var filter = new Filter(Source.EMPTY, aggregate, new IsNotNull(Source.EMPTY, alias.toAttribute()));
            var explicitAlias = new Alias(Source.EMPTY, "labels", alias.toAttribute());
            var project = new Project(Source.EMPTY, filter, List.of(alias.toAttribute(), explicitAlias));

            var result = as(new RemoveRedundantTimeSeriesAliases().apply(project), Project.class);
            var resultFilter = as(result.child(), Filter.class);
            var resultAggregate = as(resultFilter.child(), TimeSeriesAggregate.class);
            var resultField = as(resultAggregate.groupings().getFirst(), TimeSeriesMetadataAttribute.class);
            assertEquals(alias.id(), resultField.id());
            assertEquals(MetadataAttribute.TIMESERIES, resultField.name());
            assertEquals(field.excludedFields(), resultField.excludedFields());
            assertEquals(List.of(resultField), as(resultAggregate.child(), EsRelation.class).output());
            assertEquals(MetadataAttribute.TIMESERIES, resultAggregate.aggregates().getFirst().name());
            assertEquals(alias.id(), resultAggregate.aggregates().getFirst().id());
            var filterReference = as(resultFilter.condition().children().getFirst(), Attribute.class);
            assertEquals(alias.id(), filterReference.id());
            assertEquals(MetadataAttribute.TIMESERIES, filterReference.name());
            assertEquals(MetadataAttribute.TIMESERIES, result.projections().getFirst().name());
            assertEquals(alias.id(), result.projections().getFirst().id());
            assertEquals("labels", result.projections().get(1).name());
            assertEquals(explicitAlias.id(), result.projections().get(1).id());
            assertSame(result, new RemoveRedundantTimeSeriesAliases().apply(result));
        }
    }

    public void testMultipleProjectionsRetainAliases() {
        var full = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var excluded = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
        var alias = new Alias(Source.EMPTY, "_timeseries$1", excluded);
        var aggregate = aggregate(List.of(full, excluded), List.of(full, alias), randomFrom(TimeSeriesAggregate.Origin.values()));
        assertSame(aggregate, new RemoveRedundantTimeSeriesAliases().apply(aggregate));
    }

    public void testOtherMetadataInSourceRetainsAlias() {
        var full = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var excluded = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
        var alias = new Alias(Source.EMPTY, "_timeseries$1", excluded);
        var aggregate = aggregate(List.of(full, excluded), List.of(alias), randomFrom(TimeSeriesAggregate.Origin.values()));
        assertSame(aggregate, new RemoveRedundantTimeSeriesAliases().apply(aggregate));
    }

    public void testMultipleGroupingsOfOneProjectionRetainAliases() {
        var field = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var first = new Alias(Source.EMPTY, "first", field);
        var second = new Alias(Source.EMPTY, "second", field);
        var aggregate = aggregate(List.of(field), List.of(first, second), randomFrom(TimeSeriesAggregate.Origin.values()));
        assertSame(aggregate, new RemoveRedundantTimeSeriesAliases().apply(aggregate));
    }

    private static TimeSeriesAggregate aggregate(List<Attribute> fields, List<Expression> groupings, TimeSeriesAggregate.Origin origin) {
        return new TimeSeriesAggregate(
            Source.EMPTY,
            EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(fields),
            groupings,
            groupings.stream().map(g -> (NamedExpression) ((NamedExpression) g).toAttribute()).toList(),
            null,
            null,
            origin
        );
    }
}
