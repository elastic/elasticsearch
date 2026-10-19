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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

public class ReplaceUnambiguousAliasesWithAttributesTests extends ESTestCase {
    public void testMetadataProjectionUpdatesParentReferences() {
        for (var origin : TimeSeriesAggregate.Origin.values()) {
            var field = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
            var alias = new Alias(Source.EMPTY, randomFrom(MetadataAttribute.TIMESERIES, "_timeseries$1"), field, null, true);
            var aggregate = aggregate(List.of(field), List.of(alias), origin);
            var filter = new Filter(Source.EMPTY, aggregate, new IsNotNull(Source.EMPTY, alias.toAttribute()));
            var explicitAlias = new Alias(Source.EMPTY, "labels", alias.toAttribute());
            var project = new Project(Source.EMPTY, filter, List.of(alias.toAttribute(), explicitAlias));

            var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(project), Project.class);
            var resultFilter = as(result.child(), Filter.class);
            var resultAggregate = as(resultFilter.child(), TimeSeriesAggregate.class);
            var resultField = as(resultAggregate.groupings().getFirst(), TimeSeriesMetadataAttribute.class);
            assertSame(field, resultField);
            assertEquals(MetadataAttribute.TIMESERIES, resultField.name());
            assertEquals(field.excludedFields(), resultField.excludedFields());
            assertEquals(List.of(resultField), as(resultAggregate.child(), EsRelation.class).output());
            assertEquals(MetadataAttribute.TIMESERIES, resultAggregate.aggregates().getFirst().name());
            assertEquals(field.id(), resultAggregate.aggregates().getFirst().id());
            var filterReference = as(resultFilter.condition().children().getFirst(), Attribute.class);
            assertEquals(field.id(), filterReference.id());
            assertEquals(MetadataAttribute.TIMESERIES, filterReference.name());
            assertEquals(MetadataAttribute.TIMESERIES, result.projections().getFirst().name());
            assertEquals(field.id(), result.projections().getFirst().id());
            assertEquals("labels", result.projections().get(1).name());
            assertEquals(explicitAlias.id(), result.projections().get(1).id());
            assertSame(result, new ReplaceUnambiguousAliasesWithAttributes().apply(result));
        }
    }

    public void testUserAliasKeepsOutputName() {
        for (var origin : TimeSeriesAggregate.Origin.values()) {
            var field = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
            var alias = new Alias(Source.EMPTY, "labels", field);
            var aggregate = aggregate(List.of(field), List.of(alias), origin);
            var project = new Project(Source.EMPTY, aggregate, List.of(alias.toAttribute()));
            assertSame(project, new ReplaceUnambiguousAliasesWithAttributes().apply(project));
        }
    }

    public void testUnionRetainsBranchColumnNames() {
        for (var origin : TimeSeriesAggregate.Origin.values()) {
            var field = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
            var alias = new Alias(Source.EMPTY, "_timeseries$1", field, null, true);
            var aggregate = aggregate(List.of(field), List.of(alias), origin);
            var filter = new Filter(Source.EMPTY, aggregate, new IsNotNull(Source.EMPTY, alias.toAttribute()));
            var project = new Project(Source.EMPTY, filter, List.of(alias.toAttribute()));
            var otherField = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
            var otherAlias = new Alias(Source.EMPTY, alias.name(), otherField, null, true);
            var other = aggregate(List.of(otherField), List.of(otherAlias), origin);
            var union = new UnionAll(Source.EMPTY, List.of(project, other), List.of(alias.toAttribute().withId(new NameId())));

            // Merge columns are aligned by name and have independent ids. Renaming an input breaks that alignment.
            assertSame(union, new ReplaceUnambiguousAliasesWithAttributes().apply(union));
        }
    }

    public void testMultipleProjectionsRetainAliases() {
        var full = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var excluded = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
        var alias = proxy("_timeseries$1", excluded);
        var aggregate = aggregate(List.of(full, excluded), List.of(full, alias), randomFrom(TimeSeriesAggregate.Origin.values()));
        assertSame(aggregate, new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate));
    }

    public void testOtherMetadataInSourceRetainsAlias() {
        var full = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var excluded = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
        var alias = proxy("_timeseries$1", excluded);
        var aggregate = aggregate(List.of(full, excluded), List.of(alias), randomFrom(TimeSeriesAggregate.Origin.values()));
        assertSame(aggregate, new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate));
    }

    public void testMultipleGroupingsOfOneProjectionRetainAliases() {
        var field = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var first = proxy("first", field);
        var second = proxy("second", field);
        var aggregate = aggregate(List.of(field), List.of(first, second), randomFrom(TimeSeriesAggregate.Origin.values()));
        assertSame(aggregate, new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate));
    }

    public void testOrdinaryProjectionUpdatesReferences() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var source = EsqlTestUtils.relation().withAttributes(List.of(field));
        var project = new Project(Source.EMPTY, source, List.of(alias));
        var filter = new Filter(Source.EMPTY, project, new IsNotNull(Source.EMPTY, alias.toAttribute()));
        var output = new Project(Source.EMPTY, filter, List.of(alias.toAttribute()));

        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(output), Project.class);
        var resultFilter = as(result.child(), Filter.class);
        var resultProject = as(resultFilter.child(), Project.class);
        assertEquals(List.of(field), resultProject.projections());
        assertSame(source, resultProject.child());
        assertReference(field, as(resultFilter.condition().children().getFirst(), Attribute.class));
        assertReference(field, result.output().getFirst());
        assertSame(result, new ReplaceUnambiguousAliasesWithAttributes().apply(result));
    }

    public void testIndependentProjections() {
        var first = field("first");
        var second = field("second");
        var project = project(List.of(first, second), List.of(proxy("a", first), proxy("b", second)));
        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(project), Project.class);
        assertEquals(List.of(first, second), result.projections());
    }

    public void testSameNameUserAlias() {
        var field = field("value");
        var project = project(List.of(field), List.of(new Alias(Source.EMPTY, field.name(), field)));
        assertEquals(List.of(field), as(new ReplaceUnambiguousAliasesWithAttributes().apply(project), Project.class).projections());
    }

    public void testAliasWithSameIdentity() {
        var field = field("value");
        var project = project(List.of(field), List.of(new Alias(Source.EMPTY, field.name(), field, field.id())));
        assertEquals(List.of(field), as(new ReplaceUnambiguousAliasesWithAttributes().apply(project), Project.class).projections());
    }

    public void testGroupingOutputWithSameIdentity() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var output = new Alias(Source.EMPTY, alias.name(), alias.toAttribute(), alias.id(), true);
        var aggregate = new Aggregate(
            Source.EMPTY,
            EsqlTestUtils.relation().withAttributes(List.of(field)),
            List.of(alias),
            List.of(output)
        );
        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate), Aggregate.class);
        assertEquals(List.of(field), result.groupings());
        assertReference(field, result.output().getFirst());
        assertSame(result, new ReplaceUnambiguousAliasesWithAttributes().apply(result));
    }

    public void testOrdinaryUserAliasKeepsName() {
        var field = field("value");
        var project = project(List.of(field), List.of(new Alias(Source.EMPTY, "renamed", field)));
        assertSame(project, new ReplaceUnambiguousAliasesWithAttributes().apply(project));
    }

    public void testAggregateGrouping() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var aggregate = new Aggregate(
            Source.EMPTY,
            EsqlTestUtils.relation().withAttributes(List.of(field)),
            List.of(alias),
            List.of(alias.toAttribute())
        );
        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate), Aggregate.class);
        assertEquals(List.of(field), result.groupings());
        assertReference(field, result.output().getFirst());
        assertSame(aggregate.child(), result.child());
    }

    public void testValueGroupedAlongsideAlias() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var aggregate = new Aggregate(
            Source.EMPTY,
            EsqlTestUtils.relation().withAttributes(List.of(field)),
            List.of(field, alias),
            List.of(alias.toAttribute())
        );
        assertSame(aggregate, new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate));
    }

    public void testIdentityGroupingAlongsideAlias() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var identity = new Alias(Source.EMPTY, field.name(), field, field.id());
        var aggregate = new Aggregate(
            Source.EMPTY,
            EsqlTestUtils.relation().withAttributes(List.of(field)),
            List.of(identity, alias),
            List.of(alias.toAttribute())
        );
        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate), Aggregate.class);
        assertEquals(List.of(field, alias), result.groupings());
        assertEquals(aggregate.output(), result.output());
        assertSame(result, new ReplaceUnambiguousAliasesWithAttributes().apply(result));
    }

    public void testRepeatedGroupingAlias() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var aggregate = new Aggregate(
            Source.EMPTY,
            EsqlTestUtils.relation().withAttributes(List.of(field)),
            List.of(alias),
            List.of(alias)
        );
        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate), Aggregate.class);
        assertEquals(List.of(field), result.groupings());
        assertEquals(List.of(field), result.aggregates());
    }

    public void testProjectedComputedValue() {
        var computed = new Alias(Source.EMPTY, "computed", Literal.TRUE);
        var eval = new Eval(Source.EMPTY, EsqlTestUtils.relation(), List.of(computed));
        var project = new Project(Source.EMPTY, eval, List.of(proxy("temporary", computed.toAttribute())));
        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(project), Project.class);
        assertSame(eval, result.child());
        assertEquals(List.of(computed.toAttribute()), result.projections());
    }

    public void testEvalKeepsAliasDefinition() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var eval = new Eval(Source.EMPTY, EsqlTestUtils.relation().withAttributes(List.of(field)), List.of(alias));
        var project = new Project(Source.EMPTY, eval, List.of(alias.toAttribute()));
        assertSame(project, new ReplaceUnambiguousAliasesWithAttributes().apply(project));
    }

    public void testAliasChain() {
        var field = field("value");
        var first = proxy("first", field);
        var second = proxy("second", first.toAttribute());
        var lower = project(List.of(field), List.of(first));
        var upper = new Project(Source.EMPTY, lower, List.of(second));
        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(upper), Project.class);
        assertReference(field, result.output().getFirst());
        assertEquals(List.of(field), as(result.child(), Project.class).projections());
        assertSame(result, new ReplaceUnambiguousAliasesWithAttributes().apply(result));
    }

    public void testAliasChainKeepsUserOutputName() {
        var field = field("value");
        var first = proxy("temporary", field);
        var second = new Alias(Source.EMPTY, first.name(), first.toAttribute());
        var lower = project(List.of(field), List.of(first));
        var upper = new Project(Source.EMPTY, lower, List.of(second));
        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(upper), Project.class);
        assertEquals(List.of(field), as(result.child(), Project.class).projections());
        var resultAlias = as(result.projections().getFirst(), Alias.class);
        assertEquals(second.name(), resultAlias.name());
        assertEquals(second.id(), resultAlias.id());
        assertReference(field, as(resultAlias.child(), Attribute.class));
        assertSame(result, new ReplaceUnambiguousAliasesWithAttributes().apply(result));
    }

    public void testOrdinaryUnionKeepsBranchNames() {
        var first = field("value");
        var second = field("other");
        var alias = proxy("temporary", first);
        var otherAlias = proxy(alias.name(), second);
        var union = new UnionAll(
            Source.EMPTY,
            List.of(project(List.of(first), List.of(alias)), project(List.of(second), List.of(otherAlias))),
            List.of(alias.toAttribute().withId(new NameId()))
        );
        assertSame(union, new ReplaceUnambiguousAliasesWithAttributes().apply(union));
    }

    public void testMultipleAliasesOfOneValue() {
        var field = field("value");
        var project = project(List.of(field), List.of(proxy("first", field), proxy("second", field)));
        assertSame(project, new ReplaceUnambiguousAliasesWithAttributes().apply(project));
    }

    public void testValueProjectedAlongsideAlias() {
        var field = field("value");
        var project = project(List.of(field), List.of(field, proxy("temporary", field)));
        assertSame(project, new ReplaceUnambiguousAliasesWithAttributes().apply(project));
    }

    public void testDuplicateInputNames() {
        var first = field("value");
        var second = field("value");
        var project = project(List.of(first, second), List.of(proxy("temporary", second)));
        assertSame(project, new ReplaceUnambiguousAliasesWithAttributes().apply(project));
    }

    public void testAncestorNameCollision() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var lower = project(List.of(field), List.of(alias));
        var upper = new Eval(Source.EMPTY, lower, List.of(new Alias(Source.EMPTY, field.name(), Literal.TRUE)));
        assertSame(upper, new ReplaceUnambiguousAliasesWithAttributes().apply(upper));
    }

    public void testShadowedAliasStaysHidden() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var lower = project(List.of(field), List.of(alias));
        var upper = new Eval(Source.EMPTY, lower, List.of(new Alias(Source.EMPTY, alias.name(), Literal.TRUE)));
        assertSame(upper, new ReplaceUnambiguousAliasesWithAttributes().apply(upper));
    }

    public void testRepeatedDefinitionKeepsUserOutputName() {
        var field = field("value");
        var grouping = proxy("temporary", field);
        var output = new Alias(Source.EMPTY, grouping.name(), field, grouping.id());
        var aggregate = new Aggregate(
            Source.EMPTY,
            EsqlTestUtils.relation().withAttributes(List.of(field)),
            List.of(grouping),
            List.of(output)
        );
        assertSame(aggregate, new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate));
    }

    public void testSharedAliasWithDifferentValues() {
        var first = field("value");
        var second = field("value");
        var alias = new Alias(Source.EMPTY, first.name(), first);
        var otherAlias = new Alias(Source.EMPTY, second.name(), second, alias.id());
        var union = new UnionAll(
            Source.EMPTY,
            List.of(project(List.of(first), List.of(alias)), project(List.of(second), List.of(otherAlias))),
            List.of(alias.toAttribute().withId(new NameId()))
        );
        assertSame(union, new ReplaceUnambiguousAliasesWithAttributes().apply(union));
    }

    public void testGroupingOutputKeepsUserName() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var output = new Alias(Source.EMPTY, alias.name(), alias.toAttribute(), alias.id());
        var aggregate = new Aggregate(
            Source.EMPTY,
            EsqlTestUtils.relation().withAttributes(List.of(field)),
            List.of(alias),
            List.of(output)
        );
        assertSame(aggregate, new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate));
    }

    public void testAliasDoesNotRewriteAnUnrelatedSource() {
        var field = field("value");
        var alias = new Alias(Source.EMPTY, field.name(), field);
        var otherField = field("other").withName(field.name()).withId(alias.id());
        var other = EsqlTestUtils.relation().withAttributes(List.of(otherField));
        var union = new UnionAll(
            Source.EMPTY,
            List.of(project(List.of(field), List.of(alias)), other),
            List.of(alias.toAttribute().withId(new NameId()))
        );

        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(union), UnionAll.class);
        assertSame(other, result.children().get(1));
        assertEquals(union.output(), result.output());
    }

    public void testSubstitutionStopsWhenAliasIsNotOutput() {
        var field = field("value");
        var other = field("other");
        var alias = proxy("temporary", field);
        var lower = project(List.of(field, other), List.of(alias, other));
        var upper = new Project(Source.EMPTY, lower, List.of(other));
        // This is a new binding. Reusing the id outside the old binding's scope must not connect the two.
        var unrelated = new Alias(Source.EMPTY, "unrelated", Literal.TRUE, alias.id());
        var eval = new Eval(Source.EMPTY, upper, List.of(unrelated));

        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(eval), Eval.class);
        assertSame(unrelated, result.fields().getFirst());
        assertEquals(eval.output(), result.output());
        assertEquals(List.of(field, other), as(as(result.child(), Project.class).child(), Project.class).projections());
        assertSame(result, new ReplaceUnambiguousAliasesWithAttributes().apply(result));
    }

    public void testUnknownOperatorRetainsInputBinding() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var project = project(List.of(field), List.of(alias));
        // UNPACK introduces output bindings. Being unary does not make an operator safe for generic substitution.
        var unpack = new UnpackDims(Source.EMPTY, project, alias.toAttribute(), List.of(field("dimension")));
        assertSame(unpack, new ReplaceUnambiguousAliasesWithAttributes().apply(unpack));
    }

    public void testUnknownOperatorDoesNotBlockAnInternalSubstitution() {
        var field = field("value");
        var other = field("other");
        var alias = proxy("temporary", field);
        var lower = project(List.of(field, other), List.of(alias, other));
        var upper = new Project(Source.EMPTY, lower, List.of(other));
        var unpack = new UnpackDims(Source.EMPTY, upper, other, List.of(field("dimension")));

        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(unpack), UnpackDims.class);
        assertEquals(unpack.output(), result.output());
        assertSame(unpack.packed(), result.packed());
        assertEquals(List.of(field, other), as(as(result.child(), Project.class).child(), Project.class).projections());
    }

    public void testSubstitutionPreservesReferenceNullability() {
        var field = field("value").withNullability(Nullability.FALSE);
        var alias = proxy("temporary", field);
        var reference = alias.toAttribute().withNullability(Nullability.TRUE);
        var project = project(List.of(field), List.of(alias));
        var filter = new Filter(Source.EMPTY, project, new IsNotNull(Source.EMPTY, reference));
        var output = new Project(Source.EMPTY, filter, List.of(reference));

        var result = as(new ReplaceUnambiguousAliasesWithAttributes().apply(output), Project.class);
        assertReference(field, result.output().getFirst());
        assertEquals(Nullability.TRUE, result.output().getFirst().nullable());
        var resultFilter = as(result.child(), Filter.class);
        assertEquals(Nullability.TRUE, resultFilter.condition().children().getFirst().nullable());
        assertEquals(Nullability.FALSE, as(resultFilter.child(), Project.class).output().getFirst().nullable());
    }

    public void testPublicReferenceKeepsItsName() {
        var field = field("value");
        var alias = proxy("temporary", field);
        var lower = project(List.of(field), List.of(alias));
        var reference = new ReferenceAttribute(Source.EMPTY, null, "visible", field.dataType(), field.nullable(), alias.id(), false);
        var output = new Project(Source.EMPTY, lower, List.of(reference));
        assertSame(output, new ReplaceUnambiguousAliasesWithAttributes().apply(output));
    }

    public void testGroupingDoesNotCaptureAnAggregateBinding() {
        var field = field("value");
        var grouping = proxy("temporary", field);
        var count = new Alias(Source.EMPTY, "count", new Count(Source.EMPTY, Literal.TRUE), field.id());
        var aggregate = new Aggregate(
            Source.EMPTY,
            EsqlTestUtils.relation().withAttributes(List.of(field)),
            List.of(grouping),
            List.of(count)
        );
        // The grouping is not output, so checking output collisions alone cannot detect this capture.
        assertSame(aggregate, new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate));
    }

    public void testGroupingDoesNotCollideWithComputedGroupingName() {
        for (boolean projected : List.of(false, true)) {
            var field = field("value");
            var other = field("other");
            var alias = proxy("temporary", field);
            LogicalPlan child = EsqlTestUtils.relation().withAttributes(List.of(field, other));
            if (projected) {
                child = new Project(Source.EMPTY, child, List.of(alias, other));
            }
            var computed = new Alias(Source.EMPTY, field.name(), new IsNotNull(Source.EMPTY, other));
            var count = new Alias(Source.EMPTY, "count", new Count(Source.EMPTY, Literal.TRUE));
            var aggregate = new Aggregate(Source.EMPTY, child, List.of(projected ? alias.toAttribute() : alias, computed), List.of(count));
            // Neither grouping is output; their names must still remain distinct.
            assertSame(aggregate, new ReplaceUnambiguousAliasesWithAttributes().apply(aggregate));
        }
    }

    private static FieldAttribute field(String name) {
        return EsqlTestUtils.fieldAttribute(name, DataType.KEYWORD);
    }

    private static Alias proxy(String name, Attribute value) {
        return new Alias(Source.EMPTY, name, value, null, true);
    }

    private static Project project(List<Attribute> fields, List<NamedExpression> projections) {
        return new Project(Source.EMPTY, EsqlTestUtils.relation().withAttributes(fields), projections);
    }

    private static void assertReference(Attribute expected, Attribute actual) {
        assertEquals(expected.getClass(), actual.getClass());
        assertEquals(expected.id(), actual.id());
        assertEquals(expected.name(), actual.name());
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
