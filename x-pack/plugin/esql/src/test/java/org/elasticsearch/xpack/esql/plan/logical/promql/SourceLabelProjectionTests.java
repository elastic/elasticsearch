/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonMerge;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonRemove;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Verifies that the loader fallback preserves grouping grain and never reconstructs a modified label record. */
public class SourceLabelProjectionTests extends ESTestCase {
    public void testRepeatedExclusionDoesNotAddAGroupingKey() {
        var stored = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("__name__"));
        Alias key = new Alias(Source.EMPTY, "record", stored);
        var aggregate = new Aggregate(Source.EMPTY, relation(stored), List.of(key), List.of(key.toAttribute()));
        Alias renamed = new Alias(Source.EMPTY, "renamed", key.toAttribute());
        var plan = new Eval(Source.EMPTY, aggregate, List.of(renamed));
        var projected = SourceLabelProjection.excluding(plan, renamed.toAttribute(), Set.of("__name__"));
        assertSame(plan, projected.plan());
        assertEquals(renamed.toAttribute(), projected.attribute());
    }

    public void testPreservesOriginalGroupingKey() {
        var stored = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("already_removed"));
        Alias key = new Alias(Source.EMPTY, "record", stored);
        var aggregate = new Aggregate(Source.EMPTY, relation(stored), List.of(key), List.of(key.toAttribute()));
        var projected = SourceLabelProjection.excluding(aggregate, key.toAttribute(), Set.of("cpu"));
        assertNotNull(projected);
        var result = (Aggregate) projected.plan();
        assertEquals(aggregate.groupings().getFirst(), result.groupings().getFirst());
        assertEquals(aggregate.aggregates().getFirst(), result.aggregates().getFirst());
        assertEquals(2, result.groupings().size());
        assertTrue(result.outputSet().contains(projected.attribute()));
        assertEquals(Set.of("already_removed", "cpu"), projectedSource(result).excludedFields());
        assertEquals(1, aggregate.child().output().size());
    }

    public void testFollowsAliasesAndProjects() {
        var stored = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        Alias renamed = new Alias(Source.EMPTY, "renamed", stored);
        var eval = new Eval(Source.EMPTY, relation(stored), List.of(renamed));
        var plan = new Project(Source.EMPTY, eval, List.of(renamed.toAttribute()));
        var projected = SourceLabelProjection.excluding(plan, renamed.toAttribute(), Set.of("cpu"));
        assertNotNull(projected);
        assertEquals(Set.of("cpu"), projectedSource(projected.plan()).excludedFields());
        assertTrue(projected.plan().outputSet().contains(renamed.toAttribute()));
        assertTrue(projected.plan().outputSet().contains(projected.attribute()));
    }

    public void testDoesNotReadSourceAfterRecordEdit() {
        var stored = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        for (var edit : List.of(
            new JsonRemove(Source.EMPTY, stored, List.of("cpu")),
            new JsonMerge(Source.EMPTY, stored, Literal.keyword(Source.EMPTY, "{\"cpu\":\"changed\"}"))
        )) {
            Alias changed = new Alias(Source.EMPTY, "record", edit);
            var eval = new Eval(Source.EMPTY, relation(stored), List.of(changed));
            assertNull(SourceLabelProjection.excluding(eval, changed.toAttribute(), Set.of("zone")));
            var aggregate = new Aggregate(Source.EMPTY, eval, List.of(changed.toAttribute()), List.of(changed.toAttribute()));
            assertNull(SourceLabelProjection.excluding(aggregate, changed.toAttribute(), Set.of("zone")));
        }
    }

    public void testExtendsMatchingPackAndUnpackTogether() {
        var stored = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var pack = new PackDims(Source.EMPTY, relation(stored), List.of(stored), PackDims.newPackedAttribute(Source.EMPTY));
        Alias key = PackDims.newPackedGrouping(Source.EMPTY, pack.packed());
        var aggregate = new Aggregate(Source.EMPTY, pack, List.of(key), List.of(key.toAttribute()));
        var unpack = new UnpackDims(Source.EMPTY, aggregate, key.toAttribute(), List.of(stored));
        var projected = SourceLabelProjection.excluding(unpack, stored, Set.of("cpu"));
        assertNotNull(projected);
        var result = (UnpackDims) projected.plan();
        var resultingPack = result.collect(PackDims.class).getFirst();
        assertEquals(List.of(stored, projected.attribute()), resultingPack.dims());
        assertEquals(resultingPack.dims(), result.dims());
        assertEquals(aggregate.groupings(), result.collect(Aggregate.class).getFirst().groupings());
        assertEquals(Set.of("cpu"), projectedSource(result).excludedFields());
    }

    public void testDoesNotRecoverARecordDroppedByAnAggregate() {
        var stored = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
        var aggregate = new Aggregate(Source.EMPTY, relation(stored), List.of(), List.of());
        assertNull(SourceLabelProjection.excluding(aggregate, stored, Set.of("cpu")));
    }

    private static EsRelation relation(Attribute record) {
        return new EsRelation(Source.EMPTY, "metrics", IndexMode.TIME_SERIES, Map.of(), Map.of(), Map.of(), List.of(record));
    }

    private static TimeSeriesMetadataAttribute projectedSource(LogicalPlan plan) {
        return (TimeSeriesMetadataAttribute) plan.collect(EsRelation.class).getFirst().output().getLast();
    }
}
