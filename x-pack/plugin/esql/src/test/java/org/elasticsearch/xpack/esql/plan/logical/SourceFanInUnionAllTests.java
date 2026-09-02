/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SourceFanInUnionAllTests extends ESTestCase {

    public void testReconstructionPreservesType() {
        LogicalPlan first = relation("first");
        LogicalPlan second = relation("second");
        Attribute output = new ReferenceAttribute(Source.EMPTY, "value", DataType.INTEGER);
        SourceFanInUnionAll union = new SourceFanInUnionAll(Source.EMPTY, List.of(first, second), List.of(output));

        assertThat(union.replaceChildren(List.of(second, first)), instanceOf(SourceFanInUnionAll.class));
        assertThat(union.replaceSubPlans(List.of(second, first)), instanceOf(SourceFanInUnionAll.class));
        assertThat(union.replaceSubPlansAndOutput(List.of(second, first), List.of(output)), instanceOf(SourceFanInUnionAll.class));
        assertThat(union.refreshOutput(), instanceOf(SourceFanInUnionAll.class));
        assertThat(union.pruneEmptyBranches(plan -> plan == second), instanceOf(SourceFanInUnionAll.class));
    }

    public void testNotEqualToPlainUnionAll() {
        LogicalPlan first = relation("first");
        LogicalPlan second = relation("second");
        UnionAll relational = new UnionAll(Source.EMPTY, List.of(first, second), List.of());
        SourceFanInUnionAll sourceFanIn = new SourceFanInUnionAll(Source.EMPTY, List.of(first, second), List.of());

        assertNotEquals(relational, sourceFanIn);
        assertNotEquals(sourceFanIn, relational);
        assertNotEquals(relational.hashCode(), sourceFanIn.hashCode());
    }

    public void testFlattensNestedSourceFanInChildren() {
        LogicalPlan first = relation("first");
        LogicalPlan second = relation("second");
        LogicalPlan third = relation("third");
        SourceFanInUnionAll inner = new SourceFanInUnionAll(Source.EMPTY, List.of(first, second), List.of());
        SourceFanInUnionAll outer = new SourceFanInUnionAll(Source.EMPTY, List.of(inner, third), List.of());

        assertThat(outer.children(), equalTo(List.of(first, second, third)));
        assertThat(outer.replaceChildren(List.of(inner, third)).children(), equalTo(List.of(first, second, third)));
    }

    public void testExecutesOnAny() {
        SourceFanInUnionAll sourceFanIn = new SourceFanInUnionAll(Source.EMPTY, List.of(relation("first"), relation("second")), List.of());
        assertEquals(ExecuteLocation.ANY, sourceFanIn.executesOn());
        assertEquals(ExecuteLocation.COORDINATOR, new UnionAll(Source.EMPTY, List.of(relation("first")), List.of()).executesOn());
    }

    private static UnresolvedRelation relation(String name) {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, name), false, List.of(), IndexMode.STANDARD, null);
    }
}
