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

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

public class ViewUnionAllTests extends ESTestCase {

    public void testIsInstanceOfUnionAll() {
        ViewUnionAll viewUnion = new ViewUnionAll(Source.EMPTY, List.of(), List.of());
        assertThat(viewUnion, instanceOf(UnionAll.class));
        assertThat(viewUnion, instanceOf(Fork.class));
    }

    public void testReplaceChildrenPreservesType() {
        LogicalPlan child1 = relation("index1");
        LogicalPlan child2 = relation("index2");
        ViewUnionAll original = new ViewUnionAll(Source.EMPTY, List.of(child1), List.of());

        LogicalPlan replaced = original.replaceChildren(List.of(child2));
        assertThat(replaced, instanceOf(ViewUnionAll.class));
        assertEquals(List.of(child2), replaced.children());
    }

    public void testReplaceSubPlansPreservesType() {
        LogicalPlan child1 = relation("index1");
        LogicalPlan child2 = relation("index2");
        ViewUnionAll original = new ViewUnionAll(Source.EMPTY, List.of(child1), List.of());

        UnionAll replaced = original.replaceSubPlans(List.of(child2));
        assertThat(replaced, instanceOf(ViewUnionAll.class));
        assertEquals(List.of(child2), replaced.children());
    }

    public void testReplaceSubPlansAndOutputPreservesType() {
        LogicalPlan child1 = relation("index1");
        LogicalPlan child2 = relation("index2");
        List<Attribute> newOutput = List.of(new ReferenceAttribute(Source.EMPTY, "col", DataType.KEYWORD));
        ViewUnionAll original = new ViewUnionAll(Source.EMPTY, List.of(child1), List.of());

        Fork replaced = original.replaceSubPlansAndOutput(List.of(child2), newOutput);
        assertThat(replaced, instanceOf(ViewUnionAll.class));
        assertEquals(List.of(child2), replaced.children());
        assertEquals(newOutput, replaced.output());
    }

    public void testEqualsAndHashCode() {
        LogicalPlan child1 = relation("index1");
        LogicalPlan child2 = relation("index2");

        ViewUnionAll a = new ViewUnionAll(Source.EMPTY, List.of(child1, child2), List.of());
        ViewUnionAll b = new ViewUnionAll(Source.EMPTY, List.of(child1, child2), List.of());
        ViewUnionAll c = new ViewUnionAll(Source.EMPTY, List.of(child1), List.of());

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    public void testNotEqualToPlainUnionAll() {
        LogicalPlan child = relation("index1");

        ViewUnionAll viewUnion = new ViewUnionAll(Source.EMPTY, List.of(child), List.of());
        UnionAll plainUnion = new UnionAll(Source.EMPTY, List.of(child), List.of());

        // ViewUnionAll and UnionAll with same children should NOT be equal (different getClass())
        assertNotEquals(viewUnion, plainUnion);
        assertNotEquals(plainUnion, viewUnion);
    }

    private static UnresolvedRelation relation(String name) {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, name), false, List.of(), IndexMode.STANDARD, null);
    }
}
