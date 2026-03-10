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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ViewUnionAllTests extends ESTestCase {

    public void testIsInstanceOfUnionAll() {
        ViewUnionAll viewUnion = new ViewUnionAll(Source.EMPTY, viewMap(), List.of());
        assertThat(viewUnion, instanceOf(UnionAll.class));
        assertThat(viewUnion, instanceOf(Fork.class));
    }

    public void testReplaceChildrenPreservesType() {
        LogicalPlan child1 = relation("index1");
        LogicalPlan child2 = relation("index2");
        ViewUnionAll original = new ViewUnionAll(Source.EMPTY, viewMap(child1), List.of());

        LogicalPlan replaced = original.replaceChildren(List.of(child2));
        assertThat(replaced, instanceOf(ViewUnionAll.class));
        assertEquals(List.of(child2), replaced.children());
    }

    public void testReplaceSubPlansPreservesType() {
        LogicalPlan child1 = relation("index1");
        LogicalPlan child2 = relation("index2");
        ViewUnionAll original = new ViewUnionAll(Source.EMPTY, viewMap(child1), List.of());
        assertThat(original.namedSubqueries(), equalTo(Map.of("view_0", child1)));

        ViewUnionAll replaced = original.replaceSubPlans(List.of(child2));
        assertEquals(List.of(child2), replaced.children());
        assertThat(replaced.namedSubqueries(), equalTo(Map.of("view_0", child2)));
    }

    public void testReplaceSubPlansAndOutputPreservesType() {
        LogicalPlan child1 = relation("index1");
        LogicalPlan child2 = relation("index2");
        ViewUnionAll original = new ViewUnionAll(Source.EMPTY, viewMap(child1), List.of());
        assertThat(original.namedSubqueries(), equalTo(Map.of("view_0", child1)));

        Attribute col1 = new ReferenceAttribute(Source.EMPTY, null, "col", DataType.KEYWORD);
        ViewUnionAll replaced = original.replaceSubPlansAndOutput(List.of(child2), List.of(col1));
        assertEquals(List.of(child2), replaced.children());
        assertThat(replaced.namedSubqueries(), equalTo(Map.of("view_0", child2)));
        assertThat(replaced.output(), contains(col1));
    }

    public void testEqualsAndHashCode() {
        LogicalPlan child1 = relation("index1");
        LogicalPlan child2 = relation("index2");

        ViewUnionAll a = new ViewUnionAll(Source.EMPTY, viewMap(child1, child2), List.of());
        ViewUnionAll b = new ViewUnionAll(Source.EMPTY, viewMap(child1, child2), List.of());
        ViewUnionAll c = new ViewUnionAll(Source.EMPTY, viewMap(child1), List.of());
        ViewUnionAll d = new ViewUnionAll(Source.EMPTY, viewMap(child2, child1), List.of());

        // a and b are identical
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        // a and c are different
        assertNotEquals(a, c);
        assertNotEquals(a.hashCode(), c.hashCode());

        // a and d are different
        assertNotEquals(a, d);
        assertNotEquals(a.hashCode(), d.hashCode());

        // If we replace subplans we can make d match a
        d = d.replaceSubPlans(List.of(child1, child2));
        assertEquals(a, d);
        assertEquals(a.hashCode(), d.hashCode());
    }

    public void testNotEqualToPlainUnionAll() {
        LogicalPlan child = relation("index1");

        ViewUnionAll viewUnion = new ViewUnionAll(Source.EMPTY, viewMap(child), List.of());
        UnionAll plainUnion = new UnionAll(Source.EMPTY, List.of(child), List.of());

        // ViewUnionAll and UnionAll with same children should NOT be equal (different getClass())
        assertNotEquals(viewUnion, plainUnion);
        assertNotEquals(plainUnion, viewUnion);
    }

    private LinkedHashMap<String, LogicalPlan> viewMap(LogicalPlan... children) {
        LinkedHashMap<String, LogicalPlan> namedChildren = LinkedHashMap.newLinkedHashMap(children.length);
        for (int i = 0; i < children.length; i++) {
            namedChildren.put("view_" + i, children[i]);
        }
        return namedChildren;
    }

    private static UnresolvedRelation relation(String name) {
        return new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, name), false, List.of(), IndexMode.STANDARD, null);
    }
}
