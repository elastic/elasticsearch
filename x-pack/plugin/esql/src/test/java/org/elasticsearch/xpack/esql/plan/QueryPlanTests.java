/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.hamcrest.Matchers.contains;

public class QueryPlanTests extends ESTestCase {

    public void testTransformWithExpressionTopLevel() throws Exception {
        Limit limit = new Limit(EMPTY, of(42), relation());
        LogicalPlan transformed = limit.transformExpressionsOnly(Literal.class, l -> of(24));

        assertEquals(Limit.class, transformed.getClass());
        Limit l = (Limit) transformed;
        assertEquals(24, l.limit().fold());
    }

    public void testTransformWithExpressionTree() throws Exception {
        Limit limit = new Limit(EMPTY, of(42), relation());
        OrderBy o = new OrderBy(EMPTY, limit, emptyList());
        LogicalPlan transformed = o.transformExpressionsDown(Literal.class, l -> of(24));

        assertEquals(OrderBy.class, transformed.getClass());
        OrderBy order = (OrderBy) transformed;
        assertEquals(Limit.class, order.child().getClass());
        assertEquals(24, ((Limit) order.child()).limit().fold());
    }

    public void testTransformWithExpressionTopLevelInCollection() throws Exception {
        FieldAttribute one = fieldAttribute("one", INTEGER);
        FieldAttribute two = fieldAttribute("two", INTEGER);

        Project project = new Project(EMPTY, relation(), asList(one, two));
        LogicalPlan transformed = project.transformExpressionsOnly(
            NamedExpression.class,
            n -> n.name().equals("one") ? new FieldAttribute(EMPTY, "changed", one.field()) : n
        );

        assertEquals(Project.class, transformed.getClass());
        Project p = (Project) transformed;
        assertEquals(2, p.projections().size());
        assertSame(two, p.projections().get(1));

        NamedExpression o = p.projections().get(0);
        assertEquals("changed", o.name());
    }

    public void testForEachWithExpressionTopLevel() throws Exception {
        Alias one = new Alias(EMPTY, "one", of(42));
        FieldAttribute two = fieldAttribute();

        Project project = new Project(EMPTY, relation(), asList(one, two));

        List<Object> list = new ArrayList<>();
        project.forEachExpression(Literal.class, l -> {
            if (l.fold().equals(42)) {
                list.add(l.fold());
            }
        });

        assertEquals(singletonList(one.child().fold()), list);
    }

    public void testForEachWithExpressionTree() throws Exception {
        Limit limit = new Limit(EMPTY, of(42), relation());
        OrderBy o = new OrderBy(EMPTY, limit, emptyList());

        List<Object> list = new ArrayList<>();
        o.forEachExpressionDown(Literal.class, l -> {
            if (l.fold().equals(42)) {
                list.add(l.fold());
            }
        });

        assertEquals(singletonList(limit.limit().fold()), list);
    }

    public void testForEachWithExpressionTopLevelInCollection() throws Exception {
        FieldAttribute one = fieldAttribute("one", INTEGER);
        FieldAttribute two = fieldAttribute("two", INTEGER);

        Project project = new Project(EMPTY, relation(), asList(one, two));

        List<NamedExpression> list = new ArrayList<>();
        project.forEachExpression(NamedExpression.class, n -> {
            if (n.name().equals("one")) {
                list.add(n);
            }
        });

        assertEquals(singletonList(one), list);
    }

    public void testForEachWithExpressionTreeInCollection() throws Exception {
        Alias one = new Alias(EMPTY, "one", of(42));
        FieldAttribute two = fieldAttribute();

        Project project = new Project(EMPTY, relation(), asList(one, two));

        List<Object> list = new ArrayList<>();
        project.forEachExpression(Literal.class, l -> {
            if (l.fold().equals(42)) {
                list.add(l.fold());
            }
        });

        assertEquals(singletonList(one.child().fold()), list);
    }

    public void testPlanExpressions() {
        Alias one = new Alias(EMPTY, "one", of(42));
        FieldAttribute two = fieldAttribute();
        Project project = new Project(EMPTY, relation(), asList(one, two));

        assertThat(Expressions.names(project.expressions()), contains("one", two.name()));
    }

    public void testPlanReferences() {
        var one = fieldAttribute("one", INTEGER);
        var two = fieldAttribute("two", INTEGER);
        var add = new Add(EMPTY, one, two);
        var field = fieldAttribute("field", INTEGER);

        var filter = new Filter(EMPTY, relation(), equalsOf(field, add));
        assertThat(Expressions.names(filter.references()), contains("field", "one", "two"));
    }
}
