/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.InferIsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

public class InferIsNotNullTests extends LocalLogicalPlanOptimizerTests {

    public void testIsNotNullOnIsNullField() {
        var fieldA = getFieldAttribute("a");
        EsRelation relation = relation(fieldA);
        Expression inn = isNotNull(fieldA);
        Filter f = new Filter(EMPTY, relation, inn);

        assertEquals(f, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnOperatorWithOneField() {
        var fieldA = getFieldAttribute("a");
        EsRelation relation = relation(fieldA);
        Expression inn = isNotNull(new Add(EMPTY, fieldA, ONE));
        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, isNotNull(fieldA), inn));

        assertEquals(expected, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnOperatorWithTwoFields() {
        var fieldA = getFieldAttribute("a");
        var fieldB = getFieldAttribute("b");
        EsRelation relation = relation(fieldA, fieldB);
        Expression inn = isNotNull(new Add(EMPTY, fieldA, fieldB));
        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, new And(EMPTY, isNotNull(fieldA), isNotNull(fieldB)), inn));

        assertEquals(expected, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnFunctionWithOneField() {
        var fieldA = getFieldAttribute("a");
        var pattern = L("abc");
        EsRelation relation = relation(fieldA);
        Expression inn = isNotNull(new StartsWith(EMPTY, fieldA, pattern));

        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, isNotNull(fieldA), inn));

        assertEquals(expected, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnFunctionWithTwoFields() {
        var fieldA = getFieldAttribute("a");
        var fieldB = getFieldAttribute("b");
        EsRelation relation = relation(fieldA, fieldB);
        Expression inn = isNotNull(new StartsWith(EMPTY, fieldA, fieldB));

        Filter f = new Filter(EMPTY, relation, inn);
        Filter expected = new Filter(EMPTY, relation, new And(EMPTY, new And(EMPTY, isNotNull(fieldA), isNotNull(fieldB)), inn));

        assertEquals(expected, new InferIsNotNull().apply(f));
    }

    public void testIsNotNullOnCoalesce() {
        var plan = localPlan("""
              from test
            | where coalesce(emp_no, salary) is not null
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var inn = as(filter.condition(), IsNotNull.class);
        var coalesce = as(inn.children().get(0), Coalesce.class);
        assertThat(Expressions.names(coalesce.children()), contains("emp_no", "salary"));
        var source = as(filter.child(), EsRelation.class);
    }

    public void testIsNotNullOnExpression() {
        var plan = localPlan("""
              from test
            | eval x = emp_no + 1
            | where x is not null
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var inn = as(filter.condition(), IsNotNull.class);
        assertThat(Expressions.names(inn.children()), contains("x"));
        var eval = as(filter.child(), Eval.class);
        filter = as(eval.child(), Filter.class);
        inn = as(filter.condition(), IsNotNull.class);
        assertThat(Expressions.names(inn.children()), contains("emp_no"));
        var source = as(filter.child(), EsRelation.class);
    }

    public void testIsNotNullOnCase() {
        var plan = localPlan("""
              from test
            | where case(emp_no > 10000, "1", salary < 50000, "2", first_name) is not null
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var inn = as(filter.condition(), IsNotNull.class);
        var caseF = as(inn.children().get(0), Case.class);
        assertThat(Expressions.names(caseF.children()), contains("emp_no > 10000", "\"1\"", "salary < 50000", "\"2\"", "first_name"));
        var source = as(filter.child(), EsRelation.class);
    }

    public void testIsNotNullOnCase_With_IS_NULL() {
        var plan = localPlan("""
              from test
            | where case(emp_no IS NULL, "1", salary IS NOT NULL, "2", first_name) is not null
            """);

        var limit = as(plan, Limit.class);
        var filter = as(limit.child(), Filter.class);
        var inn = as(filter.condition(), IsNotNull.class);
        var caseF = as(inn.children().get(0), Case.class);
        assertThat(Expressions.names(caseF.children()), contains("emp_no IS NULL", "\"1\"", "salary IS NOT NULL", "\"2\"", "first_name"));
        var source = as(filter.child(), EsRelation.class);
    }

    public void testInferIsNotNull_inferNoFields() {
        for (String query : List.of(
            "FROM test | WHERE emp_no IS NOT NULL",
            "FROM test | WHERE (emp_no IS NOT NULL) IS NOT NULL",
            "FROM test | WHERE (emp_no > 1 OR languages > 2) IS NOT NULL",
            "FROM test | WHERE (emp_no IN (1,2,3)) IS NOT NULL",
            "FROM test | WHERE COALESCE(emp_no, 42) IS NOT NULL"
        )) {
            var plan = localPlan(query);
            assertEquals(plan, new InferIsNotNull().apply(plan));
        }
    }

    public void testInferIsNotNull_inferFields() {
        for (Map.Entry<String, List<String>> queryAndFields : Map.of(
            "FROM test | WHERE 2*(7+POW(emp_no,2)) IS NOT NULL",
            List.of("emp_no", "2*(7+POW(emp_no,2))"),
            "FROM test | WHERE (emp_no + salary) IS NOT NULL",
            List.of("emp_no", "salary", "emp_no + salary"),
            "FROM test | WHERE salary + COALESCE(emp_no, 42) IS NOT NULL",
            List.of("salary", "salary + COALESCE(emp_no, 42)"),
            "FROM test | WHERE SQRT(1/(1-SIN(emp_no*salary))) IS NOT NULL",
            List.of("emp_no", "salary", "SQRT(1/(1-SIN(emp_no*salary)))")
        ).entrySet()) {
            var plan = localPlan(queryAndFields.getKey());
            var filters = plan.collect(c -> c instanceof Filter);
            assertThat(filters, hasSize(1));
            var filter = filters.get(0);
            var and = as(((Filter) filter).condition(), And.class);
            var inns = and.collect(c -> c instanceof IsNotNull);
            var innFields = inns.stream().map(inn -> Expressions.name(((IsNotNull) inn).field())).toList();
            assertThat(innFields, containsInAnyOrder(queryAndFields.getValue().toArray()));
        }
    }

    private IsNotNull isNotNull(Expression field) {
        return new IsNotNull(EMPTY, field);
    }
}
