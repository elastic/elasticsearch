/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.approximation.Approximation;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnionAllFromDisjunctiveInSubquery;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AnalyzerInSubqueryTests extends ESTestCase {

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
    }

    // basic IN subqueries

    public void testInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertFalse(semiJoin.isAntiJoin());
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertTrue(antiJoin.isAntiJoin());
        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.output(), equalTo(antiJoin.left().output()));

        EsRelation leftRelation = as(antiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(antiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testInSubqueryAndOneMorePredicate() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
              AND salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertFalse(semiJoin.isAntiJoin());

        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        Filter filter = as(semiJoin.left(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute salary = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(50000, literal.value());

        EsRelation leftRelation = as(filter.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testInSubqueryAndManyOtherPredicates() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE salary > 50000 AND emp_no IN (FROM employees | KEEP emp_no) AND salary < 100000
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        // Remaining filters (salary > 50000 AND salary < 100000) are below the SemiJoin
        Filter filter = as(semiJoin.left(), Filter.class);
        And and = as(filter.condition(), And.class);
        GreaterThan greaterThan = as(and.left(), GreaterThan.class);
        FieldAttribute salary = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(50000, literal.value());
        LessThan lessThan = as(and.right(), LessThan.class);
        salary = as(lessThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        literal = as(lessThan.right(), Literal.class);
        assertEquals(100000, literal.value());

        EsRelation leftRelation = as(filter.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testInSubqueryAndInPredicate() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no) AND languages IN (1, 2, 3)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        // The IN value-list is a remaining filter below the SemiJoin
        Filter filter = as(semiJoin.left(), Filter.class);
        In inValueList = as(filter.condition(), In.class);
        FieldAttribute languages = as(inValueList.value(), FieldAttribute.class);
        assertEquals("languages", languages.name());
        EsRelation leftRelation = as(filter.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testInSubqueryAfterEval() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1
            | WHERE x IN (FROM employees | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("x"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        Eval eval = as(semiJoin.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = as(eval.fields().get(0), Alias.class);
        Add add = as(alias.child(), Add.class);
        FieldAttribute empNo = as(add.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        assertEquals("x", alias.name());

        EsRelation leftRelation = as(eval.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testNotInSubqueryAfterEval() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1
            | WHERE x NOT IN (FROM employees | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);

        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("x"));
        assertThat(antiJoin.config().rightFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.output(), equalTo(antiJoin.left().output()));

        Eval eval = as(antiJoin.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = as(eval.fields().get(0), Alias.class);
        Add add = as(alias.child(), Add.class);
        FieldAttribute empNo = as(add.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        assertEquals("x", alias.name());

        EsRelation leftRelation = as(eval.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(antiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testInSubqueryAndOtherPredicateAfterEval() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1
            | WHERE x IN (FROM employees | KEEP emp_no)
              AND salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);

        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("x"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        Filter filter = as(semiJoin.left(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute salary = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(50000, literal.value());

        Eval eval = as(filter.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = as(eval.fields().get(0), Alias.class);
        Add add = as(alias.child(), Add.class);
        FieldAttribute empNo = as(add.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        assertEquals("x", alias.name());

        EsRelation leftRelation = as(eval.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testInAndNotInSubqueryAfterEval() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | EVAL x = emp_no + 1, y = salary * 2
            | WHERE x IN (FROM employees | KEEP emp_no)
              AND y NOT IN (FROM employees | KEEP salary)
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("y"));
        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("salary"));
        assertThat(antiJoin.output(), equalTo(antiJoin.left().output()));

        Project project = as(antiJoin.right(), Project.class);
        EsRelation antiJoinRightRelation = as(project.child(), EsRelation.class);
        assertEquals("employees", antiJoinRightRelation.indexPattern());

        SemiJoin semiJoin = as(antiJoin.left(), SemiJoin.class);
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("x"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        project = as(antiJoin.right(), Project.class);
        EsRelation semiJoinRightRelation = as(project.child(), EsRelation.class);
        assertEquals("employees", semiJoinRightRelation.indexPattern());

        Eval eval = as(semiJoin.left(), Eval.class);
        assertEquals(2, eval.fields().size());
        Alias x = as(eval.fields().get(0), Alias.class);
        assertEquals("x", x.name());
        Add add = as(x.child(), Add.class);
        FieldAttribute empNo = as(add.left(), FieldAttribute.class);
        assertEquals("emp_no", empNo.name());
        Literal literal = as(add.right(), Literal.class);
        assertEquals(1, literal.value());
        Alias y = as(eval.fields().get(1), Alias.class);
        assertEquals("y", y.name());
        Mul mul = as(y.child(), Mul.class);
        FieldAttribute salary = as(mul.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        literal = as(mul.right(), Literal.class);
        assertEquals(2, literal.value());

        EsRelation test = as(eval.child(), EsRelation.class);
        assertEquals("test", test.indexPattern());
    }

    public void testStatsInsideInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | STATS max_emp = max(emp_no))
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("max_emp"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Aggregate aggregate = as(semiJoin.right(), Aggregate.class);
        EsRelation rightRelation = as(aggregate.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testStatsInsideNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | STATS min_emp = min(emp_no))
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("min_emp"));

        EsRelation leftRelation = as(antiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Aggregate aggregate = as(antiJoin.right(), Aggregate.class);
        EsRelation rightRelation = as(aggregate.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testStatsByInsideInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees
                              | STATS max_emp = max(emp_no) BY languages
                              | KEEP max_emp)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("max_emp"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project project = as(semiJoin.right(), Project.class);
        Aggregate aggregate = as(project.child(), Aggregate.class);
        assertThat(aggregate.groupings().size(), equalTo(1));
        EsRelation rightRelation = as(aggregate.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testMultipleCommandsInsideInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees
                              | STATS m = max(emp_no) BY y = date_trunc(1 year, hire_date)
                              | SORT y DESC
                              | LIMIT 5
                              | KEEP m)
            """);

        Limit outerLimit = as(plan, Limit.class);
        SemiJoin semiJoin = as(outerLimit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("m"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project project = as(semiJoin.right(), Project.class);
        Limit innerLimit = as(project.child(), Limit.class);
        OrderBy orderBy = as(innerLimit.child(), OrderBy.class);
        Aggregate aggregate = as(orderBy.child(), Aggregate.class);
        assertThat(aggregate.groupings().size(), equalTo(1));
        EsRelation rightRelation = as(aggregate.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testMultipleCommandsAfterInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | EVAL doubled = salary * 2
            | WHERE doubled > 100000
            | SORT doubled DESC
            | LIMIT 10
            | KEEP emp_no, doubled
            """);

        Limit outerLimit = as(plan, Limit.class);
        Project project = as(outerLimit.child(), Project.class);
        Limit innerLimit = as(project.child(), Limit.class);
        OrderBy orderBy = as(innerLimit.child(), OrderBy.class);
        Filter filter = as(orderBy.child(), Filter.class);
        Eval eval = as(filter.child(), Eval.class);
        SemiJoin semiJoin = as(eval.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());
        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testCommandsAfterNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
            | EVAL doubled = salary * 2
            | SORT doubled
            | LIMIT 5
            """);

        Limit outerLimit = as(plan, Limit.class);
        Limit innerLimit = as(outerLimit.child(), Limit.class);
        OrderBy orderBy = as(innerLimit.child(), OrderBy.class);
        Eval eval = as(orderBy.child(), Eval.class);
        AntiJoin antiJoin = as(eval.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(antiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());
        Project rightProject = as(antiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testTwoWhereCommands() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | WHERE salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute salary = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());

        SemiJoin semiJoin = as(filter.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());
        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testStatsAfterInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
            | STATS avg_salary = avg(salary) BY languages
            """);

        Limit limit = as(plan, Limit.class);
        Aggregate aggregate = as(limit.child(), Aggregate.class);
        assertThat(aggregate.groupings().size(), equalTo(1));

        SemiJoin semiJoin = as(aggregate.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());
        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testExtraParenthesizedInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE (emp_no IN (FROM employees | KEEP emp_no)) AND salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        Filter filter = as(semiJoin.left(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute salary = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        EsRelation leftRelation = as(filter.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    // -- constant left-hand side IN subquery tests --

    public void testConstantInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE 10001 IN (FROM employees | KEEP emp_no)
            """);

        // Project on top strips the synthetic constant column from the output
        Project topProject = as(plan, Project.class);
        Limit limit = as(topProject.child(), Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        Eval eval = as(semiJoin.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = as(eval.fields().get(0), Alias.class);
        Literal literal = as(alias.child(), Literal.class);
        assertEquals(10001, literal.value());
        EsRelation leftRelation = as(eval.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testConstantNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE 10001 NOT IN (FROM employees | KEEP emp_no)
            """);

        Project topProject = as(plan, Project.class);
        Limit limit = as(topProject.child(), Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        Eval eval = as(antiJoin.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = as(eval.fields().get(0), Alias.class);
        Literal literal = as(alias.child(), Literal.class);
        assertEquals(10001, literal.value());
        EsRelation leftRelation = as(eval.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(antiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testConstantInSubqueryWithRemainingFilter() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE 10001 IN (FROM employees | KEEP emp_no) AND salary > 50000
            """);

        Project topProject = as(plan, Project.class);
        Limit limit = as(topProject.child(), Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        Eval eval = as(semiJoin.left(), Eval.class);
        Filter filter = as(eval.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute salary = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        EsRelation leftRelation = as(filter.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testStringConstantInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE "Georgi" IN (FROM employees | KEEP first_name)
            """);

        Project topProject = as(plan, Project.class);
        Limit limit = as(topProject.child(), Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("first_name"));

        Eval eval = as(semiJoin.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        EsRelation leftRelation = as(eval.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    // -- date comparison inside IN subquery --

    public void testInSubqueryWithImplicitDateCast() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM employees
                | WHERE hire_date >= "1989-01-01T00:00:00.000Z"
                | KEEP emp_no
              )
            | KEEP emp_no
            """);

        Limit limit = as(plan, Limit.class);
        Project topProject = as(limit.child(), Project.class);
        SemiJoin semiJoin = as(topProject.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Right side: Project[emp_no] -> Filter[hire_date >= <date>] -> EsRelation[employees]
        Project rightProject = as(semiJoin.right(), Project.class);
        Filter filter = as(rightProject.child(), Filter.class);
        GreaterThanOrEqual gte = as(filter.condition(), GreaterThanOrEqual.class);
        FieldAttribute hireDateField = as(gte.left(), FieldAttribute.class);
        assertEquals("hire_date", hireDateField.name());
        // The string literal is implicitly cast to a date
        Literal dateLiteral = as(gte.right(), Literal.class);
        assertEquals(DataType.DATETIME, dateLiteral.dataType());

        EsRelation rightRelation = as(filter.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    // -- tests with FROM subquery and IN subquery --

    public void testFromSubqueryInsideInSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees, (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project project = as(semiJoin.right(), Project.class);
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
    }

    public void testFromSubqueryInsideNotInSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees, (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(antiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project project = as(antiJoin.right(), Project.class);
        UnionAll unionAll = as(project.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
    }

    public void testInSubqueryInsideFromSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzeInSubquery("""
            FROM test,
                 (FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // main query: Project -> EsRelation (Project added for field alignment in UnionAll)
        Project mainProject = as(unionAll.children().get(0), Project.class);
        EsRelation mainRelation = as(mainProject.child(), EsRelation.class);
        assertEquals("test", mainRelation.indexPattern());

        // FROM subquery: Project (alignment) -> Eval (null columns) -> Subquery -> Project -> SemiJoin
        Project alignProject = as(unionAll.children().get(1), Project.class);
        Eval subEval = as(alignProject.child(), Eval.class);
        Subquery subquery = as(subEval.child(), Subquery.class);
        Project subProject = as(subquery.child(), Project.class);
        SemiJoin semiJoin = as(subProject.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation semiLeft = as(semiJoin.left(), EsRelation.class);
        assertEquals("employees", semiLeft.indexPattern());
        Project semiRight = as(semiJoin.right(), Project.class);
        EsRelation semiRightRel = as(semiRight.child(), EsRelation.class);
        assertEquals("test", semiRightRel.indexPattern());
    }

    public void testNotInSubqueryInsideFromSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzeInSubquery("""
            FROM test,
                 (FROM employees | WHERE emp_no NOT IN (FROM test | KEEP emp_no) | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        Project mainProject = as(unionAll.children().get(0), Project.class);
        EsRelation mainRelation = as(mainProject.child(), EsRelation.class);
        assertEquals("test", mainRelation.indexPattern());

        Project alignProject = as(unionAll.children().get(1), Project.class);
        Eval subEval = as(alignProject.child(), Eval.class);
        Subquery subquery = as(subEval.child(), Subquery.class);
        Project subProject = as(subquery.child(), Project.class);
        AntiJoin antiJoin = as(subProject.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation antiLeft = as(antiJoin.left(), EsRelation.class);
        assertEquals("employees", antiLeft.indexPattern());
        Project antiRight = as(antiJoin.right(), Project.class);
        EsRelation antiRightRel = as(antiRight.child(), EsRelation.class);
        assertEquals("test", antiRightRel.indexPattern());
    }

    // -- nested IN/NOT IN subquery tests --

    public void testNestedInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (FROM test | KEEP salary)
                | KEEP emp_no
              )
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(outerSemiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(outerSemiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Right side: Project -> SemiJoin (the inner IN subquery)
        Project project = as(outerSemiJoin.right(), Project.class);
        SemiJoin innerSemiJoin = as(project.child(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(innerSemiJoin.config().rightFields().get(0).name(), equalTo("salary"));

        EsRelation innerLeft = as(innerSemiJoin.left(), EsRelation.class);
        assertEquals("employees", innerLeft.indexPattern());
        Project innerProject = as(innerSemiJoin.right(), Project.class);
        EsRelation innerRight = as(innerProject.child(), EsRelation.class);
        assertEquals("test", innerRight.indexPattern());
    }

    public void testNestedNotInInsideInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary NOT IN (FROM test | KEEP salary)
                | KEEP emp_no
              )
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(outerSemiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(outerSemiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project project = as(outerSemiJoin.right(), Project.class);
        AntiJoin innerAntiJoin = as(project.child(), AntiJoin.class);
        assertThat(innerAntiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(innerAntiJoin.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(innerAntiJoin.config().rightFields().get(0).name(), equalTo("salary"));

        EsRelation innerLeft = as(innerAntiJoin.left(), EsRelation.class);
        assertEquals("employees", innerLeft.indexPattern());
        Project innerProject = as(innerAntiJoin.right(), Project.class);
        EsRelation innerRight = as(innerProject.child(), EsRelation.class);
        assertEquals("test", innerRight.indexPattern());
    }

    public void testNestedInInsideNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (
                FROM employees
                | WHERE salary IN (FROM test | KEEP salary)
                | KEEP emp_no
              )
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin outerAntiJoin = as(limit.child(), AntiJoin.class);
        assertThat(outerAntiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(outerAntiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(outerAntiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(outerAntiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project project = as(outerAntiJoin.right(), Project.class);
        SemiJoin innerSemiJoin = as(project.child(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(innerSemiJoin.config().rightFields().get(0).name(), equalTo("salary"));

        EsRelation innerLeft = as(innerSemiJoin.left(), EsRelation.class);
        assertEquals("employees", innerLeft.indexPattern());
        Project innerProject = as(innerSemiJoin.right(), Project.class);
        EsRelation innerRight = as(innerProject.child(), EsRelation.class);
        assertEquals("test", innerRight.indexPattern());
    }

    public void testThreeNestedInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (
                    FROM test
                    | WHERE languages IN (FROM employees | KEEP languages)
                    | KEEP salary
                  )
                | KEEP emp_no
              )
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(outerSemiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation outerLeft = as(outerSemiJoin.left(), EsRelation.class);
        assertEquals("test", outerLeft.indexPattern());

        Project project1 = as(outerSemiJoin.right(), Project.class);
        SemiJoin middleSemiJoin = as(project1.child(), SemiJoin.class);
        assertThat(middleSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(middleSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(middleSemiJoin.config().rightFields().get(0).name(), equalTo("salary"));

        EsRelation middleLeft = as(middleSemiJoin.left(), EsRelation.class);
        assertEquals("employees", middleLeft.indexPattern());

        Project project2 = as(middleSemiJoin.right(), Project.class);
        SemiJoin innerSemiJoin = as(project2.child(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("languages"));
        assertThat(innerSemiJoin.config().rightFields().get(0).name(), equalTo("languages"));

        EsRelation innerLeft = as(innerSemiJoin.left(), EsRelation.class);
        assertEquals("test", innerLeft.indexPattern());
        Project innerProject = as(innerSemiJoin.right(), Project.class);
        EsRelation innerRight = as(innerProject.child(), EsRelation.class);
        assertEquals("employees", innerRight.indexPattern());
    }

    public void testNestedInSubqueryAndOtherPredicate() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (FROM test | KEEP salary)
                  AND languages > 2
                | KEEP emp_no
              )
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(outerSemiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation outerLeft = as(outerSemiJoin.left(), EsRelation.class);
        assertEquals("test", outerLeft.indexPattern());

        Project project = as(outerSemiJoin.right(), Project.class);
        SemiJoin innerSemiJoin = as(project.child(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));

        // The remaining filter (languages > 2) should be below the inner SemiJoin
        Filter filter = as(innerSemiJoin.left(), Filter.class);
        EsRelation innerLeft = as(filter.child(), EsRelation.class);
        assertEquals("employees", innerLeft.indexPattern());
    }

    public void testDoubleNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());
        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testTripleNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE NOT (NOT (emp_no NOT IN (FROM employees | KEEP emp_no)))
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(antiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());
        Project rightProject = as(antiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testDoubleNotInSubqueryOrOneMorePredicate() {
        // Reordered: [salary > 50000, NOT(NOT IN emp_no)] (complexity 0, 1)
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
               OR salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 1: salary > 50000 → Project -> Filter
        Project branch1Project = as(unionAll.children().get(0), Project.class);
        Filter branch1Filter = as(branch1Project.child(), Filter.class);
        GreaterThan branch1Gt = as(branch1Filter.condition(), GreaterThan.class);
        FieldAttribute branch1Salary = as(branch1Gt.left(), FieldAttribute.class);
        assertEquals("salary", branch1Salary.name());
        EsRelation branch1Left = as(branch1Filter.child(), EsRelation.class);
        assertEquals("test", branch1Left.indexPattern());

        // Branch 2: NOT(salary > 50000) AND NOT(NOT IN emp_no) → Project -> SemiJoin with exclusion filter
        Project branch2Project = as(unionAll.children().get(1), Project.class);
        SemiJoin branch2Semi = as(branch2Project.child(), SemiJoin.class);
        assertThat(branch2Semi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(branch2Semi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch2Semi.config().rightFields().get(0).name(), equalTo("emp_no"));
        Filter branch2Filter = as(branch2Semi.left(), Filter.class);
        EsRelation branch2Left = as(branch2Filter.child(), EsRelation.class);
        assertEquals("test", branch2Left.indexPattern());
    }

    public void testDoubleNotInSubqueryOrInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
               OR salary IN (FROM employees | KEEP salary)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 1: Project -> SemiJoin (double NOT → IN)
        Project branch1Project = as(unionAll.children().get(0), Project.class);
        SemiJoin branch1Semi = as(branch1Project.child(), SemiJoin.class);
        assertThat(branch1Semi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(branch1Semi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch1Semi.config().rightFields().get(0).name(), equalTo("emp_no"));
        EsRelation branch1Left = as(branch1Semi.left(), EsRelation.class);
        assertEquals("test", branch1Left.indexPattern());

        // Branch 2: Project -> SemiJoin for salary IN sub2, with AntiJoin exclusion (NOT IN sub1) below
        Project branch2Project = as(unionAll.children().get(1), Project.class);
        SemiJoin branch2Outer = as(branch2Project.child(), SemiJoin.class);
        assertThat(branch2Outer.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(branch2Outer.config().rightFields().get(0).name(), equalTo("salary"));
        AntiJoin branch2Inner = as(branch2Outer.left(), AntiJoin.class);
        assertThat(branch2Inner.config().leftFields().get(0).name(), equalTo("emp_no"));
        EsRelation branch2Left = as(branch2Inner.left(), EsRelation.class);
        assertEquals("test", branch2Left.indexPattern());
    }

    public void testDoubleNotInSubqueryAndOneMorePredicate() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
               AND salary > 50000
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        Filter filter = as(semiJoin.left(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute salary = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        EsRelation leftRelation = as(filter.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    public void testDoubleNotInSubqueryAndInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
               AND salary IN (FROM employees | KEEP salary)
            """);

        Limit limit = as(plan, Limit.class);
        // Two SemiJoins are stacked: the last one extracted is on top
        SemiJoin outerSemi = as(limit.child(), SemiJoin.class);
        assertThat(outerSemi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemi.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(outerSemi.config().rightFields().get(0).name(), equalTo("salary"));

        SemiJoin innerSemi = as(outerSemi.left(), SemiJoin.class);
        assertThat(innerSemi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(innerSemi.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(innerSemi.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());
    }

    // -- disjunctive IN/NOT IN subquery tests --

    public void testDisjunctiveInSubqueries() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR salary IN (FROM employees | KEEP salary)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 1: Project -> SemiJoin for emp_no IN (...)
        Project branch1Project = as(unionAll.children().get(0), Project.class);
        SemiJoin branch1Semi = as(branch1Project.child(), SemiJoin.class);
        assertThat(branch1Semi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(branch1Semi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch1Semi.config().rightFields().get(0).name(), equalTo("emp_no"));
        EsRelation branch1Left = as(branch1Semi.left(), EsRelation.class);
        assertEquals("test", branch1Left.indexPattern());

        // Branch 2: Project -> SemiJoin for salary IN (...), with AntiJoin exclusion below
        Project branch2Project = as(unionAll.children().get(1), Project.class);
        SemiJoin branch2Semi = as(branch2Project.child(), SemiJoin.class);
        assertThat(branch2Semi.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(branch2Semi.config().rightFields().get(0).name(), equalTo("salary"));
        AntiJoin branch2Anti = as(branch2Semi.left(), AntiJoin.class);
        assertThat(branch2Anti.config().leftFields().get(0).name(), equalTo("emp_no"));
    }

    public void testDisjunctiveInAndNotInSubqueries() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
               OR emp_no IN (FROM employees | WHERE salary > 50000 | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 1: Project -> AntiJoin for emp_no NOT IN (...)
        Project branch1Project = as(unionAll.children().get(0), Project.class);
        AntiJoin branch1Anti = as(branch1Project.child(), AntiJoin.class);
        assertThat(branch1Anti.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(branch1Anti.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch1Anti.config().rightFields().get(0).name(), equalTo("emp_no"));
        EsRelation branch1Left = as(branch1Anti.left(), EsRelation.class);
        assertEquals("test", branch1Left.indexPattern());

        // Branch 2: Project -> SemiJoin for emp_no IN (sub2), with SemiJoin exclusion below
        // NOT(NOT IN sub1) simplifies to IN sub1, so the exclusion is a SemiJoin
        Project branch2Project = as(unionAll.children().get(1), Project.class);
        SemiJoin branch2Outer = as(branch2Project.child(), SemiJoin.class);
        assertThat(branch2Outer.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch2Outer.config().rightFields().get(0).name(), equalTo("emp_no"));
        SemiJoin branch2Inner = as(branch2Outer.left(), SemiJoin.class);
        assertThat(branch2Inner.config().leftFields().get(0).name(), equalTo("emp_no"));
        EsRelation branch2Left = as(branch2Inner.left(), EsRelation.class);
        assertEquals("test", branch2Left.indexPattern());
    }

    public void testDisjunctiveInSubqueryWithOtherPredicate() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE salary > 50000
               OR emp_no IN (FROM employees | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Branch 1: Project -> Filter for salary > 50000
        Project branch1Project = as(unionAll.children().get(0), Project.class);
        Filter branch1Filter = as(branch1Project.child(), Filter.class);
        GreaterThan branch1Gt = as(branch1Filter.condition(), GreaterThan.class);
        FieldAttribute branch1Salary = as(branch1Gt.left(), FieldAttribute.class);
        assertEquals("salary", branch1Salary.name());
        EsRelation branch1Left = as(branch1Filter.child(), EsRelation.class);
        assertEquals("test", branch1Left.indexPattern());

        // Branch 2: Project -> SemiJoin for emp_no IN (...) with NOT(salary > 50000) filter below
        Project branch2Project = as(unionAll.children().get(1), Project.class);
        SemiJoin branch2Semi = as(branch2Project.child(), SemiJoin.class);
        assertThat(branch2Semi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch2Semi.config().rightFields().get(0).name(), equalTo("emp_no"));
        Filter branch2Filter = as(branch2Semi.left(), Filter.class);
        EsRelation branch2Left = as(branch2Filter.child(), EsRelation.class);
        assertEquals("test", branch2Left.indexPattern());
    }

    // -- disjunctive OR chain with IN/NOT IN subqueries --

    /**
     * {@code WHERE emp_no IN (FROM employees | KEEP emp_no) OR (salary > 50000 OR (languages < 3 OR gender NOT IN (...)))}
     * <p>
     * Nested ORs flatten into four disjuncts. Branch 1 is a SemiJoin for emp_no IN.
     * Branches 2-4 carry an AntiJoin exclusion for NOT(emp_no IN sub) from the first disjunct.
     * Branch 4 additionally has an AntiJoin for gender NOT IN.
     */
    public void testDisjunctiveOrChainWithNotInSubquery() {
        // Reordered: [salary > 50000, languages < 3, emp_no IN ..., gender NOT IN ...]
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 OR (languages < 3 OR gender NOT IN (FROM employees | KEEP gender)))
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(4, unionAll.children().size());

        // Branch 1: salary > 50000 → Project -> Filter
        Project branch1Project = as(unionAll.children().get(0), Project.class);
        Filter branch1Filter = as(branch1Project.child(), Filter.class);
        GreaterThan branch1Gt = as(branch1Filter.condition(), GreaterThan.class);
        FieldAttribute branch1Salary = as(branch1Gt.left(), FieldAttribute.class);
        assertEquals("salary", branch1Salary.name());
        EsRelation branch1Left = as(branch1Filter.child(), EsRelation.class);
        assertEquals("test", branch1Left.indexPattern());

        // Branch 3: ...AND emp_no IN (...) → Project -> SemiJoin
        Project branch3Project = as(unionAll.children().get(2), Project.class);
        SemiJoin branch3Semi = as(branch3Project.child(), SemiJoin.class);
        assertThat(branch3Semi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(branch3Semi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch3Semi.config().rightFields().get(0).name(), equalTo("emp_no"));

        // Branch 4: ...AND gender NOT IN (...) → Project -> AntiJoin
        Project branch4Project = as(unionAll.children().get(3), Project.class);
        AntiJoin branch4Anti = as(branch4Project.child(), AntiJoin.class);
        assertThat(branch4Anti.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(branch4Anti.config().leftFields().get(0).name(), equalTo("gender"));
        assertThat(branch4Anti.config().rightFields().get(0).name(), equalTo("gender"));
        Project branch4Right = as(branch4Anti.right(), Project.class);
        EsRelation branch4RightRel = as(branch4Right.child(), EsRelation.class);
        assertEquals("employees", branch4RightRel.indexPattern());
    }

    /**
     * Reordered: [salary > 50000, emp_no IN ..., languages &lt; 3 AND gender NOT IN ...] (complexity 0, 1, 2)
     */
    public void testDisjunctiveOrChainWithConjunctiveNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 OR (languages < 3 AND gender NOT IN (FROM employees | KEEP gender)))
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(3, unionAll.children().size());

        // Branch 1: salary > 50000 → Project -> Filter
        Project branch1Project = as(unionAll.children().get(0), Project.class);
        Filter branch1Filter = as(branch1Project.child(), Filter.class);
        GreaterThan branch1Gt = as(branch1Filter.condition(), GreaterThan.class);
        FieldAttribute branch1Salary = as(branch1Gt.left(), FieldAttribute.class);
        assertEquals("salary", branch1Salary.name());

        // Branch 2: NOT(salary > 50000) AND emp_no IN (...) → Project -> SemiJoin
        Project branch2Project = as(unionAll.children().get(1), Project.class);
        SemiJoin branch2Semi = as(branch2Project.child(), SemiJoin.class);
        assertThat(branch2Semi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(branch2Semi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch2Semi.config().rightFields().get(0).name(), equalTo("emp_no"));

        // Branch 3: ...AND languages < 3 AND gender NOT IN (...) → Project -> AntiJoin
        Project branch3Project = as(unionAll.children().get(2), Project.class);
        AntiJoin branch3Anti = as(branch3Project.child(), AntiJoin.class);
        assertThat(branch3Anti.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(branch3Anti.config().leftFields().get(0).name(), equalTo("gender"));
        assertThat(branch3Anti.config().rightFields().get(0).name(), equalTo("gender"));
        Project branch3Right = as(branch3Anti.right(), Project.class);
        EsRelation branch3RightRel = as(branch3Right.child(), EsRelation.class);
        assertEquals("employees", branch3RightRel.indexPattern());
    }

    /**
     * Reordered: [salary > 50000, languages &lt; 3, emp_no IN ..., gender NOT IN ...] (complexity 0, 0, 1, 1)
     */
    public void testDisjunctiveOrChainWithNotInSubqueryInMiddle() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 OR (gender NOT IN (FROM employees | KEEP gender)) OR languages < 3)
            """);

        Limit limit = as(plan, Limit.class);
        UnionAll unionAll = as(limit.child(), UnionAll.class);
        assertEquals(4, unionAll.children().size());

        // Branch 1: salary > 50000 → Project -> Filter
        Project branch1Project = as(unionAll.children().get(0), Project.class);
        as(branch1Project.child(), Filter.class);

        // Branch 3: ...AND emp_no IN (...) → Project -> SemiJoin
        Project branch3Project = as(unionAll.children().get(2), Project.class);
        SemiJoin branch3Semi = as(branch3Project.child(), SemiJoin.class);
        assertThat(branch3Semi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(branch3Semi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(branch3Semi.config().rightFields().get(0).name(), equalTo("emp_no"));

        // Branch 4: ...AND gender NOT IN (...) → Project -> AntiJoin
        Project branch4Project = as(unionAll.children().get(3), Project.class);
        AntiJoin branch4Anti = as(branch4Project.child(), AntiJoin.class);
        assertThat(branch4Anti.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(branch4Anti.config().leftFields().get(0).name(), equalTo("gender"));
        assertThat(branch4Anti.config().rightFields().get(0).name(), equalTo("gender"));
        Project branch4Right = as(branch4Anti.right(), Project.class);
        EsRelation branch4RightRel = as(branch4Right.child(), EsRelation.class);
        assertEquals("employees", branch4RightRel.indexPattern());
    }

    // data types on join keys related tests

    /**
     * Verifies that KEYWORD left vs TEXT right is compatible in IN subquery.
     */
    public void testKeywordVsTextInSubquery() {
        LogicalPlan plan = analyzeWithAllTypes("""
            FROM all_types
            | WHERE keyword IN (FROM all_types | KEEP text)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("keyword"));
        assertEquals(DataType.KEYWORD, semiJoin.config().leftFields().get(0).dataType());
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("text"));
        assertEquals(DataType.TEXT, semiJoin.config().rightFields().get(0).dataType());

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("all_types", leftRelation.indexPattern());
        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("all_types", rightRelation.indexPattern());
    }

    /**
     * Verifies that TEXT left vs KEYWORD right is compatible in IN subquery.
     */
    public void testTextVsKeywordInSubquery() {
        LogicalPlan plan = analyzeWithAllTypes("""
            FROM all_types
            | WHERE text IN (FROM all_types | KEEP keyword)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("text"));
        assertEquals(DataType.TEXT, semiJoin.config().leftFields().get(0).dataType());
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("keyword"));
        assertEquals(DataType.KEYWORD, semiJoin.config().rightFields().get(0).dataType());

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("all_types", leftRelation.indexPattern());
        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("all_types", rightRelation.indexPattern());
    }

    /**
     * Verifies that IP left vs IP right is compatible in IN subquery.
     */
    public void testIpVsIpInSubquery() {
        LogicalPlan plan = analyzeWithAllTypes("""
            FROM all_types
            | WHERE ip IN (FROM all_types | KEEP ip)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("ip"));
        assertEquals(DataType.IP, semiJoin.config().leftFields().get(0).dataType());
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("ip"));
        assertEquals(DataType.IP, semiJoin.config().rightFields().get(0).dataType());

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("all_types", leftRelation.indexPattern());
        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("all_types", rightRelation.indexPattern());
    }

    /**
     * Verifies that VERSION left vs VERSION right is compatible in IN subquery.
     */
    public void testVersionVsVersionInSubquery() {
        LogicalPlan plan = analyzeWithAllTypes("""
            FROM all_types
            | WHERE version IN (FROM all_types | KEEP version)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("version"));
        assertEquals(DataType.VERSION, semiJoin.config().leftFields().get(0).dataType());
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("version"));
        assertEquals(DataType.VERSION, semiJoin.config().rightFields().get(0).dataType());

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("all_types", leftRelation.indexPattern());
        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("all_types", rightRelation.indexPattern());
    }

    // union types with explicit casting

    /**
     * Verifies that casting a union type field to a concrete type on the left side resolves the issue.
     */
    public void testUnionTypeLeftFieldWithCastInSubquery() {
        LogicalPlan plan = analyzeWithUnionIndex("""
            FROM union_index*
            | EVAL id_kw = id::keyword
            | WHERE id_kw IN (FROM test | KEEP first_name)
            | KEEP id_kw
            """);

        Limit limit = as(plan, Limit.class);
        Project topProject = as(limit.child(), Project.class);
        SemiJoin semiJoin = as(topProject.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("id_kw"));
        assertEquals(DataType.KEYWORD, semiJoin.config().leftFields().get(0).dataType());
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("first_name"));
        assertEquals(DataType.KEYWORD, semiJoin.config().rightFields().get(0).dataType());

        Eval eval = as(semiJoin.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = as(eval.fields().get(0), Alias.class);
        assertEquals("id_kw", alias.name());

        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("test", rightRelation.indexPattern());
    }

    /**
     * Verifies that casting a union type field to a concrete type on the right side resolves the issue.
     */
    public void testUnionTypeRightFieldWithCastInSubquery() {
        LogicalPlan plan = analyzeWithUnionIndex("""
            FROM test
            | WHERE first_name IN (FROM union_index* | EVAL id_kw = id::keyword | KEEP id_kw)
            | KEEP first_name
            """);

        Limit limit = as(plan, Limit.class);
        Project topProject = as(limit.child(), Project.class);
        SemiJoin semiJoin = as(topProject.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("first_name"));
        assertEquals(DataType.KEYWORD, semiJoin.config().leftFields().get(0).dataType());
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("id_kw"));
        assertEquals(DataType.KEYWORD, semiJoin.config().rightFields().get(0).dataType());

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(semiJoin.right(), Project.class);
        Eval eval = as(rightProject.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = as(eval.fields().get(0), Alias.class);
        assertEquals("id_kw", alias.name());
    }

    /**
     * Verifies that NOT IN with a cast union type field on the left succeeds.
     */
    public void testUnionTypeLeftFieldWithCastInAntiJoin() {
        LogicalPlan plan = analyzeWithUnionIndex("""
            FROM union_index*
            | EVAL id_kw = id::keyword
            | WHERE id_kw NOT IN (FROM test | KEEP first_name)
            | KEEP id_kw
            """);

        Limit limit = as(plan, Limit.class);
        Project topProject = as(limit.child(), Project.class);
        AntiJoin antiJoin = as(topProject.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("id_kw"));
        assertEquals(DataType.KEYWORD, antiJoin.config().leftFields().get(0).dataType());
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("first_name"));
        assertEquals(DataType.KEYWORD, antiJoin.config().rightFields().get(0).dataType());

        Eval eval = as(antiJoin.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = as(eval.fields().get(0), Alias.class);
        assertEquals("id_kw", alias.name());

        Project rightProject = as(antiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("test", rightRelation.indexPattern());
    }

    // union types with from subqueries

    /**
     * Verifies that casting resolves FROM subquery union type on the left side.
     */
    public void testFromSubqueryUnionTypeLeftFieldWithCast() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzeWithIncompatible("""
            FROM test, (FROM employees_incompatible | KEEP emp_no, first_name, salary)
            | EVAL id = emp_no::long
            | WHERE id IN (FROM employees_incompatible | WHERE salary > 70000 | KEEP emp_no)
            | KEEP id
            """);

        Limit limit = as(plan, Limit.class);
        Project topProject = as(limit.child(), Project.class);
        SemiJoin semiJoin = as(topProject.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("id"));
        assertEquals(DataType.LONG, semiJoin.config().leftFields().get(0).dataType());
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        // Left side: Eval[id = emp_no::long] -> UnionAll[EsRelation[test], ...]
        Eval eval = as(semiJoin.left(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = as(eval.fields().get(0), Alias.class);
        assertEquals("id", alias.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());

        // Right side: Project[emp_no] -> Filter[salary > 70000] -> EsRelation[employees_incompatible]
        Project rightProject = as(semiJoin.right(), Project.class);
        Filter filter = as(rightProject.child(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute salary = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        Literal literal = as(greaterThan.right(), Literal.class);
        assertEquals(70000, literal.value());
        EsRelation rightRelation = as(filter.child(), EsRelation.class);
        assertEquals("employees_incompatible", rightRelation.indexPattern());
    }

    /**
     * Verifies that casting resolves FROM subquery union type on the right side.
     */
    public void testFromSubqueryUnionTypeRightFieldWithCast() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzeWithIncompatible("""
            FROM test
            | WHERE emp_no IN (FROM test, (FROM employees_incompatible | KEEP emp_no) | EVAL id = emp_no::integer | KEEP id)
            | KEEP emp_no
            """);

        Limit limit = as(plan, Limit.class);
        Project topProject = as(limit.child(), Project.class);
        SemiJoin semiJoin = as(topProject.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertEquals(DataType.INTEGER, semiJoin.config().leftFields().get(0).dataType());
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("id"));
        assertEquals(DataType.INTEGER, semiJoin.config().rightFields().get(0).dataType());

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Right side: Project[id] -> Eval[id = emp_no::integer] -> UnionAll[...]
        Project rightProject = as(semiJoin.right(), Project.class);
        Eval eval = as(rightProject.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias alias = as(eval.fields().get(0), Alias.class);
        assertEquals("id", alias.name());
        UnionAll unionAll = as(eval.child(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
    }

    // -- negative test cases

    /**
     * {@code WHERE emp_no IN (...) OR (salary > 50000 AND (languages < 3 OR gender NOT IN (...)))}
     * <p>
     * The inner OR ({@code languages < 3 OR gender NOT IN (...)}) is an AND-conjunct that is not a bare
     * InSubquery, so the NOT IN inside it cannot be extracted. This is rejected.
     */
    public void testRejectsNestedConjunctiveAndDisjunctiveInSubquery() {
        errorInSubquery(
            """
                FROM test
                | WHERE emp_no IN (FROM employees | KEEP emp_no)
                   OR (salary > 50000 AND (languages < 3 OR gender NOT IN (FROM employees | KEEP gender)))
                """,
            containsString(
                "Complicated IN subquery is not yet supported in the WHERE command [WHERE emp_no IN (FROM employees | KEEP emp_no)"
            )
        );
    }

    /**
     * Verifies that an IN subquery in STATS WHERE filter is rejected.
     */
    public void testRejectsInSubqueryInStatsWhereFilter() {
        errorInSubquery("""
            FROM test
            | STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery in STATS WHERE filter is rejected.
     */
    public void testRejectsNotInSubqueryInStatsWhereFilter() {
        errorInSubquery(
            """
                FROM test
                | STATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
                """,
            containsString("IN subquery is not supported in [STATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)]")
        );
    }

    /**
     * Verifies that IN subquery in STATS WHERE with BY grouping is rejected.
     */
    public void testRejectsInSubqueryInStatsWhereFilterWithGrouping() {
        errorInSubquery(
            """
                FROM test
                | STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages
                """,
            containsString(
                "IN subquery is not supported in [STATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages]"
            )
        );
    }

    // -- negative: IN subquery in INLINESTATS --

    /**
     * Verifies that an IN subquery in INLINESTATS WHERE filter is rejected.
     */
    public void testRejectsInSubqueryInInlineStatsWhereFilter() {
        errorInSubquery(
            """
                FROM test
                | INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)
                """,
            containsString("IN subquery is not supported in [INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no)]")
        );
    }

    /**
     * Verifies that a NOT IN subquery in INLINESTATS WHERE filter is rejected.
     */
    public void testRejectsNotInSubqueryInInlineStatsWhereFilter() {
        errorInSubquery(
            """
                FROM test
                | INLINESTATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
                """,
            containsString(
                "IN subquery is not supported in [INLINESTATS cnt = COUNT(*) WHERE emp_no NOT IN (FROM employees | KEEP emp_no)]"
            )
        );
    }

    /**
     * Verifies that IN subquery in INLINESTATS WHERE with BY grouping is rejected.
     */
    public void testRejectsInSubqueryInInlineStatsWhereFilterWithGrouping() {
        errorInSubquery(
            """
                FROM test
                | INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages
                """,
            containsString(
                "IN subquery is not supported in [INLINESTATS cnt = COUNT(*) WHERE emp_no IN (FROM employees | KEEP emp_no) BY languages]"
            )
        );
    }

    // -- negative: IN subquery in EVAL --

    /**
     * Verifies that an IN subquery inside EVAL is rejected.
     */
    public void testRejectsInSubqueryInEval() {
        errorInSubquery("""
            FROM test
            | EVAL x = emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [EVAL x = emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery inside EVAL is rejected.
     */
    public void testRejectsNotInSubqueryInEval() {
        errorInSubquery("""
            FROM test
            | EVAL x = emp_no NOT IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [EVAL x = emp_no NOT IN (FROM employees | KEEP emp_no)]"));
    }

    // -- approximation incompatibility tests --

    /**
     * Verifies that IN subquery before STATS is incompatible with approximation.
     */
    public void testApproximationRejectsInSubqueryBeforeStats() {
        assertApproximationRejects("FROM test | WHERE emp_no IN (FROM employees | KEEP emp_no) | STATS COUNT()");
    }

    /**
     * Verifies that NOT IN subquery before STATS is incompatible with approximation.
     */
    public void testApproximationRejectsNotInSubqueryBeforeStats() {
        assertApproximationRejects("FROM test | WHERE emp_no NOT IN (FROM employees | KEEP emp_no) | STATS COUNT()");
    }

    /**
     * Verifies that IN subquery after STATS is incompatible with approximation.
     */
    public void testApproximationRejectsInSubqueryAfterStats() {
        assertApproximationRejects("FROM test | STATS cnt = COUNT() BY emp_no | WHERE emp_no IN (FROM employees | KEEP emp_no)");
    }

    // -- negative analyzer/verifier tests --

    /**
     * Verifies that an IN subquery returning two columns (KEEP emp_no, salary) is rejected.
     */
    public void testRejectsInSubqueryWithMultipleColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no, salary)
            """, containsString("IN subquery must return exactly one column, found [emp_no, salary]"));
    }

    /**
     * Verifies that a NOT IN subquery returning two columns is rejected.
     */
    public void testRejectsNotInSubqueryWithMultipleColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no, salary)
            """, containsString("IN subquery must return exactly one column, found [emp_no, salary]"));
    }

    /**
     * Verifies that an IN subquery returning all columns (no KEEP) is rejected.
     */
    public void testRejectsInSubqueryWithAllColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees)
            """, containsString("IN subquery must return exactly one column"));
    }

    /**
     * Verifies that an IN subquery with STATS ... BY returning two columns is rejected.
     */
    public void testRejectsInSubqueryWithStatsByReturningMultipleColumns() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | STATS max(emp_no) BY languages)
            """, containsString("IN subquery must return exactly one column, found [max(emp_no), languages]"));
    }

    /**
     * Verifies that an IN subquery returning no column is rejected.
     */
    public void testRejectsInSubqueryReturningNoColumn() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | STATS m = max(emp_no) BY languages | DROP m ,languages)
            """, containsString("IN subquery must return exactly one column, found []"));
    }

    /**
     * Verifies that an IN subquery with integer left side and keyword right side is rejected.
     */
    public void testRejectsTypeMismatchIntegerVsKeyword() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP first_name)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [first_name] of type [KEYWORD]"));
    }

    /**
     * Verifies that a NOT IN subquery with keyword left side and integer right side is rejected.
     */
    public void testRejectsTypeMismatchKeywordVsInteger() {
        errorInSubquery("""
            FROM test
            | WHERE first_name NOT IN (FROM employees | KEEP emp_no)
            """, containsString("left field [first_name] of type [KEYWORD] is incompatible with right field [emp_no] of type [INTEGER]"));
    }

    /**
     * Verifies that an IN subquery with integer left side and date right side is rejected.
     */
    public void testRejectsTypeMismatchIntegerVsDate() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP hire_date)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [hire_date] of type [DATETIME]"));
    }

    // -- non-comparable types in IN subquery --

    /**
     * Verifies that counter types (COUNTER_LONG) are rejected as IN subquery join keys.
     */
    public void testRejectsCounterLongInSubquery() {
        errorWithK8s("""
            FROM k8s
            | WHERE network.total_bytes_in IN (FROM k8s | KEEP network.total_bytes_in)
            """, containsString("IN/NOT IN subquery with right field [network.total_bytes_in] of type [COUNTER_LONG] is not supported"));
    }

    /**
     * Verifies that counter types (COUNTER_DOUBLE) are rejected as IN subquery join keys.
     */
    public void testRejectsCounterDoubleInSubquery() {
        errorWithK8s("""
            FROM k8s
            | WHERE network.total_cost IN (FROM k8s | KEEP network.total_cost)
            """, containsString("IN/NOT IN subquery with right field [network.total_cost] of type [COUNTER_DOUBLE] is not supported"));
    }

    /**
     * Verifies that aggregate_metric_double is rejected as IN subquery join key.
     */
    public void testRejectsAggregateMetricDoubleInSubquery() {
        errorWithK8sDownsampled(
            """
                FROM k8s
                | WHERE network.eth0.tx IN (FROM k8s | KEEP network.eth0.tx)
                """,
            containsString("IN/NOT IN subquery with right field [network.eth0.tx] of type [AGGREGATE_METRIC_DOUBLE] is not supported")
        );
    }

    /**
     * Verifies that numeric type mismatch (INTEGER vs LONG) is rejected — SemiJoin requires exact type match.
     */
    public void testRejectsNumericTypeMismatchIntegerVsLong() {
        errorInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | EVAL x = languages::long | KEEP x)
            """, containsString("left field [emp_no] of type [INTEGER] is incompatible with right field [x] of type [LONG]"));
    }

    /**
     * Verifies that KEYWORD left vs IP right is compatible in IN subquery.
     */
    public void testRejectsKeywordVsIpInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE keyword IN (FROM all_types | KEEP ip)
            """, containsString("left field [keyword] of type [KEYWORD] is incompatible with right field [ip] of type [IP]"));
    }

    /**
     * Verifies that IP left vs VERSION right is incompatible in IN subquery.
     */
    public void testRejectsIpVsVersionInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE ip IN (FROM all_types | KEEP version)
            """, containsString("left field [ip] of type [IP] is incompatible with right field [version] of type [VERSION]"));
    }

    /**
     * Verifies that VERSION left vs TEXT right is incompatible in IN subquery.
     */
    public void testRejectsVersionVsTextInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE version IN (FROM all_types | KEEP text)
            """, containsString("left field [version] of type [VERSION] is incompatible with right field [text] of type [TEXT]"));
    }

    /**
     * Verifies that IP left vs KEYWORD right is incompatible in IN subquery.
     */
    public void testRejectsIpVsKeywordInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE ip IN (FROM all_types | KEEP keyword)
            """, containsString("left field [ip] of type [IP] is incompatible with right field [keyword] of type [KEYWORD]"));
    }

    /**
     * Verifies that VERSION left vs KEYWORD right is incompatible in IN subquery.
     */
    public void testRejectsVersionVsKeywordInSubquery() {
        errorWithAllTypes("""
            FROM all_types
            | WHERE version IN (FROM all_types | KEEP keyword)
            """, containsString("left field [version] of type [VERSION] is incompatible with right field [keyword] of type [KEYWORD]"));
    }

    // -- date vs date_nanos incompatibility --

    /**
     * Verifies that DATETIME left vs DATE_NANOS right is incompatible in IN subquery.
     * employees has hire_date:date (DATETIME), employees_incompatible has hire_date:date_nanos (DATE_NANOS).
     */
    public void testRejectsDateVsDateNanosInSubquery() {
        errorWithIncompatible(
            """
                FROM test
                | WHERE hire_date IN (FROM employees_incompatible | KEEP hire_date)
                """,
            containsString("left field [hire_date] of type [DATETIME] is incompatible with right field [hire_date] of type [DATE_NANOS]")
        );
    }

    /**
     * Verifies that DATE_NANOS left vs DATETIME right is incompatible in IN subquery.
     */
    public void testRejectsDateNanosVsDateInSubquery() {
        errorWithIncompatible(
            """
                FROM employees_incompatible
                | WHERE hire_date IN (FROM test | KEEP hire_date)
                """,
            containsString("left field [hire_date] of type [DATE_NANOS] is incompatible with right field [hire_date] of type [DATETIME]")
        );
    }

    // -- union type tests --

    /**
     * Verifies that a union type field (id: keyword + integer) as the left join key of IN subquery
     * fails without explicit casting.
     */
    public void testRejectsUnionTypeLeftFieldInSubquery() {
        errorWithUnionIndex(
            """
                FROM union_index*
                | WHERE id IN (FROM test | KEEP emp_no)
                | KEEP id
                """,
            containsString(
                "Cannot use field [id] due to ambiguities being mapped as [2] incompatible types:"
                    + " [keyword] in [union_index_1], [integer] in [union_index_2]"
            )
        );
    }

    /**
     * Verifies that a union type field as the right join key of IN subquery fails without explicit casting.
     */
    public void testRejectsUnionTypeRightFieldInSubquery() {
        errorWithUnionIndex(
            """
                FROM test
                | WHERE first_name IN (FROM union_index* | KEEP id)
                | KEEP first_name
                """,
            containsString(
                "Cannot use field [id] due to ambiguities being mapped as [2] incompatible types:"
                    + " [keyword] in [union_index_1], [integer] in [union_index_2]"
            )
        );
    }

    /**
     * Verifies that NOT IN with a union type field on the left fails without casting.
     */
    public void testRejectsUnionTypeLeftFieldInAntiJoin() {
        errorWithUnionIndex(
            """
                FROM union_index*
                | WHERE id NOT IN (FROM test | KEEP emp_no)
                | KEEP id
                """,
            containsString(
                "Cannot use field [id] due to ambiguities being mapped as [2] incompatible types:"
                    + " [keyword] in [union_index_1], [integer] in [union_index_2]"
            )
        );
    }

    // -- union type tests with FROM subqueries --

    /**
     * Verifies that FROM subqueries with conflicting types for emp_no (integer + long) fail without casting.
     */
    public void testRejectsFromSubqueryUnionTypeLeftField() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        errorWithIncompatible("""
            FROM test, (FROM employees_incompatible | KEEP emp_no, first_name, salary)
            | WHERE emp_no IN (FROM test | WHERE salary > 70000 | KEEP emp_no)
            | KEEP emp_no
            """, containsString("Column [emp_no] has conflicting data types in subqueries: [integer, long]"));
    }

    /**
     * Verifies that FROM subqueries with conflicting types on the right side fail without casting.
     */
    public void testRejectsFromSubqueryUnionTypeRightField() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        errorWithIncompatible("""
            FROM test
            | WHERE emp_no IN (FROM test, (FROM employees_incompatible | KEEP emp_no) | KEEP emp_no)
            | KEEP emp_no
            """, containsString("Column [emp_no] has conflicting data types in subqueries: [integer, long]"));
    }

    // -- IN subquery in processing commands (rejected by analyzer) --

    /**
     * Verifies that an IN subquery inside SORT is rejected.
     */
    public void testRejectsInSubqueryInSort() {
        errorInSubquery("""
            FROM test
            | SORT emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [SORT emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery inside SORT is rejected.
     */
    public void testRejectsNotInSubqueryInSort() {
        errorInSubquery("""
            FROM test
            | SORT emp_no NOT IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [SORT emp_no NOT IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that an IN subquery in STATS BY clause is rejected.
     */
    public void testRejectsInSubqueryInStatsBy() {
        errorInSubquery("""
            FROM test
            | STATS cnt = COUNT(*) BY emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [STATS cnt = COUNT(*) BY emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery in STATS BY clause is rejected.
     */
    public void testRejectsNotInSubqueryInStatsBy() {
        errorInSubquery("""
            FROM test
            | STATS cnt = COUNT(*) BY emp_no NOT IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [STATS cnt = COUNT(*) BY emp_no NOT IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that an IN subquery in LIMIT BY clause is rejected.
     */
    public void testRejectsInSubqueryInLimitBy() {
        errorInSubquery("""
            FROM test
            | SORT emp_no
            | LIMIT 10 BY emp_no IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [LIMIT 10 BY emp_no IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that a NOT IN subquery in LIMIT BY clause is rejected.
     */
    public void testRejectsNotInSubqueryInLimitBy() {
        errorInSubquery("""
            FROM test
            | SORT emp_no
            | LIMIT 10 BY emp_no NOT IN (FROM employees | KEEP emp_no)
            """, containsString("IN subquery is not supported in [LIMIT 10 BY emp_no NOT IN (FROM employees | KEEP emp_no)]"));
    }

    /**
     * Verifies that an IN subquery inside EVAL with multiple fields (one being the IN subquery) is rejected.
     */
    public void testRejectsInSubqueryInEvalAmongMultipleFields() {
        errorInSubquery(
            """
                FROM test
                | EVAL a = 1, is_match = emp_no IN (FROM employees | KEEP emp_no), b = salary
                """,
            containsString("IN subquery is not supported in [EVAL a = 1, is_match = emp_no IN (FROM employees | KEEP emp_no), b = salary]")
        );
    }

    /**
     * Verifies that an IN subquery as a function argument inside EVAL is rejected.
     * The InSubquery inside COALESCE is unresolved, and the verifier reports
     * that IN/NOT IN subquery is not supported in Eval.
     */
    public void testRejectsInSubqueryAsFunctionArgInEval() {
        errorInSubquery(
            """
                FROM test
                | EVAL result = COALESCE(emp_no IN (FROM employees | KEEP emp_no), false)
                """,
            containsString("IN subquery is not supported in [EVAL result = COALESCE(emp_no IN (FROM employees | KEEP emp_no), false)]")
        );
    }

    // -- IN subquery nested in WHERE expressions --

    /**
     * Verifies that an IN subquery nested inside a CASE function in WHERE is rejected.
     * The analyzer cannot extract InSubquery from inside a function call.
     */
    public void testRejectsInSubqueryInCaseFunctionInWhere() {
        errorInSubquery(
            """
                FROM test
                | WHERE CASE(emp_no IN (FROM employees | KEEP emp_no), true, false)
                """,
            containsString(
                "IN subquery is not supported within other expressions [CASE(emp_no IN (FROM employees | KEEP emp_no), true, false)]"
            )
        );
    }

    /**
     * Verifies that an IN subquery wrapped in IS NOT NULL in WHERE is rejected.
     * The analyzer cannot extract InSubquery from inside IS NULL expressions.
     */
    public void testRejectsInSubqueryInIsNullInWhere() {
        errorInSubquery(
            """
                FROM test
                | WHERE (emp_no IN (FROM employees | KEEP emp_no)) IS NOT NULL
                """,
            containsString("IN subquery is not supported within other expressions [(emp_no IN (FROM employees | KEEP emp_no)) IS NOT NULL]")
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    @Override
    protected boolean enableWarningsCheck() {
        // Some tests call Approximation.verifyPlan which adds header warnings that can't be consumed in unit tests
        return false;
    }

    // -- IN subquery with views --

    /**
     * IN subquery references a view: {@code WHERE emp_no IN (FROM high_earners)}
     * where {@code high_earners} is defined as {@code FROM employees | WHERE salary > 70000 | KEEP emp_no}.
     * The iterative resolution first resolves the InSubquery to SemiJoin (exposing the view reference),
     * then resolves the view in the next iteration.
     */
    public void testInSubqueryReferencingView() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no")
            .query("FROM test | WHERE emp_no IN (FROM high_earners)");

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Right side: the view was expanded to Project -> Filter[salary > 70000] -> EsRelation[employees]
        Project rightProject = as(semiJoin.right(), Project.class);
        Filter rightFilter = as(rightProject.child(), Filter.class);
        GreaterThan gt = as(rightFilter.condition(), GreaterThan.class);
        FieldAttribute salary = as(gt.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        EsRelation rightRelation = as(rightFilter.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    /**
     * View definition contains an IN subquery: the view {@code filtered_emps} is defined as
     * {@code FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no, salary}.
     * The iterative resolution first expands the view (introducing the InSubquery), then resolves it.
     */
    public void testViewContainingInSubquery() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("filtered_emps", "FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no, salary")
            .query("FROM filtered_emps | WHERE salary > 50000 | KEEP emp_no");

        Limit limit = as(plan, Limit.class);
        Project topProject = as(limit.child(), Project.class);
        Filter filter = as(topProject.child(), Filter.class);
        GreaterThan gt = as(filter.condition(), GreaterThan.class);
        FieldAttribute salary = as(gt.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());

        // Below the filter: SemiJoin from the view's IN subquery
        Project viewProject = as(filter.child(), Project.class);
        SemiJoin semiJoin = as(viewProject.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation semiLeft = as(semiJoin.left(), EsRelation.class);
        assertEquals("employees", semiLeft.indexPattern());
    }

    /**
     * NOT IN subquery references a view: {@code WHERE emp_no NOT IN (FROM high_earners)}.
     */
    public void testNotInSubqueryReferencingView() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no")
            .query("FROM test | WHERE emp_no NOT IN (FROM high_earners)");

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(antiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(antiJoin.right(), Project.class);
        Filter rightFilter = as(rightProject.child(), Filter.class);
        EsRelation rightRelation = as(rightFilter.child(), EsRelation.class);
        assertEquals("employees", rightRelation.indexPattern());
    }

    /**
     * IN subquery references a view whose definition itself contains an IN subquery.
     * View {@code top3_emps} is defined as {@code FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no, salary}.
     * Query: {@code FROM test | WHERE emp_no IN (FROM top3_emps | WHERE salary > 50000 | KEEP emp_no)}
     * <p>
     * Requires 3 iterations of the ViewAndInSubqueryResolver loop:
     * <ol>
     *   <li>InSubqueryResolver converts the outer IN subquery to SemiJoin, exposing the view reference</li>
     *   <li>ViewResolver expands {@code top3_emps}, introducing the view's IN subquery;
     *       InSubqueryResolver converts the inner IN subquery to SemiJoin</li>
     *   <li>Fixed point — no more changes</li>
     * </ol>
     */
    public void testInSubqueryReferencingViewWithInSubquery() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("top3_emps", "FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no, salary")
            .query("FROM test | WHERE emp_no IN (FROM top3_emps | WHERE salary > 50000 | KEEP emp_no)");

        // Outer: Limit -> SemiJoin[emp_no]
        Limit limit = as(plan, Limit.class);
        SemiJoin outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(outerSemiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(outerSemiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Right side: Project -> Filter[salary > 50000] -> Project -> inner SemiJoin
        // (the view's KEEP + the query's WHERE + the view's IN subquery)
        Project rightProject = as(outerSemiJoin.right(), Project.class);
        Filter rightFilter = as(rightProject.child(), Filter.class);
        GreaterThan gt = as(rightFilter.condition(), GreaterThan.class);
        FieldAttribute salary = as(gt.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());

        Project viewProject = as(rightFilter.child(), Project.class);
        SemiJoin innerSemiJoin = as(viewProject.child(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(innerSemiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation innerLeft = as(innerSemiJoin.left(), EsRelation.class);
        assertEquals("employees", innerLeft.indexPattern());

        Project innerRight = as(innerSemiJoin.right(), Project.class);
        EsRelation innerRightRel = as(innerRight.child(), EsRelation.class);
        assertEquals("test", innerRightRel.indexPattern());
    }

    /**
     * NOT IN subquery references a view whose definition contains an IN subquery.
     * View {@code filtered_emps} is defined as
     * {@code FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no}.
     * Query: {@code FROM test | WHERE emp_no NOT IN (FROM filtered_emps)}
     */
    public void testNotInSubqueryReferencingViewWithInSubquery() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("filtered_emps", "FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no")
            .query("FROM test | WHERE emp_no NOT IN (FROM filtered_emps)");

        Limit limit = as(plan, Limit.class);
        AntiJoin outerAntiJoin = as(limit.child(), AntiJoin.class);
        assertThat(outerAntiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(outerAntiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(outerAntiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(outerAntiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Right side: the view was expanded, containing an inner SemiJoin from the view's IN subquery
        Project rightProject = as(outerAntiJoin.right(), Project.class);
        SemiJoin innerSemiJoin = as(rightProject.child(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        EsRelation innerLeft = as(innerSemiJoin.left(), EsRelation.class);
        assertEquals("employees", innerLeft.indexPattern());
    }

    /**
     * IN subquery references a view, combined with a regular predicate.
     * View {@code in_sub_view} is defined as
     * {@code FROM employees | WHERE salary IN (FROM test | KEEP salary) | KEEP emp_no}.
     * Query: {@code FROM test | WHERE salary > 50000 AND emp_no IN (FROM in_sub_view)}
     */
    public void testInSubqueryReferencingViewWithInSubqueryAndPredicate() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("in_sub_view", "FROM employees | WHERE salary IN (FROM test | KEEP salary) | KEEP emp_no")
            .query("FROM test | WHERE salary > 50000 AND emp_no IN (FROM in_sub_view)");

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        // Left side: Filter[salary > 50000] -> EsRelation[test]
        Filter leftFilter = as(semiJoin.left(), Filter.class);
        GreaterThan gt = as(leftFilter.condition(), GreaterThan.class);
        FieldAttribute salary = as(gt.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        EsRelation leftRelation = as(leftFilter.child(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Right side: view expanded with inner SemiJoin from the view's salary IN subquery
        Project rightProject = as(semiJoin.right(), Project.class);
        SemiJoin innerSemiJoin = as(rightProject.child(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));

        EsRelation innerLeft = as(innerSemiJoin.left(), EsRelation.class);
        assertEquals("employees", innerLeft.indexPattern());
        Project innerRight = as(innerSemiJoin.right(), Project.class);
        EsRelation innerRightRel = as(innerRight.child(), EsRelation.class);
        assertEquals("test", innerRightRel.indexPattern());
    }

    /**
     * Two IN subqueries: one references a view, the other references a FROM subquery.
     * View {@code high_earners} is {@code FROM employees | WHERE salary > 70000 | KEEP emp_no}.
     * Query:
     * {@code FROM test | WHERE emp_no IN (FROM high_earners) AND salary IN (FROM (FROM employees | KEEP salary) | KEEP salary)}
     */
    public void testMultipleInSubqueriesWithViewAndFromSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no")
            .query("""
                FROM test
                | WHERE emp_no IN (FROM high_earners)
                    AND salary IN (FROM (FROM employees | KEEP salary) | KEEP salary)
                """);

        Limit limit = as(plan, Limit.class);
        // Two stacked SemiJoins: salary IN on top, emp_no IN below
        SemiJoin outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(outerSemiJoin.config().rightFields().get(0).name(), equalTo("salary"));

        EsRelation outerRightRelation = as(unwrapProjects(outerSemiJoin.right()), EsRelation.class);
        assertEquals("employees", outerRightRelation.indexPattern());

        SemiJoin innerSemiJoin = as(outerSemiJoin.left(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(innerSemiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        // Inner left: EsRelation[test]
        EsRelation leftRelation = as(innerSemiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Inner right: the view expanded to Project -> Filter[salary > 70000] -> EsRelation[employees]
        Project innerRight = as(innerSemiJoin.right(), Project.class);
        Filter viewFilter = as(innerRight.child(), Filter.class);
        GreaterThan gt = as(viewFilter.condition(), GreaterThan.class);
        FieldAttribute salary = as(gt.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        EsRelation viewRelation = as(viewFilter.child(), EsRelation.class);
        assertEquals("employees", viewRelation.indexPattern());
    }

    /**
     * IN subquery references a view, NOT IN subquery references a FROM subquery.
     * View {@code high_earners} is {@code FROM employees | WHERE salary > 70000 | KEEP emp_no}.
     * Query:
     * {@code FROM test | WHERE emp_no IN (FROM high_earners) AND emp_no NOT IN (FROM (FROM test | KEEP emp_no)
     * | WHERE emp_no > 10050 | KEEP emp_no)}
     */
    public void testInViewAndNotInFromSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no")
            .query("""
                FROM test
                | WHERE emp_no IN (FROM high_earners)
                    AND emp_no NOT IN (FROM (FROM test | KEEP emp_no) | WHERE emp_no > 10050 | KEEP emp_no)
                """);

        Limit limit = as(plan, Limit.class);
        // AntiJoin on top (NOT IN), SemiJoin below (IN view)
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        Project outerProject = as(antiJoin.right(), Project.class);
        Filter outerFilter = as(outerProject.child(), Filter.class);
        GreaterThan gt = as(outerFilter.condition(), GreaterThan.class);
        FieldAttribute emp_no = as(gt.left(), FieldAttribute.class);
        assertEquals("emp_no", emp_no.name());
        outerProject = as(outerFilter.child(), Project.class);
        EsRelation outerRelation = as(outerProject.child(), EsRelation.class);
        assertEquals("test", outerRelation.indexPattern());

        SemiJoin semiJoin = as(antiJoin.left(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // SemiJoin right: view expanded
        Project viewProject = as(semiJoin.right(), Project.class);
        Filter viewFilter = as(viewProject.child(), Filter.class);
        EsRelation viewRelation = as(viewFilter.child(), EsRelation.class);
        assertEquals("employees", viewRelation.indexPattern());
    }

    /**
     * Two IN subqueries, each referencing a different view, both views contain IN subqueries.
     * View {@code view_a} is {@code FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no}.
     * View {@code view_b} is {@code FROM employees | WHERE salary IN (FROM test | KEEP salary) | KEEP salary}.
     * Query: {@code FROM test | WHERE emp_no IN (FROM view_a) AND salary IN (FROM view_b)}
     */
    public void testMultipleInSubqueriesReferencingViewsWithInSubqueries() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("view_a", "FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no")
            .addView("view_b", "FROM employees | WHERE salary IN (FROM test | KEEP salary) | KEEP salary")
            .query("FROM test | WHERE emp_no IN (FROM view_a) AND salary IN (FROM view_b)");

        Limit limit = as(plan, Limit.class);
        // Two stacked SemiJoins
        SemiJoin outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(outerSemiJoin.config().rightFields().get(0).name(), equalTo("salary"));

        SemiJoin innerSemiJoin = as(outerSemiJoin.left(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(innerSemiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(innerSemiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Inner right: view_a expanded, containing a SemiJoin from the view's IN subquery
        Project viewAProject = as(innerSemiJoin.right(), Project.class);
        SemiJoin viewASemiJoin = as(viewAProject.child(), SemiJoin.class);
        assertThat(viewASemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        EsRelation viewALeft = as(viewASemiJoin.left(), EsRelation.class);
        assertEquals("employees", viewALeft.indexPattern());

        // Outer right: view_b expanded, containing a SemiJoin from the view's IN subquery
        Project viewBProject = as(outerSemiJoin.right(), Project.class);
        SemiJoin viewBSemiJoin = as(viewBProject.child(), SemiJoin.class);
        assertThat(viewBSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));
        EsRelation viewBLeft = as(viewBSemiJoin.left(), EsRelation.class);
        assertEquals("employees", viewBLeft.indexPattern());
    }

    /**
     * View whose definition nests an IN subquery inside another IN subquery (same shape as
     * {@code employees_in_subquery_nested} in csv tests): employees filtered by {@code emp_no IN}
     * a subquery that itself restricts {@code languages} via {@code IN (1, 2)}.
     * Query: {@code FROM test | WHERE emp_no IN (FROM nested_in_view | KEEP emp_no)}.
     */
    public void testInSubqueryReferencingViewWithNestedInSubqueryInDefinition() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("nested_in_view", """
                FROM employees
                | WHERE emp_no IN (
                    FROM employees
                    | WHERE languages IN (1, 2)
                    | SORT emp_no ASC
                    | LIMIT 10
                    | KEEP emp_no
                  )
                | KEEP emp_no
                """)
            .query("FROM test | WHERE emp_no IN (FROM nested_in_view | KEEP emp_no)");

        Limit limit = as(plan, Limit.class);
        SemiJoin outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(outerSemiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation outerLeft = as(outerSemiJoin.left(), EsRelation.class);
        assertEquals("test", outerLeft.indexPattern());

        // View expansion: SemiJoin over emp_no; inner subquery uses Filter IN (languages) not a second SemiJoin.
        SemiJoin innerSemiJoin = as(unwrapProjects(outerSemiJoin.right()), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(innerSemiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation innerLeft = as(innerSemiJoin.left(), EsRelation.class);
        assertEquals("employees", innerLeft.indexPattern());
    }

    /**
     * Conjunction view (like {@code employees_in_subquery_conjunction}): {@code emp_no IN (subquery)}
     * {@code AND languages IN (subquery)}.
     * Query: {@code FROM test | WHERE emp_no IN (FROM conj_in_view | KEEP emp_no)}.
     */
    public void testInSubqueryReferencingConjunctionViewWithTwoInSubqueriesInDefinition() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("conj_in_view", """
                FROM employees
                | WHERE emp_no IN (FROM test | SORT emp_no ASC | LIMIT 3 | KEEP emp_no)
                    AND languages IN (FROM test | KEEP languages)
                | KEEP emp_no
                """)
            .query("FROM test | WHERE emp_no IN (FROM conj_in_view | KEEP emp_no)");

        Limit limit = as(plan, Limit.class);
        SemiJoin outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        // Conjunction view: stacked SemiJoins (languages IN ... on top of emp_no IN ...).
        SemiJoin languagesSemi = as(unwrapProjects(outerSemiJoin.right()), SemiJoin.class);
        assertThat(languagesSemi.config().leftFields().get(0).name(), equalTo("languages"));
        SemiJoin empNoSemi = as(languagesSemi.left(), SemiJoin.class);
        assertThat(empNoSemi.config().leftFields().get(0).name(), equalTo("emp_no"));
        EsRelation innerLeft = as(empNoSemi.left(), EsRelation.class);
        assertEquals("employees", innerLeft.indexPattern());
    }

    /**
     * Three IN subqueries on {@code emp_no}, each referencing a different view; every view body contains
     * an IN subquery (same idea as {@code threeInSubqueriesIntersectNestedConjunctionDisjunctionViews}).
     */
    public void testThreeInSubqueriesIntersectingViewsEachWithInnerInSubquery() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("v_nested", """
                FROM employees
                | WHERE emp_no IN (
                    FROM employees
                    | WHERE languages IN (1, 2)
                    | SORT emp_no ASC
                    | LIMIT 10
                    | KEEP emp_no
                  )
                | KEEP emp_no
                """)
            .addView("v_conj", """
                FROM employees
                | WHERE emp_no IN (FROM test | SORT emp_no ASC | LIMIT 3 | KEEP emp_no)
                    AND languages IN (FROM test | KEEP languages)
                | KEEP emp_no
                """)
            .addView("v_disj", """
                FROM employees
                | WHERE emp_no IN (FROM test | KEEP emp_no)
                    OR languages IN (1, 2)
                | KEEP emp_no
                """)
            .query("""
                FROM test
                | WHERE emp_no IN (FROM v_nested | KEEP emp_no)
                    AND emp_no IN (FROM v_conj | KEEP emp_no)
                    AND emp_no IN (FROM v_disj | KEEP emp_no)
                """);

        Limit limit = as(plan, Limit.class);
        // Three stacked SemiJoins on emp_no (order: outer ... inner)
        SemiJoin outer = as(limit.child(), SemiJoin.class);
        assertThat(outer.config().leftFields().get(0).name(), equalTo("emp_no"));

        UnionAllFromDisjunctiveInSubquery unionAll = as(unwrapProjects(outer.right()), UnionAllFromDisjunctiveInSubquery.class);
        assertEquals(2, unionAll.children().size());
        Project project = as(unionAll.children().get(0), Project.class);
        Filter filter = as(project.child(), Filter.class);
        In in = as(filter.condition(), In.class);
        assertEquals(2, in.list().size());
        as(in.value(), FieldAttribute.class);
        project = as(unionAll.children().get(1), Project.class);
        SemiJoin viewIn = as(project.child(), SemiJoin.class);
        filter = as(viewIn.left(), Filter.class);
        Not not = as(filter.condition(), Not.class);
        as(not.field(), In.class);
        EsRelation viewRelation = as(filter.child(), EsRelation.class);
        assertEquals("employees", viewRelation.indexPattern());
        project = as(viewIn.right(), Project.class);
        EsRelation viewInRelation = as(project.child(), EsRelation.class);
        assertEquals("test", viewInRelation.indexPattern());

        SemiJoin middle = as(outer.left(), SemiJoin.class);
        assertThat(middle.config().leftFields().get(0).name(), equalTo("emp_no"));
        SemiJoin inner = as(middle.left(), SemiJoin.class);
        assertThat(inner.config().leftFields().get(0).name(), equalTo("emp_no"));
        EsRelation innerLeft = as(inner.left(), EsRelation.class);
        assertEquals("test", innerLeft.indexPattern());
    }

    /**
     * Two IN subqueries and one NOT IN subquery referencing views whose definitions contain IN subqueries
     * (same structure as {@code inNestedAndDisjunctionNotInConjunctionViews}).
     */
    public void testInSubqueryInSubqueryNotInSubqueryReferencingViewsWithInnerInSubqueries() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("v_nested", """
                FROM employees
                | WHERE emp_no IN (
                    FROM employees
                    | WHERE languages IN (1, 2)
                    | SORT emp_no ASC
                    | LIMIT 10
                    | KEEP emp_no
                  )
                | KEEP emp_no
                """)
            .addView("v_disj", """
                FROM employees
                | WHERE emp_no IN (FROM test | KEEP emp_no)
                    OR languages IN (1, 2)
                | KEEP emp_no
                """)
            .addView("v_conj", "FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no")
            .query("""
                FROM test
                | WHERE emp_no IN (FROM v_nested | KEEP emp_no)
                    AND emp_no IN (FROM v_disj | KEEP emp_no)
                    AND emp_no NOT IN (FROM v_conj | KEEP emp_no)
                """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        SemiJoin middleSemi = as(antiJoin.left(), SemiJoin.class);
        assertThat(middleSemi.config().leftFields().get(0).name(), equalTo("emp_no"));
        SemiJoin innerSemi = as(middleSemi.left(), SemiJoin.class);
        assertThat(innerSemi.config().leftFields().get(0).name(), equalTo("emp_no"));

        EsRelation testForNestedAndDisj = as(innerSemi.left(), EsRelation.class);
        assertEquals("test", testForNestedAndDisj.indexPattern());
        assertExpandedVNestedInSubquerySemiJoin(as(unwrapProjects(innerSemi.right()), SemiJoin.class));

        assertExpandedVDisjUnionAllFromDisjunctiveInSubquery(
            as(unwrapProjects(middleSemi.right()), UnionAllFromDisjunctiveInSubquery.class)
        );

        SemiJoin conjSemi = as(unwrapProjects(antiJoin.right()), SemiJoin.class);
        assertExpandedVConjViewSemiJoin(conjSemi);
    }

    /**
     * NOT IN, IN, and NOT IN subqueries referencing views with inner IN subqueries (same idea as
     * {@code notInNestedInDisjunctionNotInConjunctionViews}).
     */
    public void testNotInNestedInDisjunctionNotInConjunctionViews() {
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("v_nested", """
                FROM employees
                | WHERE emp_no IN (
                    FROM employees
                    | WHERE languages IN (1, 2)
                    | SORT emp_no ASC
                    | LIMIT 10
                    | KEEP emp_no
                  )
                | KEEP emp_no
                """)
            .addView("v_disj", """
                FROM employees
                | WHERE emp_no IN (FROM test | KEEP emp_no)
                    OR languages IN (1, 2)
                | KEEP emp_no
                """)
            .addView("v_conj", "FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no")
            .query("""
                FROM test
                | WHERE emp_no NOT IN (FROM v_nested | KEEP emp_no)
                    AND emp_no IN (FROM v_disj | KEEP emp_no)
                    AND emp_no NOT IN (FROM v_conj | KEEP emp_no)
                """);

        Limit limit = as(plan, Limit.class);
        AntiJoin outerAnti = as(limit.child(), AntiJoin.class);
        assertThat(outerAnti.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(outerAnti.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(outerAnti.config().rightFields().get(0).name(), equalTo("emp_no"));

        SemiJoin inDisj = as(outerAnti.left(), SemiJoin.class);
        assertThat(inDisj.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(inDisj.config().leftFields().get(0).name(), equalTo("emp_no"));

        AntiJoin notInNested = as(inDisj.left(), AntiJoin.class);
        assertThat(notInNested.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(notInNested.config().leftFields().get(0).name(), equalTo("emp_no"));
        EsRelation testForNested = as(notInNested.left(), EsRelation.class);
        assertEquals("test", testForNested.indexPattern());
        assertExpandedVNestedInSubquerySemiJoin(as(unwrapProjects(notInNested.right()), SemiJoin.class));

        assertExpandedVDisjUnionAllFromDisjunctiveInSubquery(as(unwrapProjects(inDisj.right()), UnionAllFromDisjunctiveInSubquery.class));

        SemiJoin conjSemi = as(unwrapProjects(outerAnti.right()), SemiJoin.class);
        assertExpandedVConjViewSemiJoin(conjSemi);
    }

    // -- IN subquery with UnionAll (FROM view, (FROM subquery)) --

    /**
     * IN subquery whose FROM combines a view and a FROM subquery via UnionAll:
     * {@code WHERE emp_no IN (FROM employees_view, (FROM employees | WHERE salary > 70000) | KEEP emp_no)}
     * View {@code employees_view} is {@code FROM employees | WHERE salary > 60000 | KEEP emp_no}.
     * The subquery plan is a UnionAll of the view expansion and the FROM subquery.
     */
    public void testInSubqueryWithUnionAllOfViewAndFromSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("employees_view", "FROM employees | WHERE salary > 60000 | KEEP emp_no")
            .query("""
                FROM test
                | WHERE emp_no IN (FROM employees_view, (FROM employees | WHERE salary > 70000) | KEEP emp_no)
                """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Right side: Project -> UnionAll[view expansion, FROM subquery]
        Project rightProject = as(semiJoin.right(), Project.class);
        UnionAll unionAll = as(rightProject.child(), UnionAll.class);
        assertUnionAllOfSalaryFilteredViewAndFromEmployeesSalarySubquery(unionAll, 60000, 70000);
    }

    /**
     * NOT IN subquery whose FROM combines a view and a FROM subquery:
     * {@code WHERE emp_no NOT IN (FROM employees_view, (FROM test | KEEP emp_no) | KEEP emp_no)}
     */
    public void testNotInSubqueryWithUnionAllOfViewAndFromSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("employees_view", "FROM employees | WHERE salary > 60000 | KEEP emp_no")
            .query("""
                FROM test
                | WHERE emp_no NOT IN (FROM employees_view, (FROM test | KEEP emp_no) | KEEP emp_no)
                """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(antiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(antiJoin.right(), Project.class);
        UnionAll unionAll = as(rightProject.child(), UnionAll.class);
        assertUnionAllOfSalaryFilteredViewAndFromTestSubquery(unionAll, 60000);
    }

    /**
     * Two IN subqueries, each with a UnionAll FROM combining a view and a FROM subquery:
     * {@code FROM test | WHERE emp_no IN (FROM view_a, (FROM test | KEEP emp_no) | KEEP emp_no)
     *                  AND salary IN (FROM view_b, (FROM employees | KEEP salary) | KEEP salary)}
     */
    public void testMultipleInSubqueriesWithUnionAllViewAndFromSubquery() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("view_a", "FROM employees | KEEP emp_no")
            .addView("view_b", "FROM employees | KEEP salary")
            .query("""
                FROM test
                | WHERE emp_no IN (FROM view_a, (FROM test | KEEP emp_no) | KEEP emp_no)
                    AND salary IN (FROM view_b, (FROM employees | KEEP salary) | KEEP salary)
                """);

        Limit limit = as(plan, Limit.class);
        // Two stacked SemiJoins
        SemiJoin outerSemiJoin = as(limit.child(), SemiJoin.class);
        assertThat(outerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(outerSemiJoin.config().leftFields().get(0).name(), equalTo("salary"));

        SemiJoin innerSemiJoin = as(outerSemiJoin.left(), SemiJoin.class);
        assertThat(innerSemiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(innerSemiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        EsRelation leftRelation = as(innerSemiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Inner right: UnionAll from view_a and FROM subquery
        Project innerRight = as(innerSemiJoin.right(), Project.class);
        UnionAll innerUnionAll = as(innerRight.child(), UnionAll.class);
        assertUnionAllOfPlainEmployeesViewAndFromTestEmpNoSubquery(innerUnionAll);

        Project outerRight = as(outerSemiJoin.right(), Project.class);
        UnionAll outerUnionAll = as(outerRight.child(), UnionAll.class);
        assertUnionAllOfPlainEmployeesSalaryAndFromEmployeesSalarySubquery(outerUnionAll);
    }

    /**
     * IN and NOT IN subqueries, one with UnionAll FROM, the other with a plain view:
     * {@code FROM test | WHERE emp_no IN (FROM view_a, (FROM test | KEEP emp_no) | KEEP emp_no)
     *                  AND emp_no NOT IN (FROM high_earners)}
     */
    public void testInSubqueryUnionAllAndNotInSubqueryView() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("view_a", "FROM employees | KEEP emp_no")
            .addView("high_earners", "FROM employees | WHERE salary > 70000 | KEEP emp_no")
            .query("""
                FROM test
                | WHERE emp_no IN (FROM view_a, (FROM test | KEEP emp_no) | KEEP emp_no)
                    AND emp_no NOT IN (FROM high_earners)
                """);

        Limit limit = as(plan, Limit.class);
        // AntiJoin (NOT IN view) on top, SemiJoin (IN union) below
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        // AntiJoin right: view high_earners expanded
        Project antiRight = as(antiJoin.right(), Project.class);
        Filter antiRightFilter = as(antiRight.child(), Filter.class);
        GreaterThan gt = as(antiRightFilter.condition(), GreaterThan.class);
        FieldAttribute salary = as(gt.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        assertEquals(70000, as(gt.right(), Literal.class).value());
        EsRelation antiRightRelation = as(antiRightFilter.child(), EsRelation.class);
        assertEquals("employees", antiRightRelation.indexPattern());

        // AntiJoin left: SemiJoin (IN union)
        SemiJoin semiJoin = as(antiJoin.left(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));

        EsRelation semiLeft = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", semiLeft.indexPattern());

        // SemiJoin right: UnionAll from view_a and FROM subquery
        Project semiRight = as(semiJoin.right(), Project.class);
        UnionAll unionAll = as(semiRight.child(), UnionAll.class);
        assertUnionAllOfPlainEmployeesViewAndFromTestEmpNoSubquery(unionAll);
    }

    // -- helpers --

    private static LogicalPlan analyzeInSubquery(String query) {
        return analyzer().addIndex("test", "mapping-basic.json").addIndex("employees", "mapping-basic.json").query(query);
    }

    private static void errorInSubquery(String query, Matcher<String> messageMatcher) {
        analyzer().addIndex("test", "mapping-basic.json").addIndex("employees", "mapping-basic.json").error(query, messageMatcher);
    }

    private static void errorWithK8s(String query, Matcher<String> messageMatcher) {
        analyzer().addK8s().error(query, messageMatcher);
    }

    private static void errorWithK8sDownsampled(String query, Matcher<String> messageMatcher) {
        analyzer().addK8sDownsampled().error(query, messageMatcher);
    }

    private static LogicalPlan analyzeWithAllTypes(String query) {
        return analyzer().addIndex("all_types", "mapping-all-types.json").query(query);
    }

    private static void errorWithAllTypes(String query, Matcher<String> messageMatcher) {
        analyzer().addIndex("all_types", "mapping-all-types.json").error(query, messageMatcher);
    }

    private static LogicalPlan analyzeWithIncompatible(String query) {
        return analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees_incompatible", "mapping-default-incompatible.json")
            .query(query);
    }

    private static void errorWithIncompatible(String query, Matcher<String> messageMatcher) {
        analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees_incompatible", "mapping-default-incompatible.json")
            .error(query, messageMatcher);
    }

    private static IndexResolution unionIndexResolution() {
        LinkedHashMap<String, Set<String>> typesToIndices = new LinkedHashMap<>();
        typesToIndices.put("keyword", Set.of("union_index_1"));
        typesToIndices.put("integer", Set.of("union_index_2"));
        EsField idField = new InvalidMappedField("id", typesToIndices);
        EsField nameField = new EsField("name", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        EsIndex index = new EsIndex(
            "union_index*",
            Map.of("id", idField, "name", nameField),
            Map.of("union_index_1", IndexMode.STANDARD, "union_index_2", IndexMode.STANDARD),
            Map.of(),
            Map.of()
        );
        return IndexResolution.valid(index);
    }

    private static LogicalPlan analyzeWithUnionIndex(String query) {
        return analyzer().addIndex("test", "mapping-basic.json").addIndex(unionIndexResolution()).query(query);
    }

    private static void errorWithUnionIndex(String query, Matcher<String> messageMatcher) {
        analyzer().addIndex("test", "mapping-basic.json").addIndex(unionIndexResolution()).error(query, messageMatcher);
    }

    private void assertApproximationRejects(String query) {
        LogicalPlan plan = analyzeInSubquery(query);
        // verifyPlan returns null when the plan is incompatible with approximation (and adds a warning)
        assertThat("Approximation should reject this query", Approximation.verifyPlan(plan), nullValue());
    }

    /** Strip leading Project nodes introduced when expanding views. */
    private static LogicalPlan unwrapProjects(LogicalPlan plan) {
        LogicalPlan p = plan;
        while (p instanceof Project) {
            p = p.children().get(0);
        }
        return p;
    }

    /**
     * View {@code v_nested}: {@code emp_no IN (FROM employees | WHERE languages IN (1,2) | SORT | LIMIT | KEEP emp_no)} expanded as a
     * SemiJoin over {@code employees} with the subquery branch {@code OrderBy -> Filter(IN languages) -> EsRelation[employees]}.
     */
    private static void assertExpandedVNestedInSubquerySemiJoin(SemiJoin nestedSemi) {
        assertThat(nestedSemi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(nestedSemi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(nestedSemi.config().rightFields().get(0).name(), equalTo("emp_no"));
        EsRelation nestedLeft = as(nestedSemi.left(), EsRelation.class);
        assertEquals("employees", nestedLeft.indexPattern());
        Project nestedRightProject = as(nestedSemi.right(), Project.class);
        Limit limit = as(nestedRightProject.child(), Limit.class);
        assertEquals(10, as(limit.limit(), Literal.class).value());
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        assertEquals(1, orderBy.order().size());
        Order empNoOrder = orderBy.order().get(0);
        assertEquals("emp_no", as(empNoOrder.child(), FieldAttribute.class).name());
        assertThat(empNoOrder.direction(), equalTo(Order.OrderDirection.ASC));
        Filter langFilter = as(orderBy.child(), Filter.class);
        In langIn = as(langFilter.condition(), In.class);
        assertEquals(2, langIn.list().size());
        assertEquals(1, as(langIn.list().get(0), Literal.class).value());
        assertEquals(2, as(langIn.list().get(1), Literal.class).value());
        assertEquals("languages", as(langIn.value(), FieldAttribute.class).name());
        EsRelation innerEmployees = as(langFilter.child(), EsRelation.class);
        assertEquals("employees", innerEmployees.indexPattern());
    }

    /**
     * View {@code v_disj}: {@code emp_no IN (FROM test) OR languages IN (1,2)} becomes a
     * {@link UnionAllFromDisjunctiveInSubquery} (value-list branch + IN subquery branch with negated languages predicate).
     */
    private static void assertExpandedVDisjUnionAllFromDisjunctiveInSubquery(UnionAllFromDisjunctiveInSubquery unionAll) {
        assertEquals(2, unionAll.children().size());
        Project languagesBranch = as(unionAll.children().get(0), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, languagesBranch);
        Filter filter = as(languagesBranch.child(), Filter.class);
        In in = as(filter.condition(), In.class);
        assertEquals(2, in.list().size());
        assertEquals(1, as(in.list().get(0), Literal.class).value());
        assertEquals(2, as(in.list().get(1), Literal.class).value());
        assertEquals("languages", as(in.value(), FieldAttribute.class).name());
        EsRelation languagesBranchRelation = as(filter.child(), EsRelation.class);
        assertEquals("employees", languagesBranchRelation.indexPattern());

        Project empNoBranchWrap = as(unionAll.children().get(1), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, empNoBranchWrap);
        SemiJoin viewIn = as(empNoBranchWrap.child(), SemiJoin.class);
        assertThat(viewIn.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(viewIn.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(viewIn.config().rightFields().get(0).name(), equalTo("emp_no"));
        filter = as(viewIn.left(), Filter.class);
        Not not = as(filter.condition(), Not.class);
        In negatedLanguagesIn = as(not.field(), In.class);
        assertEquals("languages", as(negatedLanguagesIn.value(), FieldAttribute.class).name());
        assertEquals(2, negatedLanguagesIn.list().size());
        assertEquals(1, as(negatedLanguagesIn.list().get(0), Literal.class).value());
        assertEquals(2, as(negatedLanguagesIn.list().get(1), Literal.class).value());
        EsRelation viewRelation = as(filter.child(), EsRelation.class);
        assertEquals("employees", viewRelation.indexPattern());
        Project empNoSubqueryProject = as(viewIn.right(), Project.class);
        EsRelation viewInRelation = as(empNoSubqueryProject.child(), EsRelation.class);
        assertEquals("test", viewInRelation.indexPattern());
    }

    /** View {@code v_conj}: {@code FROM employees | WHERE emp_no IN (FROM test | KEEP emp_no) | KEEP emp_no}. */
    private static void assertExpandedVConjViewSemiJoin(SemiJoin conjSemi) {
        assertThat(conjSemi.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(conjSemi.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(conjSemi.config().rightFields().get(0).name(), equalTo("emp_no"));
        EsRelation conjLeft = as(conjSemi.left(), EsRelation.class);
        assertEquals("employees", conjLeft.indexPattern());
        Project conjRight = as(conjSemi.right(), Project.class);
        EsRelation conjRightRelation = as(conjRight.child(), EsRelation.class);
        assertEquals("test", conjRightRelation.indexPattern());
    }

    /**
     * UnionAll from {@code FROM employees_view, (FROM employees | WHERE salary > subqMin)} where the view applies
     * {@code salary > viewMin}; the view branch pads missing columns with {@link Eval} so it unions with the subquery.
     */
    private static void assertUnionAllOfSalaryFilteredViewAndFromEmployeesSalarySubquery(
        UnionAll unionAll,
        int viewMinSalary,
        int subqueryMinSalary
    ) {
        assertEquals(2, unionAll.children().size());
        assertTrue(unionAll.output().size() > 1);
        assertEquals("emp_no", unionAll.output().get(0).name());
        assertEquals(DataType.INTEGER, unionAll.output().get(0).dataType());

        Project viewBranch = as(unionAll.children().get(0), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, viewBranch);
        Eval eval = as(viewBranch.child(), Eval.class);
        assertFalse(eval.fields().isEmpty());
        for (Alias nullPad : eval.fields()) {
            assertNull(as(nullPad.child(), Literal.class).value());
        }
        Project keepEmpNo = as(eval.child(), Project.class);
        assertEquals(1, keepEmpNo.output().size());
        assertEquals("emp_no", keepEmpNo.output().get(0).name());
        assertEquals(DataType.INTEGER, keepEmpNo.output().get(0).dataType());
        assertEquals(eval.output().size(), unionAll.output().size());
        Filter salaryFilter = as(keepEmpNo.child(), Filter.class);
        GreaterThan gt = as(salaryFilter.condition(), GreaterThan.class);
        assertEquals("salary", as(gt.left(), FieldAttribute.class).name());
        assertEquals(viewMinSalary, as(gt.right(), Literal.class).value());
        assertEquals(DataType.INTEGER, as(gt.right(), Literal.class).dataType());
        assertEquals("employees", as(salaryFilter.child(), EsRelation.class).indexPattern());

        Project subBranch = as(unionAll.children().get(1), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, subBranch);
        Subquery sub = as(subBranch.child(), Subquery.class);
        Filter subFilter = as(sub.child(), Filter.class);
        GreaterThan subGt = as(subFilter.condition(), GreaterThan.class);
        assertEquals("salary", as(subGt.left(), FieldAttribute.class).name());
        assertEquals(subqueryMinSalary, as(subGt.right(), Literal.class).value());
        assertEquals(DataType.INTEGER, as(subGt.right(), Literal.class).dataType());
        assertEquals("employees", as(subFilter.child(), EsRelation.class).indexPattern());
    }

    /**
     * UnionAll from {@code FROM employees_view, (FROM test | KEEP emp_no)} with {@code salary > viewMinSalary} on the
     * expanded view branch.
     */
    private static void assertUnionAllOfSalaryFilteredViewAndFromTestSubquery(UnionAll unionAll, int viewMinSalary) {
        assertEquals(2, unionAll.children().size());
        assertUnionAllSingleIntegerColumn(unionAll, "emp_no");

        Project viewBranch = as(unionAll.children().get(0), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, viewBranch);
        assertEquals(1, viewBranch.projections().size());
        Filter salaryFilter = as(viewBranch.child(), Filter.class);
        GreaterThan gt = as(salaryFilter.condition(), GreaterThan.class);
        assertEquals("salary", as(gt.left(), FieldAttribute.class).name());
        assertEquals(viewMinSalary, as(gt.right(), Literal.class).value());
        assertEquals(DataType.INTEGER, as(gt.right(), Literal.class).dataType());
        assertEquals("employees", as(salaryFilter.child(), EsRelation.class).indexPattern());

        Project subBranch = as(unionAll.children().get(1), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, subBranch);
        assertEquals(1, subBranch.projections().size());
        Subquery sub = as(subBranch.child(), Subquery.class);
        Project innerProj = as(sub.child(), Project.class);
        assertEquals(1, innerProj.output().size());
        assertEquals("emp_no", innerProj.output().get(0).name());
        assertEquals(DataType.INTEGER, innerProj.output().get(0).dataType());
        assertEquals("test", as(innerProj.child(), EsRelation.class).indexPattern());
    }

    /** UnionAll from {@code FROM view_a, (FROM test | KEEP emp_no)} with {@code view_a = FROM employees | KEEP emp_no}. */
    private static void assertUnionAllOfPlainEmployeesViewAndFromTestEmpNoSubquery(UnionAll unionAll) {
        assertEquals(2, unionAll.children().size());
        assertUnionAllSingleIntegerColumn(unionAll, "emp_no");

        Project viewBranch = as(unionAll.children().get(0), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, viewBranch);
        assertEquals(1, viewBranch.projections().size());
        EsRelation viewEmployees = as(viewBranch.child(), EsRelation.class);
        assertEquals("employees", viewEmployees.indexPattern());

        Project subBranch = as(unionAll.children().get(1), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, subBranch);
        assertEquals(1, subBranch.projections().size());
        Subquery sub = as(subBranch.child(), Subquery.class);
        Project innerProj = as(sub.child(), Project.class);
        assertEquals(1, innerProj.output().size());
        assertEquals("emp_no", innerProj.output().get(0).name());
        assertEquals(DataType.INTEGER, innerProj.output().get(0).dataType());
        assertEquals("test", as(innerProj.child(), EsRelation.class).indexPattern());
    }

    /**
     * UnionAll from {@code FROM view_b, (FROM employees | KEEP salary)} with both branches reading {@code employees}.
     */
    private static void assertUnionAllOfPlainEmployeesSalaryAndFromEmployeesSalarySubquery(UnionAll unionAll) {
        assertEquals(2, unionAll.children().size());
        assertUnionAllSingleIntegerColumn(unionAll, "salary");

        Project viewBranch = as(unionAll.children().get(0), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, viewBranch);
        assertEquals(1, viewBranch.projections().size());
        EsRelation viewEmployees = as(viewBranch.child(), EsRelation.class);
        assertEquals("employees", viewEmployees.indexPattern());

        Project subBranch = as(unionAll.children().get(1), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, subBranch);
        assertEquals(1, subBranch.projections().size());
        Subquery sub = as(subBranch.child(), Subquery.class);
        Project innerProj = as(sub.child(), Project.class);
        assertEquals(1, innerProj.output().size());
        assertEquals("salary", innerProj.output().get(0).name());
        assertEquals(DataType.INTEGER, innerProj.output().get(0).dataType());
        assertEquals("employees", as(innerProj.child(), EsRelation.class).indexPattern());
    }

    /** UnionAll combines branches: each branch {@link Project} must expose the same names and types as the union output. */
    private static void assertUnionAllBranchMatchesUnionOutput(UnionAll unionAll, Project branch) {
        List<Attribute> unionOut = unionAll.output();
        List<Attribute> branchOut = branch.output();
        assertEquals(unionOut.size(), branchOut.size());
        for (int i = 0; i < unionOut.size(); i++) {
            assertEquals(unionOut.get(i).name(), branchOut.get(i).name());
            assertEquals(unionOut.get(i).dataType(), branchOut.get(i).dataType());
        }
    }

    private static void assertUnionAllSingleIntegerColumn(UnionAll unionAll, String columnName) {
        assertEquals(1, unionAll.output().size());
        assertEquals(columnName, unionAll.output().get(0).name());
        assertEquals(DataType.INTEGER, unionAll.output().get(0).dataType());
    }
}
