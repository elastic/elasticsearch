/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.approximation.ApproximationVerifier;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
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
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class AnalyzerInSubqueryTests extends ESTestCase {

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
    }

    private static void requireInSubqueryViewSupport() {
        assumeTrue("Requires IN subquery with view support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
    }

    private static void requireSubqueryInFromSupport() {
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireExternalDatasetSupport() {
        assumeTrue("Requires external dataset in FROM command support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
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

        // No wrapping Project is needed: the Aggregate already pins the {@code max_emp} attribute
        // for InsertFieldExtraction on the data node.
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

        // No wrapping Project is needed: the Aggregate already pins the {@code min_emp} attribute
        // for InsertFieldExtraction on the data node.
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

        // The synthetic Eval column is stripped by a top-level Project added by the analyzer.
        Project topProject = as(plan, Project.class);
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
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
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
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
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
        Limit limit = as(topProject.child(), Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        // The remaining `salary > 50000` filter sits between the SemiJoin and the synthetic Eval that
        // materializes the constant LHS for the IN subquery.
        Filter filter = as(semiJoin.left(), Filter.class);
        GreaterThan greaterThan = as(filter.condition(), GreaterThan.class);
        FieldAttribute salary = as(greaterThan.left(), FieldAttribute.class);
        assertEquals("salary", salary.name());
        Eval eval = as(filter.child(), Eval.class);
        EsRelation leftRelation = as(eval.child(), EsRelation.class);
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
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
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
        requireSubqueryInFromSupport();
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
        requireSubqueryInFromSupport();
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
        requireSubqueryInFromSupport();
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
        requireSubqueryInFromSupport();
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
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
               OR salary > 50000
            """);

        // emp_no NOT IN parses as Not(InSubquery); the outer NOT yields Not(Not(InSubquery)).
        // Inside OR, the InSubquery is replaced by a MarkJoin's mark attribute, leaving the
        // surrounding double-NOT in place.
        Project topProject = as(plan, Project.class);
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
        Limit limit = as(topProject.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        Not outerNot = as(or.left(), Not.class);
        Not innerNot = as(outerNot.field(), Not.class);
        as(innerNot.field(), Attribute.class);
        as(or.right(), GreaterThan.class);

        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertThat(mj.config().type(), equalTo(JoinTypes.MARK));
        assertThat(mj.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(mj.config().rightFields().get(0).name(), equalTo("emp_no"));
        Project subqueryProject = as(mj.right(), Project.class);
        EsRelation subqueryRelation = as(subqueryProject.child(), EsRelation.class);
        assertEquals("employees", subqueryRelation.indexPattern());
        EsRelation main = as(mj.left(), EsRelation.class);
        assertEquals("test", main.indexPattern());
    }

    public void testDoubleNotInSubqueryOrInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE NOT (emp_no NOT IN (FROM employees | KEEP emp_no))
               OR salary IN (FROM employees | KEEP salary)
            """);

        // Both InSubquery operands of OR become MarkJoins; the rewritten Filter references their marks.
        Project topProject = as(plan, Project.class);
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
        Limit limit = as(topProject.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        // emp_no NOT IN inside outer NOT: parses as Not(Not(InSubquery)); the inner InSubquery
        // becomes a MarkJoin mark, leaving the double NOT in place.
        Not outerNot = as(or.left(), Not.class);
        Not innerNot = as(outerNot.field(), Not.class);
        as(innerNot.field(), Attribute.class);
        as(or.right(), Attribute.class);

        MarkJoin salaryJoin = as(filter.child(), MarkJoin.class);
        assertThat(salaryJoin.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(salaryJoin.config().rightFields().get(0).name(), equalTo("salary"));
        Project subqueryProject = as(salaryJoin.right(), Project.class);
        EsRelation subqueryRelation = as(subqueryProject.child(), EsRelation.class);
        assertEquals("employees", subqueryRelation.indexPattern());

        MarkJoin empNoJoin = as(salaryJoin.left(), MarkJoin.class);
        assertThat(empNoJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(empNoJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        subqueryProject = as(empNoJoin.right(), Project.class);
        subqueryRelation = as(subqueryProject.child(), EsRelation.class);
        assertEquals("employees", subqueryRelation.indexPattern());
        EsRelation main = as(empNoJoin.left(), EsRelation.class);
        assertEquals("test", main.indexPattern());
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
        Project subqueryProject = as(semiJoin.right(), Project.class);
        EsRelation subqueryRelation = as(subqueryProject.child(), EsRelation.class);
        assertEquals("employees", subqueryRelation.indexPattern());

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
    //
    // These now produce MarkJoin per InSubquery; each MarkJoin emits a synthetic boolean
    // mark attribute that the rewritten WHERE condition references. The plan shape is:
    // Project (drop marks)
    // Filter (mark1 OR mark2 OR ...) -- referencing the mark attributes
    // MarkJoin (last InSubquery → mark)
    // MarkJoin (...)
    // ...
    // EsRelation
    // This preserves SQL three-valued logic across the disjunction (the previous UnionAll rewrite
    // dropped rows when NULLs were involved).

    public void testDisjunctiveInSubqueries() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR salary IN (FROM employees | KEEP salary)
            """);

        Project topProject = as(plan, Project.class);
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
        Limit limit = as(topProject.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        as(or.left(), Attribute.class);
        as(or.right(), Attribute.class);

        MarkJoin salaryJoin = as(filter.child(), MarkJoin.class);
        assertThat(salaryJoin.config().type(), equalTo(JoinTypes.MARK));
        assertThat(salaryJoin.config().leftFields().get(0).name(), equalTo("salary"));
        assertThat(salaryJoin.config().rightFields().get(0).name(), equalTo("salary"));
        MarkJoin empNoJoin = as(salaryJoin.left(), MarkJoin.class);
        assertThat(empNoJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(empNoJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        EsRelation main = as(empNoJoin.left(), EsRelation.class);
        assertEquals("test", main.indexPattern());
    }

    public void testDisjunctiveInAndNotInSubqueries() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no)
               OR emp_no IN (FROM employees | WHERE salary > 50000 | KEEP emp_no)
            """);

        Project topProject = as(plan, Project.class);
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
        Limit limit = as(topProject.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        Not leftNot = as(or.left(), Not.class);
        as(leftNot.field(), Attribute.class);
        as(or.right(), Attribute.class);

        // Outer MarkJoin for the second IN (right-hand emp_no IN sub2)
        MarkJoin innerJoin = as(filter.child(), MarkJoin.class);
        assertThat(innerJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(innerJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        // Subquery has WHERE salary > 50000
        Project innerRightProject = as(innerJoin.right(), Project.class);
        Filter innerRightFilter = as(innerRightProject.child(), Filter.class);
        as(innerRightFilter.condition(), GreaterThan.class);
        EsRelation subqueryRelation = as(innerRightFilter.child(), EsRelation.class);
        assertEquals("employees", subqueryRelation.indexPattern());

        // Inner MarkJoin for the first NOT IN (which became NOT $mark below).
        MarkJoin outerJoin = as(innerJoin.left(), MarkJoin.class);
        assertThat(outerJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(outerJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        Project outerRightProject = as(outerJoin.right(), Project.class);
        EsRelation outerRightRelation = as(outerRightProject.child(), EsRelation.class);
        assertEquals("employees", outerRightRelation.indexPattern());
        EsRelation main = as(outerJoin.left(), EsRelation.class);
        assertEquals("test", main.indexPattern());
    }

    public void testDisjunctiveInSubqueryWithOtherPredicate() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE salary > 50000
               OR emp_no IN (FROM employees | KEEP emp_no)
            """);

        Project topProject = as(plan, Project.class);
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
        Limit limit = as(topProject.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        as(or.left(), GreaterThan.class);
        as(or.right(), Attribute.class);

        MarkJoin mj = as(filter.child(), MarkJoin.class);
        assertThat(mj.config().type(), equalTo(JoinTypes.MARK));
        assertThat(mj.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(mj.config().rightFields().get(0).name(), equalTo("emp_no"));
        Project innerProject = as(mj.right(), Project.class);
        EsRelation innerRelation = as(innerProject.child(), EsRelation.class);
        assertEquals("employees", innerRelation.indexPattern());
        EsRelation main = as(mj.left(), EsRelation.class);
        assertEquals("test", main.indexPattern());
    }

    // -- disjunctive OR chain with IN/NOT IN subqueries --

    /**
     * {@code WHERE emp_no IN (FROM employees | KEEP emp_no) OR (salary > 50000 OR (languages < 3 OR gender NOT IN (...)))}
     * <p>
     * Both InSubqueries appear under {@code OR}, so each is rewritten to a {@link MarkJoin}
     * with a mark attribute and the entire boolean expression is preserved unchanged in a single
     * Filter on top of the join stack.
     */
    public void testDisjunctiveOrChainWithNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 OR (languages < 3 OR gender NOT IN (FROM employees | KEEP gender)))
            """);

        Project topProject = as(plan, Project.class);
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
        Limit limit = as(topProject.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        as(or.left(), Attribute.class);
        or = as(or.right(), Or.class);
        as(or.left(), GreaterThan.class);
        or = as(or.right(), Or.class);
        as(or.left(), LessThan.class);
        Not not = as(or.right(), Not.class);
        as(not.field(), Attribute.class);
        // Two MarkJoins (emp_no first, gender on top).
        MarkJoin genderJoin = as(filter.child(), MarkJoin.class);
        assertThat(genderJoin.config().type(), equalTo(JoinTypes.MARK));
        assertThat(genderJoin.config().leftFields().get(0).name(), equalTo("gender"));
        assertThat(genderJoin.config().rightFields().get(0).name(), equalTo("gender"));
        MarkJoin empNoJoin = as(genderJoin.left(), MarkJoin.class);
        assertThat(empNoJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(empNoJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        EsRelation main = as(empNoJoin.left(), EsRelation.class);
        assertEquals("test", main.indexPattern());
    }

    /**
     * Inner {@code AND} containing a NOT IN is itself a child of OR — the NOT IN is in boolean
     * position, so it becomes a {@link MarkJoin}. Previous rewrite required a special
     * "complexity 2" disjunct ordering trick; the MarkJoin path handles it uniformly.
     */
    public void testDisjunctiveOrChainWithConjunctiveNotInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 OR (languages < 3 AND gender NOT IN (FROM employees | KEEP gender)))
            """);

        Project topProject = as(plan, Project.class);
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
        Limit limit = as(topProject.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        as(or.left(), Attribute.class);
        or = as(or.right(), Or.class);
        as(or.left(), GreaterThan.class);
        And and = as(or.right(), And.class);
        as(and.left(), LessThan.class);
        Not not = as(and.right(), Not.class);
        as(not.field(), Attribute.class);
        MarkJoin genderJoin = as(filter.child(), MarkJoin.class);
        assertThat(genderJoin.config().leftFields().get(0).name(), equalTo("gender"));
        MarkJoin empNoJoin = as(genderJoin.left(), MarkJoin.class);
        assertThat(empNoJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
    }

    /**
     * NOT IN appears in the middle of the OR chain. With the new rewrite this still produces a
     * single Filter over a stack of two MarkJoins; the order of OR operands does not affect
     * the structural outcome.
     */
    public void testDisjunctiveOrChainWithNotInSubqueryInMiddle() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 OR (gender NOT IN (FROM employees | KEEP gender)) OR languages < 3)
            """);

        Project topProject = as(plan, Project.class);
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
        Limit limit = as(topProject.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        as(or.left(), Attribute.class);
        or = as(or.right(), Or.class);
        as(or.right(), LessThan.class);
        or = as(or.left(), Or.class);
        as(or.left(), GreaterThan.class);
        Not not = as(or.right(), Not.class);
        as(not.field(), Attribute.class);
        MarkJoin genderJoin = as(filter.child(), MarkJoin.class);
        assertThat(genderJoin.config().leftFields().get(0).name(), equalTo("gender"));
        MarkJoin empNoJoin = as(genderJoin.left(), MarkJoin.class);
        assertThat(empNoJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
    }

    /**
     * {@code WHERE emp_no IN (...) OR (salary > 50000 AND (languages < 3 OR gender NOT IN (...)))}
     * <p>
     * Previously rejected as "Complicated IN subquery". The {@code OR}/{@code AND}/{@code NOT}
     * tree above each {@code InSubquery} is all boolean operators, so each becomes a
     * {@link MarkJoin} and the whole condition is evaluated by the standard expression
     * machinery. This preserves SQL three-valued logic.
     */
    public void testNestedConjunctiveAndDisjunctiveInSubquery() {
        LogicalPlan plan = analyzeInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM employees | KEEP emp_no)
               OR (salary > 50000 AND (languages < 3 OR gender NOT IN (FROM employees | KEEP gender)))
            """);

        Project topProject = as(plan, Project.class);
        assertEquals(11, topProject.projections().size());
        assertFalse(topProject.projections().stream().anyMatch(p -> p instanceof Alias a && a.synthetic()));
        Limit limit = as(topProject.child(), Limit.class);
        Filter filter = as(limit.child(), Filter.class);
        Or or = as(filter.condition(), Or.class);
        as(or.left(), Attribute.class);
        And and = as(or.right(), And.class);
        as(and.left(), GreaterThan.class);
        or = as(and.right(), Or.class);
        as(or.left(), LessThan.class);
        Not not = as(or.right(), Not.class);
        as(not.field(), Attribute.class);
        MarkJoin genderJoin = as(filter.child(), MarkJoin.class);
        assertThat(genderJoin.config().leftFields().get(0).name(), equalTo("gender"));
        MarkJoin empNoJoin = as(genderJoin.left(), MarkJoin.class);
        assertThat(empNoJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        EsRelation main = as(empNoJoin.left(), EsRelation.class);
        assertEquals("test", main.indexPattern());
    }

    // data types on join keys related tests

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
        requireSubqueryInFromSupport();
        LogicalPlan plan = analyzeWithIncompatible("""
            FROM test, (FROM employees_incompatible | KEEP emp_no, first_name, salary)
            | EVAL id = emp_no::long
            | WHERE id IN (FROM employees_incompatible | WHERE salary > 70000 | KEEP emp_no)
            | KEEP id
            """);

        Project topProject = as(plan, Project.class);
        assertEquals(1, topProject.projections().size());
        Limit limit = as(topProject.child(), Limit.class);
        topProject = as(limit.child(), Project.class);
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
        requireSubqueryInFromSupport();
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
     * An IN subquery against an index with empty mapping (only the {@code <no-fields>} placeholder)
     * has no real column to compare against. The analyzer should surface a clear error rather than
     * letting the placeholder leak into type-compatibility checking.
     */
    public void testRejectsInSubqueryAgainstIndexWithEmptyMapping() {
        analyzer().addIndex("test", "mapping-basic.json").addEmptyIndex().error("""
            FROM test
            | WHERE emp_no IN (FROM empty_index)
            """, containsString("IN subquery cannot reference an index with empty mapping"));
    }

    /**
     * Same as {@link #testRejectsInSubqueryAgainstIndexWithEmptyMapping}, but for an index whose
     * concrete indices entry exists yet the mapping is still empty (no_fields_index).
     */
    public void testRejectsInSubqueryAgainstNoFieldsIndex() {
        analyzer().addIndex("test", "mapping-basic.json").addNoFieldsIndex().error("""
            FROM test
            | WHERE emp_no IN (FROM no_fields_index)
            """, containsString("IN subquery cannot reference an index with empty mapping"));
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
            """, containsString("IN subquery with right field [network.total_bytes_in] of type [COUNTER_LONG] is not supported"));
    }

    /**
     * Verifies that counter types (COUNTER_DOUBLE) are rejected as IN subquery join keys.
     */
    public void testRejectsCounterDoubleInSubquery() {
        errorWithK8s("""
            FROM k8s
            | WHERE network.total_cost IN (FROM k8s | KEEP network.total_cost)
            """, containsString("IN subquery with right field [network.total_cost] of type [COUNTER_DOUBLE] is not supported"));
    }

    /**
     * Verifies that aggregate_metric_double is rejected as IN subquery join key.
     */
    public void testRejectsAggregateMetricDoubleInSubquery() {
        errorWithK8sDownsampled("""
            FROM k8s
            | WHERE network.eth0.tx IN (FROM k8s | KEEP network.eth0.tx)
            """, containsString("IN subquery with right field [network.eth0.tx] of type [AGGREGATE_METRIC_DOUBLE] is not supported"));
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
        requireSubqueryInFromSupport();
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
        requireSubqueryInFromSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[emp_no]]
     *   \_Filter[salary > 50000[INTEGER]]
     *     \_Project[[emp_no, salary]]
     *       \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *         |_EsRelation[employees]
     *         \_Project[[emp_no]]
     *           \_EsRelation[test]
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   |_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_Filter[salary > 70000[INTEGER]]
     *       \_EsRelation[employees]
     */
    public void testInSubqueryReferencingView() {
        requireInSubqueryViewSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_AntiJoin[[emp_no],[emp_no]]
     *   |_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_Filter[salary > 70000[INTEGER]]
     *       \_EsRelation[employees]
     */
    public void testNotInSubqueryReferencingView() {
        requireInSubqueryViewSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   |_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_Filter[salary > 50000[INTEGER]]
     *       \_Project[[emp_no, salary]]
     *         \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *           |_EsRelation[employees]
     *           \_Project[[emp_no]]
     *             \_EsRelation[test]
     */
    public void testInSubqueryReferencingViewWithInSubquery() {
        requireInSubqueryViewSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_AntiJoin[[emp_no],[emp_no]]
     *   |_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *       |_EsRelation[employees]
     *       \_Project[[emp_no]]
     *         \_EsRelation[test]
     */
    public void testNotInSubqueryReferencingViewWithInSubquery() {
        requireInSubqueryViewSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   |_Filter[salary > 50000[INTEGER]]
     *   | \_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_SemiJoin[SEMI,[salary],[salary]]
     *       |_EsRelation[employees]
     *       \_Project[[salary]]
     *         \_EsRelation[test]
     */
    public void testInSubqueryReferencingViewWithInSubqueryAndPredicate() {
        requireInSubqueryViewSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[salary],[salary]]
     *   |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |_EsRelation[test]
     *   | \_Project[[emp_no]]
     *   |   \_Filter[salary > 70000[INTEGER]]
     *   |     \_EsRelation[employees]
     *   \_Project[[salary]]
     *     \_Project[[salary]]
     *       \_EsRelation[employees]
     */
    public void testMultipleInSubqueriesWithViewAndFromSubquery() {
        requireInSubqueryViewSupport();
        requireSubqueryInFromSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_AntiJoin[[emp_no],[emp_no]]
     *   |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |_EsRelation[test]
     *   | \_Project[[emp_no]]
     *   |   \_Filter[salary > 70000[INTEGER]]
     *   |     \_EsRelation[employees]
     *   \_Project[[emp_no]]
     *     \_Filter[emp_no > 10050[INTEGER]]
     *       \_Project[[emp_no]]
     *         \_EsRelation[test]
     */
    public void testInViewAndNotInFromSubquery() {
        requireInSubqueryViewSupport();
        requireSubqueryInFromSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[salary],[salary]]
     *   |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |_EsRelation[test]
     *   | \_Project[[emp_no]]
     *   |   \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   |     |_EsRelation[employees]
     *   |     \_Project[[emp_no]]
     *   |       \_EsRelation[test]
     *   \_Project[[salary]]
     *     \_SemiJoin[SEMI,[salary],[salary]]
     *       |_EsRelation[employees]
     *       \_Project[[salary]]
     *         \_EsRelation[test]
     */
    public void testMultipleInSubqueriesReferencingViewsWithInSubqueries() {
        requireInSubqueryViewSupport();
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

    /*
     * Limit[10000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   |_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_Project[[emp_no]]
     *       \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *         |_EsRelation[employees]
     *         \_Project[[emp_no]]
     *           \_Limit[10[INTEGER],false,false]
     *             \_OrderBy[[Order[emp_no,ASC,LAST]]]
     *               \_Filter[IN(1[INTEGER],2[INTEGER],languages)]
     *                 \_EsRelation[employees]
     */
    public void testInSubqueryReferencingViewWithNestedInSubqueryInDefinition() {
        requireInSubqueryViewSupport();
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

    /*
     * Limit[10000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   |_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_Project[[emp_no]]
     *       \_SemiJoin[SEMI,[languages],[languages]]
     *         |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *         | |_EsRelation[employees]
     *         | \_Project[[emp_no]]
     *         |   \_Limit[3[INTEGER],false,false]
     *         |     \_OrderBy[[Order[emp_no,ASC,LAST]]]
     *         |       \_EsRelation[test]
     *         \_Project[[languages]]
     *           \_EsRelation[test]
     */
    public void testInSubqueryReferencingConjunctionViewWithTwoInSubqueriesInDefinition() {
        requireInSubqueryViewSupport();
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

    /*
     * Limit[10000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | | |_EsRelation[test]
     *   | | \_Project[[emp_no]]
     *   | |   \_Project[[emp_no]]
     *   | |     \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |       |_EsRelation[employees]
     *   | |       \_Project[[emp_no]]
     *   | |         \_Limit[10[INTEGER],false,false]
     *   | |           \_OrderBy[[Order[emp_no,ASC,LAST]]]
     *   | |             \_Filter[IN(1[INTEGER],2[INTEGER],languages)]
     *   | |               \_EsRelation[employees]
     *   | \_Project[[emp_no]]
     *   |   \_Project[[emp_no]]
     *   |     \_SemiJoin[SEMI,[languages],[languages]]
     *   |       |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   |       | |_EsRelation[employees]
     *   |       | \_Project[[emp_no]]
     *   |       |   \_Limit[3[INTEGER],false,false]
     *   |       |     \_OrderBy[[Order[emp_no,ASC,LAST]]]
     *   |       |       \_EsRelation[test]
     *   |       \_Project[[languages]]
     *   |         \_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_Project[[emp_no]]
     *       \_Filter[$$in_subquery_mark OR IN(1[INTEGER],2[INTEGER],languages)]
     *         \_LeftSemiJoin[[emp_no],[emp_no],$$in_subquery_mark]
     *           |_EsRelation[employees]
     *           \_Project[[emp_no]]
     *             \_EsRelation[test]
     */
    public void testThreeInSubqueriesIntersectingViewsEachWithInnerInSubquery() {
        requireInSubqueryViewSupport();
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
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_AntiJoin[[emp_no],[emp_no]]
     *   |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | | |_EsRelation[test]
     *   | | \_Project[[emp_no]]
     *   | |   \_Project[[emp_no]]
     *   | |     \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |       |_EsRelation[employees]
     *   | |       \_Project[[emp_no]]
     *   | |         \_Limit[10[INTEGER],false,false]
     *   | |           \_OrderBy[[Order[emp_no,ASC,LAST]]]
     *   | |             \_Filter[IN(1[INTEGER],2[INTEGER],languages)]
     *   | |               \_EsRelation[employees]
     *   | \_Project[[emp_no]]
     *   |   \_Project[[emp_no]]
     *   |     \_Filter[$$in_subquery_mark OR IN(1[INTEGER],2[INTEGER],languages)]
     *   |       \_LeftSemiJoin[[emp_no],[emp_no],$$in_subquery_mark]
     *   |         |_EsRelation[employees]
     *   |         \_Project[[emp_no]]
     *   |           \_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_Project[[emp_no]]
     *       \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *         |_EsRelation[employees]
     *         \_Project[[emp_no]]
     *           \_EsRelation[test]
     */
    public void testInSubqueryInSubqueryNotInSubqueryReferencingViewsWithInnerInSubqueries() {
        requireInSubqueryViewSupport();
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
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_AntiJoin[[emp_no],[emp_no]]
     *   |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |_AntiJoin[[emp_no],[emp_no]]
     *   | | |_EsRelation[test]
     *   | | \_Project[[emp_no]]
     *   | |   \_Project[[emp_no]]
     *   | |     \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |       |_EsRelation[employees]
     *   | |       \_Project[[emp_no]]
     *   | |         \_Limit[10[INTEGER],false,false]
     *   | |           \_OrderBy[[Order[emp_no,ASC,LAST]]]
     *   | |             \_Filter[IN(1[INTEGER],2[INTEGER],languages)]
     *   | |               \_EsRelation[employees]
     *   | \_Project[[emp_no]]
     *   |   \_Project[[emp_no]]
     *   |     \_Filter[$$in_subquery_mark OR IN(1[INTEGER],2[INTEGER],languages)]
     *   |       \_LeftSemiJoin[[emp_no],[emp_no],$$in_subquery_mark]
     *   |         |_EsRelation[employees]
     *   |         \_Project[[emp_no]]
     *   |           \_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_Project[[emp_no]]
     *       \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *         |_EsRelation[employees]
     *         \_Project[[emp_no]]
     *           \_EsRelation[test]
     */
    public void testNotInNestedInDisjunctionNotInConjunctionViews() {
        requireInSubqueryViewSupport();
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
    }

    // -- IN subquery with UnionAll (FROM view, (FROM subquery)) --

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   |_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_UnionAll[[emp_no, ...employees columns...]]
     *       |_Project[[emp_no, ...employees columns...]]
     *       | \_Eval[[null AS <employees columns dropped by the view's KEEP emp_no>]]
     *       |   \_Project[[emp_no]]
     *       |     \_Filter[salary > 60000[INTEGER]]
     *       |       \_EsRelation[employees]
     *       \_Project[[emp_no, ...employees columns...]]
     *         \_Subquery[]
     *           \_Filter[salary > 70000[INTEGER]]
     *             \_EsRelation[employees]
     */
    public void testInSubqueryWithUnionAllOfViewAndFromSubquery() {
        requireInSubqueryViewSupport();
        requireSubqueryInFromSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_AntiJoin[[emp_no],[emp_no]]
     *   |_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_UnionAll[[emp_no]]
     *       |_Project[[emp_no]]
     *       | \_Filter[salary > 60000[INTEGER]]
     *       |   \_EsRelation[employees]
     *       \_Project[[emp_no]]
     *         \_Subquery[]
     *           \_Project[[emp_no]]
     *             \_EsRelation[test]
     */
    public void testNotInSubqueryWithUnionAllOfViewAndFromSubquery() {
        requireInSubqueryViewSupport();
        requireSubqueryInFromSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[salary],[salary]]
     *   |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |_EsRelation[test]
     *   | \_Project[[emp_no]]
     *   |   \_UnionAll[[emp_no]]
     *   |     |_Project[[emp_no]]
     *   |     | \_EsRelation[employees]
     *   |     \_Project[[emp_no]]
     *   |       \_Subquery[]
     *   |         \_Project[[emp_no]]
     *   |           \_EsRelation[test]
     *   \_Project[[salary]]
     *     \_UnionAll[[salary]]
     *       |_Project[[salary]]
     *       | \_EsRelation[employees]
     *       \_Project[[salary]]
     *         \_Subquery[]
     *           \_Project[[salary]]
     *             \_EsRelation[employees]
     */
    public void testMultipleInSubqueriesWithUnionAllViewAndFromSubquery() {
        requireInSubqueryViewSupport();
        requireSubqueryInFromSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_AntiJoin[[emp_no],[emp_no]]
     *   |_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   | |_EsRelation[test]
     *   | \_Project[[emp_no]]
     *   |   \_UnionAll[[emp_no]]
     *   |     |_Project[[emp_no]]
     *   |     | \_EsRelation[employees]
     *   |     \_Project[[emp_no]]
     *   |       \_Subquery[]
     *   |         \_Project[[emp_no]]
     *   |           \_EsRelation[test]
     *   \_Project[[emp_no]]
     *     \_Filter[salary > 70000[INTEGER]]
     *       \_EsRelation[employees]
     */
    public void testInSubqueryUnionAllAndNotInSubqueryView() {
        requireInSubqueryViewSupport();
        requireSubqueryInFromSupport();
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

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[SEMI,[emp_no],[emp_no]]
     *   |_UnionAll[[emp_no]]                              // main FROM: two view-referencing subqueries
     *   | |_Project[[emp_no]]
     *   | | \_Subquery[]
     *   | |   \_Project[[emp_no]]
     *   | |     \_Project[[emp_no]]
     *   | |       \_EsRelation[employees]                 // main_view_a
     *   | \_Project[[emp_no]]
     *   |   \_Subquery[]
     *   |     \_Project[[emp_no]]
     *   |       \_Project[[emp_no]]
     *   |         \_Filter[salary > 50000[INTEGER]]
     *   |           \_EsRelation[employees]               // main_view_b
     *   \_Project[[emp_no]]
     *     \_UnionAll[[emp_no]]                            // IN subquery: two view-referencing subqueries
     *       |_Project[[emp_no]]
     *       | \_Subquery[]
     *       |   \_Project[[emp_no]]
     *       |     \_Project[[emp_no]]
     *       |       \_EsRelation[employees]               // in_view_a
     *       \_Project[[emp_no]]
     *         \_Subquery[]
     *           \_Project[[emp_no]]
     *             \_Project[[emp_no]]
     *               \_Filter[salary > 60000[INTEGER]]
     *                 \_EsRelation[employees]             // in_view_b
     */
    public void testMainFromAndInSubqueryEachReferenceMultipleViewSubqueries() {
        requireInSubqueryViewSupport();
        requireSubqueryInFromSupport();
        LogicalPlan plan = analyzer().addIndex("test", "mapping-basic.json")
            .addIndex("employees", "mapping-basic.json")
            .addView("main_view_a", "FROM employees | KEEP emp_no")
            .addView("main_view_b", "FROM employees | WHERE salary > 50000 | KEEP emp_no")
            .addView("in_view_a", "FROM employees | KEEP emp_no")
            .addView("in_view_b", "FROM employees | WHERE salary > 60000 | KEEP emp_no")
            .query("""
                FROM (FROM main_view_a | KEEP emp_no), (FROM main_view_b | KEEP emp_no)
                | WHERE emp_no IN (FROM (FROM in_view_a | KEEP emp_no), (FROM in_view_b | KEEP emp_no) | KEEP emp_no)
                """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));

        // Left side: the main FROM is a UnionAll of two view-referencing subqueries (main_view_a unfiltered, main_view_b salary > 50000).
        UnionAll mainFrom = as(semiJoin.left(), UnionAll.class);
        assertUnionAllOfPlainAndSalaryFilteredEmployeesSubqueries(mainFrom, 50000);

        // Right side: the IN subquery is also a UnionAll of two view-referencing subqueries (in_view_a unfiltered, in_view_b > 60000).
        Project rightProject = as(semiJoin.right(), Project.class);
        UnionAll inSubquery = as(rightProject.child(), UnionAll.class);
        assertUnionAllOfPlainAndSalaryFilteredEmployeesSubqueries(inSubquery, 60000);
    }

    // -- tests with TS source inside IN subquery --

    /*
     * Limit[10000[INTEGER],false,false]
     * \_Project[[max_rate{r}#13, cluster{r}#16]]
     *   \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#78) AS cluster#16]]
     *     \_Aggregate[[pack_cluster_$1{r}#77 AS group_cluster_$1#78],[MAX(RATE_$1{r}#75,true[BOOLEAN],
     *                  PT0S[TIME_DURATION]) AS max_rate#13, group_cluster_$1{r}#78]]
     *       \_Eval[[PACKDIMENSION(cluster{r}#76) AS pack_cluster_$1#77]]
     *         \_TimeSeriesAggregate[[_tsid{m}#74],
     *                                [RATE(network.total_bytes_in{f}#30,true[BOOLEAN],PT0S[TIME_DURATION],@timestamp{f}#15) AS RATE_$1#75,
     *                                VALUES(cluster{f}#16,true[BOOLEAN],PT0S[TIME_DURATION]) AS cluster#76, _tsid{m}#74],
     *                                null,null,@timestamp{f}#15,TS_COMMAND]
     *           \_SemiJoin[SEMI,[cluster{f}#16],[cluster{f}#42]]
     *             |_EsRelation[k8s][TIME_SERIES][@timestamp{f}#15, client.ip{f}#19, cluster{f}#16, e..]
     *             \_Project[[cluster{f}#42]]
     *               \_Project[[m{r}#7, cluster{r}#42]]
     *                 \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#73) AS cluster#42]]
     *                   \_Aggregate[[pack_cluster_$1{r}#72 AS group_cluster_$1#73],
     *                               [MAX(RATE_$1{r}#70,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#7, group_cluster_$1{r}#73]]
     *                     \_Eval[[PACKDIMENSION(cluster{r}#71) AS pack_cluster_$1#72]]
     *                       \_TimeSeriesAggregate[[_tsid{m}#69],
     *                                             [RATE(network.total_bytes_in{f}#56,true[BOOLEAN],PT0S[TIME_DURATION],@timestamp{f}#41)
     *                                              AS RATE_$1#70, VALUES(cluster{f}#42,true[BOOLEAN],PT0S[TIME_DURATION]) AS cluster#71,
     *                                              _tsid{m}#69],null,null,@timestamp{f}#41,TS_COMMAND]
     *                         \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#41, client.ip{f}#45, cluster{f}#42, e..]
     */
    public void testTsRateInsideInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzeInSubqueryWithK8s("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """);

        Limit limit = as(plan, Limit.class);
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(limit.child(), "max_rate", "cluster");

        SemiJoin semiJoin = as(agg.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("cluster"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("cluster"));

        // Left side is the outer TS source itself
        assertK8sTimeSeriesRelation(semiJoin.left());

        // Right side: Project[cluster] (alignment) -> [rewritten subquery TS aggregation] -> EsRelation[k8s][TIME_SERIES]
        Project alignProject = as(semiJoin.right(), Project.class);
        agg = unwrapTsAggregationOverDimension(alignProject.child(), "m", "cluster");
        assertK8sTimeSeriesRelation(agg.child());
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_Project[[max_rate{r}#13, cluster{r}#16]]
     *   \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#78) AS cluster#16]]
     *     \_Aggregate[[pack_cluster_$1{r}#77 AS group_cluster_$1#78],[MAX(RATE_$1{r}#75,true[BOOLEAN],
     *                  PT0S[TIME_DURATION]) AS max_rate#13, group_cluster_$1{r}#78]]
     *       \_Eval[[PACKDIMENSION(cluster{r}#76) AS pack_cluster_$1#77]]
     *         \_TimeSeriesAggregate[[_tsid{m}#74],
     *                                [RATE(network.total_bytes_in{f}#30,true[BOOLEAN],PT0S[TIME_DURATION],@timestamp{f}#15) AS RATE_$1#75,
     *                                VALUES(cluster{f}#16,true[BOOLEAN],PT0S[TIME_DURATION]) AS cluster#76, _tsid{m}#74],
     *                                null,null,@timestamp{f}#15,TS_COMMAND]
     *           \_AntiJoin[ANTI,[cluster{f}#16],[cluster{f}#42]]
     *             |_EsRelation[k8s][TIME_SERIES][@timestamp{f}#15, client.ip{f}#19, cluster{f}#16, e..]
     *             \_Project[[cluster{f}#42]]
     *               \_Project[[m{r}#7, cluster{r}#42]]
     *                 \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#73) AS cluster#42]]
     *                   \_Aggregate[[pack_cluster_$1{r}#72 AS group_cluster_$1#73],
     *                               [MAX(RATE_$1{r}#70,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#7, group_cluster_$1{r}#73]]
     *                     \_Eval[[PACKDIMENSION(cluster{r}#71) AS pack_cluster_$1#72]]
     *                       \_TimeSeriesAggregate[[_tsid{m}#69],
     *                                             [RATE(network.total_bytes_in{f}#56,true[BOOLEAN],PT0S[TIME_DURATION],@timestamp{f}#41)
     *                                              AS RATE_$1#70, VALUES(cluster{f}#42,true[BOOLEAN],PT0S[TIME_DURATION]) AS cluster#71,
     *                                              _tsid{m}#69],null,null,@timestamp{f}#41,TS_COMMAND]
     *                         \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#41, client.ip{f}#45, cluster{f}#42, e..]
     */
    public void testTsRateInsideNotInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzeInSubqueryWithK8s("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """);

        Limit limit = as(plan, Limit.class);
        // Same translated wrapping as the IN-subquery variant above; the AntiJoin replaces the SemiJoin.
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(limit.child(), "max_rate", "cluster");

        AntiJoin antiJoin = as(agg.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("cluster"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("cluster"));

        assertK8sTimeSeriesRelation(antiJoin.left());

        Project alignProject = as(antiJoin.right(), Project.class);
        agg = unwrapTsAggregationOverDimension(alignProject.child(), "m", "cluster");
        assertK8sTimeSeriesRelation(agg.child());
    }

    // -- tests with a TS source that groups BY WITHOUT(...) above the IN subquery --

    /*
     * Limit[10000[INTEGER],false,false]
     * \_Project[[total_cost{r}#15, _timeseries{r}#12]]
     *   \_Eval[[UNPACKDIMENSION(group__timeseries_$1{r}#80) AS _timeseries#12]]
     *     \_Aggregate[[pack__timeseries_$1{r}#79 AS group__timeseries_$1#80],
     *                 [SUM(LASTOVERTIME_$1{r}#77,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD],
     *                  long_overflow_throw[KEYWORD]) AS total_cost#15, group__timeseries_$1{r}#80]]
     *       \_Eval[[PACKDIMENSION(_timeseries{r}#78) AS pack__timeseries_$1#79]]
     *         \_TimeSeriesAggregate[[_tsid{m}#76],
     *                               [LASTOVERTIME(network.cost{f}#35,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                @timestamp{f}#18) AS LASTOVERTIME_$1#77,
     *                                VALUES(_timeseries{f}#12,true[BOOLEAN],PT0S[TIME_DURATION]) AS _timeseries#78, _tsid{m}#76],
     *                               null,null,@timestamp{f}#18,TS_COMMAND]
     *           \_SemiJoin[SEMI,[cluster{f}#19],[cluster{f}#45]]
     *             |_EsRelation[k8s][@timestamp{f}#18, client.ip{f}#22, cluster{f}#19, e..]
     *             \_Project[[cluster{f}#45]]
     *               \_Project[[m{r}#7, cluster{r}#45]]
     *                 \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#75) AS cluster#45]]
     *                   \_Aggregate[[pack_cluster_$1{r}#74 AS group_cluster_$1#75],
     *                               [MAX(RATE_$1{r}#72,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#7, group_cluster_$1{r}#75]]
     *                     \_Eval[[PACKDIMENSION(cluster{r}#73) AS pack_cluster_$1#74]]
     *                       \_TimeSeriesAggregate[[_tsid{m}#71],
     *                                             [RATE(network.total_bytes_in{f}#59,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                              @timestamp{f}#44) AS RATE_$1#72,
     *                                              VALUES(cluster{f}#45,true[BOOLEAN],PT0S[TIME_DURATION]) AS cluster#73, _tsid{m}#71],
     *                                             null,null,@timestamp{f}#44,TS_COMMAND]
     *                         \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#44, client.ip{f}#48, cluster{f}#45, e..]
     */
    public void testTsWithoutAndRateInsideInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires WITHOUT grouping support", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());

        LogicalPlan plan = analyzeInSubqueryWithK8s("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """);

        Limit limit = as(plan, Limit.class);
        // TranslateTimeSeriesWithout has rewritten the WITHOUT grouping into a `_timeseries` TimeSeriesMetadataAttribute, after which
        // TranslateTimeSeriesAggregate adds the same Project/Eval/Aggregate/Eval wrapping seen above.
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(limit.child(), "total_cost", "_timeseries");

        SemiJoin semiJoin = as(agg.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("cluster"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("cluster"));

        // Only the outer (left-hand) k8s relation should carry the lowered `_timeseries` attribute with the expected skipFieldNames; the
        // right-hand subquery relation must not be polluted. The outer aggregate uses sum() (not a TS-required function) so the left
        // relation's index mode is rewritten to STANDARD; the subquery uses rate() and so its relation stays TIME_SERIES.
        assertK8sRelationWithTimeseriesWithout(semiJoin.left(), IndexMode.STANDARD, Set.of("pod", "region"));

        Project alignProject = as(semiJoin.right(), Project.class);
        agg = unwrapTsAggregationOverDimension(alignProject.child(), "m", "cluster");
        assertK8sTimeSeriesRelation(agg.child());
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_Project[[total_cost{r}#15, _timeseries{r}#12]]
     *   \_Eval[[UNPACKDIMENSION(group__timeseries_$1{r}#80) AS _timeseries#12]]
     *     \_Aggregate[[pack__timeseries_$1{r}#79 AS group__timeseries_$1#80],
     *                 [SUM(LASTOVERTIME_$1{r}#77,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD],
     *                  long_overflow_throw[KEYWORD]) AS total_cost#15, group__timeseries_$1{r}#80]]
     *       \_Eval[[PACKDIMENSION(_timeseries{r}#78) AS pack__timeseries_$1#79]]
     *         \_TimeSeriesAggregate[[_tsid{m}#76],
     *                               [LASTOVERTIME(network.cost{f}#35,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                @timestamp{f}#18) AS LASTOVERTIME_$1#77,
     *                                VALUES(_timeseries{f}#12,true[BOOLEAN],PT0S[TIME_DURATION]) AS _timeseries#78, _tsid{m}#76],
     *                               null,null,@timestamp{f}#18,TS_COMMAND]
     *           \_AntiJoin[ANTI,[cluster{f}#19],[cluster{f}#45]]
     *             |_EsRelation[k8s][@timestamp{f}#18, client.ip{f}#22, cluster{f}#19, e..]
     *             \_Project[[cluster{f}#45]]
     *               \_Project[[m{r}#7, cluster{r}#45]]
     *                 \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#75) AS cluster#45]]
     *                   \_Aggregate[[pack_cluster_$1{r}#74 AS group_cluster_$1#75],
     *                               [MAX(RATE_$1{r}#72,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#7, group_cluster_$1{r}#75]]
     *                     \_Eval[[PACKDIMENSION(cluster{r}#73) AS pack_cluster_$1#74]]
     *                       \_TimeSeriesAggregate[[_tsid{m}#71],
     *                                             [RATE(network.total_bytes_in{f}#59,true[BOOLEAN],PT0S[TIME_DURATION],
     *                                              @timestamp{f}#44) AS RATE_$1#72,
     *                                              VALUES(cluster{f}#45,true[BOOLEAN],PT0S[TIME_DURATION]) AS cluster#73, _tsid{m}#71],
     *                                             null,null,@timestamp{f}#44,TS_COMMAND]
     *                         \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#44, client.ip{f}#48, cluster{f}#45, e..]
     */
    public void testTsWithoutAndRateInsideNotInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires WITHOUT grouping support", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzeInSubqueryWithK8s("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """);

        Limit limit = as(plan, Limit.class);
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(limit.child(), "total_cost", "_timeseries");

        AntiJoin antiJoin = as(agg.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("cluster"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("cluster"));

        assertK8sRelationWithTimeseriesWithout(antiJoin.left(), IndexMode.STANDARD, Set.of("pod", "region"));

        Project alignProject = as(antiJoin.right(), Project.class);
        agg = unwrapTsAggregationOverDimension(alignProject.child(), "m", "cluster");
        assertK8sTimeSeriesRelation(agg.child());
    }

    // -- multiple TS subqueries combined with UnionAll inside IN/NOT IN --

    /*
     * Limit[10000[INTEGER],false,false]
     * \_OrderBy[[Order[cluster{f}#25,ASC,LAST]]]
     *   \_Project[[max_bytes{r}#21, cluster{r}#25]]
     *     \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#117) AS cluster#25]]
     *       \_Aggregate[[pack_cluster_$1{r}#116 AS group_cluster_$1#117],
     *                   [MAX(LASTOVERTIME_$1{r}#114,true[BOOLEAN],PT0S[TIME_DURATION]) AS max_bytes#21, group_cluster_$1{r}#117]]
     *         \_Eval[[PACKDIMENSION(cluster{r}#115) AS pack_cluster_$1#116]]
     *           \_TimeSeriesAggregate[[_tsid{m}#113],
     *                                 [LASTOVERTIME(TOLONGSURROGATE(network.total_bytes_in{f}#39),true[BOOLEAN],PT0S[TIME_DURATION],
     *                                  @timestamp{f}#24) AS LASTOVERTIME_$1#114,
     *                                  DIMENSIONVALUES(cluster{f}#25,true[BOOLEAN],PT0S[TIME_DURATION]) AS cluster#115, _tsid{m}#113],
     *                                 null,null,@timestamp{f}#24,TS_COMMAND]
     *             \_SemiJoin[SEMI,[cluster{f}#25],[cluster{r}#102]]
     *               |_EsRelation[k8s][@timestamp{f}#24, client.ip{f}#28, cluster{f}#25, e..]
     *               \_UnionAll[[cluster{r}#102]]
     *                 |_Project[[cluster{f}#51]]
     *                 | \_Subquery[]
     *                 |   \_Project[[cluster{f}#51]]
     *                 |     \_Filter[max_bytes{r}#7 > 10500[INTEGER]]
     *                 |       \_Project[[max_bytes{r}#7, cluster{r}#51]]
     *                 |         \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#107) AS cluster#51]]
     *                 |           \_Aggregate[[pack_cluster_$1{r}#106 AS group_cluster_$1#107],
     *                                         [MAX(LASTOVERTIME_$1{r}#104,true[BOOLEAN],PT0S[TIME_DURATION]) AS max_bytes#7,
     *                                          group_cluster_$1{r}#107]]
     *                 |             \_Eval[[PACKDIMENSION(cluster{r}#105) AS pack_cluster_$1#106]]
     *                 |               \_TimeSeriesAggregate[[_tsid{m}#103],
     *                                                       [LASTOVERTIME(TOLONGSURROGATE(network.total_bytes_in{f}#65),true[BOOLEAN],
     *                                                        PT0S[TIME_DURATION],@timestamp{f}#50) AS LASTOVERTIME_$1#104,
     *                                                        DIMENSIONVALUES(cluster{f}#51,true[BOOLEAN],
     *                                                        PT0S[TIME_DURATION]) AS cluster#105, _tsid{m}#103],
     *                                                       null,null,@timestamp{f}#50,TS_COMMAND]
     *                 |                 \_EsRelation[k8s][@timestamp{f}#50, client.ip{f}#54, cluster{f}#51, e..]
     *                 \_Project[[cluster{f}#77]]
     *                   \_Subquery[]
     *                     \_Project[[cluster{f}#77]]
     *                       \_Filter[max_bytes{r}#14 < 8000[INTEGER]]
     *                         \_Project[[max_bytes{r}#14, cluster{r}#77]]
     *                           \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#112) AS cluster#77]]
     *                             \_Aggregate[[pack_cluster_$1{r}#111 AS group_cluster_$1#112],
     *                                         [MAX(LASTOVERTIME_$1{r}#109,true[BOOLEAN],PT0S[TIME_DURATION]) AS max_bytes#14,
     *                                          group_cluster_$1{r}#112]]
     *                               \_Eval[[PACKDIMENSION(cluster{r}#110) AS pack_cluster_$1#111]]
     *                                 \_TimeSeriesAggregate[[_tsid{m}#108],
     *                                                       [LASTOVERTIME(TOLONGSURROGATE(network.total_bytes_in{f}#91),true[BOOLEAN],
     *                                                        PT0S[TIME_DURATION],@timestamp{f}#76) AS LASTOVERTIME_$1#109,
     *                                                        DIMENSIONVALUES(cluster{f}#77,true[BOOLEAN],
     *                                                        PT0S[TIME_DURATION]) AS cluster#110, _tsid{m}#108],
     *                                                       null,null,@timestamp{f}#76,TS_COMMAND]
     *                                   \_EsRelation[k8s][@timestamp{f}#76, client.ip{f}#80, cluster{f}#77, e..]
     */
    public void testMultipleTsSubqueriesInsideInSubquery() {
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzeInSubqueryWithK8s("""
            TS k8s
            | WHERE cluster IN (FROM
                                   (TS k8s
                                    | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                    | WHERE max_bytes > 10500
                                    | KEEP cluster),
                                   (TS k8s
                                    | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                    | WHERE max_bytes < 8000
                                    | KEEP cluster)
                               )
            | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
            | SORT cluster
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(orderBy.child(), "max_bytes", "cluster");

        SemiJoin semiJoin = as(agg.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("cluster"));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("cluster"));

        // The outer aggregate uses max(to_long(...)) (not a TS-required function), so the relation's
        // index mode is rewritten to STANDARD by addTsidToTimeSeriesSource.
        assertK8sStandardRelation(semiJoin.left());

        UnionAll unionAll = as(semiJoin.right(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
        assertTsUnionBranch(unionAll.children().get(0));
        assertTsUnionBranch(unionAll.children().get(1));
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_OrderBy[[Order[cluster{f}#25,ASC,LAST]]]
     *   \_Project[[max_bytes{r}#21, cluster{r}#25]]
     *     \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#117) AS cluster#25]]
     *       \_Aggregate[[pack_cluster_$1{r}#116 AS group_cluster_$1#117],
     *                   [MAX(LASTOVERTIME_$1{r}#114,true[BOOLEAN],PT0S[TIME_DURATION]) AS max_bytes#21, group_cluster_$1{r}#117]]
     *         \_Eval[[PACKDIMENSION(cluster{r}#115) AS pack_cluster_$1#116]]
     *           \_TimeSeriesAggregate[[_tsid{m}#113],
     *                                 [LASTOVERTIME(TOLONGSURROGATE(network.total_bytes_in{f}#39),true[BOOLEAN],PT0S[TIME_DURATION],
     *                                  @timestamp{f}#24) AS LASTOVERTIME_$1#114,
     *                                  DIMENSIONVALUES(cluster{f}#25,true[BOOLEAN],PT0S[TIME_DURATION]) AS cluster#115, _tsid{m}#113],
     *                                 null,null,@timestamp{f}#24,TS_COMMAND]
     *             \_AntiJoin[ANTI,[cluster{f}#25],[cluster{r}#102]]
     *               |_EsRelation[k8s][@timestamp{f}#24, client.ip{f}#28, cluster{f}#25, e..]
     *               \_UnionAll[[cluster{r}#102]]
     *                 |_Project[[cluster{f}#51]]
     *                 | \_Subquery[]
     *                 |   \_Project[[cluster{f}#51]]
     *                 |     \_Filter[max_bytes{r}#7 > 10500[INTEGER]]
     *                 |       \_Project[[max_bytes{r}#7, cluster{r}#51]]
     *                 |         \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#107) AS cluster#51]]
     *                 |           \_Aggregate[[pack_cluster_$1{r}#106 AS group_cluster_$1#107],
     *                                         [MAX(LASTOVERTIME_$1{r}#104,true[BOOLEAN],PT0S[TIME_DURATION]) AS max_bytes#7,
     *                                          group_cluster_$1{r}#107]]
     *                 |             \_Eval[[PACKDIMENSION(cluster{r}#105) AS pack_cluster_$1#106]]
     *                 |               \_TimeSeriesAggregate[[_tsid{m}#103],
     *                                                       [LASTOVERTIME(TOLONGSURROGATE(network.total_bytes_in{f}#65),true[BOOLEAN],
     *                                                        PT0S[TIME_DURATION],@timestamp{f}#50) AS LASTOVERTIME_$1#104,
     *                                                        DIMENSIONVALUES(cluster{f}#51,true[BOOLEAN],
     *                                                        PT0S[TIME_DURATION]) AS cluster#105, _tsid{m}#103],
     *                                                       null,null,@timestamp{f}#50,TS_COMMAND]
     *                 |                 \_EsRelation[k8s][@timestamp{f}#50, client.ip{f}#54, cluster{f}#51, e..]
     *                 \_Project[[cluster{f}#77]]
     *                   \_Subquery[]
     *                     \_Project[[cluster{f}#77]]
     *                       \_Filter[max_bytes{r}#14 < 8000[INTEGER]]
     *                         \_Project[[max_bytes{r}#14, cluster{r}#77]]
     *                           \_Eval[[UNPACKDIMENSION(group_cluster_$1{r}#112) AS cluster#77]]
     *                             \_Aggregate[[pack_cluster_$1{r}#111 AS group_cluster_$1#112],
     *                                         [MAX(LASTOVERTIME_$1{r}#109,true[BOOLEAN],PT0S[TIME_DURATION]) AS max_bytes#14,
     *                                          group_cluster_$1{r}#112]]
     *                               \_Eval[[PACKDIMENSION(cluster{r}#110) AS pack_cluster_$1#111]]
     *                                 \_TimeSeriesAggregate[[_tsid{m}#108],
     *                                                       [LASTOVERTIME(TOLONGSURROGATE(network.total_bytes_in{f}#91),true[BOOLEAN],
     *                                                        PT0S[TIME_DURATION],@timestamp{f}#76) AS LASTOVERTIME_$1#109,
     *                                                        DIMENSIONVALUES(cluster{f}#77,true[BOOLEAN],
     *                                                        PT0S[TIME_DURATION]) AS cluster#110, _tsid{m}#108],
     *                                                       null,null,@timestamp{f}#76,TS_COMMAND]
     *                                   \_EsRelation[k8s][@timestamp{f}#76, client.ip{f}#80, cluster{f}#77, e..]
     */
    public void testMultipleTsSubqueriesInsideNotInSubquery() {
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzeInSubqueryWithK8s("""
            TS k8s
            | WHERE cluster NOT IN (FROM
                                       (TS k8s
                                        | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                        | WHERE max_bytes > 10500
                                        | KEEP cluster),
                                       (TS k8s
                                        | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                        | WHERE max_bytes < 8000
                                        | KEEP cluster)
                                   )
            | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
            | SORT cluster
            """);

        Limit limit = as(plan, Limit.class);
        OrderBy orderBy = as(limit.child(), OrderBy.class);
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(orderBy.child(), "max_bytes", "cluster");

        AntiJoin antiJoin = as(agg.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("cluster"));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("cluster"));

        assertK8sStandardRelation(antiJoin.left());

        UnionAll unionAll = as(antiJoin.right(), UnionAll.class);
        assertEquals(2, unionAll.children().size());
        assertTsUnionBranch(unionAll.children().get(0));
        assertTsUnionBranch(unionAll.children().get(1));
    }

    /**
     * After TranslateTimeSeriesAggregate runs, the subquery's `STATS max_bytes = ... BY cluster` is also wrapped with
     * Project > Eval[UNPACK] > Aggregate > Eval[PACK] > TimeSeriesAggregate. max(to_long(...)) is not a TS-required function so the
     * subquery relation's index mode is also rewritten to STANDARD.
     */
    private static void assertTsUnionBranch(LogicalPlan branch) {
        Project alignProject = as(branch, Project.class);
        Subquery subquery = as(alignProject.child(), Subquery.class);
        Project keepProject = as(subquery.child(), Project.class);
        Filter filter = as(keepProject.child(), Filter.class);
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(filter.child(), "max_bytes", "cluster");
        assertK8sStandardRelation(agg.child());
    }

    /**
     * Walks through the wrapping that {@code TranslateTimeSeriesAggregate} adds around a TS aggregation that groups by a single dimension
     * attribute, namely {@code Project > Eval[UNPACKDIMENSION] > Aggregate > Eval[PACKDIMENSION] > TimeSeriesAggregate}, and returns the
     * child of the inner {@code TimeSeriesAggregate}.
     */
    private static TimeSeriesAggregate unwrapTsAggregationOverDimension(LogicalPlan top, String aggName, String groupingName) {
        Project topProject = as(top, Project.class);
        assertThat(topProject.projections().size(), equalTo(2));
        assertEquals(aggName, topProject.projections().get(0).name());
        assertEquals(groupingName, topProject.projections().get(1).name());

        Eval unpackEval = as(topProject.child(), Eval.class);
        assertThat(unpackEval.fields().size(), equalTo(1));
        assertEquals(groupingName, unpackEval.fields().get(0).name());

        Aggregate aggregate = as(unpackEval.child(), Aggregate.class);
        assertThat(aggregate.groupings().size(), equalTo(1));

        Eval packEval = as(aggregate.child(), Eval.class);
        assertThat(packEval.fields().size(), equalTo(1));

        return as(packEval.child(), TimeSeriesAggregate.class);
    }

    private static void assertK8sTimeSeriesRelation(LogicalPlan plan) {
        assertK8sRelation(plan, IndexMode.TIME_SERIES);
    }

    private static void assertK8sStandardRelation(LogicalPlan plan) {
        assertK8sRelation(plan, IndexMode.STANDARD);
    }

    /**
     * Asserts that {@code plan} is the k8s relation with the given {@code IndexMode}.
     * {@code TranslateTimeSeriesAggregate.addTsidToTimeSeriesSource} rewrites the source relation's index mode to
     * {@code IndexMode.STANDARD} unless an outer TS aggregate function (e.g. {@code rate}) requires it to stay
     * {@code IndexMode.TIME_SERIES}.
     */
    private static EsRelation assertK8sRelation(LogicalPlan plan, IndexMode expectedIndexMode) {
        EsRelation relation = as(plan, EsRelation.class);
        assertEquals("k8s", relation.indexPattern());
        assertThat(relation.indexMode(), equalTo(expectedIndexMode));
        return relation;
    }

    /**
     * Asserts that the given plan is the k8s TS source relation and that it carries a lowered {@code _timeseries}
     * {@code TimeSeriesMetadataAttribute} with the expected {@code skipFieldNames}. {@code TranslateTimeSeriesWithout} injects this
     * attribute only into the main (left-hand) TS source feeding the outer aggregate, never into the subquery (right-hand) relation.
     */
    private static void assertK8sRelationWithTimeseriesWithout(
        LogicalPlan plan,
        IndexMode expectedIndexMode,
        Set<String> expectedWithoutFields
    ) {
        EsRelation relation = assertK8sRelation(plan, expectedIndexMode);
        TimeSeriesMetadataAttribute lowered = relation.output()
            .stream()
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .map(TimeSeriesMetadataAttribute.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected _timeseries metadata attribute on the k8s relation"));
        assertThat(lowered.excludedFields(), equalTo(expectedWithoutFields));
    }

    // -- IN / NOT IN (subquery) crossed with external datasets --

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[[emp_no{r}#2],[emp_no{r}#7]]
     *   |_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][emp_no{r}#2, name{r}#3, salary{r}#4]
     *   \_Project[[emp_no{f}#7]]
     *     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ..]
     */
    public void testInSubqueryMainExternalDatasetSubqueryIndex() {
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeInSubqueryWithExternalDataset("""
            FROM salaries_int
            | WHERE emp_no IN (FROM test | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        // Main query: external dataset on the left.
        ExternalRelation leftRelation = as(semiJoin.left(), ExternalRelation.class);
        assertEquals(SALARIES_INT_RESOURCE, leftRelation.sourcePath());

        // Subquery: regular index on the right.
        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("test", rightRelation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_AntiJoin[[emp_no{r}#2],[emp_no{r}#7]]
     *   |_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][emp_no{r}#2, name{r}#3, salary{r}#4]
     *   \_Project[[emp_no{f}#7]]
     *     \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ..]
     */
    public void testNotInSubqueryMainExternalDatasetSubqueryIndex() {
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeInSubqueryWithExternalDataset("""
            FROM salaries_int
            | WHERE emp_no NOT IN (FROM test | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.output(), equalTo(antiJoin.left().output()));

        ExternalRelation leftRelation = as(antiJoin.left(), ExternalRelation.class);
        assertEquals(SALARIES_INT_RESOURCE, leftRelation.sourcePath());

        Project rightProject = as(antiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("test", rightRelation.indexPattern());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[[emp_no{f}#7],[emp_no{r}#2]]
     *   |_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ..]
     *   \_Project[[emp_no{r}#2]]
     *     \_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][emp_no{r}#2, name{r}#3, salary{r}#4]
     */
    public void testInSubqueryMainIndexSubqueryExternalDataset() {
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeInSubqueryWithExternalDataset("""
            FROM test
            | WHERE emp_no IN (FROM salaries_int | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        // Main query: regular index on the left.
        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Subquery: external dataset on the right.
        Project rightProject = as(semiJoin.right(), Project.class);
        ExternalRelation rightRelation = as(rightProject.child(), ExternalRelation.class);
        assertEquals(SALARIES_INT_RESOURCE, rightRelation.sourcePath());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_AntiJoin[[emp_no{f}#7],[emp_no{r}#2]]
     *   |_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ..]
     *   \_Project[[emp_no{r}#2]]
     *     \_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][emp_no{r}#2, name{r}#3, salary{r}#4]
     */
    public void testNotInSubqueryMainIndexSubqueryExternalDataset() {
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeInSubqueryWithExternalDataset("""
            FROM test
            | WHERE emp_no NOT IN (FROM salaries_int | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.output(), equalTo(antiJoin.left().output()));

        EsRelation leftRelation = as(antiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project rightProject = as(antiJoin.right(), Project.class);
        ExternalRelation rightRelation = as(rightProject.child(), ExternalRelation.class);
        assertEquals(SALARIES_INT_RESOURCE, rightRelation.sourcePath());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_SemiJoin[[emp_no{r}#2],[emp_no{r}#7]]
     *   |_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][emp_no{r}#2, name{r}#3, salary{r}#4]
     *   \_Project[[emp_no{r}#7]]
     *     \_ExternalRelation[s3://bucket/salaries_long.parquet][parquet][emp_no{r}#7, name{r}#8, salary{r}#9]
     */
    public void testInSubqueryMainAndSubqueryExternalDataset() {
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeInSubqueryWithExternalDataset("""
            FROM salaries_int
            | WHERE emp_no IN (FROM salaries_long | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        // Both sides resolve to external datasets.
        ExternalRelation leftRelation = as(semiJoin.left(), ExternalRelation.class);
        assertEquals(SALARIES_INT_RESOURCE, leftRelation.sourcePath());

        Project rightProject = as(semiJoin.right(), Project.class);
        ExternalRelation rightRelation = as(rightProject.child(), ExternalRelation.class);
        assertEquals(SALARIES_LONG_RESOURCE, rightRelation.sourcePath());
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_AntiJoin[[emp_no{r}#2],[emp_no{r}#7]]
     *   |_ExternalRelation[s3://bucket/salaries_int.parquet][parquet][emp_no{r}#2, name{r}#3, salary{r}#4]
     *   \_Project[[emp_no{r}#7]]
     *     \_ExternalRelation[s3://bucket/salaries_long.parquet][parquet][emp_no{r}#7, name{r}#8, salary{r}#9]
     */
    public void testNotInSubqueryMainAndSubqueryExternalDataset() {
        requireExternalDatasetSupport();
        LogicalPlan plan = analyzeInSubqueryWithExternalDataset("""
            FROM salaries_int
            | WHERE emp_no NOT IN (FROM salaries_long | KEEP emp_no)
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.config().rightFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("emp_no"));
        assertThat(antiJoin.output(), equalTo(antiJoin.left().output()));

        ExternalRelation leftRelation = as(antiJoin.left(), ExternalRelation.class);
        assertEquals(SALARIES_INT_RESOURCE, leftRelation.sourcePath());

        Project rightProject = as(antiJoin.right(), Project.class);
        ExternalRelation rightRelation = as(rightProject.child(), ExternalRelation.class);
        assertEquals(SALARIES_LONG_RESOURCE, rightRelation.sourcePath());
    }

    // -- IN / NOT IN (subquery) crossed with time-series indices --

    /*
     * Limit[..]
     * \_Project[[max_rate{r}#.., cluster{r}#..]]
     *   \_Eval[[UNPACKDIMENSION(..) AS cluster#..]]
     *     \_Aggregate[[..],[MAX(RATE_$1{r}#..,..) AS max_rate#.., ..]]
     *       \_Eval[[PACKDIMENSION(cluster{r}#..) AS ..]]
     *         \_TimeSeriesAggregate[[_tsid{m}#..],[RATE(network.total_bytes_in{f}#..,..) AS RATE_$1#.., ..],..,TS_COMMAND]
     *           \_SemiJoin[[cluster{f}#..],[first_name{f}#..]]
     *             |_EsRelation[k8s][TIME_SERIES][@timestamp{f}#.., cluster{f}#.., ..]
     *             \_Project[[first_name{f}#..]]
     *               \_EsRelation[test][_meta_field{f}#.., emp_no{f}#.., first_name{f}#.., ..]
     */
    public void testInSubqueryMainTimeSeriesSubqueryIndex() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzeInSubqueryWithK8s("""
            TS k8s
            | WHERE cluster IN (FROM test | KEEP first_name)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """);

        Limit limit = as(plan, Limit.class);
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(limit.child(), "max_rate", "cluster");

        SemiJoin semiJoin = as(agg.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("cluster"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("first_name"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        // Main query: the k8s time-series source on the left, kept in IndexMode.TIME_SERIES by the rate aggregation.
        assertK8sTimeSeriesRelation(semiJoin.left());

        // Subquery: regular index on the right.
        Project rightProject = as(semiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("test", rightRelation.indexPattern());
    }

    /*
     * Limit[..]
     * \_Project[[max_rate{r}#.., cluster{r}#..]]
     *   \_..(lowered time-series aggregation)..
     *     \_AntiJoin[[cluster{f}#..],[first_name{f}#..]]
     *       |_EsRelation[k8s][TIME_SERIES][@timestamp{f}#.., cluster{f}#.., ..]
     *       \_Project[[first_name{f}#..]]
     *         \_EsRelation[test][_meta_field{f}#.., emp_no{f}#.., first_name{f}#.., ..]
     */
    public void testNotInSubqueryMainTimeSeriesSubqueryIndex() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzeInSubqueryWithK8s("""
            TS k8s
            | WHERE cluster NOT IN (FROM test | KEEP first_name)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """);

        Limit limit = as(plan, Limit.class);
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(limit.child(), "max_rate", "cluster");

        AntiJoin antiJoin = as(agg.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("cluster"));
        assertThat(antiJoin.config().rightFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("first_name"));
        assertThat(antiJoin.output(), equalTo(antiJoin.left().output()));

        assertK8sTimeSeriesRelation(antiJoin.left());

        Project rightProject = as(antiJoin.right(), Project.class);
        EsRelation rightRelation = as(rightProject.child(), EsRelation.class);
        assertEquals("test", rightRelation.indexPattern());
    }

    /*
     * Limit[..]
     * \_SemiJoin[[first_name{f}#..],[cluster{f}#..]]
     *   |_EsRelation[test][_meta_field{f}#.., emp_no{f}#.., first_name{f}#.., ..]
     *   \_Project[[cluster{r}#..]]
     *     \_Project[[max_rate{r}#.., cluster{r}#..]]
     *       \_..(lowered time-series aggregation)..
     *         \_TimeSeriesAggregate[[_tsid{m}#..],[RATE(network.total_bytes_in{f}#..,..) AS RATE_$1#.., ..],..,TS_COMMAND]
     *           \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#.., cluster{f}#.., ..]
     */
    public void testInSubqueryMainIndexSubqueryTimeSeries() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzeInSubqueryWithK8s("""
            FROM test
            | WHERE first_name IN (TS k8s
                                  | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
                                  | KEEP cluster)
            """);

        Limit limit = as(plan, Limit.class);
        SemiJoin semiJoin = as(limit.child(), SemiJoin.class);
        assertThat(semiJoin.config().type(), equalTo(JoinTypes.SEMI));
        assertThat(semiJoin.config().leftFields().size(), equalTo(1));
        assertThat(semiJoin.config().leftFields().get(0).name(), equalTo("first_name"));
        assertThat(semiJoin.config().rightFields().size(), equalTo(1));
        assertThat(semiJoin.config().rightFields().get(0).name(), equalTo("cluster"));
        assertThat(semiJoin.output(), equalTo(semiJoin.left().output()));

        // Main query: regular index on the left.
        EsRelation leftRelation = as(semiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        // Subquery: the k8s time-series source on the right, kept in IndexMode.TIME_SERIES by the rate aggregation.
        Project alignProject = as(semiJoin.right(), Project.class);
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(alignProject.child(), "max_rate", "cluster");
        assertK8sTimeSeriesRelation(agg.child());
    }

    /*
     * Limit[..]
     * \_AntiJoin[[first_name{f}#..],[cluster{f}#..]]
     *   |_EsRelation[test][_meta_field{f}#.., emp_no{f}#.., first_name{f}#.., ..]
     *   \_Project[[cluster{r}#..]]
     *     \_..(lowered time-series aggregation over k8s)..
     *       \_EsRelation[k8s][TIME_SERIES][@timestamp{f}#.., cluster{f}#.., ..]
     */
    public void testNotInSubqueryMainIndexSubqueryTimeSeries() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        LogicalPlan plan = analyzeInSubqueryWithK8s("""
            FROM test
            | WHERE first_name NOT IN (TS k8s
                                      | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
                                      | KEEP cluster)
            """);

        Limit limit = as(plan, Limit.class);
        AntiJoin antiJoin = as(limit.child(), AntiJoin.class);
        assertThat(antiJoin.config().type(), equalTo(JoinTypes.ANTI));
        assertThat(antiJoin.config().leftFields().size(), equalTo(1));
        assertThat(antiJoin.config().leftFields().get(0).name(), equalTo("first_name"));
        assertThat(antiJoin.config().rightFields().size(), equalTo(1));
        assertThat(antiJoin.config().rightFields().get(0).name(), equalTo("cluster"));
        assertThat(antiJoin.output(), equalTo(antiJoin.left().output()));

        EsRelation leftRelation = as(antiJoin.left(), EsRelation.class);
        assertEquals("test", leftRelation.indexPattern());

        Project alignProject = as(antiJoin.right(), Project.class);
        TimeSeriesAggregate agg = unwrapTsAggregationOverDimension(alignProject.child(), "max_rate", "cluster");
        assertK8sTimeSeriesRelation(agg.child());
    }

    // -- helpers --

    private static LogicalPlan analyzeInSubquery(String query) {
        return analyzer().addIndex("test", "mapping-basic.json").addIndex("employees", "mapping-basic.json").query(query);
    }

    private static final String SALARIES_INT_RESOURCE = "s3://bucket/salaries_int.parquet";
    private static final String SALARIES_LONG_RESOURCE = "s3://bucket/salaries_long.parquet";

    /**
     * Analyzes a {@code WHERE <field> IN/NOT IN (subquery)} query that mixes the IN-subquery feature with
     * external datasets, faithfully replaying the production pipeline order from {@code EsqlSession}:
     * {@link InSubqueryResolver#resolve} runs first (rewriting every top-level {@code IN}/{@code NOT IN}
     * into a {@link SemiJoin}/{@link AntiJoin}, detaching the subquery plan into the join's right child),
     * and only then does {@link DatasetRewriter} turn each {@code FROM <dataset>} — wherever it now sits,
     * the main relation or the detached subquery branch — into the {@code UnresolvedExternalRelation} the
     * analyzer resolves against the configured external source schema.
     *
     * <p>The regular index {@code test} ({@code mapping-basic.json}, which carries an
     * {@code emp_no:integer} column) is registered alongside the two external datasets
     * ({@code salaries_int}/{@code salaries_long}, both exposing {@code emp_no:integer}), so combinations
     * referencing an index, a dataset, or both on either side of the {@code IN} all resolve on the shared
     * {@code emp_no} key.
     */
    private static LogicalPlan analyzeInSubqueryWithExternalDataset(String query) {
        DataSource dataSource = new DataSource("external_ds", "test", null, Map.of());
        Dataset intDataset = new Dataset("salaries_int", new DataSourceReference("external_ds"), SALARIES_INT_RESOURCE, null, Map.of());
        Dataset longDataset = new Dataset("salaries_long", new DataSourceReference("external_ds"), SALARIES_LONG_RESOURCE, null, Map.of());
        ProjectMetadata projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("external_ds", dataSource)))
            .datasets(Map.of("salaries_int", intDataset, "salaries_long", longDataset))
            .build();
        // Production order (EsqlSession#execute then #analyzedPlan): resolve IN subqueries into
        // SemiJoin/AntiJoin first, then rewrite FROM <dataset> targets into external relations.
        LogicalPlan afterInSubquery = InSubqueryResolver.resolve(TEST_PARSER.parseQuery(query));
        LogicalPlan rewritten = DatasetRewriter.rewriteUnsecured(
            afterInSubquery,
            projectMetadata,
            TestIndexNameExpressionResolver.newInstance()
        );
        ExternalSourceResolution resolution = new ExternalSourceResolution(
            Map.of(
                SALARIES_INT_RESOURCE,
                externalSource(SALARIES_INT_RESOURCE, DataType.INTEGER),
                SALARIES_LONG_RESOURCE,
                externalSource(SALARIES_LONG_RESOURCE, DataType.LONG)
            )
        );
        return analyzer().addEmployees("test").externalSourceResolution(resolution).buildAnalyzer().analyze(rewritten);
    }

    /** A resolved external source named {@code emp_no}/{@code name}/{@code salary} with the given salary type. */
    private static ExternalSourceResolution.ResolvedSource externalSource(String path, DataType salaryType) {
        List<Attribute> schema = List.of(
            referenceAttribute("emp_no", DataType.INTEGER),
            referenceAttribute("name", DataType.KEYWORD),
            referenceAttribute("salary", salaryType)
        );
        ExternalSourceMetadata metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return path;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };
        return new ExternalSourceResolution.ResolvedSource(metadata, FileList.UNRESOLVED, Map.of());
    }

    private static LogicalPlan analyzeInSubqueryWithK8s(String query) {
        return analyzer().addIndex("test", "mapping-basic.json").addK8s().query(query);
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
        assertThat(
            "Approximation should reject this query",
            ApproximationVerifier.verifyPlan(plan, TransportVersion.current()),
            nullValue()
        );
    }

    private static LogicalPlan unwrapProjects(LogicalPlan plan) {
        LogicalPlan p = plan;
        while (p instanceof Project) {
            p = p.children().get(0);
        }
        return p;
    }

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

    /**
     * Asserts {@code unionAll} has exactly two {@code emp_no} branches, each a FROM subquery over a view of {@code employees}: the
     * first an unfiltered view, the second a {@code salary > filteredMinSalary} view. Used for the UnionAlls produced both by the
     * main FROM command and by the IN/NOT IN subquery when each lists several view-referencing subqueries. Leading {@link Project}s
     * (from the chained KEEPs of the FROM subquery and the view body) are skipped via {@link #unwrapProjects}.
     */
    private static void assertUnionAllOfPlainAndSalaryFilteredEmployeesSubqueries(UnionAll unionAll, int filteredMinSalary) {
        assertEquals(2, unionAll.children().size());
        assertUnionAllSingleIntegerColumn(unionAll, "emp_no");

        Project plainBranch = as(unionAll.children().get(0), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, plainBranch);
        Subquery plainSub = as(unwrapProjects(plainBranch), Subquery.class);
        assertEquals("employees", as(unwrapProjects(plainSub.child()), EsRelation.class).indexPattern());

        Project filteredBranch = as(unionAll.children().get(1), Project.class);
        assertUnionAllBranchMatchesUnionOutput(unionAll, filteredBranch);
        Subquery filteredSub = as(unwrapProjects(filteredBranch), Subquery.class);
        Filter salaryFilter = as(unwrapProjects(filteredSub.child()), Filter.class);
        GreaterThan gt = as(salaryFilter.condition(), GreaterThan.class);
        assertEquals("salary", as(gt.left(), FieldAttribute.class).name());
        assertEquals(filteredMinSalary, as(gt.right(), Literal.class).value());
        assertEquals(DataType.INTEGER, as(gt.right(), Literal.class).dataType());
        assertEquals("employees", as(salaryFilter.child(), EsRelation.class).indexPattern());
    }

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
