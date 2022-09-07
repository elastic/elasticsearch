/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class StatementParserTests extends ESTestCase {

    EsqlParser parser = new EsqlParser();

    public void testRowCommand() {
        assertEquals(
            new Row(
                EMPTY,
                List.of(
                    new Alias(EMPTY, "a", new Literal(EMPTY, 1, DataTypes.INTEGER)),
                    new Alias(EMPTY, "b", new Literal(EMPTY, 2, DataTypes.INTEGER))
                )
            ),
            statement("row a = 1, b = 2")
        );
    }

    public void testRowCommandImplicitFieldName() {
        assertEquals(
            new Row(
                EMPTY,
                List.of(
                    new Alias(EMPTY, "1", new Literal(EMPTY, 1, DataTypes.INTEGER)),
                    new Alias(EMPTY, "2", new Literal(EMPTY, 2, DataTypes.INTEGER)),
                    new Alias(EMPTY, "c", new Literal(EMPTY, 3, DataTypes.INTEGER))
                )
            ),
            statement("row 1, 2, c = 3")
        );
    }

    public void testRowCommandWithEscapedFieldName() {
        assertEquals(
            new Row(
                EMPTY,
                List.of(
                    new Alias(EMPTY, "a.b.c", new Literal(EMPTY, 1, DataTypes.INTEGER)),
                    new Alias(EMPTY, "b", new Literal(EMPTY, 2, DataTypes.INTEGER)),
                    new Alias(EMPTY, "@timestamp", new Literal(EMPTY, "2022-26-08T00:00:00", DataTypes.KEYWORD))
                )
            ),
            statement("row a.b.c = 1, `b` = 2, `@timestamp`=\"2022-26-08T00:00:00\"")
        );
    }

    public void testIdentifiersAsIndexPattern() {
        assertIdentifierAsIndexPattern("foo", "from `foo`");
        assertIdentifierAsIndexPattern("foo,test-*", "from `foo`,`test-*`");
        assertIdentifierAsIndexPattern("foo,test-*,abc", "from `foo`,`test-*`,abc");
        assertIdentifierAsIndexPattern("foo,     test-*, abc, xyz", "from `foo,     test-*, abc, xyz`");
        assertIdentifierAsIndexPattern("foo,     test-*, abc, xyz,test123", "from `foo,     test-*, abc, xyz`,     test123");
        assertIdentifierAsIndexPattern("foo,test,xyz", "from foo,   test,xyz");
    }

    public void testIdentifierAsFieldName() {
        String[] operators = new String[] { "==", "!=", ">", "<", ">=", "<=" };
        Class<?>[] expectedOperators = new Class<?>[] {
            Equals.class,
            Not.class,
            GreaterThan.class,
            LessThan.class,
            GreaterThanOrEqual.class,
            LessThanOrEqual.class };
        String[] identifiers = new String[] { "abc", "`abc`", "ab_c", "a.b.c", "`a@b.c`" };
        String[] expectedIdentifiers = new String[] { "abc", "abc", "ab_c", "a.b.c", "a@b.c" };
        LogicalPlan where;
        for (int i = 0; i < operators.length; i++) {
            for (int j = 0; j < identifiers.length; j++) {
                where = whereCommand("where " + identifiers[j] + operators[i] + "123");
                assertThat(where, instanceOf(Filter.class));
                Filter w = (Filter) where;
                assertThat(w.children().size(), equalTo(1));
                assertThat(w.children().get(0), equalTo(LogicalPlanBuilder.RELATION));
                assertThat(w.condition(), instanceOf(expectedOperators[i]));
                BinaryComparison comparison;
                if (w.condition()instanceof Not not) {
                    assertThat(not.children().get(0), instanceOf(Equals.class));
                    comparison = (BinaryComparison) (not.children().get(0));
                } else {
                    comparison = (BinaryComparison) w.condition();
                }
                assertThat(comparison.left(), instanceOf(UnresolvedAttribute.class));
                assertThat(((UnresolvedAttribute) comparison.left()).name(), equalTo(expectedIdentifiers[j]));
                assertThat(comparison.right(), instanceOf(Literal.class));
                assertThat(((Literal) comparison.right()).value(), equalTo(123));
            }
        }
    }

    public void testBooleanLiteralCondition() {
        LogicalPlan where = whereCommand("where true");
        assertThat(where, instanceOf(Filter.class));
        Filter w = (Filter) where;
        assertThat(w.children().size(), equalTo(1));
        assertThat(w.children().get(0), equalTo(LogicalPlanBuilder.RELATION));
        assertThat(w.condition(), equalTo(Literal.TRUE));
    }

    private void assertIdentifierAsIndexPattern(String identifier, String statement) {
        LogicalPlan from = statement(statement);
        assertThat(from, instanceOf(Project.class));
        Project p = (Project) from;
        assertThat(p.resolved(), is(false));
        assertThat(p.projections().size(), equalTo(1));
        assertThat(p.projections().get(0), instanceOf(UnresolvedStar.class));
        assertThat(p.children().size(), is(1));
        assertThat(p.children().get(0), instanceOf(UnresolvedRelation.class));
        UnresolvedRelation table = (UnresolvedRelation) p.children().get(0);
        assertThat(table.table().index(), is(identifier));
    }

    private LogicalPlan statement(String e) {
        return parser.createStatement(e);
    }

    private LogicalPlan whereCommand(String e) {
        return parser.createStatement("from a | " + e);
    }
}
