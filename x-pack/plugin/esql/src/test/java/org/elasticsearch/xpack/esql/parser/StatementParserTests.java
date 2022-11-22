/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.Literal.FALSE;
import static org.elasticsearch.xpack.ql.expression.Literal.TRUE;
import static org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class StatementParserTests extends ESTestCase {

    private static String FROM = "from test";
    EsqlParser parser = new EsqlParser();

    public void testRowCommand() {
        assertEquals(
            new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1)), new Alias(EMPTY, "b", integer(2)))),
            statement("row a = 1, b = 2")
        );
    }

    public void testRowCommandImplicitFieldName() {
        assertEquals(
            new Row(
                EMPTY,
                List.of(new Alias(EMPTY, "1", integer(1)), new Alias(EMPTY, "2", integer(2)), new Alias(EMPTY, "c", integer(3)))
            ),
            statement("row 1, 2, c = 3")
        );
    }

    public void testRowCommandWithEscapedFieldName() {
        assertEquals(
            new Row(
                EMPTY,
                List.of(
                    new Alias(EMPTY, "a.b.c", integer(1)),
                    new Alias(EMPTY, "b", integer(2)),
                    new Alias(EMPTY, "@timestamp", new Literal(EMPTY, "2022-26-08T00:00:00", KEYWORD))
                )
            ),
            statement("row a.b.c = 1, `b` = 2, `@timestamp`=\"2022-26-08T00:00:00\"")
        );
    }

    public void testCompositeCommand() {
        assertEquals(
            new Filter(EMPTY, new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1)))), TRUE),
            statement("row a = 1 | where true")
        );
    }

    public void testMultipleCompositeCommands() {
        assertEquals(
            new Filter(
                EMPTY,
                new Filter(EMPTY, new Filter(EMPTY, new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1)))), TRUE), FALSE),
                TRUE
            ),
            statement("row a = 1 | where true | where false | where true")
        );
    }

    public void testEval() {
        assertEquals(
            new Eval(EMPTY, PROCESSING_CMD_INPUT, List.of(new Alias(EMPTY, "b", attribute("a")))),
            processingCommand("eval b = a")
        );

        assertEquals(
            new Eval(
                EMPTY,
                PROCESSING_CMD_INPUT,
                List.of(new Alias(EMPTY, "b", attribute("a")), new Alias(EMPTY, "c", new Add(EMPTY, attribute("a"), integer(1))))
            ),
            processingCommand("eval b = a, c = a + 1")
        );
    }

    public void testEvalImplicitNames() {
        assertEquals(new Eval(EMPTY, PROCESSING_CMD_INPUT, List.of(new Alias(EMPTY, "a", attribute("a")))), processingCommand("eval a"));

        assertEquals(
            new Eval(
                EMPTY,
                PROCESSING_CMD_INPUT,
                List.of(
                    new Alias(
                        EMPTY,
                        "fn(a+1)",
                        new UnresolvedFunction(EMPTY, "fn", DEFAULT, List.of(new Add(EMPTY, attribute("a"), integer(1))))
                    )
                )
            ),
            processingCommand("eval fn(a + 1)")
        );
    }

    public void testStatsWithGroups() {
        assertEquals(
            new Aggregate(
                EMPTY,
                PROCESSING_CMD_INPUT,
                List.of(attribute("c"), attribute("d.e")),
                List.of(
                    new Alias(EMPTY, "b", new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(attribute("a")))),
                    attribute("c"),
                    attribute("d.e")
                )
            ),
            processingCommand("stats b = min(a) by c, d.e")
        );
    }

    public void testStatsWithoutGroups() {
        assertEquals(
            new Aggregate(
                EMPTY,
                PROCESSING_CMD_INPUT,
                List.of(),
                List.of(
                    new Alias(EMPTY, "min(a)", new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(attribute("a")))),
                    new Alias(EMPTY, "c", integer(1))
                )
            ),
            processingCommand("stats min(a), c = 1")
        );
    }

    public void testIdentifiersAsIndexPattern() {
        assertIdentifierAsIndexPattern("foo", "from `foo`");
        assertIdentifierAsIndexPattern("foo,test-*", "from `foo`,`test-*`");
        assertIdentifierAsIndexPattern("foo,test-*", "from foo,test-*");
        assertIdentifierAsIndexPattern("123-test@foo_bar+baz1", "from 123-test@foo_bar+baz1");
        assertIdentifierAsIndexPattern("foo,test-*,abc", "from `foo`,`test-*`,abc");
        assertIdentifierAsIndexPattern("foo,     test-*, abc, xyz", "from `foo,     test-*, abc, xyz`");
        assertIdentifierAsIndexPattern("foo,     test-*, abc, xyz,test123", "from `foo,     test-*, abc, xyz`,     test123");
        assertIdentifierAsIndexPattern("foo,test,xyz", "from foo,   test,xyz");
        assertIdentifierAsIndexPattern(
            "<logstash-{now/M{yyyy.MM}}>,<logstash-{now/d{yyyy.MM.dd|+12:00}}>",
            "from <logstash-{now/M{yyyy.MM}}>, `<logstash-{now/d{yyyy.MM.dd|+12:00}}>`"
        );
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
                where = processingCommand("where " + identifiers[j] + operators[i] + "123");
                assertThat(where, instanceOf(Filter.class));
                Filter filter = (Filter) where;
                assertThat(filter.children().size(), equalTo(1));
                assertThat(filter.condition(), instanceOf(expectedOperators[i]));
                BinaryComparison comparison;
                if (filter.condition()instanceof Not not) {
                    assertThat(not.children().get(0), instanceOf(Equals.class));
                    comparison = (BinaryComparison) (not.children().get(0));
                } else {
                    comparison = (BinaryComparison) filter.condition();
                }
                assertThat(comparison.left(), instanceOf(UnresolvedAttribute.class));
                assertThat(((UnresolvedAttribute) comparison.left()).name(), equalTo(expectedIdentifiers[j]));
                assertThat(comparison.right(), instanceOf(Literal.class));
                assertThat(((Literal) comparison.right()).value(), equalTo(123));
                assertThat(filter.child(), equalTo(PROCESSING_CMD_INPUT));
            }
        }
    }

    public void testBooleanLiteralCondition() {
        LogicalPlan where = processingCommand("where true");
        assertThat(where, instanceOf(Filter.class));
        Filter w = (Filter) where;
        assertThat(w.child(), equalTo(PROCESSING_CMD_INPUT));
        assertThat(w.condition(), equalTo(TRUE));
    }

    public void testBasicLimitCommand() {
        LogicalPlan plan = statement("from text | where true | limit 5");
        assertThat(plan, instanceOf(Limit.class));
        Limit limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(5));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Filter.class));
        assertThat(limit.children().get(0).children().size(), equalTo(1));
        assertThat(limit.children().get(0).children().get(0), instanceOf(UnresolvedRelation.class));
    }

    public void testLimitConstraints() {
        ParsingException e = expectThrows(ParsingException.class, "Expected syntax error", () -> statement("from text | limit -1"));
        assertThat(e.getMessage(), startsWith("line 1:19: extraneous input '-' expecting INTEGER_LITERAL"));
    }

    public void testBasicSortCommand() {
        LogicalPlan plan = statement("from text | where true | sort a+b asc nulls first, x desc nulls last | sort y asc | sort z desc");
        assertThat(plan, instanceOf(OrderBy.class));
        OrderBy orderBy = (OrderBy) plan;
        assertThat(orderBy.order().size(), equalTo(1));
        Order order = orderBy.order().get(0);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        assertThat(order.child(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) order.child()).name(), equalTo("z"));

        assertThat(orderBy.children().size(), equalTo(1));
        assertThat(orderBy.children().get(0), instanceOf(OrderBy.class));
        orderBy = (OrderBy) orderBy.children().get(0);
        assertThat(orderBy.order().size(), equalTo(1));
        order = orderBy.order().get(0);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(order.child(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) order.child()).name(), equalTo("y"));

        assertThat(orderBy.children().size(), equalTo(1));
        assertThat(orderBy.children().get(0), instanceOf(OrderBy.class));
        orderBy = (OrderBy) orderBy.children().get(0);
        assertThat(orderBy.order().size(), equalTo(2));
        order = orderBy.order().get(0);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        assertThat(order.child(), instanceOf(Add.class));
        Add add = (Add) order.child();
        assertThat(add.left(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) add.left()).name(), equalTo("a"));
        assertThat(add.right(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) add.right()).name(), equalTo("b"));
        order = orderBy.order().get(1);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(order.child(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) order.child()).name(), equalTo("x"));

        assertThat(orderBy.children().size(), equalTo(1));
        assertThat(orderBy.children().get(0), instanceOf(Filter.class));
        assertThat(orderBy.children().get(0).children().size(), equalTo(1));
        assertThat(orderBy.children().get(0).children().get(0), instanceOf(UnresolvedRelation.class));
    }

    public void testSubquery() {
        assertEquals(new Explain(EMPTY, PROCESSING_CMD_INPUT), statement("explain [ row a = 1 ]"));
    }

    public void testSubqueryWithPipe() {
        assertEquals(
            new Limit(EMPTY, integer(10), new Explain(EMPTY, PROCESSING_CMD_INPUT)),
            statement("explain [ row a = 1 ] | limit 10")
        );
    }

    public void testNestedSubqueries() {
        assertEquals(
            new Limit(
                EMPTY,
                integer(10),
                new Explain(EMPTY, new Limit(EMPTY, integer(5), new Explain(EMPTY, new Limit(EMPTY, integer(1), PROCESSING_CMD_INPUT))))
            ),
            statement("explain [ explain [ row a = 1 | limit 1 ] | limit 5 ] | limit 10")
        );
    }

    public void testSubquerySpacing() {
        assertEquals(statement("explain [ explain [ from a ] | where b == 1 ]"), statement("explain[explain[from a]|where b==1]"));
    }

    public void testBlockComments() {
        String query = " explain [ from foo ] | limit 10 ";
        LogicalPlan expected = statement(query);

        int wsIndex = query.indexOf(' ');

        do {
            String queryWithComment = query.substring(0, wsIndex)
                + "/*explain [ \nfrom bar ] | where a > b*/"
                + query.substring(wsIndex + 1);

            assertEquals(expected, statement(queryWithComment));

            wsIndex = query.indexOf(' ', wsIndex + 1);
        } while (wsIndex >= 0);
    }

    public void testSingleLineComments() {
        String query = " explain [ from foo ] | limit 10 ";
        LogicalPlan expected = statement(query);

        int wsIndex = query.indexOf(' ');

        do {
            String queryWithComment = query.substring(0, wsIndex)
                + "//explain [ from bar ] | where a > b \n"
                + query.substring(wsIndex + 1);

            assertEquals(expected, statement(queryWithComment));

            wsIndex = query.indexOf(' ', wsIndex + 1);
        } while (wsIndex >= 0);
    }

    public void testSuggestAvailableSourceCommandsOnParsingError() {
        for (Tuple<String, String> queryWithUnexpectedCmd : List.of(
            Tuple.tuple("frm foo", "frm"),
            Tuple.tuple("expln[from bar]", "expln"),
            Tuple.tuple("not-a-thing logs", "not-a-thing"),
            Tuple.tuple("high5 a", "high5"),
            Tuple.tuple("a+b = c", "a+b"),
            Tuple.tuple("a//hi", "a"),
            Tuple.tuple("a/*hi*/", "a"),
            Tuple.tuple("explain [ frm a ]", "frm")
        )) {
            ParsingException pe = expectThrows(ParsingException.class, () -> statement(queryWithUnexpectedCmd.v1()));
            assertThat(
                pe.getMessage(),
                allOf(
                    containsString("mismatched input '" + queryWithUnexpectedCmd.v2() + "'"),
                    containsString("'explain'"),
                    containsString("'from'"),
                    containsString("'row'")
                )
            );
        }
    }

    public void testSuggestAvailableProcessingCommandsOnParsingError() {
        for (Tuple<String, String> queryWithUnexpectedCmd : List.of(
            Tuple.tuple("from a | filter b > 1", "filter"),
            Tuple.tuple("from a | explain [ row 1 ]", "explain"),
            Tuple.tuple("from a | not-a-thing", "not-a-thing"),
            Tuple.tuple("from a | high5 a", "high5"),
            Tuple.tuple("from a | a+b = c", "a+b"),
            Tuple.tuple("from a | a//hi", "a"),
            Tuple.tuple("from a | a/*hi*/", "a"),
            Tuple.tuple("explain [ from a | evl b = c ]", "evl")
        )) {
            ParsingException pe = expectThrows(ParsingException.class, () -> statement(queryWithUnexpectedCmd.v1()));
            assertThat(
                pe.getMessage(),
                allOf(
                    containsString("mismatched input '" + queryWithUnexpectedCmd.v2() + "'"),
                    containsString("'eval'"),
                    containsString("'stats'"),
                    containsString("'where'")
                )
            );
        }
    }

    private void assertIdentifierAsIndexPattern(String identifier, String statement) {
        LogicalPlan from = statement(statement);
        assertThat(from, instanceOf(UnresolvedRelation.class));
        UnresolvedRelation table = (UnresolvedRelation) from;
        assertThat(table.table().index(), is(identifier));
    }

    private LogicalPlan statement(String e) {
        return parser.createStatement(e);
    }

    private LogicalPlan processingCommand(String e) {
        return parser.createStatement("row a = 1 | " + e);
    }

    private static final LogicalPlan PROCESSING_CMD_INPUT = new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1))));

    private static UnresolvedAttribute attribute(String name) {
        return new UnresolvedAttribute(EMPTY, name);
    }

    private static Literal integer(int i) {
        return new Literal(EMPTY, i, INTEGER);
    }
}
