/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsqlAggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsqlUnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.ql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.EmptyAttribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.versionfield.Version;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.parser.ExpressionBuilder.breakIntoFragments;
import static org.elasticsearch.xpack.ql.expression.Literal.FALSE;
import static org.elasticsearch.xpack.ql.expression.Literal.TRUE;
import static org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

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

    public void testRowCommandLong() {
        assertEquals(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalLong(2147483648L)))), statement("row c = 2147483648"));
    }

    public void testRowCommandHugeInt() {
        assertEquals(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalUnsignedLong("9223372036854775808")))),
            statement("row c = 9223372036854775808")
        );
        assertEquals(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDouble(18446744073709551616.)))),
            statement("row c = 18446744073709551616")
        );
    }

    public void testRowCommandDouble() {
        assertEquals(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDouble(1.0)))), statement("row c = 1.0"));
    }

    public void testRowCommandMultivalueInt() {
        assertEquals(new Row(EMPTY, List.of(new Alias(EMPTY, "c", integers(1, 2, -5)))), statement("row c = [1, 2, -5]"));
    }

    public void testRowCommandMultivalueLong() {
        assertEquals(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalLongs(2147483648L, 2147483649L, -434366649L)))),
            statement("row c = [2147483648, 2147483649, -434366649]")
        );
    }

    public void testRowCommandMultivalueLongAndInt() {
        assertEquals(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalLongs(2147483648L, 1L)))), statement("row c = [2147483648, 1]"));
    }

    public void testRowCommandMultivalueHugeInts() {
        assertEquals(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDoubles(18446744073709551616., 18446744073709551617.)))),
            statement("row c = [18446744073709551616, 18446744073709551617]")
        );
        assertEquals(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalUnsignedLongs("9223372036854775808", "9223372036854775809")))),
            statement("row c = [9223372036854775808, 9223372036854775809]")
        );
    }

    public void testRowCommandMultivalueHugeIntAndNormalInt() {
        assertEquals(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDoubles(18446744073709551616., 1.0)))),
            statement("row c = [18446744073709551616, 1]")
        );
        assertEquals(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalUnsignedLongs("9223372036854775808", "1")))),
            statement("row c = [9223372036854775808, 1]")
        );
    }

    public void testRowCommandMultivalueDouble() {
        assertEquals(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDoubles(1.0, 2.0, -3.4)))), statement("row c = [1.0, 2.0, -3.4]"));
    }

    public void testRowCommandBoolean() {
        assertEquals(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalBoolean(false)))), statement("row c = false"));
    }

    public void testRowCommandMultivalueBoolean() {
        assertEquals(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalBooleans(false, true)))), statement("row c = [false, true]"));
    }

    public void testRowCommandString() {
        assertEquals(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalString("chicken")))), statement("row c = \"chicken\""));
    }

    public void testRowCommandMultivalueString() {
        assertEquals(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalStrings("cat", "dog")))), statement("row c = [\"cat\", \"dog\"]"));
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
                        "fn(a + 1)",
                        new UnresolvedFunction(EMPTY, "fn", DEFAULT, List.of(new Add(EMPTY, attribute("a"), integer(1))))
                    )
                )
            ),
            processingCommand("eval fn(a + 1)")
        );
    }

    public void testStatsWithGroups() {
        assertEquals(
            new EsqlAggregate(
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
            new EsqlAggregate(
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

    public void testStatsWithoutAggs() throws Exception {
        assertEquals(
            new EsqlAggregate(EMPTY, PROCESSING_CMD_INPUT, List.of(attribute("a")), List.of(attribute("a"))),
            processingCommand("stats by a")
        );
    }

    public void testStatsWithoutAggsOrGroup() throws Exception {
        expectError("from text | stats", "At least one aggregation or grouping expression required in [stats]");
    }

    public void testAggsWithGroupKeyAsAgg() throws Exception {
        var queries = new String[] { """
            row a = 1, b = 2
            | stats a by a
            """, """
            row a = 1, b = 2
            | stats a by a
            | sort a
            """, """
            row a = 1, b = 2
            | stats a = a by a
            """, """
            row a = 1, b = 2
            | stats x = a by a
            """ };

        for (String query : queries) {
            expectVerificationError(query, "grouping key [a] already specified in the STATS BY clause");
        }
    }

    public void testInlineStatsWithGroups() {
        assertEquals(
            new InlineStats(
                EMPTY,
                PROCESSING_CMD_INPUT,
                List.of(attribute("c"), attribute("d.e")),
                List.of(
                    new Alias(EMPTY, "b", new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(attribute("a")))),
                    attribute("c"),
                    attribute("d.e")
                )
            ),
            processingCommand("inlinestats b = min(a) by c, d.e")
        );
    }

    public void testInlineStatsWithoutGroups() {
        assertEquals(
            new InlineStats(
                EMPTY,
                PROCESSING_CMD_INPUT,
                List.of(),
                List.of(
                    new Alias(EMPTY, "min(a)", new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(attribute("a")))),
                    new Alias(EMPTY, "c", integer(1))
                )
            ),
            processingCommand("inlinestats min(a), c = 1")
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
        String[] identifiers = new String[] { "abc", "`abc`", "ab_c", "a.b.c", "@a", "a.@b", "`a@b.c`" };
        String[] expectedIdentifiers = new String[] { "abc", "abc", "ab_c", "a.b.c", "@a", "a.@b", "a@b.c" };
        LogicalPlan where;
        for (int i = 0; i < operators.length; i++) {
            for (int j = 0; j < identifiers.length; j++) {
                where = processingCommand("where " + identifiers[j] + operators[i] + "123");
                assertThat(where, instanceOf(Filter.class));
                Filter filter = (Filter) where;
                assertThat(filter.children().size(), equalTo(1));
                assertThat(filter.condition(), instanceOf(expectedOperators[i]));
                BinaryComparison comparison;
                if (filter.condition() instanceof Not not) {
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
        assertThat(limit.children().get(0).children().get(0), instanceOf(EsqlUnresolvedRelation.class));
    }

    public void testLimitConstraints() {
        expectError("from text | limit -1", "extraneous input '-' expecting INTEGER_LITERAL");
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
        assertThat(orderBy.children().get(0).children().get(0), instanceOf(EsqlUnresolvedRelation.class));
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

    public void testNewLines() {
        String[] delims = new String[] { "", "\r", "\n", "\r\n" };
        Function<String, String> queryFun = d -> d + "from " + d + " foo " + d + "| eval " + d + " x = concat(bar, \"baz\")" + d;
        LogicalPlan reference = statement(queryFun.apply(delims[0]));
        for (int i = 1; i < delims.length; i++) {
            LogicalPlan candidate = statement(queryFun.apply(delims[i]));
            assertThat(candidate, equalTo(reference));
        }
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

    public void testDeprecatedIsNullFunction() {
        expectError(
            "from test | eval x = is_null(f)",
            "line 1:23: is_null function is not supported anymore, please use 'is null'/'is not null' predicates instead"
        );
        expectError(
            "row x = is_null(f)",
            "line 1:10: is_null function is not supported anymore, please use 'is null'/'is not null' predicates instead"
        );
    }

    public void testMetadataFieldOnOtherSources() {
        expectError("row a = 1 metadata _index", "line 1:20: extraneous input '_index' expecting <EOF>");
        expectError("meta functions metadata _index", "line 1:16: token recognition error at: 'm'");
        expectError("show info metadata _index", "line 1:11: token recognition error at: 'm'");
        expectError(
            "explain [from foo] metadata _index",
            "line 1:20: mismatched input 'metadata' expecting {'|', ',', OPENING_BRACKET, ']', 'options', 'metadata'}"
        );
    }

    public void testMetadataFieldMultipleDeclarations() {
        expectError("from test metadata _index, _version, _index", "1:39: metadata field [_index] already declared [@1:20]");
    }

    public void testMetadataFieldUnsupportedPrimitiveType() {
        expectError("from test metadata _tier", "line 1:21: unsupported metadata field [_tier]");
    }

    public void testMetadataFieldUnsupportedCustomType() {
        expectError("from test metadata _feature", "line 1:21: unsupported metadata field [_feature]");
    }

    public void testMetadataFieldNotFoundNonExistent() {
        expectError("from test metadata _doesnot_compute", "line 1:21: unsupported metadata field [_doesnot_compute]");
    }

    public void testMetadataFieldNotFoundNormalField() {
        expectError("from test metadata emp_no", "line 1:21: unsupported metadata field [emp_no]");
    }

    public void testFromOptionsUnknownName() {
        expectError(FROM + " options \"foo\"=\"oof\",\"bar\"=\"rab\"", "line 1:20: invalid options provided: unknown option named [foo]");
    }

    public void testFromOptionsPartialInvalid() {
        expectError(
            FROM + " options \"allow_no_indices\"=\"true\",\"bar\"=\"rab\"",
            "line 1:46: invalid options provided: unknown option named [bar]"
        );
    }

    public void testFromOptionsInvalidIndicesOptionValue() {
        expectError(
            FROM + " options \"allow_no_indices\"=\"foo\"",
            "line 1:20: invalid options provided: Could not convert [allow_no_indices] to boolean"
        );
    }

    public void testFromOptionsEmptyIndicesOptionName() {
        expectError(FROM + " options \"\"=\"true\"", "line 1:20: invalid options provided: unknown option named []");
    }

    public void testFromOptionsEmptyIndicesOptionValue() {
        expectError(
            FROM + " options \"allow_no_indices\"=\"\"",
            "line 1:20: invalid options provided: Could not convert [allow_no_indices] to boolean. "
                + "Failed to parse value [] as only [true] or [false] are allowed."
        );
        expectError(
            FROM + " options \"ignore_unavailable\"=\"TRUE\"",
            "line 1:20: invalid options provided: Could not convert [ignore_unavailable] to boolean. "
                + "Failed to parse value [TRUE] as only [true] or [false] are allowed."
        );
        expectError(FROM + " options \"preference\"=\"\"", "line 1:20: invalid options provided: no Preference for []");
    }

    public void testFromOptionsSuggestedOptionName() {
        expectError(
            FROM + " options \"allow_indices\"=\"true\"",
            "line 1:20: invalid options provided: unknown option named [allow_indices], did you mean [allow_no_indices]?"
        );
    }

    public void testFromOptionsInvalidPreferValue() {
        expectError(FROM + " options \"preference\"=\"_foo\"", "line 1:20: invalid options provided: no Preference for [_foo]");
    }

    public void testFromOptionsUnquotedName() {
        expectError(FROM + " options allow_no_indices=\"oof\"", "line 1:19: mismatched input 'allow_no_indices' expecting QUOTED_STRING");
    }

    public void testFromOptionsUnquotedValue() {
        expectError(FROM + " options \"allow_no_indices\"=oof", "line 1:38: mismatched input 'oof' expecting QUOTED_STRING");
    }

    public void testFromOptionsDuplicates() {
        for (var name : List.of("allow_no_indices", "ignore_unavailable", "preference")) {
            String options = '"' + name + "\"=\"false\"";
            options += ',' + options;
            expectError(FROM + " options " + options, "invalid options provided: option [" + name + "] has already been provided");
        }
    }

    public void testFromOptionsValues() {
        boolean allowNoIndices = randomBoolean();
        boolean ignoreUnavailable = randomBoolean();
        String idsList = String.join(",", randomList(1, 5, () -> randomAlphaOfLengthBetween(1, 25)));
        String preference = randomFrom(
            "_only_local",
            "_local",
            "_only_nodes:" + idsList,
            "_prefer_nodes:" + idsList,
            "_shards:" + idsList,
            randomAlphaOfLengthBetween(1, 25)
        );
        List<String> options = new ArrayList<>(3);
        options.add("\"allow_no_indices\"=\"" + allowNoIndices + "\"");
        options.add("\"ignore_unavailable\"=\"" + ignoreUnavailable + "\"");
        options.add("\"preference\"=\"" + preference + "\"");
        Randomness.shuffle(options);
        String optionsList = String.join(",", options);

        var plan = statement(FROM + " OPTIONS " + optionsList);
        var unresolved = as(plan, EsqlUnresolvedRelation.class);
        assertNotNull(unresolved.esSourceOptions());
        var indicesOptions = unresolved.esSourceOptions().indicesOptions(SearchRequest.DEFAULT_INDICES_OPTIONS);
        assertThat(indicesOptions.allowNoIndices(), is(allowNoIndices));
        assertThat(indicesOptions.ignoreUnavailable(), is(ignoreUnavailable));
        assertThat(unresolved.esSourceOptions().preference(), is(preference));
    }

    public void testFromOptionsWithMetadata() {
        var plan = statement(FROM + " METADATA _id OPTIONS \"preference\"=\"foo\"");
        var unresolved = as(plan, EsqlUnresolvedRelation.class);
        assertNotNull(unresolved.esSourceOptions());
        assertThat(unresolved.esSourceOptions().preference(), is("foo"));
        assertFalse(unresolved.metadataFields().isEmpty());
        assertThat(unresolved.metadataFields().get(0).qualifiedName(), is("_id"));
    }

    public void testDissectPattern() {
        LogicalPlan cmd = processingCommand("dissect a \"%{foo}\"");
        assertEquals(Dissect.class, cmd.getClass());
        Dissect dissect = (Dissect) cmd;
        assertEquals("%{foo}", dissect.parser().pattern());
        assertEquals("", dissect.parser().appendSeparator());
        assertEquals(List.of(referenceAttribute("foo", KEYWORD)), dissect.extractedFields());

        for (String separatorName : List.of("append_separator", "APPEND_SEPARATOR", "AppEnd_SeparAtor")) {
            cmd = processingCommand("dissect a \"%{foo}\" " + separatorName + "=\",\"");
            assertEquals(Dissect.class, cmd.getClass());
            dissect = (Dissect) cmd;
            assertEquals("%{foo}", dissect.parser().pattern());
            assertEquals(",", dissect.parser().appendSeparator());
            assertEquals(List.of(referenceAttribute("foo", KEYWORD)), dissect.extractedFields());
        }

        for (Tuple<String, String> queryWithUnexpectedCmd : List.of(
            Tuple.tuple("from a | dissect foo \"\"", "[]"),
            Tuple.tuple("from a | dissect foo \" \"", "[ ]"),
            Tuple.tuple("from a | dissect foo \"no fields\"", "[no fields]")
        )) {
            expectError(queryWithUnexpectedCmd.v1(), "Invalid pattern for dissect: " + queryWithUnexpectedCmd.v2());
        }

        expectError("from a | dissect foo \"%{*a}:%{&a}\"", "Reference keys not supported in dissect patterns: [%{*a}]");
        expectError("from a | dissect foo \"%{bar}\" invalid_option=3", "Invalid option for dissect: [invalid_option]");
        expectError(
            "from a | dissect foo \"%{bar}\" append_separator=3",
            "Invalid value for dissect append_separator: expected a string, but was [3]"
        );
        expectError("from a | dissect foo \"%{}\"", "Invalid pattern for dissect: [%{}]");
    }

    public void testGrokPattern() {
        LogicalPlan cmd = processingCommand("grok a \"%{WORD:foo}\"");
        assertEquals(Grok.class, cmd.getClass());
        Grok dissect = (Grok) cmd;
        assertEquals("%{WORD:foo}", dissect.parser().pattern());
        assertEquals(List.of(referenceAttribute("foo", KEYWORD)), dissect.extractedFields());

        ParsingException pe = expectThrows(ParsingException.class, () -> statement("row a = \"foo bar\" | grok a \"%{_invalid_:x}\""));
        assertThat(
            pe.getMessage(),
            containsString("Invalid pattern [%{_invalid_:x}] for grok: Unable to find pattern [_invalid_] in Grok's pattern dictionary")
        );
    }

    public void testLikeRLike() {
        LogicalPlan cmd = processingCommand("where foo like \"*bar*\"");
        assertEquals(Filter.class, cmd.getClass());
        Filter filter = (Filter) cmd;
        assertEquals(WildcardLike.class, filter.condition().getClass());
        WildcardLike like = (WildcardLike) filter.condition();
        assertEquals("*bar*", like.pattern().pattern());

        cmd = processingCommand("where foo rlike \".*bar.*\"");
        assertEquals(Filter.class, cmd.getClass());
        filter = (Filter) cmd;
        assertEquals(RLike.class, filter.condition().getClass());
        RLike rlike = (RLike) filter.condition();
        assertEquals(".*bar.*", rlike.pattern().asJavaRegex());

        expectError("from a | where foo like 12", "mismatched input '12'");
        expectError("from a | where foo rlike 12", "mismatched input '12'");
    }

    public void testEnrich() {
        assertEquals(
            new Enrich(
                EMPTY,
                PROCESSING_CMD_INPUT,
                null,
                new Literal(EMPTY, "countries", KEYWORD),
                new EmptyAttribute(EMPTY),
                null,
                Map.of(),
                List.of()
            ),
            processingCommand("enrich countries")
        );

        assertEquals(
            new Enrich(
                EMPTY,
                PROCESSING_CMD_INPUT,
                null,
                new Literal(EMPTY, "index-policy", KEYWORD),
                new UnresolvedAttribute(EMPTY, "field_underscore"),
                null,
                Map.of(),
                List.of()
            ),
            processingCommand("enrich index-policy ON field_underscore")
        );

        Enrich.Mode mode = randomFrom(Enrich.Mode.values());
        assertEquals(
            new Enrich(
                EMPTY,
                PROCESSING_CMD_INPUT,
                mode,
                new Literal(EMPTY, "countries", KEYWORD),
                new UnresolvedAttribute(EMPTY, "country_code"),
                null,
                Map.of(),
                List.of()
            ),
            processingCommand("enrich _" + mode.name() + ":countries ON country_code")
        );

        expectError("from a | enrich countries on foo* ", "Using wildcards (*) in ENRICH WITH projections is not allowed [foo*]");
        expectError("from a | enrich countries on foo with bar*", "Using wildcards (*) in ENRICH WITH projections is not allowed [bar*]");
        expectError(
            "from a | enrich countries on foo with x = bar* ",
            "Using wildcards (*) in ENRICH WITH projections is not allowed [bar*]"
        );
        expectError(
            "from a | enrich countries on foo with x* = bar ",
            "Using wildcards (*) in ENRICH WITH projections is not allowed [x*]"
        );
        expectError(
            "from a | enrich typo:countries on foo",
            "line 1:18: Unrecognized value [typo], ENRICH policy qualifier needs to be one of [_ANY, _COORDINATOR, _REMOTE]"
        );
    }

    public void testMvExpand() {
        LogicalPlan cmd = processingCommand("mv_expand a");
        assertEquals(MvExpand.class, cmd.getClass());
        MvExpand expand = (MvExpand) cmd;
        assertThat(expand.target(), equalTo(attribute("a")));
    }

    // see https://github.com/elastic/elasticsearch/issues/103331
    public void testKeepStarMvExpand() {
        try {
            String query = "from test | keep * | mv_expand a";
            var plan = statement(query);
        } catch (UnresolvedException e) {
            fail(e, "Regression: https://github.com/elastic/elasticsearch/issues/103331");
        }

    }

    public void testUsageOfProject() {
        String query = "from test | project foo, bar";
        ParsingException e = expectThrows(ParsingException.class, "Expected syntax error for " + query, () -> statement(query));
        assertThat(e.getMessage(), containsString("mismatched input 'project' expecting"));
    }

    public void testInputParams() {
        LogicalPlan stm = statement(
            "row x = ?, y = ?, a = ?, b = ?, c = ?",
            List.of(
                new TypedParamValue("integer", 1),
                new TypedParamValue("keyword", "2"),
                new TypedParamValue("date_period", "2 days"),
                new TypedParamValue("time_duration", "4 hours"),
                new TypedParamValue("version", "1.2.3")
            )
        );
        assertThat(stm, instanceOf(Row.class));
        Row row = (Row) stm;
        assertThat(row.fields().size(), is(5));

        NamedExpression field = row.fields().get(0);
        assertThat(field.name(), is("x"));
        assertThat(field, instanceOf(Alias.class));
        Alias alias = (Alias) field;
        assertThat(alias.child().fold(), is(1));

        field = row.fields().get(1);
        assertThat(field.name(), is("y"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(), is("2"));

        field = row.fields().get(2);
        assertThat(field.name(), is("a"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(), is(Period.ofDays(2)));

        field = row.fields().get(3);
        assertThat(field.name(), is("b"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(), is(Duration.ofHours(4)));

        field = row.fields().get(4);
        assertThat(field.name(), is("c"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold().getClass(), is(Version.class));
        assertThat(alias.child().fold().toString(), is("1.2.3"));
    }

    public void testWrongIntervalParams() {
        expectError("row x = ?", List.of(new TypedParamValue("date_period", "12")), "Cannot parse [12] to DATE_PERIOD");
        expectError("row x = ?", List.of(new TypedParamValue("time_duration", "12")), "Cannot parse [12] to TIME_DURATION");
        expectError(
            "row x = ?",
            List.of(new TypedParamValue("date_period", "12 months foo")),
            "Cannot parse [12 months foo] to DATE_PERIOD"
        );
        expectError(
            "row x = ?",
            List.of(new TypedParamValue("time_duration", "12 minutes bar")),
            "Cannot parse [12 minutes bar] to TIME_DURATION"
        );
        expectError("row x = ?", List.of(new TypedParamValue("date_period", "12 foo")), "Unexpected time interval qualifier: 'foo'");
        expectError("row x = ?", List.of(new TypedParamValue("time_duration", "12 bar")), "Unexpected time interval qualifier: 'bar'");
        expectError("row x = ?", List.of(new TypedParamValue("date_period", "foo days")), "Cannot parse [foo days] to DATE_PERIOD");
        expectError(
            "row x = ?",
            List.of(new TypedParamValue("time_duration", "bar seconds")),
            "Cannot parse [bar seconds] to TIME_DURATION"
        );

        expectError(
            "row x = ?",
            List.of(new TypedParamValue("date_period", "2 minutes")),
            "Cannot parse [2 minutes] to DATE_PERIOD, did you mean TIME_DURATION?"
        );
        expectError(
            "row x = ?",
            List.of(new TypedParamValue("time_duration", "11 months")),
            "Cannot parse [11 months] to TIME_DURATION, did you mean DATE_PERIOD?"
        );
    }

    public void testMissingInputParams() {
        expectError("row x = ?, y = ?", List.of(new TypedParamValue("integer", 1)), "Not enough actual parameters 1");
    }

    public void testFieldContainingDotsAndNumbers() {
        LogicalPlan where = processingCommand("where `a.b.1m.4321`");
        assertThat(where, instanceOf(Filter.class));
        Filter w = (Filter) where;
        assertThat(w.child(), equalTo(PROCESSING_CMD_INPUT));
        assertThat(Expressions.name(w.condition()), equalTo("a.b.1m.4321"));
    }

    public void testFieldQualifiedName() {
        LogicalPlan where = processingCommand("where a.b.`1m`.`4321`");
        assertThat(where, instanceOf(Filter.class));
        Filter w = (Filter) where;
        assertThat(w.child(), equalTo(PROCESSING_CMD_INPUT));
        assertThat(Expressions.name(w.condition()), equalTo("a.b.1m.4321"));
    }

    public void testQuotedName() {
        // row `my-field`=123 | stats count(`my-field`) | eval x = `count(`my-field`)`
        LogicalPlan plan = processingCommand("stats count(`my-field`) |  keep `count(``my-field``)`");
        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("count(`my-field`)"));
    }

    private void assertIdentifierAsIndexPattern(String identifier, String statement) {
        LogicalPlan from = statement(statement);
        assertThat(from, instanceOf(EsqlUnresolvedRelation.class));
        EsqlUnresolvedRelation table = (EsqlUnresolvedRelation) from;
        assertThat(table.table().index(), is(identifier));
    }

    public void testIdPatternUnquoted() throws Exception {
        var string = "regularString";
        assertThat(breakIntoFragments(string), contains(string));
    }

    public void testIdPatternQuoted() throws Exception {
        var string = "`escaped string`";
        assertThat(breakIntoFragments(string), contains(string));
    }

    public void testIdPatternQuotedWithDoubleBackticks() throws Exception {
        var string = "`escaped``string`";
        assertThat(breakIntoFragments(string), contains(string));
    }

    public void testIdPatternUnquotedAndQuoted() throws Exception {
        var string = "this`is`a`mix`of`ids`";
        assertThat(breakIntoFragments(string), contains("this", "`is`", "a", "`mix`", "of", "`ids`"));
    }

    public void testIdPatternQuotedTraling() throws Exception {
        var string = "`foo`*";
        assertThat(breakIntoFragments(string), contains("`foo`", "*"));
    }

    public void testIdPatternWithDoubleQuotedStrings() throws Exception {
        var string = "`this``is`a`quoted `` string``with`backticks";
        assertThat(breakIntoFragments(string), contains("`this``is`", "a", "`quoted `` string``with`", "backticks"));
    }

    public void testSpaceNotAllowedInIdPattern() throws Exception {
        expectError("ROW a = 1| RENAME a AS this is `not okay`", "mismatched input 'is' expecting {'.', 'as'}");
    }

    public void testSpaceNotAllowedInIdPatternKeep() throws Exception {
        expectError("ROW a = 1, b = 1| KEEP a b", "extraneous input 'b'");
    }

    public void testEnrichOnMatchField() {
        var plan = statement("ROW a = \"1\" | ENRICH languages_policy ON a WITH ```name``* = language_name`");
        var enrich = as(plan, Enrich.class);
        var lists = enrich.enrichFields();
        assertThat(lists, hasSize(1));
        var ua = as(lists.get(0), UnresolvedAttribute.class);
        assertThat(ua.name(), is("`name`* = language_name"));
    }

    private LogicalPlan statement(String e) {
        return statement(e, List.of());
    }

    private LogicalPlan statement(String e, List<TypedParamValue> params) {
        return parser.createStatement(e, params);
    }

    private LogicalPlan processingCommand(String e) {
        return parser.createStatement("row a = 1 | " + e);
    }

    private static final LogicalPlan PROCESSING_CMD_INPUT = new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1))));

    private static UnresolvedAttribute attribute(String name) {
        return new UnresolvedAttribute(EMPTY, name);
    }

    private static ReferenceAttribute referenceAttribute(String name, DataType type) {
        return new ReferenceAttribute(EMPTY, name, type);
    }

    private static Literal integer(int i) {
        return new Literal(EMPTY, i, DataTypes.INTEGER);
    }

    private static Literal integers(int... ints) {
        return new Literal(EMPTY, Arrays.stream(ints).boxed().toList(), DataTypes.INTEGER);
    }

    private static Literal literalLong(long i) {
        return new Literal(EMPTY, i, DataTypes.LONG);
    }

    private static Literal literalLongs(long... longs) {
        return new Literal(EMPTY, Arrays.stream(longs).boxed().toList(), DataTypes.LONG);
    }

    private static Literal literalDouble(double d) {
        return new Literal(EMPTY, d, DataTypes.DOUBLE);
    }

    private static Literal literalDoubles(double... doubles) {
        return new Literal(EMPTY, Arrays.stream(doubles).boxed().toList(), DataTypes.DOUBLE);
    }

    private static Literal literalUnsignedLong(String ulong) {
        return new Literal(EMPTY, asLongUnsigned(new BigInteger(ulong)), DataTypes.UNSIGNED_LONG);
    }

    private static Literal literalUnsignedLongs(String... ulongs) {
        return new Literal(EMPTY, Arrays.stream(ulongs).map(s -> asLongUnsigned(new BigInteger(s))).toList(), DataTypes.UNSIGNED_LONG);
    }

    private static Literal literalBoolean(boolean b) {
        return new Literal(EMPTY, b, DataTypes.BOOLEAN);
    }

    private static Literal literalBooleans(boolean... booleans) {
        List<Boolean> v = new ArrayList<>(booleans.length);
        for (boolean b : booleans) {
            v.add(b);
        }
        return new Literal(EMPTY, v, DataTypes.BOOLEAN);
    }

    private static Literal literalString(String s) {
        return new Literal(EMPTY, s, DataTypes.KEYWORD);
    }

    private static Literal literalStrings(String... strings) {
        return new Literal(EMPTY, Arrays.asList(strings), DataTypes.KEYWORD);
    }

    private void expectError(String query, String errorMessage) {
        ParsingException e = expectThrows(ParsingException.class, "Expected syntax error for " + query, () -> statement(query));
        assertThat(e.getMessage(), containsString(errorMessage));
    }

    private void expectVerificationError(String query, String errorMessage) {
        VerificationException e = expectThrows(VerificationException.class, "Expected syntax error for " + query, () -> statement(query));
        assertThat(e.getMessage(), containsString(errorMessage));
    }

    private void expectError(String query, List<TypedParamValue> params, String errorMessage) {
        ParsingException e = expectThrows(ParsingException.class, "Expected syntax error for " + query, () -> statement(query, params));
        assertThat(e.getMessage(), containsString(errorMessage));
    }
}
