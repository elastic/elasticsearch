/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.plan.TableIdentifier;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.elasticsearch.xpack.esql.parser.ExpressionBuilder.breakIntoFragments;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class StatementParserTests extends AbstractStatementParserTests {

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

    public void testRowCommandHugeNegativeInt() {
        assertEquals(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDouble(-92233720368547758080d)))),
            statement("row c = -92233720368547758080")
        );
        assertEquals(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDouble(-18446744073709551616d)))),
            statement("row c = -18446744073709551616")
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
            new Aggregate(
                EMPTY,
                PROCESSING_CMD_INPUT,
                Aggregate.AggregateType.STANDARD,
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
                Aggregate.AggregateType.STANDARD,
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
            new Aggregate(EMPTY, PROCESSING_CMD_INPUT, Aggregate.AggregateType.STANDARD, List.of(attribute("a")), List.of(attribute("a"))),
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
        assumeTrue("INLINESTATS requires snapshot builds", Build.current().isSnapshot());
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
        assumeTrue("INLINESTATS requires snapshot builds", Build.current().isSnapshot());
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

    public void testStringAsIndexPattern() {
        for (String command : List.of("FROM", "METRICS")) {
            assertStringAsIndexPattern("foo", command + " \"foo\"");
            assertStringAsIndexPattern("foo,test-*", command + """
                 "foo","test-*"
                """);
            assertStringAsIndexPattern("foo,test-*", command + " foo,test-*");
            assertStringAsIndexPattern("123-test@foo_bar+baz1", command + " 123-test@foo_bar+baz1");
            assertStringAsIndexPattern("foo,test-*,abc", command + """
                 "foo","test-*",abc
                """);
            assertStringAsIndexPattern("foo, test-*, abc, xyz", command + """
                     "foo, test-*, abc, xyz"
                """);
            assertStringAsIndexPattern("foo, test-*, abc, xyz,test123", command + """
                     "foo, test-*, abc, xyz", test123
                """);
            assertStringAsIndexPattern("foo,test,xyz", command + " foo,   test,xyz");
            assertStringAsIndexPattern(
                "<logstash-{now/M{yyyy.MM}}>,<logstash-{now/d{yyyy.MM.dd|+12:00}}>",
                command + " <logstash-{now/M{yyyy.MM}}>, \"<logstash-{now/d{yyyy.MM.dd|+12:00}}>\""
            );

            assertStringAsIndexPattern("foo,test,xyz", command + " \"\"\"foo\"\"\",   test,\"xyz\"");

            assertStringAsIndexPattern("`backtick`,``multiple`back``ticks```", command + " `backtick`, ``multiple`back``ticks```");

            assertStringAsIndexPattern("test,metadata,metaata,.metadata", command + " test,\"metadata\", metaata, .metadata");

            assertStringAsIndexPattern(".dot", command + " .dot");

            assertStringAsIndexPattern("cluster:index", command + " cluster:index");
            assertStringAsIndexPattern("cluster:index|pattern", command + " cluster:\"index|pattern\"");
            assertStringAsIndexPattern("cluster:.index", command + " cluster:.index");
            assertStringAsIndexPattern("cluster*:index*", command + " cluster*:index*");
            assertStringAsIndexPattern("cluster*:*", command + " cluster*:*");
            assertStringAsIndexPattern("*:index*", command + " *:index*");
            assertStringAsIndexPattern("*:index|pattern", command + " *:\"index|pattern\"");
            assertStringAsIndexPattern("*:*", command + " *:*");
            assertStringAsIndexPattern("*:*,cluster*:index|pattern,i|p", command + " *:*, cluster*:\"index|pattern\", \"i|p\"");
        }
    }

    public void testStringAsLookupIndexPattern() {
        assertStringAsLookupIndexPattern("foo", "ROW x = 1 | LOOKUP \"foo\" ON j");
        assertStringAsLookupIndexPattern("test-*", """
            ROW x = 1 | LOOKUP "test-*" ON j
            """);
        assertStringAsLookupIndexPattern("test-*", "ROW x = 1 | LOOKUP test-* ON j");
        assertStringAsLookupIndexPattern("123-test@foo_bar+baz1", "ROW x = 1 | LOOKUP 123-test@foo_bar+baz1 ON j");
        assertStringAsLookupIndexPattern("foo, test-*, abc, xyz", """
            ROW x = 1 | LOOKUP     "foo, test-*, abc, xyz"  ON j
            """);
        assertStringAsLookupIndexPattern("<logstash-{now/M{yyyy.MM}}>", "ROW x = 1 | LOOKUP <logstash-{now/M{yyyy.MM}}> ON j");
        assertStringAsLookupIndexPattern(
            "<logstash-{now/d{yyyy.MM.dd|+12:00}}>",
            "ROW x = 1 | LOOKUP \"<logstash-{now/d{yyyy.MM.dd|+12:00}}>\" ON j"
        );

        assertStringAsLookupIndexPattern("foo", "ROW x = 1 | LOOKUP \"\"\"foo\"\"\" ON j");

        assertStringAsLookupIndexPattern("`backtick`", "ROW x = 1 | LOOKUP `backtick` ON j");
        assertStringAsLookupIndexPattern("``multiple`back``ticks```", "ROW x = 1 | LOOKUP ``multiple`back``ticks``` ON j");

        assertStringAsLookupIndexPattern(".dot", "ROW x = 1 | LOOKUP .dot ON j");

        assertStringAsLookupIndexPattern("cluster:index", "ROW x = 1 | LOOKUP cluster:index ON j");
        assertStringAsLookupIndexPattern("cluster:.index", "ROW x = 1 | LOOKUP cluster:.index ON j");
        assertStringAsLookupIndexPattern("cluster*:index*", "ROW x = 1 | LOOKUP cluster*:index* ON j");
        assertStringAsLookupIndexPattern("cluster*:*", "ROW x = 1 | LOOKUP cluster*:* ON j");
        assertStringAsLookupIndexPattern("*:index*", "ROW x = 1 | LOOKUP  *:index* ON j");
        assertStringAsLookupIndexPattern("*:*", "ROW x = 1 | LOOKUP  *:* ON j");

    }

    public void testInvalidQuotingAsFromIndexPattern() {
        expectError("FROM \"foo", ": token recognition error at: '\"foo'");
        expectError("FROM \"foo | LIMIT 1", ": token recognition error at: '\"foo | LIMIT 1'");
        expectError("FROM \"\"\"foo", ": token recognition error at: '\"foo'");

        expectError("FROM foo\"", ": token recognition error at: '\"'");
        expectError("FROM foo\" | LIMIT 2", ": token recognition error at: '\" | LIMIT 2'");
        expectError("FROM foo\"\"\"", ": token recognition error at: '\"'");

        expectError("FROM \"foo\"bar\"", ": token recognition error at: '\"'");
        expectError("FROM \"foo\"\"bar\"", ": extraneous input '\"bar\"' expecting <EOF>");

        expectError("FROM \"\"\"foo\"\"\"bar\"\"\"", ": mismatched input 'bar' expecting {<EOF>, '|', ',', OPENING_BRACKET, 'metadata'}");
        expectError(
            "FROM \"\"\"foo\"\"\"\"\"\"bar\"\"\"",
            ": mismatched input '\"bar\"' expecting {<EOF>, '|', ',', OPENING_BRACKET, 'metadata'}"
        );
    }

    public void testInvalidQuotingAsMetricsIndexPattern() {
        expectError("METRICS \"foo", ": token recognition error at: '\"foo'");
        expectError("METRICS \"foo | LIMIT 1", ": token recognition error at: '\"foo | LIMIT 1'");
        expectError("METRICS \"\"\"foo", ": token recognition error at: '\"'");

        expectError("METRICS foo\"", ": token recognition error at: '\"'");
        expectError("METRICS foo\" | LIMIT 2", ": token recognition error at: '\"'");
        expectError("METRICS foo\"\"\"", ": token recognition error at: '\"'");

        expectError("METRICS \"foo\"bar\"", ": token recognition error at: '\"'");
        expectError("METRICS \"foo\"\"bar\"", ": token recognition error at: '\"'");

        expectError("METRICS \"\"\"foo\"\"\"bar\"\"\"", ": token recognition error at: '\"'");
        expectError("METRICS \"\"\"foo\"\"\"\"\"\"bar\"\"\"", ": token recognition error at: '\"'");
    }

    public void testInvalidQuotingAsLookupIndexPattern() {
        expectError("ROW x = 1 | LOOKUP \"foo ON j", ": token recognition error at: '\"foo ON j'");
        expectError("ROW x = 1 | LOOKUP \"\"\"foo ON j", ": token recognition error at: '\"foo ON j'");

        expectError("ROW x = 1 | LOOKUP foo\" ON j", ": token recognition error at: '\" ON j'");
        expectError("ROW x = 1 | LOOKUP foo\"\"\" ON j", ": token recognition error at: '\" ON j'");

        expectError("ROW x = 1 | LOOKUP \"foo\"bar\" ON j", ": token recognition error at: '\" ON j'");
        expectError("ROW x = 1 | LOOKUP \"foo\"\"bar\" ON j", ": extraneous input '\"bar\"' expecting 'on'");

        expectError("ROW x = 1 | LOOKUP \"\"\"foo\"\"\"bar\"\"\" ON j", ": mismatched input 'bar' expecting 'on'");
        expectError("ROW x = 1 | LOOKUP \"\"\"foo\"\"\"\"\"\"bar\"\"\" ON j", "line 1:31: mismatched input '\"bar\"' expecting 'on'");
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
        assertThat(limit.children().get(0).children().get(0), instanceOf(UnresolvedRelation.class));
    }

    public void testLimitConstraints() {
        expectError("from text | limit -1", "line 1:19: extraneous input '-' expecting INTEGER_LITERAL");
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
            "line 1:20: mismatched input 'metadata' expecting {'|', ',', OPENING_BRACKET, ']', 'metadata'}"
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
        Grok grok = (Grok) cmd;
        assertEquals("%{WORD:foo}", grok.parser().pattern());
        assertEquals(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());

        ParsingException pe = expectThrows(ParsingException.class, () -> statement("row a = \"foo bar\" | grok a \"%{_invalid_:x}\""));
        assertThat(
            pe.getMessage(),
            containsString("Invalid pattern [%{_invalid_:x}] for grok: Unable to find pattern [_invalid_] in Grok's pattern dictionary")
        );

        cmd = processingCommand("grok a \"%{WORD:foo} %{WORD:foo}\"");
        assertEquals(Grok.class, cmd.getClass());
        grok = (Grok) cmd;
        assertEquals("%{WORD:foo} %{WORD:foo}", grok.parser().pattern());
        assertEquals(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());

        expectError(
            "row a = \"foo bar\" | GROK a \"%{NUMBER:foo} %{WORD:foo}\"",
            "line 1:22: Invalid GROK pattern [%{NUMBER:foo} %{WORD:foo}]:"
                + " the attribute [foo] is defined multiple times with different types"
        );

        expectError(
            "row a = \"foo\" | GROK a \"(?P<justification>.+)\"",
            "line 1:18: Invalid grok pattern [(?P<justification>.+)]: [undefined group option]"
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

        expectError("from a | enrich countries on foo* ", "Using wildcards [*] in ENRICH WITH projections is not allowed [foo*]");
        expectError("from a | enrich countries on foo with bar*", "Using wildcards [*] in ENRICH WITH projections is not allowed [bar*]");
        expectError(
            "from a | enrich countries on foo with x = bar* ",
            "Using wildcards [*] in ENRICH WITH projections is not allowed [bar*]"
        );
        expectError(
            "from a | enrich countries on foo with x* = bar ",
            "Using wildcards [*] in ENRICH WITH projections is not allowed [x*]"
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
            "row x = ?, y = ?, a = ?, b = ?, c = ?, d = ?, e = ?-1, f = ?+1",
            new QueryParams(
                List.of(
                    new QueryParam(null, 1, INTEGER),
                    new QueryParam(null, "2", KEYWORD),
                    new QueryParam(null, "2 days", KEYWORD),
                    new QueryParam(null, "4 hours", KEYWORD),
                    new QueryParam(null, "1.2.3", KEYWORD),
                    new QueryParam(null, "127.0.0.1", KEYWORD),
                    new QueryParam(null, 10, INTEGER),
                    new QueryParam(null, 10, INTEGER)
                )
            )
        );
        assertThat(stm, instanceOf(Row.class));
        Row row = (Row) stm;
        assertThat(row.fields().size(), is(8));

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
        assertThat(alias.child().fold(), is("2 days"));

        field = row.fields().get(3);
        assertThat(field.name(), is("b"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(), is("4 hours"));

        field = row.fields().get(4);
        assertThat(field.name(), is("c"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold().getClass(), is(String.class));
        assertThat(alias.child().fold().toString(), is("1.2.3"));

        field = row.fields().get(5);
        assertThat(field.name(), is("d"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold().getClass(), is(String.class));
        assertThat(alias.child().fold().toString(), is("127.0.0.1"));

        field = row.fields().get(6);
        assertThat(field.name(), is("e"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(), is(9));

        field = row.fields().get(7);
        assertThat(field.name(), is("f"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(), is(11));
    }

    public void testMatchCommand() throws IOException {
        assumeTrue("Match command available just for snapshots", Build.current().isSnapshot());
        String queryString = "field: value";
        assertEquals(
            new Filter(EMPTY, PROCESSING_CMD_INPUT, new StringQueryPredicate(EMPTY, queryString, null)),
            processingCommand("match \"" + queryString + "\"")
        );

        expectError("from a | match an unquoted string", "mismatched input 'an' expecting QUOTED_STRING");
    }

    public void testMissingInputParams() {
        expectError("row x = ?, y = ?", List.of(new QueryParam(null, 1, INTEGER)), "Not enough actual parameters 1");
    }

    public void testNamedParams() {
        LogicalPlan stm = statement("row x=?name1, y = ?name1", new QueryParams(List.of(new QueryParam("name1", 1, INTEGER))));
        assertThat(stm, instanceOf(Row.class));
        Row row = (Row) stm;
        assertThat(row.fields().size(), is(2));

        NamedExpression field = row.fields().get(0);
        assertThat(field.name(), is("x"));
        assertThat(field, instanceOf(Alias.class));
        Alias alias = (Alias) field;
        assertThat(alias.child().fold(), is(1));

        field = row.fields().get(1);
        assertThat(field.name(), is("y"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(), is(1));
    }

    public void testInvalidNamedParams() {
        expectError(
            "from test | where x < ?n1 | eval y = ?n2",
            List.of(new QueryParam("n1", 5, INTEGER)),
            "Unknown query parameter [n2], did you mean [n1]?"
        );

        expectError(
            "from test | where x < ?n1 | eval y = ?n2",
            List.of(new QueryParam("n1", 5, INTEGER), new QueryParam("n3", 5, INTEGER)),
            "Unknown query parameter [n2], did you mean any of [n3, n1]?"
        );

        expectError("from test | where x < ?@1", List.of(new QueryParam("@1", 5, INTEGER)), "extraneous input '@1' expecting <EOF>");

        expectError("from test | where x < ?#1", List.of(new QueryParam("#1", 5, INTEGER)), "token recognition error at: '#'");

        expectError(
            "from test | where x < ??",
            List.of(new QueryParam("n_1", 5, INTEGER), new QueryParam("n_2", 5, INTEGER)),
            "extraneous input '?' expecting <EOF>"
        );

        expectError("from test | where x < ?Å", List.of(new QueryParam("Å", 5, INTEGER)), "line 1:24: token recognition error at: 'Å'");

        expectError("from test | eval x = ?Å", List.of(new QueryParam("Å", 5, INTEGER)), "line 1:23: token recognition error at: 'Å'");
    }

    public void testPositionalParams() {
        LogicalPlan stm = statement("row x=?1, y=?1", new QueryParams(List.of(new QueryParam(null, 1, INTEGER))));
        assertThat(stm, instanceOf(Row.class));
        Row row = (Row) stm;
        assertThat(row.fields().size(), is(2));

        NamedExpression field = row.fields().get(0);
        assertThat(field.name(), is("x"));
        assertThat(field, instanceOf(Alias.class));
        Alias alias = (Alias) field;
        assertThat(alias.child().fold(), is(1));

        field = row.fields().get(1);
        assertThat(field.name(), is("y"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(), is(1));
    }

    public void testInvalidPositionalParams() {
        expectError(
            "from test | where x < ?0",
            List.of(new QueryParam(null, 5, INTEGER)),
            "No parameter is defined for position 0, did you mean position 1"
        );

        expectError(
            "from test | where x < ?2",
            List.of(new QueryParam(null, 5, INTEGER)),
            "No parameter is defined for position 2, did you mean position 1"
        );

        expectError(
            "from test | where x < ?0 and y < ?2",
            List.of(new QueryParam(null, 5, INTEGER)),
            "line 1:24: No parameter is defined for position 0, did you mean position 1?; "
                + "line 1:35: No parameter is defined for position 2, did you mean position 1?"
        );

        expectError(
            "from test | where x < ?0",
            List.of(new QueryParam(null, 5, INTEGER), new QueryParam(null, 10, INTEGER)),
            "No parameter is defined for position 0, did you mean any position between 1 and 2?"
        );
    }

    public void testParamInWhere() {
        LogicalPlan plan = statement("from test | where x < ? |  limit 10", new QueryParams(List.of(new QueryParam(null, 5, INTEGER))));
        assertThat(plan, instanceOf(Limit.class));
        Limit limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(10));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Filter.class));
        Filter w = (Filter) limit.children().get(0);
        assertThat(((Literal) w.condition().children().get(1)).value(), equalTo(5));
        assertThat(limit.children().get(0).children().size(), equalTo(1));
        assertThat(limit.children().get(0).children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement("from test | where x < ?n1 |  limit 10", new QueryParams(List.of(new QueryParam("n1", 5, INTEGER))));
        assertThat(plan, instanceOf(Limit.class));
        limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(10));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Filter.class));
        w = (Filter) limit.children().get(0);
        assertThat(((Literal) w.condition().children().get(1)).value(), equalTo(5));
        assertThat(limit.children().get(0).children().size(), equalTo(1));
        assertThat(limit.children().get(0).children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement("from test | where x < ?_n1 |  limit 10", new QueryParams(List.of(new QueryParam("_n1", 5, INTEGER))));
        assertThat(plan, instanceOf(Limit.class));
        limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(10));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Filter.class));
        w = (Filter) limit.children().get(0);
        assertThat(((Literal) w.condition().children().get(1)).value(), equalTo(5));
        assertThat(limit.children().get(0).children().size(), equalTo(1));
        assertThat(limit.children().get(0).children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement("from test | where x < ?1 |  limit 10", new QueryParams(List.of(new QueryParam(null, 5, INTEGER))));
        assertThat(plan, instanceOf(Limit.class));
        limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(10));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Filter.class));
        w = (Filter) limit.children().get(0);
        assertThat(((Literal) w.condition().children().get(1)).value(), equalTo(5));
        assertThat(limit.children().get(0).children().size(), equalTo(1));
        assertThat(limit.children().get(0).children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement("from test | where x < ?__1 |  limit 10", new QueryParams(List.of(new QueryParam("__1", 5, INTEGER))));
        assertThat(plan, instanceOf(Limit.class));
        limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(10));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Filter.class));
        w = (Filter) limit.children().get(0);
        assertThat(((Literal) w.condition().children().get(1)).value(), equalTo(5));
        assertThat(limit.children().get(0).children().size(), equalTo(1));
        assertThat(limit.children().get(0).children().get(0), instanceOf(UnresolvedRelation.class));
    }

    public void testParamInEval() {
        LogicalPlan plan = statement(
            "from test | where x < ? | eval y = ? + ? |  limit 10",
            new QueryParams(
                List.of(new QueryParam(null, 5, INTEGER), new QueryParam(null, -1, INTEGER), new QueryParam(null, 100, INTEGER))
            )
        );
        assertThat(plan, instanceOf(Limit.class));
        Limit limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(10));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Eval.class));
        Eval eval = (Eval) limit.children().get(0);
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left()).value(), equalTo(-1));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right()).value(), equalTo(100));
        Filter f = (Filter) eval.children().get(0);
        assertThat(((Literal) f.condition().children().get(1)).value(), equalTo(5));
        assertThat(f.children().size(), equalTo(1));
        assertThat(f.children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement(
            "from test | where x < ?n1 | eval y = ?n2 + ?n3 |  limit 10",
            new QueryParams(
                List.of(new QueryParam("n1", 5, INTEGER), new QueryParam("n2", -1, INTEGER), new QueryParam("n3", 100, INTEGER))
            )
        );
        assertThat(plan, instanceOf(Limit.class));
        limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(10));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Eval.class));
        eval = (Eval) limit.children().get(0);
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left()).value(), equalTo(-1));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right()).value(), equalTo(100));
        f = (Filter) eval.children().get(0);
        assertThat(((Literal) f.condition().children().get(1)).value(), equalTo(5));
        assertThat(f.children().size(), equalTo(1));
        assertThat(f.children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement(
            "from test | where x < ?_n1 | eval y = ?_n2 + ?_n3 |  limit 10",
            new QueryParams(
                List.of(new QueryParam("_n1", 5, INTEGER), new QueryParam("_n2", -1, INTEGER), new QueryParam("_n3", 100, INTEGER))
            )
        );
        assertThat(plan, instanceOf(Limit.class));
        limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(10));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Eval.class));
        eval = (Eval) limit.children().get(0);
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left()).value(), equalTo(-1));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right()).value(), equalTo(100));
        f = (Filter) eval.children().get(0);
        assertThat(((Literal) f.condition().children().get(1)).value(), equalTo(5));
        assertThat(f.children().size(), equalTo(1));
        assertThat(f.children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement(
            "from test | where x < ?1 | eval y = ?2 + ?1 |  limit 10",
            new QueryParams(List.of(new QueryParam(null, 5, INTEGER), new QueryParam(null, -1, INTEGER)))
        );
        assertThat(plan, instanceOf(Limit.class));
        limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(10));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Eval.class));
        eval = (Eval) limit.children().get(0);
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left()).value(), equalTo(-1));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right()).value(), equalTo(5));
        f = (Filter) eval.children().get(0);
        assertThat(((Literal) f.condition().children().get(1)).value(), equalTo(5));
        assertThat(f.children().size(), equalTo(1));
        assertThat(f.children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement(
            "from test | where x < ?_1 | eval y = ?_2 + ?_1 |  limit 10",
            new QueryParams(List.of(new QueryParam("_1", 5, INTEGER), new QueryParam("_2", -1, INTEGER)))
        );
        assertThat(plan, instanceOf(Limit.class));
        limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(10));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Eval.class));
        eval = (Eval) limit.children().get(0);
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left()).value(), equalTo(-1));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right()).value(), equalTo(5));
        f = (Filter) eval.children().get(0);
        assertThat(((Literal) f.condition().children().get(1)).value(), equalTo(5));
        assertThat(f.children().size(), equalTo(1));
        assertThat(f.children().get(0), instanceOf(UnresolvedRelation.class));
    }

    public void testParamInAggFunction() {
        LogicalPlan plan = statement(
            "from test | where x < ? | eval y = ? + ? |  stats count(?) by z",
            new QueryParams(
                List.of(
                    new QueryParam(null, 5, INTEGER),
                    new QueryParam(null, -1, INTEGER),
                    new QueryParam(null, 100, INTEGER),
                    new QueryParam(null, "*", KEYWORD)
                )
            )
        );
        assertThat(plan, instanceOf(Aggregate.class));
        Aggregate agg = (Aggregate) plan;
        assertThat(((Literal) agg.aggregates().get(0).children().get(0).children().get(0)).value(), equalTo("*"));
        assertThat(agg.child(), instanceOf(Eval.class));
        assertThat(agg.children().size(), equalTo(1));
        assertThat(agg.children().get(0), instanceOf(Eval.class));
        Eval eval = (Eval) agg.children().get(0);
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left()).value(), equalTo(-1));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right()).value(), equalTo(100));
        Filter f = (Filter) eval.children().get(0);
        assertThat(((Literal) f.condition().children().get(1)).value(), equalTo(5));
        assertThat(f.children().size(), equalTo(1));
        assertThat(f.children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement(
            "from test | where x < ?n1 | eval y = ?n2 + ?n3 |  stats count(?n4) by z",
            new QueryParams(
                List.of(
                    new QueryParam("n1", 5, INTEGER),
                    new QueryParam("n2", -1, INTEGER),
                    new QueryParam("n3", 100, INTEGER),
                    new QueryParam("n4", "*", KEYWORD)
                )
            )
        );
        assertThat(plan, instanceOf(Aggregate.class));
        agg = (Aggregate) plan;
        assertThat(((Literal) agg.aggregates().get(0).children().get(0).children().get(0)).value(), equalTo("*"));
        assertThat(agg.child(), instanceOf(Eval.class));
        assertThat(agg.children().size(), equalTo(1));
        assertThat(agg.children().get(0), instanceOf(Eval.class));
        eval = (Eval) agg.children().get(0);
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left()).value(), equalTo(-1));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right()).value(), equalTo(100));
        f = (Filter) eval.children().get(0);
        assertThat(((Literal) f.condition().children().get(1)).value(), equalTo(5));
        assertThat(f.children().size(), equalTo(1));
        assertThat(f.children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement(
            "from test | where x < ?_n1 | eval y = ?_n2 + ?_n3 |  stats count(?_n4) by z",
            new QueryParams(
                List.of(
                    new QueryParam("_n1", 5, INTEGER),
                    new QueryParam("_n2", -1, INTEGER),
                    new QueryParam("_n3", 100, INTEGER),
                    new QueryParam("_n4", "*", KEYWORD)
                )
            )
        );
        assertThat(plan, instanceOf(Aggregate.class));
        agg = (Aggregate) plan;
        assertThat(((Literal) agg.aggregates().get(0).children().get(0).children().get(0)).value(), equalTo("*"));
        assertThat(agg.child(), instanceOf(Eval.class));
        assertThat(agg.children().size(), equalTo(1));
        assertThat(agg.children().get(0), instanceOf(Eval.class));
        eval = (Eval) agg.children().get(0);
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left()).value(), equalTo(-1));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right()).value(), equalTo(100));
        f = (Filter) eval.children().get(0);
        assertThat(((Literal) f.condition().children().get(1)).value(), equalTo(5));
        assertThat(f.children().size(), equalTo(1));
        assertThat(f.children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement(
            "from test | where x < ?1 | eval y = ?2 + ?1 |  stats count(?3) by z",
            new QueryParams(
                List.of(new QueryParam(null, 5, INTEGER), new QueryParam(null, -1, INTEGER), new QueryParam(null, "*", KEYWORD))
            )
        );
        assertThat(plan, instanceOf(Aggregate.class));
        agg = (Aggregate) plan;
        assertThat(((Literal) agg.aggregates().get(0).children().get(0).children().get(0)).value(), equalTo("*"));
        assertThat(agg.child(), instanceOf(Eval.class));
        assertThat(agg.children().size(), equalTo(1));
        assertThat(agg.children().get(0), instanceOf(Eval.class));
        eval = (Eval) agg.children().get(0);
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left()).value(), equalTo(-1));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right()).value(), equalTo(5));
        f = (Filter) eval.children().get(0);
        assertThat(((Literal) f.condition().children().get(1)).value(), equalTo(5));
        assertThat(f.children().size(), equalTo(1));
        assertThat(f.children().get(0), instanceOf(UnresolvedRelation.class));

        plan = statement(
            "from test | where x < ?_1 | eval y = ?_2 + ?_1 |  stats count(?_3) by z",
            new QueryParams(
                List.of(new QueryParam("_1", 5, INTEGER), new QueryParam("_2", -1, INTEGER), new QueryParam("_3", "*", KEYWORD))
            )
        );
        assertThat(plan, instanceOf(Aggregate.class));
        agg = (Aggregate) plan;
        assertThat(((Literal) agg.aggregates().get(0).children().get(0).children().get(0)).value(), equalTo("*"));
        assertThat(agg.child(), instanceOf(Eval.class));
        assertThat(agg.children().size(), equalTo(1));
        assertThat(agg.children().get(0), instanceOf(Eval.class));
        eval = (Eval) agg.children().get(0);
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left()).value(), equalTo(-1));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right()).value(), equalTo(5));
        f = (Filter) eval.children().get(0);
        assertThat(((Literal) f.condition().children().get(1)).value(), equalTo(5));
        assertThat(f.children().size(), equalTo(1));
        assertThat(f.children().get(0), instanceOf(UnresolvedRelation.class));
    }

    public void testParamMixed() {
        expectError(
            "from test | where x < ? | eval y = ?n2 + ?n3 |  limit ?n4",
            List.of(
                new QueryParam("n1", 5, INTEGER),
                new QueryParam("n2", -1, INTEGER),
                new QueryParam("n3", 100, INTEGER),
                new QueryParam("n4", 10, INTEGER)
            ),
            "Inconsistent parameter declaration, "
                + "use one of positional, named or anonymous params but not a combination of named and anonymous"
        );

        expectError(
            "from test | where x < ? | eval y = ?_n2 + ?n3 |  limit ?_4",
            List.of(
                new QueryParam("n1", 5, INTEGER),
                new QueryParam("_n2", -1, INTEGER),
                new QueryParam("n3", 100, INTEGER),
                new QueryParam("n4", 10, INTEGER)
            ),
            "Inconsistent parameter declaration, "
                + "use one of positional, named or anonymous params but not a combination of named and anonymous"
        );

        expectError(
            "from test | where x < ?1 | eval y = ?n2 + ?_n3 |  limit ?n4",
            List.of(
                new QueryParam("n1", 5, INTEGER),
                new QueryParam("n2", -1, INTEGER),
                new QueryParam("_n3", 100, INTEGER),
                new QueryParam("n4", 10, INTEGER)
            ),
            "Inconsistent parameter declaration, "
                + "use one of positional, named or anonymous params but not a combination of named and positional"
        );

        expectError(
            "from test | where x < ? | eval y = ?2 + ?n3 |  limit ?_n4",
            List.of(
                new QueryParam("n1", 5, INTEGER),
                new QueryParam("n2", -1, INTEGER),
                new QueryParam("n3", 100, INTEGER),
                new QueryParam("_n4", 10, INTEGER)
            ),
            "Inconsistent parameter declaration, "
                + "use one of positional, named or anonymous params but not a combination of positional and anonymous"
        );
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

    private void assertStringAsIndexPattern(String string, String statement) {
        if (Build.current().isProductionRelease() && statement.contains("METRIC")) {
            var e = expectThrows(IllegalArgumentException.class, () -> statement(statement));
            assertThat(e.getMessage(), containsString("METRICS command currently requires a snapshot build"));
            return;
        }
        LogicalPlan from = statement(statement);
        assertThat(from, instanceOf(UnresolvedRelation.class));
        UnresolvedRelation table = (UnresolvedRelation) from;
        assertThat(table.table().index(), is(string));
    }

    private void assertStringAsLookupIndexPattern(String string, String statement) {
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> statement(statement));
            assertThat(e.getMessage(), containsString("line 1:14: LOOKUP is in preview and only available in SNAPSHOT build"));
            return;
        }
        var plan = statement(statement);
        var lookup = as(plan, Lookup.class);
        var tableName = as(lookup.tableName(), Literal.class);
        assertThat(tableName.fold(), equalTo(string));
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

    public void testInlineConvertWithNonexistentType() {
        expectError("ROW 1::doesnotexist", "line 1:9: Unknown data type named [doesnotexist]");
        expectError("ROW \"1\"::doesnotexist", "line 1:11: Unknown data type named [doesnotexist]");
        expectError("ROW false::doesnotexist", "line 1:13: Unknown data type named [doesnotexist]");
        expectError("ROW abs(1)::doesnotexist", "line 1:14: Unknown data type named [doesnotexist]");
        expectError("ROW (1+2)::doesnotexist", "line 1:13: Unknown data type named [doesnotexist]");
    }

    public void testLookup() {
        String query = "ROW a = 1 | LOOKUP t ON j";
        if (Build.current().isProductionRelease()) {
            var e = expectThrows(ParsingException.class, () -> statement(query));
            assertThat(e.getMessage(), containsString("line 1:14: LOOKUP is in preview and only available in SNAPSHOT build"));
            return;
        }
        var plan = statement(query);
        var lookup = as(plan, Lookup.class);
        var tableName = as(lookup.tableName(), Literal.class);
        assertThat(tableName.fold(), equalTo("t"));
        assertThat(lookup.matchFields(), hasSize(1));
        var matchField = as(lookup.matchFields().get(0), UnresolvedAttribute.class);
        assertThat(matchField.name(), equalTo("j"));
    }

    public void testInlineConvertUnsupportedType() {
        expectError("ROW 3::BYTE", "line 1:6: Unsupported conversion to type [BYTE]");
    }

    public void testMetricsWithoutStats() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());

        assertStatement("METRICS foo", unresolvedRelation("foo"));
        assertStatement("METRICS foo,bar", unresolvedRelation("foo,bar"));
        assertStatement("METRICS foo*,bar", unresolvedRelation("foo*,bar"));
        assertStatement("METRICS foo-*,bar", unresolvedRelation("foo-*,bar"));
        assertStatement("METRICS foo-*,bar+*", unresolvedRelation("foo-*,bar+*"));
    }

    public void testMetricsIdentifiers() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());
        Map<String, String> patterns = Map.ofEntries(
            Map.entry("metrics foo,test-*", "foo,test-*"),
            Map.entry("metrics 123-test@foo_bar+baz1", "123-test@foo_bar+baz1"),
            Map.entry("metrics foo,   test,xyz", "foo,test,xyz"),
            Map.entry("metrics <logstash-{now/M{yyyy.MM}}>>", "<logstash-{now/M{yyyy.MM}}>>")
        );
        for (Map.Entry<String, String> e : patterns.entrySet()) {
            assertStatement(e.getKey(), unresolvedRelation(e.getValue()));
        }
    }

    public void testSimpleMetricsWithStats() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());
        assertStatement(
            "METRICS foo load=avg(cpu) BY ts",
            new Aggregate(
                EMPTY,
                unresolvedTSRelation("foo"),
                Aggregate.AggregateType.METRICS,
                List.of(attribute("ts")),
                List.of(new Alias(EMPTY, "load", new UnresolvedFunction(EMPTY, "avg", DEFAULT, List.of(attribute("cpu")))), attribute("ts"))
            )
        );
        assertStatement(
            "METRICS foo,bar load=avg(cpu) BY ts",
            new Aggregate(
                EMPTY,
                unresolvedTSRelation("foo,bar"),
                Aggregate.AggregateType.METRICS,
                List.of(attribute("ts")),
                List.of(new Alias(EMPTY, "load", new UnresolvedFunction(EMPTY, "avg", DEFAULT, List.of(attribute("cpu")))), attribute("ts"))
            )
        );
        assertStatement(
            "METRICS foo,bar load=avg(cpu),max(rate(requests)) BY ts",
            new Aggregate(
                EMPTY,
                unresolvedTSRelation("foo,bar"),
                Aggregate.AggregateType.METRICS,
                List.of(attribute("ts")),
                List.of(
                    new Alias(EMPTY, "load", new UnresolvedFunction(EMPTY, "avg", DEFAULT, List.of(attribute("cpu")))),
                    new Alias(
                        EMPTY,
                        "max(rate(requests))",
                        new UnresolvedFunction(
                            EMPTY,
                            "max",
                            DEFAULT,
                            List.of(new UnresolvedFunction(EMPTY, "rate", DEFAULT, List.of(attribute("requests"))))
                        )
                    ),
                    attribute("ts")
                )
            )
        );
        assertStatement(
            "METRICS foo* count(errors)",
            new Aggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                Aggregate.AggregateType.METRICS,
                List.of(),
                List.of(new Alias(EMPTY, "count(errors)", new UnresolvedFunction(EMPTY, "count", DEFAULT, List.of(attribute("errors")))))
            )
        );
        assertStatement(
            "METRICS foo* a(b)",
            new Aggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                Aggregate.AggregateType.METRICS,
                List.of(),
                List.of(new Alias(EMPTY, "a(b)", new UnresolvedFunction(EMPTY, "a", DEFAULT, List.of(attribute("b")))))
            )
        );
        assertStatement(
            "METRICS foo* a(b)",
            new Aggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                Aggregate.AggregateType.METRICS,
                List.of(),
                List.of(new Alias(EMPTY, "a(b)", new UnresolvedFunction(EMPTY, "a", DEFAULT, List.of(attribute("b")))))
            )
        );
        assertStatement(
            "METRICS foo* a1(b2)",
            new Aggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                Aggregate.AggregateType.METRICS,
                List.of(),
                List.of(new Alias(EMPTY, "a1(b2)", new UnresolvedFunction(EMPTY, "a1", DEFAULT, List.of(attribute("b2")))))
            )
        );
        assertStatement(
            "METRICS foo*,bar* b = min(a) by c, d.e",
            new Aggregate(
                EMPTY,
                unresolvedTSRelation("foo*,bar*"),
                Aggregate.AggregateType.METRICS,
                List.of(attribute("c"), attribute("d.e")),
                List.of(
                    new Alias(EMPTY, "b", new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(attribute("a")))),
                    attribute("c"),
                    attribute("d.e")
                )
            )
        );
    }

    public void testInvalidAlias() {
        expectError("row Å = 1", "line 1:5: token recognition error at: 'Å'");
        expectError("from test | eval Å = 1", "line 1:18: token recognition error at: 'Å'");
        expectError("from test | where Å == 1", "line 1:19: token recognition error at: 'Å'");
        expectError("from test | keep Å", "line 1:18: token recognition error at: 'Å'");
        expectError("from test | drop Å", "line 1:18: token recognition error at: 'Å'");
        expectError("from test | sort Å", "line 1:18: token recognition error at: 'Å'");
        expectError("from test | rename Å as A", "line 1:20: token recognition error at: 'Å'");
        expectError("from test | rename A as Å", "line 1:25: token recognition error at: 'Å'");
        expectError("from test | rename Å as Å", "line 1:20: token recognition error at: 'Å'");
        expectError("from test | stats Å = count(*)", "line 1:19: token recognition error at: 'Å'");
        expectError("from test | stats count(Å)", "line 1:25: token recognition error at: 'Å'");
        expectError("from test | eval A = coalesce(Å, null)", "line 1:31: token recognition error at: 'Å'");
        expectError("from test | eval A = coalesce(\"Å\", Å)", "line 1:36: token recognition error at: 'Å'");
    }

    private LogicalPlan unresolvedRelation(String index) {
        return new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, index), false, List.of(), IndexMode.STANDARD, null);
    }

    private LogicalPlan unresolvedTSRelation(String index) {
        List<Attribute> metadata = List.of(new MetadataAttribute(EMPTY, MetadataAttribute.TSID_FIELD, DataType.KEYWORD, false));
        return new UnresolvedRelation(EMPTY, new TableIdentifier(EMPTY, null, index), false, metadata, IndexMode.TIME_SERIES, null);
    }

    public void testMetricWithGroupKeyAsAgg() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());
        var queries = List.of("METRICS foo a BY a");
        for (String query : queries) {
            expectVerificationError(query, "grouping key [a] already specified in the STATS BY clause");
        }
    }

    private static final LogicalPlan PROCESSING_CMD_INPUT = new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1))));

}
