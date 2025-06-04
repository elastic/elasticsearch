/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dedup;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.RrfScoreEval;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsIdentifier;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsPattern;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.CROSS_CLUSTER;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.DATE_MATH;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.INDEX_SELECTOR;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.WILDCARD_PATTERN;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPattern;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPatterns;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.unquoteIndexPattern;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.without;
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

    private static final LogicalPlan PROCESSING_CMD_INPUT = new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1))));

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

    public void testStatsWithoutAggs() {
        assertEquals(
            new Aggregate(EMPTY, PROCESSING_CMD_INPUT, List.of(attribute("a")), List.of(attribute("a"))),
            processingCommand("stats by a")
        );
    }

    public void testStatsWithoutAggsOrGroup() {
        expectError("from text | stats", "At least one aggregation or grouping expression required in [stats]");
    }

    public void testAggsWithGroupKeyAsAgg() {
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

    public void testStatsWithGroupKeyAndAggFilter() {
        var a = attribute("a");
        var f = new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(a));
        var filter = new Alias(EMPTY, "min(a) where a > 1", new FilteredExpression(EMPTY, f, new GreaterThan(EMPTY, a, integer(1))));
        assertEquals(
            new Aggregate(EMPTY, PROCESSING_CMD_INPUT, List.of(a), List.of(filter, a)),
            processingCommand("stats min(a) where a > 1 by a")
        );
    }

    public void testStatsWithGroupKeyAndMixedAggAndFilter() {
        var a = attribute("a");
        var min = new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(a));
        var max = new UnresolvedFunction(EMPTY, "max", DEFAULT, List.of(a));
        var avg = new UnresolvedFunction(EMPTY, "avg", DEFAULT, List.of(a));
        var min_alias = new Alias(EMPTY, "min", min);

        var max_filter_ex = new Or(
            EMPTY,
            new GreaterThan(EMPTY, new Mod(EMPTY, a, integer(3)), integer(10)),
            new GreaterThan(EMPTY, new Div(EMPTY, a, integer(2)), integer(100))
        );
        var max_filter = new Alias(EMPTY, "max", new FilteredExpression(EMPTY, max, max_filter_ex));

        var avg_filter_ex = new GreaterThan(EMPTY, new Div(EMPTY, a, integer(2)), integer(100));
        var avg_filter = new Alias(EMPTY, "avg", new FilteredExpression(EMPTY, avg, avg_filter_ex));

        assertEquals(
            new Aggregate(EMPTY, PROCESSING_CMD_INPUT, List.of(a), List.of(min_alias, max_filter, avg_filter, a)),
            processingCommand("""
                stats
                min = min(a),
                max = max(a) WHERE (a % 3 > 10 OR a / 2 > 100),
                avg = avg(a) WHERE a / 2 > 100
                BY a
                """)
        );
    }

    public void testStatsWithoutGroupKeyMixedAggAndFilter() {
        var a = attribute("a");
        var f = new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(a));
        var filter = new Alias(EMPTY, "min(a) where a > 1", new FilteredExpression(EMPTY, f, new GreaterThan(EMPTY, a, integer(1))));
        assertEquals(new Aggregate(EMPTY, PROCESSING_CMD_INPUT, List.of(), List.of(filter)), processingCommand("stats min(a) where a > 1"));
    }

    public void testInlineStatsWithGroups() {
        var query = "inlinestats b = min(a) by c, d.e";
        if (Build.current().isSnapshot() == false) {
            expectThrows(
                ParsingException.class,
                containsString("line 1:13: mismatched input 'inlinestats' expecting {"),
                () -> processingCommand(query)
            );
            return;
        }
        assertEquals(
            new InlineStats(
                EMPTY,
                new Aggregate(
                    EMPTY,
                    PROCESSING_CMD_INPUT,
                    List.of(attribute("c"), attribute("d.e")),
                    List.of(
                        new Alias(EMPTY, "b", new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(attribute("a")))),
                        attribute("c"),
                        attribute("d.e")
                    )
                )
            ),
            processingCommand(query)
        );
    }

    public void testInlineStatsWithoutGroups() {
        var query = "inlinestats min(a), c = 1";
        if (Build.current().isSnapshot() == false) {
            expectThrows(
                ParsingException.class,
                containsString("line 1:13: mismatched input 'inlinestats' expecting {"),
                () -> processingCommand(query)
            );
            return;
        }
        assertEquals(
            new InlineStats(
                EMPTY,
                new Aggregate(
                    EMPTY,
                    PROCESSING_CMD_INPUT,
                    List.of(),
                    List.of(
                        new Alias(EMPTY, "min(a)", new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(attribute("a")))),
                        new Alias(EMPTY, "c", integer(1))
                    )
                )
            ),
            processingCommand(query)
        );
    }

    public void testStringAsIndexPattern() {
        List<String> commands = new ArrayList<>();
        commands.add("FROM");
        if (Build.current().isSnapshot()) {
            commands.add("TS");
        }
        for (String command : commands) {
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
            assertStringAsIndexPattern(
                "-<logstash-{now/M{yyyy.MM}}>,-<-logstash-{now/M{yyyy.MM}}>,"
                    + "-<logstash-{now/d{yyyy.MM.dd|+12:00}}>,-<-logstash-{now/d{yyyy.MM.dd|+12:00}}>",
                command
                    + " -<logstash-{now/M{yyyy.MM}}>, -<-logstash-{now/M{yyyy.MM}}>, "
                    + "\"-<logstash-{now/d{yyyy.MM.dd|+12:00}}>\", \"-<-logstash-{now/d{yyyy.MM.dd|+12:00}}>\""
            );
            assertStringAsIndexPattern("foo,test,xyz", command + " \"\"\"foo\"\"\",   test,\"xyz\"");
            assertStringAsIndexPattern("`backtick`,``multiple`back``ticks```", command + " `backtick`, ``multiple`back``ticks```");
            assertStringAsIndexPattern("test,metadata,metaata,.metadata", command + " test,\"metadata\", metaata, .metadata");
            assertStringAsIndexPattern(".dot", command + " .dot");
            String lineNumber = command.equals("FROM") ? "line 1:14: " : "line 1:12: ";
            expectErrorWithLineNumber(
                command + " cluster:\"index|pattern\"",
                " cluster:\"index|pattern\"",
                lineNumber,
                "mismatched input '\"index|pattern\"' expecting UNQUOTED_SOURCE"
            );
            assertStringAsIndexPattern("*:index|pattern", command + " \"*:index|pattern\"");
            clusterAndIndexAsIndexPattern(command, "cluster:index");
            clusterAndIndexAsIndexPattern(command, "cluster:.index");
            clusterAndIndexAsIndexPattern(command, "cluster*:index*");
            clusterAndIndexAsIndexPattern(command, "cluster*:<logstash-{now/D}>*");// this is not a valid pattern, * should be inside <>
            clusterAndIndexAsIndexPattern(command, "cluster*:<logstash-{now/D}*>");
            clusterAndIndexAsIndexPattern(command, "cluster*:*");
            clusterAndIndexAsIndexPattern(command, "*:index*");
            clusterAndIndexAsIndexPattern(command, "*:*");
            if (EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled()) {
                assertStringAsIndexPattern("foo::data", command + " foo::data");
                assertStringAsIndexPattern("foo::failures", command + " foo::failures");
                expectErrorWithLineNumber(
                    command + " cluster:\"foo::data\"",
                    " cluster:\"foo::data\"",
                    lineNumber,
                    "mismatched input '\"foo::data\"' expecting UNQUOTED_SOURCE"
                );
                expectErrorWithLineNumber(
                    command + " cluster:\"foo::failures\"",
                    " cluster:\"foo::failures\"",
                    lineNumber,
                    "mismatched input '\"foo::failures\"' expecting UNQUOTED_SOURCE"
                );
                lineNumber = command.equals("FROM") ? "line 1:15: " : "line 1:13: ";
                expectErrorWithLineNumber(
                    command + " *, \"-foo\"::data",
                    " *, \"-foo\"::data",
                    lineNumber,
                    "mismatched input '::' expecting {<EOF>, '|', ',', 'metadata'}"
                );
                assertStringAsIndexPattern("*,-foo::data", command + " *, \"-foo::data\"");
                assertStringAsIndexPattern("*::data", command + " *::data");
                lineNumber = command.equals("FROM") ? "line 1:79: " : "line 1:77: ";
                expectErrorWithLineNumber(
                    command + " \"<logstash-{now/M{yyyy.MM}}>::data,<logstash-{now/d{yyyy.MM.dd|+12:00}}>\"::failures",
                    " \"<logstash-{now/M{yyyy.MM}}>::data,<logstash-{now/d{yyyy.MM.dd|+12:00}}>\"::failures",
                    lineNumber,
                    "mismatched input '::' expecting {<EOF>, '|', ',', 'metadata'}"
                );
                assertStringAsIndexPattern(
                    "<logstash-{now/M{yyyy.MM}}>::data,<logstash-{now/d{yyyy.MM.dd|+12:00}}>::failures",
                    command + " <logstash-{now/M{yyyy.MM}}>::data, \"<logstash-{now/d{yyyy.MM.dd|+12:00}}>::failures\""
                );
            }
        }
    }

    private void clusterAndIndexAsIndexPattern(String command, String clusterAndIndex) {
        assertStringAsIndexPattern(clusterAndIndex, command + " " + clusterAndIndex);
        assertStringAsIndexPattern(clusterAndIndex, command + " \"" + clusterAndIndex + "\"");
    }

    public void testStringAsLookupIndexPattern() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());
        assertStringAsLookupIndexPattern("foo", "ROW x = 1 | LOOKUP_🐔 \"foo\" ON j");
        assertStringAsLookupIndexPattern("test-*", """
            ROW x = 1 | LOOKUP_🐔 "test-*" ON j
            """);
        assertStringAsLookupIndexPattern("test-*", "ROW x = 1 | LOOKUP_🐔 test-* ON j");
        assertStringAsLookupIndexPattern("123-test@foo_bar+baz1", "ROW x = 1 | LOOKUP_🐔 123-test@foo_bar+baz1 ON j");
        assertStringAsLookupIndexPattern("foo, test-*, abc, xyz", """
            ROW x = 1 | LOOKUP_🐔     "foo, test-*, abc, xyz"  ON j
            """);
        assertStringAsLookupIndexPattern("<logstash-{now/M{yyyy.MM}}>", "ROW x = 1 | LOOKUP_🐔 <logstash-{now/M{yyyy.MM}}> ON j");
        assertStringAsLookupIndexPattern(
            "<logstash-{now/d{yyyy.MM.dd|+12:00}}>",
            "ROW x = 1 | LOOKUP_🐔 \"<logstash-{now/d{yyyy.MM.dd|+12:00}}>\" ON j"
        );

        assertStringAsLookupIndexPattern("foo", "ROW x = 1 | LOOKUP_🐔 \"\"\"foo\"\"\" ON j");
        assertStringAsLookupIndexPattern("`backtick`", "ROW x = 1 | LOOKUP_🐔 `backtick` ON j");
        assertStringAsLookupIndexPattern("``multiple`back``ticks```", "ROW x = 1 | LOOKUP_🐔 ``multiple`back``ticks``` ON j");
        assertStringAsLookupIndexPattern(".dot", "ROW x = 1 | LOOKUP_🐔 .dot ON j");
        clusterAndIndexAsLookupIndexPattern("cluster:index");
        clusterAndIndexAsLookupIndexPattern("cluster:.index");
        clusterAndIndexAsLookupIndexPattern("cluster*:index*");
        clusterAndIndexAsLookupIndexPattern("cluster*:*");
        clusterAndIndexAsLookupIndexPattern("*:index*");
        clusterAndIndexAsLookupIndexPattern("*:*");
    }

    private void clusterAndIndexAsLookupIndexPattern(String clusterAndIndex) {
        assertStringAsLookupIndexPattern(clusterAndIndex, "ROW x = 1 | LOOKUP_🐔 " + clusterAndIndex + " ON j");
        assertStringAsLookupIndexPattern(clusterAndIndex, "ROW x = 1 | LOOKUP_🐔 \"" + clusterAndIndex + "\"" + " ON j");
    }

    public void testInvalidCharacterInIndexPattern() {
        Map<String, String> commands = new HashMap<>();
        commands.put("FROM {}", "line 1:6: ");
        if (Build.current().isSnapshot()) {
            commands.put("ROW x = 1 | LOOKUP_🐔 {} ON j", "line 1:22: ");
        }
        String lineNumber;
        for (String command : commands.keySet()) {
            lineNumber = commands.get(command);
            expectInvalidIndexNameErrorWithLineNumber(command, "index|pattern", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "index pattern", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "index#pattern", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "index?pattern", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "index>pattern", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "index<pattern", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "index/pattern", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "_indexpattern", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "+indexpattern", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "..", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "+<logstash-{now/d}>", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "_<logstash-{now/d}>", lineNumber);
            expectInvalidIndexNameErrorWithLineNumber(command, "index\\pattern", lineNumber, "index\\pattern");
            expectInvalidIndexNameErrorWithLineNumber(command, "\"index\\\\pattern\"", lineNumber, "index\\pattern");
            expectInvalidIndexNameErrorWithLineNumber(command, "\"--indexpattern\"", lineNumber, "-indexpattern");
            expectInvalidIndexNameErrorWithLineNumber(command, "--indexpattern", lineNumber, "-indexpattern");
            expectInvalidIndexNameErrorWithLineNumber(command, "<--logstash-{now/M{yyyy.MM}}>", lineNumber, "-logstash-");
            expectInvalidIndexNameErrorWithLineNumber(
                command,
                "\"--<logstash-{now/M{yyyy.MM}}>\"",
                lineNumber,
                "-<logstash-{now/M{yyyy.MM}}>"
            );
            expectInvalidIndexNameErrorWithLineNumber(command, "<logstash#{now/d}>", lineNumber, "logstash#");
            expectInvalidIndexNameErrorWithLineNumber(command, "\"<logstash#{now/d}>\"", lineNumber, "logstash#");
            expectInvalidIndexNameErrorWithLineNumber(command, "<<logstash{now/d}>>", lineNumber, "<logstash");
            expectInvalidIndexNameErrorWithLineNumber(command, "\"<<logstash{now/d}>>\"", lineNumber, "<logstash");
            expectInvalidIndexNameErrorWithLineNumber(command, "<<logstash<{now/d}>>>", lineNumber, "<logstash<");
            expectInvalidIndexNameErrorWithLineNumber(command, "\"<<logstash<{now/d}>>>\"", lineNumber, "<logstash<");
            expectInvalidIndexNameErrorWithLineNumber(command, "\"-<logstash- {now/d{yyyy.MM.dd|+12:00}}>\"", lineNumber, "logstash- ");
            if (EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled() && command.contains("LOOKUP_🐔") == false) {
                expectInvalidIndexNameErrorWithLineNumber(command, "index::dat", lineNumber);
                expectInvalidIndexNameErrorWithLineNumber(command, "index::failure", lineNumber);

                // Cluster name cannot be combined with selector yet.
                var parseLineNumber = command.contains("FROM") ? 6 : 9;
                expectDoubleColonErrorWithLineNumber(command, "cluster:foo::data", parseLineNumber + 11);
                expectDoubleColonErrorWithLineNumber(command, "cluster:foo::failures", parseLineNumber + 11);

                // Index pattern cannot be quoted if cluster string is present.
                expectErrorWithLineNumber(
                    command,
                    "cluster:\"foo\"::data",
                    "line 1:14: ",
                    "mismatched input '\"foo\"' expecting UNQUOTED_SOURCE"
                );
                expectErrorWithLineNumber(
                    command,
                    "cluster:\"foo\"::failures",
                    "line 1:14: ",
                    "mismatched input '\"foo\"' expecting UNQUOTED_SOURCE"
                );

                // TODO: Edge case that will be invalidated in follow up (https://github.com/elastic/elasticsearch/issues/122651)
                // expectDoubleColonErrorWithLineNumber(command, "\"cluster:foo\"::data", parseLineNumber + 13);
                // expectDoubleColonErrorWithLineNumber(command, "\"cluster:foo\"::failures", parseLineNumber + 13);

                expectErrorWithLineNumber(
                    command,
                    "\"cluster:foo::data\"",
                    lineNumber,
                    "Invalid index name [cluster:foo::data], Selectors are not yet supported on remote cluster patterns"
                );
                expectErrorWithLineNumber(
                    command,
                    "\"cluster:foo::failures\"",
                    lineNumber,
                    "Invalid index name [cluster:foo::failures], Selectors are not yet supported on remote cluster patterns"
                );

                // Wildcards
                expectDoubleColonErrorWithLineNumber(command, "cluster:*::data", parseLineNumber + 9);
                expectDoubleColonErrorWithLineNumber(command, "cluster:*::failures", parseLineNumber + 9);
                expectDoubleColonErrorWithLineNumber(command, "*:index::data", parseLineNumber + 7);
                expectDoubleColonErrorWithLineNumber(command, "*:index::failures", parseLineNumber + 7);
                expectDoubleColonErrorWithLineNumber(command, "*:index*::data", parseLineNumber + 8);
                expectDoubleColonErrorWithLineNumber(command, "*:index*::failures", parseLineNumber + 8);
                expectDoubleColonErrorWithLineNumber(command, "*:*::data", parseLineNumber + 3);
                expectDoubleColonErrorWithLineNumber(command, "*:*::failures", parseLineNumber + 3);

                // Too many colons
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "\"index:::data\"",
                    lineNumber,
                    "index:::data",
                    "Selectors are not yet supported on remote cluster patterns"
                );
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "\"index::::data\"",
                    lineNumber,
                    "index::::data",
                    "Invalid usage of :: separator"
                );

                expectErrorWithLineNumber(
                    command,
                    "cluster:\"index,index2\"::failures",
                    "line 1:14: ",
                    "mismatched input '\"index,index2\"' expecting UNQUOTED_SOURCE"
                );
            }
        }

        // comma separated indices, with exclusions
        // Invalid index names after removing exclusion fail, when there is no index name with wildcard before it
        for (String command : commands.keySet()) {
            if (command.contains("LOOKUP_🐔") || command.contains("TS")) {
                continue;
            }

            lineNumber = command.contains("FROM") ? "line 1:20: " : "line 1:23: ";
            expectInvalidIndexNameErrorWithLineNumber(command, "indexpattern, --indexpattern", lineNumber, "-indexpattern");
            expectInvalidIndexNameErrorWithLineNumber(command, "indexpattern, \"--indexpattern\"", lineNumber, "-indexpattern");
            expectInvalidIndexNameErrorWithLineNumber(command, "\"indexpattern, --indexpattern\"", commands.get(command), "-indexpattern");
            expectInvalidIndexNameErrorWithLineNumber(command, "\"- , -\"", commands.get(command), "", "must not be empty");
            expectInvalidIndexNameErrorWithLineNumber(command, "\"indexpattern,-\"", commands.get(command), "", "must not be empty");
            clustersAndIndices(command, "indexpattern", "*-");
            clustersAndIndices(command, "indexpattern", "-indexpattern");
            if (EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled()) {
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "indexpattern, --index::data",
                    lineNumber,
                    "-index",
                    "must not start with '_', '-', or '+'"
                );
                expectErrorWithLineNumber(
                    command,
                    "indexpattern, \"--index\"::data",
                    "line 1:29: ",
                    "mismatched input '::' expecting {<EOF>, '|', ',', 'metadata'}"
                );
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "\"indexpattern, --index::data\"",
                    commands.get(command),
                    "-index",
                    "must not start with '_', '-', or '+'"
                );
            }
        }

        // Invalid index names, except invalid DateMath, are ignored if there is an index name with wildcard before it
        String dateMathError = "unit [D] not supported for date math [/D]";
        for (String command : commands.keySet()) {
            if (command.contains("LOOKUP_🐔") || command.contains("TS")) {
                continue;
            }
            lineNumber = command.contains("FROM") ? "line 1:9: " : "line 1:12: ";
            String indexStarLineNumber = command.contains("FROM") ? "line 1:14: " : "line 1:17: ";
            clustersAndIndices(command, "*", "-index#pattern");
            clustersAndIndices(command, "index*", "-index#pattern");
            clustersAndIndices(command, "*", "-<--logstash-{now/M{yyyy.MM}}>");
            clustersAndIndices(command, "index*", "-<--logstash#-{now/M{yyyy.MM}}>");
            expectDateMathErrorWithLineNumber(command, "*, \"-<-logstash-{now/D}>\"", lineNumber, dateMathError);
            expectDateMathErrorWithLineNumber(command, "*, -<-logstash-{now/D}>", lineNumber, dateMathError);
            expectDateMathErrorWithLineNumber(command, "\"*, -<-logstash-{now/D}>\"", commands.get(command), dateMathError);
            expectDateMathErrorWithLineNumber(command, "\"*, -<-logst:ash-{now/D}>\"", commands.get(command), dateMathError);
            if (EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled()) {
                clustersAndIndices(command, "*", "-index#pattern::data");
                clustersAndIndices(command, "*", "-index#pattern::data");
                clustersAndIndices(command, "index*", "-index#pattern::data");
                clustersAndIndices(command, "*", "-<--logstash-{now/M{yyyy.MM}}>::data");
                clustersAndIndices(command, "index*", "-<--logstash#-{now/M{yyyy.MM}}>::data");
                // Throw on invalid date math
                expectDateMathErrorWithLineNumber(
                    command,
                    "*, \"-<-logstash-{now/D}>\"::data",
                    "line 1:31: ",
                    "mismatched input '::' expecting {<EOF>, '|', ',', 'metadata'}"
                );
                expectDateMathErrorWithLineNumber(command, "*, -<-logstash-{now/D}>::data", lineNumber, dateMathError);
                // Check that invalid selectors throw (they're resolved first in /_search, and always validated)
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "*, -index::garbage",
                    lineNumber,
                    "-index::garbage",
                    "invalid usage of :: separator, [garbage] is not a recognized selector"
                );
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "index*, -index::garbage",
                    indexStarLineNumber,
                    "-index::garbage",
                    "invalid usage of :: separator, [garbage] is not a recognized selector"
                );
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "*, -<logstash-{now/M{yyyy.MM}}>::garbage",
                    lineNumber,
                    "-<logstash-{now/M{yyyy.MM}}>::garbage",
                    "invalid usage of :: separator, [garbage] is not a recognized selector"
                );
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "index*, -<logstash-{now/M{yyyy.MM}}>::garbage",
                    indexStarLineNumber,
                    "-<logstash-{now/M{yyyy.MM}}>::garbage",
                    "invalid usage of :: separator, [garbage] is not a recognized selector"
                );
                // Invalid selectors will throw validation errors before invalid date math
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "\"*, -<-logstash-{now/D}>::d\"",
                    commands.get(command),
                    "-<-logstash-{now/D}>::d",
                    "invalid usage of :: separator, [d] is not a recognized selector"
                );
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "\"*, -<-logstash-{now/D}>::\"",
                    commands.get(command),
                    "-<-logstash-{now/D}>::",
                    "invalid usage of :: separator, [] is not a recognized selector"
                );
            }
        }
    }

    private void clustersAndIndices(String command, String indexString1, String indexString2) {
        assertEquals(unresolvedRelation(indexString1 + "," + indexString2), statement(command, indexString1 + ", " + indexString2));
        assertEquals(
            unresolvedRelation(indexString1 + "," + indexString2),
            statement(command, indexString1 + ", \"" + indexString2 + "\"")
        );
        assertEquals(
            unresolvedRelation(indexString1 + ", " + indexString2),
            statement(command, "\"" + indexString1 + ", " + indexString2 + "\"")
        );
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

        expectError("FROM \"\"\"foo\"\"\"bar\"\"\"", ": mismatched input 'bar' expecting {<EOF>, '|', ',', 'metadata'}");
        expectError("FROM \"\"\"foo\"\"\"\"\"\"bar\"\"\"", ": mismatched input '\"bar\"' expecting {<EOF>, '|', ',', 'metadata'}");
    }

    public void testInvalidQuotingAsLookupIndexPattern() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        expectError("ROW x = 1 | LOOKUP_🐔 \"foo ON j", ": token recognition error at: '\"foo ON j'");
        expectError("ROW x = 1 | LOOKUP_🐔 \"\"\"foo ON j", ": token recognition error at: '\"foo ON j'");

        expectError("ROW x = 1 | LOOKUP_🐔 foo\" ON j", ": token recognition error at: '\" ON j'");
        expectError("ROW x = 1 | LOOKUP_🐔 foo\"\"\" ON j", ": token recognition error at: '\" ON j'");

        expectError("ROW x = 1 | LOOKUP_🐔 \"foo\"bar\" ON j", ": token recognition error at: '\" ON j'");
        expectError("ROW x = 1 | LOOKUP_🐔 \"foo\"\"bar\" ON j", ": extraneous input '\"bar\"' expecting 'on'");

        expectError("ROW x = 1 | LOOKUP_🐔 \"\"\"foo\"\"\"bar\"\"\" ON j", ": mismatched input 'bar' expecting 'on'");
        expectError("ROW x = 1 | LOOKUP_🐔 \"\"\"foo\"\"\"\"\"\"bar\"\"\" ON j", ": mismatched input '\"bar\"' expecting 'on'");
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
        expectError("from text | limit -1", "line 1:13: Invalid value for LIMIT [-1], expecting a non negative integer");
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
            expectThrows(
                ParsingException.class,
                allOf(
                    containsString("mismatched input '" + queryWithUnexpectedCmd.v2() + "'"),
                    containsString("'explain'"),
                    containsString("'from'"),
                    containsString("'row'")
                ),
                () -> statement(queryWithUnexpectedCmd.v1())
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
            expectThrows(
                ParsingException.class,
                allOf(
                    containsString("mismatched input '" + queryWithUnexpectedCmd.v2() + "'"),
                    containsString("'eval'"),
                    containsString("'stats'"),
                    containsString("'where'")
                ),
                () -> statement(queryWithUnexpectedCmd.v1())
            );
        }
    }

    public void testDeprecatedIsNullFunction() {
        expectError(
            "from test | eval x = is_null(f)",
            "line 1:22: is_null function is not supported anymore, please use 'is null'/'is not null' predicates instead"
        );
        expectError(
            "row x = is_null(f)",
            "line 1:9: is_null function is not supported anymore, please use 'is null'/'is not null' predicates instead"
        );

        if (Build.current().isSnapshot()) {
            expectError(
                "from test | eval x = ?fn1(f)",
                List.of(paramAsIdentifier("fn1", "IS_NULL")),
                "line 1:22: is_null function is not supported anymore, please use 'is null'/'is not null' predicates instead"
            );
        }
        if (EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled()) {
            expectError(
                "from test | eval x = ??fn1(f)",
                List.of(paramAsConstant("fn1", "IS_NULL")),
                "line 1:22: is_null function is not supported anymore, please use 'is null'/'is not null' predicates instead"
            );
        }
    }

    public void testMetadataFieldOnOtherSources() {
        expectError("row a = 1 metadata _index", "line 1:20: extraneous input '_index' expecting <EOF>");
        expectError("show info metadata _index", "line 1:11: token recognition error at: 'm'");
        expectError("explain [from foo] metadata _index", "line 1:20: mismatched input 'metadata' expecting {'|', ',', ']', 'metadata'}");
    }

    public void testMetadataFieldMultipleDeclarations() {
        expectError("from test metadata _index, _version, _index", "1:38: metadata field [_index] already declared [@1:20]");
    }

    public void testMetadataFieldUnsupportedPrimitiveType() {
        expectError("from test metadata _tier", "line 1:20: unsupported metadata field [_tier]");
    }

    public void testMetadataFieldUnsupportedCustomType() {
        expectError("from test metadata _feature", "line 1:20: unsupported metadata field [_feature]");
    }

    public void testMetadataFieldNotFoundNonExistent() {
        expectError("from test metadata _doesnot_compute", "line 1:20: unsupported metadata field [_doesnot_compute]");
    }

    public void testMetadataFieldNotFoundNormalField() {
        expectError("from test metadata emp_no", "line 1:20: unsupported metadata field [emp_no]");
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

        expectThrows(
            ParsingException.class,
            containsString("Invalid pattern [%{_invalid_:x}] for grok: Unable to find pattern [_invalid_] in Grok's pattern dictionary"),
            () -> statement("row a = \"foo bar\" | grok a \"%{_invalid_:x}\"")
        );

        cmd = processingCommand("grok a \"%{WORD:foo} %{WORD:foo}\"");
        assertEquals(Grok.class, cmd.getClass());
        grok = (Grok) cmd;
        assertEquals("%{WORD:foo} %{WORD:foo}", grok.parser().pattern());
        assertEquals(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());

        expectError(
            "row a = \"foo bar\" | GROK a \"%{NUMBER:foo} %{WORD:foo}\"",
            "line 1:21: Invalid GROK pattern [%{NUMBER:foo} %{WORD:foo}]:"
                + " the attribute [foo] is defined multiple times with different types"
        );

        expectError(
            "row a = \"foo\" | GROK a \"(?P<justification>.+)\"",
            "line 1:17: Invalid grok pattern [(?P<justification>.+)]: [undefined group option]"
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

        expectError(
            "from a | where foo like \"(?i)(^|[^a-zA-Z0-9_-])nmap($|\\\\.)\"",
            "line 1:16: Invalid pattern for LIKE [(?i)(^|[^a-zA-Z0-9_-])nmap($|\\.)]: "
                + "[Invalid sequence - escape character is not followed by special wildcard char]"
        );
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

        expectError("from a | enrich countries on foo* ", "Using wildcards [*] in ENRICH WITH projections is not allowed, found [foo*]");
        expectError("from a | enrich countries on * ", "Using wildcards [*] in ENRICH WITH projections is not allowed, found [*]");
        expectError(
            "from a | enrich countries on foo with bar*",
            "Using wildcards [*] in ENRICH WITH projections is not allowed, found [bar*]"
        );
        expectError("from a | enrich countries on foo with *", "Using wildcards [*] in ENRICH WITH projections is not allowed, found [*]");
        expectError(
            "from a | enrich countries on foo with x = bar* ",
            "Using wildcards [*] in ENRICH WITH projections is not allowed, found [bar*]"
        );
        expectError(
            "from a | enrich countries on foo with x = * ",
            "Using wildcards [*] in ENRICH WITH projections is not allowed, found [*]"
        );
        expectError(
            "from a | enrich countries on foo with x* = bar ",
            "Using wildcards [*] in ENRICH WITH projections is not allowed, found [x*]"
        );
        expectError(
            "from a | enrich countries on foo with * = bar ",
            "Using wildcards [*] in ENRICH WITH projections is not allowed, found [*]"
        );
        expectError(
            "from a | enrich typo:countries on foo",
            "line 1:17: Unrecognized value [typo], ENRICH policy qualifier needs to be one of [_ANY, _COORDINATOR, _REMOTE]"
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
        expectThrows(ParsingException.class, containsString("mismatched input 'project' expecting"), () -> statement(query));
    }

    public void testInputParams() {
        LogicalPlan stm = statement(
            "row x = ?, y = ?, a = ?, b = ?, c = ?, d = ?, e = ?-1, f = ?+1",
            new QueryParams(
                List.of(
                    paramAsConstant(null, 1),
                    paramAsConstant(null, "2"),
                    paramAsConstant(null, "2 days"),
                    paramAsConstant(null, "4 hours"),
                    paramAsConstant(null, "1.2.3"),
                    paramAsConstant(null, "127.0.0.1"),
                    paramAsConstant(null, 10),
                    paramAsConstant(null, 10)
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
        assertThat(alias.child().fold(FoldContext.small()), is(1));

        field = row.fields().get(1);
        assertThat(field.name(), is("y"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is("2"));

        field = row.fields().get(2);
        assertThat(field.name(), is("a"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is("2 days"));

        field = row.fields().get(3);
        assertThat(field.name(), is("b"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is("4 hours"));

        field = row.fields().get(4);
        assertThat(field.name(), is("c"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()).getClass(), is(String.class));
        assertThat(alias.child().fold(FoldContext.small()).toString(), is("1.2.3"));

        field = row.fields().get(5);
        assertThat(field.name(), is("d"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()).getClass(), is(String.class));
        assertThat(alias.child().fold(FoldContext.small()).toString(), is("127.0.0.1"));

        field = row.fields().get(6);
        assertThat(field.name(), is("e"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is(9));

        field = row.fields().get(7);
        assertThat(field.name(), is("f"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is(11));
    }

    public void testMissingInputParams() {
        expectError("row x = ?, y = ?", List.of(paramAsConstant(null, 1)), "Not enough actual parameters 1");

        if (EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled()) {
            expectError("from test | eval x = ??, y = ??", List.of(paramAsConstant(null, 1)), "Not enough actual parameters 1");
            expectError("from test | eval x = ??, y = ?", List.of(paramAsConstant(null, 1)), "Not enough actual parameters 1");
        }
    }

    public void testNamedParams() {
        LogicalPlan stm = statement("row x=?name1, y = ?name1", new QueryParams(List.of(paramAsConstant("name1", 1))));
        assertThat(stm, instanceOf(Row.class));
        Row row = (Row) stm;
        assertThat(row.fields().size(), is(2));

        NamedExpression field = row.fields().get(0);
        assertThat(field.name(), is("x"));
        assertThat(field, instanceOf(Alias.class));
        Alias alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is(1));

        field = row.fields().get(1);
        assertThat(field.name(), is("y"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is(1));
    }

    public void testInvalidNamedParams() {
        expectError(
            "from test | where x < ?n1 | eval y = ?n2",
            List.of(paramAsConstant("n1", 5)),
            "Unknown query parameter [n2], did you mean [n1]?"
        );

        expectError(
            "from test | where x < ?n1 | eval y = ?n2",
            List.of(paramAsConstant("n1", 5), paramAsConstant("n3", 5)),
            "Unknown query parameter [n2], did you mean any of [n3, n1]?"
        );

        expectError("from test | where x < ?@1", List.of(paramAsConstant("@1", 5)), "extraneous input '@1' expecting <EOF>");

        expectError("from test | where x < ?#1", List.of(paramAsConstant("#1", 5)), "token recognition error at: '#'");

        expectError("from test | where x < ?Å", List.of(paramAsConstant("Å", 5)), "line 1:24: token recognition error at: 'Å'");

        expectError("from test | eval x = ?Å", List.of(paramAsConstant("Å", 5)), "line 1:23: token recognition error at: 'Å'");

        if (EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled()) {
            expectError(
                "from test | where x < ???",
                List.of(paramAsConstant("n_1", 5), paramAsConstant("n_2", 5)),
                "extraneous input '?' expecting <EOF>"
            );
        } else {
            expectError(
                "from test | where x < ??",
                List.of(paramAsConstant("n_1", 5), paramAsConstant("n_2", 5)),
                "extraneous input '?' expecting <EOF>"
            );
        }
    }

    public void testPositionalParams() {
        LogicalPlan stm = statement("row x=?1, y=?1", new QueryParams(List.of(paramAsConstant(null, 1))));
        assertThat(stm, instanceOf(Row.class));
        Row row = (Row) stm;
        assertThat(row.fields().size(), is(2));

        NamedExpression field = row.fields().get(0);
        assertThat(field.name(), is("x"));
        assertThat(field, instanceOf(Alias.class));
        Alias alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is(1));

        field = row.fields().get(1);
        assertThat(field.name(), is("y"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is(1));
    }

    public void testInvalidPositionalParams() {
        expectError(
            "from test | where x < ?0",
            List.of(paramAsConstant(null, 5)),
            "No parameter is defined for position 0, did you mean position 1"
        );

        expectError(
            "from test | where x < ?2",
            List.of(paramAsConstant(null, 5)),
            "No parameter is defined for position 2, did you mean position 1"
        );

        expectError(
            "from test | where x < ?0 and y < ?2",
            List.of(paramAsConstant(null, 5)),
            "line 1:23: No parameter is defined for position 0, did you mean position 1?; "
                + "line 1:34: No parameter is defined for position 2, did you mean position 1?"
        );

        expectError(
            "from test | where x < ?0",
            List.of(paramAsConstant(null, 5), paramAsConstant(null, 10)),
            "No parameter is defined for position 0, did you mean any position between 1 and 2?"
        );
    }

    public void testParamInWhere() {
        LogicalPlan plan = statement("from test | where x < ? |  limit 10", new QueryParams(List.of(paramAsConstant(null, 5))));
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

        plan = statement("from test | where x < ?n1 |  limit 10", new QueryParams(List.of(paramAsConstant("n1", 5))));
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

        plan = statement("from test | where x < ?_n1 |  limit 10", new QueryParams(List.of(paramAsConstant("_n1", 5))));
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

        plan = statement("from test | where x < ?1 |  limit 10", new QueryParams(List.of(paramAsConstant(null, 5))));
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

        plan = statement("from test | where x < ?__1 |  limit 10", new QueryParams(List.of(paramAsConstant("__1", 5))));
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
            new QueryParams(List.of(paramAsConstant(null, 5), paramAsConstant(null, -1), paramAsConstant(null, 100)))
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
            new QueryParams(List.of(paramAsConstant("n1", 5), paramAsConstant("n2", -1), paramAsConstant("n3", 100)))
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
            new QueryParams(List.of(paramAsConstant("_n1", 5), paramAsConstant("_n2", -1), paramAsConstant("_n3", 100)))
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
            new QueryParams(List.of(paramAsConstant(null, 5), paramAsConstant(null, -1)))
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
            new QueryParams(List.of(paramAsConstant("_1", 5), paramAsConstant("_2", -1)))
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
                List.of(paramAsConstant(null, 5), paramAsConstant(null, -1), paramAsConstant(null, 100), paramAsConstant(null, "*"))
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
                List.of(paramAsConstant("n1", 5), paramAsConstant("n2", -1), paramAsConstant("n3", 100), paramAsConstant("n4", "*"))
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
                List.of(paramAsConstant("_n1", 5), paramAsConstant("_n2", -1), paramAsConstant("_n3", 100), paramAsConstant("_n4", "*"))
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
            new QueryParams(List.of(paramAsConstant(null, 5), paramAsConstant(null, -1), paramAsConstant(null, "*")))
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
            new QueryParams(List.of(paramAsConstant("_1", 5), paramAsConstant("_2", -1), paramAsConstant("_3", "*")))
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
        Map<List<String>, String> mixedParams = new HashMap<>(
            Map.ofEntries(
                Map.entry(List.of("?", "?n2", "?n3"), "named and anonymous"),
                Map.entry(List.of("?", "?_n2", "?n3"), "named and anonymous"),
                Map.entry(List.of("?1", "?n2", "?_n3"), "named and positional"),
                Map.entry(List.of("?", "?2", "?n3"), "positional and anonymous")
            )
        );

        if (EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled()) {
            mixedParams.put(List.of("??", "??n2", "??n3"), "named and anonymous");
            mixedParams.put(List.of("?", "??_n2", "?n3"), "named and anonymous");
            mixedParams.put(List.of("??1", "?n2", "?_n3"), "named and positional");
            mixedParams.put(List.of("?", "??2", "?n3"), "positional and anonymous");
        }
        for (Map.Entry<List<String>, String> mixedParam : mixedParams.entrySet()) {
            List<String> params = mixedParam.getKey();
            String errorMessage = mixedParam.getValue();
            String query = LoggerMessageFormat.format(
                null,
                "from test | where x < {} | eval y = {}() + {}",
                params.get(0),
                params.get(1),
                params.get(2)
            );
            expectError(
                query,
                List.of(paramAsConstant("n1", "f1"), paramAsConstant("n2", "fn2"), paramAsConstant("n3", "f3")),
                "Inconsistent parameter declaration, "
                    + "use one of positional, named or anonymous params but not a combination of "
                    + errorMessage
            );
        }
    }

    public void testIntervalParam() {
        LogicalPlan stm = statement(
            "row x = ?1::datetime | eval y = ?1::datetime + ?2::date_period",
            new QueryParams(List.of(paramAsConstant("datetime", "2024-01-01"), paramAsConstant("date_period", "3 days")))
        );
        assertThat(stm, instanceOf(Eval.class));
        Eval eval = (Eval) stm;
        assertThat(eval.fields().size(), is(1));

        NamedExpression field = eval.fields().get(0);
        assertThat(field.name(), is("y"));
        assertThat(field, instanceOf(Alias.class));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).left().children().get(0)).value(), equalTo("2024-01-01"));
        assertThat(((Literal) ((Add) eval.fields().get(0).child()).right().children().get(0)).value(), equalTo("3 days"));
    }

    public void testParamForIdentifier() {
        // TODO will be replaced by testDoubleParamsForIdentifier after providing an identifier with a single parameter marker is deprecated
        // field names can appear in eval/where/stats/sort/keep/drop/rename/dissect/grok/enrich/mvexpand
        // eval, where
        assertEquals(
            new Limit(
                EMPTY,
                new Literal(EMPTY, 1, INTEGER),
                new Filter(
                    EMPTY,
                    new Eval(EMPTY, relation("test"), List.of(new Alias(EMPTY, "x", function("toString", List.of(attribute("f1.")))))),
                    new Equals(EMPTY, attribute("f1."), attribute("f.2"))
                )
            ),
            statement(
                """
                    from test
                    | eval ?f0 = ?fn1(?f1)
                    | where ?f1 == ?f2
                    | limit 1""",
                new QueryParams(
                    List.of(
                        paramAsIdentifier("f0", "x"),
                        paramAsIdentifier("f1", "f1."),
                        paramAsIdentifier("f2", "f.2"),
                        paramAsIdentifier("fn1", "toString")
                    )
                )
            )
        );

        assertEquals(
            new Limit(
                EMPTY,
                new Literal(EMPTY, 1, INTEGER),
                new Filter(
                    EMPTY,
                    new Eval(EMPTY, relation("test"), List.of(new Alias(EMPTY, "x", function("toString", List.of(attribute("f1..f.2")))))),
                    new Equals(EMPTY, attribute("f3.*.f.4."), attribute("f.5.*.f.*.6"))
                )
            ),
            statement(
                """
                    from test
                    | eval ?f0 = ?fn1(?f1.?f2)
                    | where ?f3.?f4 == ?f5.?f6
                    | limit 1""",
                new QueryParams(
                    List.of(
                        paramAsIdentifier("f0", "x"),
                        paramAsIdentifier("f1", "f1."),
                        paramAsIdentifier("f2", "f.2"),
                        paramAsIdentifier("f3", "f3.*"),
                        paramAsIdentifier("f4", "f.4."),
                        paramAsIdentifier("f5", "f.5.*"),
                        paramAsIdentifier("f6", "f.*.6"),
                        paramAsIdentifier("fn1", "toString")
                    )
                )
            )
        );

        // stats, sort, mv_expand
        assertEquals(
            new MvExpand(
                EMPTY,
                new OrderBy(
                    EMPTY,
                    new Aggregate(
                        EMPTY,
                        relation("test"),
                        List.of(attribute("f.4.")),
                        List.of(new Alias(EMPTY, "y", function("count", List.of(attribute("f3.*")))), attribute("f.4."))
                    ),
                    List.of(new Order(EMPTY, attribute("f.5.*"), Order.OrderDirection.ASC, Order.NullsPosition.LAST))
                ),
                attribute("f.6*"),
                attribute("f.6*")
            ),
            statement(
                """
                    from test
                    | stats y = ?fn2(?f3) by ?f4
                    | sort ?f5
                    | mv_expand ?f6""",
                new QueryParams(
                    List.of(
                        paramAsIdentifier("f3", "f3.*"),
                        paramAsIdentifier("f4", "f.4."),
                        paramAsIdentifier("f5", "f.5.*"),
                        paramAsIdentifier("f6", "f.6*"),
                        paramAsIdentifier("fn2", "count")
                    )
                )
            )
        );

        assertEquals(
            new MvExpand(
                EMPTY,
                new OrderBy(
                    EMPTY,
                    new Aggregate(
                        EMPTY,
                        relation("test"),
                        List.of(attribute("f.9.f10.*")),
                        List.of(new Alias(EMPTY, "y", function("count", List.of(attribute("f.7*.f8.")))), attribute("f.9.f10.*"))
                    ),
                    List.of(new Order(EMPTY, attribute("f.11..f.12.*"), Order.OrderDirection.ASC, Order.NullsPosition.LAST))
                ),
                attribute("f.*.13.f.14*"),
                attribute("f.*.13.f.14*")
            ),
            statement(
                """
                    from test
                    | stats y = ?fn2(?f7.?f8) by ?f9.?f10
                    | sort ?f11.?f12
                    | mv_expand ?f13.?f14""",
                new QueryParams(
                    List.of(
                        paramAsIdentifier("f7", "f.7*"),
                        paramAsIdentifier("f8", "f8."),
                        paramAsIdentifier("f9", "f.9"),
                        paramAsIdentifier("f10", "f10.*"),
                        paramAsIdentifier("f11", "f.11."),
                        paramAsIdentifier("f12", "f.12.*"),
                        paramAsIdentifier("f13", "f.*.13"),
                        paramAsIdentifier("f14", "f.14*"),
                        paramAsIdentifier("fn2", "count")
                    )
                )
            )
        );

        // keep, drop, rename, grok, dissect
        LogicalPlan plan = statement(
            """
                from test | keep ?f1, ?f2 | drop ?f3, ?f4 | dissect ?f5 "%{bar}" | grok ?f6 "%{WORD:foo}" | rename ?f7 as ?f8 | limit 1""",
            new QueryParams(
                List.of(
                    paramAsIdentifier("f1", "f.1.*"),
                    paramAsIdentifier("f2", "f.2"),
                    paramAsIdentifier("f3", "f3."),
                    paramAsIdentifier("f4", "f4.*"),
                    paramAsIdentifier("f5", "f.5*"),
                    paramAsIdentifier("f6", "f.6."),
                    paramAsIdentifier("f7", "f7*."),
                    paramAsIdentifier("f8", "f.8")
                )
            )
        );
        Limit limit = as(plan, Limit.class);
        Rename rename = as(limit.child(), Rename.class);
        assertEquals(rename.renamings(), List.of(new Alias(EMPTY, "f.8", attribute("f7*."))));
        Grok grok = as(rename.child(), Grok.class);
        assertEquals(grok.input(), attribute("f.6."));
        assertEquals("%{WORD:foo}", grok.parser().pattern());
        assertEquals(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
        Dissect dissect = as(grok.child(), Dissect.class);
        assertEquals(dissect.input(), attribute("f.5*"));
        assertEquals("%{bar}", dissect.parser().pattern());
        assertEquals("", dissect.parser().appendSeparator());
        assertEquals(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
        Drop drop = as(dissect.child(), Drop.class);
        List<? extends NamedExpression> removals = drop.removals();
        assertEquals(removals, List.of(attribute("f3."), attribute("f4.*")));
        Keep keep = as(drop.child(), Keep.class);
        assertEquals(keep.projections(), List.of(attribute("f.1.*"), attribute("f.2")));

        plan = statement(
            """
                from test | keep ?f1.?f2 | drop ?f3.?f4
                | dissect ?f5.?f6 "%{bar}" | grok ?f7.?f8 "%{WORD:foo}"
                | rename ?f9.?f10 as ?f11.?f12
                | limit 1""",
            new QueryParams(
                List.of(
                    paramAsIdentifier("f1", "f.1.*"),
                    paramAsIdentifier("f2", "f.2"),
                    paramAsIdentifier("f3", "f3."),
                    paramAsIdentifier("f4", "f4.*"),
                    paramAsIdentifier("f5", "f.5*"),
                    paramAsIdentifier("f6", "f.6."),
                    paramAsIdentifier("f7", "f7*."),
                    paramAsIdentifier("f8", "f.8"),
                    paramAsIdentifier("f9", "f.9*"),
                    paramAsIdentifier("f10", "f.10."),
                    paramAsIdentifier("f11", "f11*."),
                    paramAsIdentifier("f12", "f.12")
                )
            )
        );
        limit = as(plan, Limit.class);
        rename = as(limit.child(), Rename.class);
        assertEquals(rename.renamings(), List.of(new Alias(EMPTY, "f11*..f.12", attribute("f.9*.f.10."))));
        grok = as(rename.child(), Grok.class);
        assertEquals(grok.input(), attribute("f7*..f.8"));
        assertEquals("%{WORD:foo}", grok.parser().pattern());
        assertEquals(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
        dissect = as(grok.child(), Dissect.class);
        assertEquals(dissect.input(), attribute("f.5*.f.6."));
        assertEquals("%{bar}", dissect.parser().pattern());
        assertEquals("", dissect.parser().appendSeparator());
        assertEquals(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
        drop = as(dissect.child(), Drop.class);
        removals = drop.removals();
        assertEquals(removals, List.of(attribute("f3..f4.*")));
        keep = as(drop.child(), Keep.class);
        assertEquals(keep.projections(), List.of(attribute("f.1.*.f.2")));

        // enrich
        assertEquals(
            new Enrich(
                EMPTY,
                relation("idx1"),
                null,
                new Literal(EMPTY, "idx2", KEYWORD),
                attribute("f.1.*"),
                null,
                Map.of(),
                List.of(new Alias(EMPTY, "f.2", attribute("f.3*")))
            ),
            statement(
                "from idx1 | ENRICH idx2 ON ?f1 WITH ?f2 = ?f3",
                new QueryParams(List.of(paramAsIdentifier("f1", "f.1.*"), paramAsIdentifier("f2", "f.2"), paramAsIdentifier("f3", "f.3*")))
            )
        );

        assertEquals(
            new Enrich(
                EMPTY,
                relation("idx1"),
                null,
                new Literal(EMPTY, "idx2", KEYWORD),
                attribute("f.1.*.f.2"),
                null,
                Map.of(),
                List.of(new Alias(EMPTY, "f.3*.f.4.*", attribute("f.5.f.6*")))
            ),
            statement(
                "from idx1 | ENRICH idx2 ON ?f1.?f2 WITH ?f3.?f4 = ?f5.?f6",
                new QueryParams(
                    List.of(
                        paramAsIdentifier("f1", "f.1.*"),
                        paramAsIdentifier("f2", "f.2"),
                        paramAsIdentifier("f3", "f.3*"),
                        paramAsIdentifier("f4", "f.4.*"),
                        paramAsIdentifier("f5", "f.5"),
                        paramAsIdentifier("f6", "f.6*")
                    )
                )
            )
        );
    }

    public void testParamForIdentifierPattern() {
        // name patterns can appear in keep and drop
        // all patterns
        LogicalPlan plan = statement(
            "from test | keep ?f1, ?f2 | drop ?f3, ?f4",
            new QueryParams(
                List.of(
                    paramAsPattern("f1", "f*1."),
                    paramAsPattern("f2", "f.2*"),
                    paramAsPattern("f3", "f3.*"),
                    paramAsPattern("f4", "f.4.*")
                )
            )
        );

        Drop drop = as(plan, Drop.class);
        List<? extends NamedExpression> removals = drop.removals();
        assertEquals(removals.size(), 2);
        UnresolvedNamePattern up = as(removals.get(0), UnresolvedNamePattern.class);
        assertEquals(up.name(), "f3.*");
        assertEquals(up.pattern(), "f3.*");
        up = as(removals.get(1), UnresolvedNamePattern.class);
        assertEquals(up.name(), "f.4.*");
        assertEquals(up.pattern(), "f.4.*");
        Keep keep = as(drop.child(), Keep.class);
        assertEquals(keep.projections().size(), 2);
        up = as(keep.projections().get(0), UnresolvedNamePattern.class);
        assertEquals(up.name(), "f*1.");
        assertEquals(up.pattern(), "f*1.");
        up = as(keep.projections().get(1), UnresolvedNamePattern.class);
        assertEquals(up.name(), "f.2*");
        assertEquals(up.pattern(), "f.2*");
        UnresolvedRelation ur = as(keep.child(), UnresolvedRelation.class);
        assertEquals(ur, relation("test"));

        plan = statement(
            "from test | keep ?f1.?f2 | drop ?f3.?f4",
            new QueryParams(
                List.of(
                    paramAsPattern("f1", "f*1."),
                    paramAsPattern("f2", "f.2*"),
                    paramAsPattern("f3", "f3.*"),
                    paramAsPattern("f4", "f.4.*")
                )
            )
        );

        drop = as(plan, Drop.class);
        removals = drop.removals();
        assertEquals(removals.size(), 1);
        up = as(removals.get(0), UnresolvedNamePattern.class);
        assertEquals(up.name(), "f3.*.f.4.*");
        assertEquals(up.pattern(), "f3.*.f.4.*");
        keep = as(drop.child(), Keep.class);
        assertEquals(keep.projections().size(), 1);
        up = as(keep.projections().get(0), UnresolvedNamePattern.class);
        assertEquals(up.name(), "f*1..f.2*");
        assertEquals(up.pattern(), "f*1..f.2*");
        ur = as(keep.child(), UnresolvedRelation.class);
        assertEquals(ur, relation("test"));

        // mixed names and patterns
        plan = statement(
            "from test | keep ?f1.?f2 | drop ?f3.?f4",
            new QueryParams(
                List.of(
                    paramAsPattern("f1", "f*1."),
                    paramAsPattern("f2", "`f.2*`*"),
                    paramAsPattern("f3", "f3.*"),
                    paramAsIdentifier("f4", "f.4.*")
                )
            )
        );

        drop = as(plan, Drop.class);
        removals = drop.removals();
        assertEquals(removals.size(), 1);
        up = as(removals.get(0), UnresolvedNamePattern.class);
        assertEquals("f3.*.f.4.*", up.name());
        assertEquals("f3.*.`f.4.*`", up.pattern());
        keep = as(drop.child(), Keep.class);
        assertEquals(keep.projections().size(), 1);
        up = as(keep.projections().get(0), UnresolvedNamePattern.class);
        assertEquals("f*1..f.2**", up.name());
        assertEquals("f*1..`f.2*`*", up.pattern());
        ur = as(keep.child(), UnresolvedRelation.class);
        assertEquals(ur, relation("test"));
    }

    public void testParamInInvalidPosition() {
        // param for pattern is not supported in eval/where/stats/sort/rename/dissect/grok/enrich/mvexpand
        // where/stats/sort/dissect/grok are covered in RestEsqlTestCase
        List<String> invalidParamPositions = List.of("eval ?f1 = 1", "stats x = ?f1(*)", "mv_expand ?f1", "rename ?f1 as ?f2");
        for (String invalidParamPosition : invalidParamPositions) {
            for (String pattern : List.of("f1*", "*", "`f1*`", "`*`")) {
                // pattern is not supported
                expectError(
                    "from test | " + invalidParamPosition,
                    List.of(paramAsPattern("f1", pattern), paramAsPattern("f2", "f*2")),
                    invalidParamPosition.contains("rename")
                        ? "Using wildcards [*] in RENAME is not allowed [?f1 as ?f2]"
                        : "Query parameter [?f1][" + pattern + "] declared as a pattern, cannot be used as an identifier"
                );
                // constant is not supported
                expectError(
                    "from test | " + invalidParamPosition,
                    List.of(paramAsConstant("f1", pattern), paramAsConstant("f2", "f*2")),
                    invalidParamPosition.contains("rename")
                        ? "Query parameter [?f2] with value [f*2] declared as a constant, cannot be used as an identifier or pattern"
                        : "Query parameter [?f1] with value [" + pattern + "] declared as a constant, cannot be used as an identifier"
                );
            }
            // nulls
            if (invalidParamPosition.contains("rename")) {
                // rename null as null is allowed, there is no ParsingException or VerificationException thrown
                // named parameter doesn't change this behavior, it will need to be revisited
                continue;
            }
            expectError(
                "from test | " + invalidParamPosition,
                List.of(paramAsConstant("f1", null), paramAsConstant("f2", null)),
                "Query parameter [?f1] is null or undefined"
            );
        }
        // enrich with wildcard as pattern or constant is not supported
        String enrich = "ENRICH idx2 ON ?f1 WITH ?f2 = ?f3";
        for (String pattern : List.of("f.1.*", "*")) {
            expectError(
                "from idx1 | " + enrich,
                List.of(paramAsPattern("f1", pattern), paramAsIdentifier("f2", "f.2"), paramAsIdentifier("f3", "f.3*")),
                "Using wildcards [*] in ENRICH WITH projections is not allowed, found [" + pattern + "]"
            );
            expectError(
                "from idx1 | " + enrich,
                List.of(paramAsConstant("f1", pattern), paramAsIdentifier("f2", "f.2"), paramAsIdentifier("f3", "f.3*")),
                "Query parameter [?f1] with value [" + pattern + "] declared as a constant, cannot be used as an identifier or pattern"
            );
        }
    }

    public void testMissingParam() {
        // cover all processing commands eval/where/stats/sort/rename/dissect/grok/enrich/mvexpand/keep/drop
        String error = "Unknown query parameter [f1], did you mean [f4]?";
        String errorMvExpandFunctionNameCommandOption = "Query parameter [?f1] is null or undefined, cannot be used as an identifier";
        List<String> missingParamGroupA = List.of(
            "eval x = ?f1",
            "where ?f1 == \"a\"",
            "stats x = count(?f1)",
            "sort ?f1",
            "rename ?f1 as ?f2",
            "dissect ?f1 \"%{bar}\"",
            "grok ?f1 \"%{WORD:foo}\"",
            "enrich idx2 ON ?f1 WITH ?f2 = ?f3",
            "keep ?f1",
            "drop ?f1"
        );
        List<String> missingParamGroupB = List.of("eval x = ?f1(f1)", "mv_expand ?f1");
        for (String missingParam : Stream.concat(missingParamGroupA.stream(), missingParamGroupB.stream()).toList()) {
            for (String identifierOrPattern : List.of("identifier", "identifierpattern")) {
                expectError(
                    "from test | " + missingParam,
                    List.of(identifierOrPattern.equals("identifier") ? paramAsIdentifier("f4", "f1*") : paramAsPattern("f4", "f1*")),
                    missingParamGroupB.contains(missingParam) ? errorMvExpandFunctionNameCommandOption : error
                );
            }
            if (EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled()) {
                expectError("from test | " + missingParam.replace("?", "??"), List.of(paramAsConstant("f4", "f1*")), error);
            }
        }
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
        if (Build.current().isSnapshot() == false && statement.startsWith("TS ")) {
            expectThrows(ParsingException.class, containsString("mismatched input 'TS' expecting {"), () -> statement(statement));
            return;
        }
        LogicalPlan from = statement(statement);
        assertThat(from, instanceOf(UnresolvedRelation.class));
        UnresolvedRelation table = (UnresolvedRelation) from;
        assertThat(table.indexPattern().indexPattern(), is(string));
    }

    private void assertStringAsLookupIndexPattern(String string, String statement) {
        if (Build.current().isSnapshot() == false) {
            expectThrows(
                ParsingException.class,
                containsString("line 1:14: LOOKUP_🐔 is in preview and only available in SNAPSHOT build"),
                () -> statement(statement)
            );
            return;
        }
        var plan = statement(statement);
        var lookup = as(plan, Lookup.class);
        var tableName = as(lookup.tableName(), Literal.class);
        assertThat(tableName.fold(FoldContext.small()), equalTo(string));
    }

    public void testIdPatternUnquoted() {
        var string = "regularString";
        assertThat(breakIntoFragments(string), contains(string));
    }

    public void testIdPatternQuoted() {
        var string = "`escaped string`";
        assertThat(breakIntoFragments(string), contains(string));
    }

    public void testIdPatternQuotedWithDoubleBackticks() {
        var string = "`escaped``string`";
        assertThat(breakIntoFragments(string), contains(string));
    }

    public void testIdPatternUnquotedAndQuoted() {
        var string = "this`is`a`mix`of`ids`";
        assertThat(breakIntoFragments(string), contains("this", "`is`", "a", "`mix`", "of", "`ids`"));
    }

    public void testIdPatternQuotedTraling() {
        var string = "`foo`*";
        assertThat(breakIntoFragments(string), contains("`foo`", "*"));
    }

    public void testIdPatternWithDoubleQuotedStrings() {
        var string = "`this``is`a`quoted `` string``with`backticks";
        assertThat(breakIntoFragments(string), contains("`this``is`", "a", "`quoted `` string``with`", "backticks"));
    }

    public void testSpaceNotAllowedInIdPattern() {
        expectError("ROW a = 1| RENAME a AS this is `not okay`", "mismatched input 'is' expecting {<EOF>, '|', ',', '.'}");
    }

    public void testSpaceNotAllowedInIdPatternKeep() {
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
        expectError("ROW 1::doesnotexist", "line 1:8: Unknown data type named [doesnotexist]");
        expectError("ROW \"1\"::doesnotexist", "line 1:10: Unknown data type named [doesnotexist]");
        expectError("ROW false::doesnotexist", "line 1:12: Unknown data type named [doesnotexist]");
        expectError("ROW abs(1)::doesnotexist", "line 1:13: Unknown data type named [doesnotexist]");
        expectError("ROW (1+2)::doesnotexist", "line 1:12: Unknown data type named [doesnotexist]");
    }

    public void testLookup() {
        String query = "ROW a = 1 | LOOKUP_🐔 t ON j";
        if (Build.current().isSnapshot() == false) {
            expectThrows(
                ParsingException.class,
                containsString("line 1:13: mismatched input 'LOOKUP_🐔' expecting {"),
                () -> statement(query)
            );
            return;
        }
        var plan = statement(query);
        var lookup = as(plan, Lookup.class);
        var tableName = as(lookup.tableName(), Literal.class);
        assertThat(tableName.fold(FoldContext.small()), equalTo("t"));
        assertThat(lookup.matchFields(), hasSize(1));
        var matchField = as(lookup.matchFields().get(0), UnresolvedAttribute.class);
        assertThat(matchField.name(), equalTo("j"));
    }

    public void testInlineConvertUnsupportedType() {
        expectError("ROW 3::BYTE", "line 1:5: Unsupported conversion to type [BYTE]");
    }

    public void testMetricsWithoutStats() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());

        assertStatement("TS foo", unresolvedTSRelation("foo"));
        assertStatement("TS foo,bar", unresolvedTSRelation("foo,bar"));
        assertStatement("TS foo*,bar", unresolvedTSRelation("foo*,bar"));
        assertStatement("TS foo-*,bar", unresolvedTSRelation("foo-*,bar"));
        assertStatement("TS foo-*,bar+*", unresolvedTSRelation("foo-*,bar+*"));
    }

    public void testMetricsIdentifiers() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());
        Map<String, String> patterns = Map.ofEntries(
            Map.entry("ts foo,test-*", "foo,test-*"),
            Map.entry("ts 123-test@foo_bar+baz1", "123-test@foo_bar+baz1"),
            Map.entry("ts foo,   test,xyz", "foo,test,xyz"),
            Map.entry("ts <logstash-{now/M{yyyy.MM}}>", "<logstash-{now/M{yyyy.MM}}>")
        );
        for (Map.Entry<String, String> e : patterns.entrySet()) {
            assertStatement(e.getKey(), unresolvedTSRelation(e.getValue()));
        }
    }

    public void testSimpleMetricsWithStats() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());
        assertStatement(
            "TS foo | STATS load=avg(cpu) BY ts",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo"),
                List.of(attribute("ts")),
                List.of(
                    new Alias(EMPTY, "load", new UnresolvedFunction(EMPTY, "avg", DEFAULT, List.of(attribute("cpu")))),
                    attribute("ts")
                ),
                null
            )
        );
        assertStatement(
            "TS foo,bar | STATS load=avg(cpu) BY ts",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo,bar"),
                List.of(attribute("ts")),
                List.of(
                    new Alias(EMPTY, "load", new UnresolvedFunction(EMPTY, "avg", DEFAULT, List.of(attribute("cpu")))),
                    attribute("ts")
                ),
                null
            )
        );
        assertStatement(
            "TS foo,bar | STATS load=avg(cpu),max(rate(requests)) BY ts",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo,bar"),
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
                ),
                null
            )
        );
        assertStatement(
            "TS foo* | STATS count(errors)",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                List.of(),
                List.of(new Alias(EMPTY, "count(errors)", new UnresolvedFunction(EMPTY, "count", DEFAULT, List.of(attribute("errors"))))),
                null
            )
        );
        assertStatement(
            "TS foo* | STATS a(b)",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                List.of(),
                List.of(new Alias(EMPTY, "a(b)", new UnresolvedFunction(EMPTY, "a", DEFAULT, List.of(attribute("b"))))),
                null
            )
        );
        assertStatement(
            "TS foo* | STATS a(b)",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                List.of(),
                List.of(new Alias(EMPTY, "a(b)", new UnresolvedFunction(EMPTY, "a", DEFAULT, List.of(attribute("b"))))),
                null
            )
        );
        assertStatement(
            "TS foo* | STATS a1(b2)",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                List.of(),
                List.of(new Alias(EMPTY, "a1(b2)", new UnresolvedFunction(EMPTY, "a1", DEFAULT, List.of(attribute("b2"))))),
                null
            )
        );
        assertStatement(
            "TS foo*,bar* | STATS b = min(a) by c, d.e",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo*,bar*"),
                List.of(attribute("c"), attribute("d.e")),
                List.of(
                    new Alias(EMPTY, "b", new UnresolvedFunction(EMPTY, "min", DEFAULT, List.of(attribute("a")))),
                    attribute("c"),
                    attribute("d.e")
                ),
                null
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

    public void testInvalidRemoteClusterPattern() {
        expectError("from \"rem:ote\":index", "mismatched input ':' expecting {<EOF>, '|', ',', 'metadata'}");
    }

    private LogicalPlan unresolvedRelation(String index) {
        return new UnresolvedRelation(EMPTY, new IndexPattern(EMPTY, index), false, List.of(), IndexMode.STANDARD, null, "FROM");
    }

    private LogicalPlan unresolvedTSRelation(String index) {
        return new UnresolvedRelation(EMPTY, new IndexPattern(EMPTY, index), false, List.of(), IndexMode.TIME_SERIES, null, "TS");
    }

    public void testMetricWithGroupKeyAsAgg() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());
        var queries = List.of("TS foo | STATS a BY a");
        for (String query : queries) {
            expectVerificationError(query, "grouping key [a] already specified in the STATS BY clause");
        }
    }

    public void testMatchOperatorConstantQueryString() {
        var plan = statement("FROM test | WHERE field:\"value\"");
        var filter = as(plan, Filter.class);
        var match = (MatchOperator) filter.condition();
        var matchField = (UnresolvedAttribute) match.field();
        assertThat(matchField.name(), equalTo("field"));
        assertThat(match.query().fold(FoldContext.small()), equalTo("value"));
    }

    public void testInvalidMatchOperator() {
        expectError("from test | WHERE field:", "line 1:25: mismatched input '<EOF>' expecting {QUOTED_STRING, ");
        expectError(
            "from test | WHERE field:CONCAT(\"hello\", \"world\")",
            "line 1:25: mismatched input 'CONCAT' expecting {QUOTED_STRING, INTEGER_LITERAL, DECIMAL_LITERAL, "
        );
        expectError(
            "from test | WHERE field:(true OR false)",
            "line 1:25: extraneous input '(' expecting {QUOTED_STRING, INTEGER_LITERAL, DECIMAL_LITERAL, "
        );
        expectError(
            "from test | WHERE field:another_field_or_value",
            "line 1:25: mismatched input 'another_field_or_value' expecting {QUOTED_STRING, INTEGER_LITERAL, DECIMAL_LITERAL, "
        );
        expectError("from test | WHERE field:2+3", "line 1:26: mismatched input '+'");
        expectError(
            "from test | WHERE \"field\":\"value\"",
            "line 1:26: mismatched input ':' expecting {<EOF>, '|', 'and', '::', 'or', '+', '-', '*', '/', '%'}"
        );
        expectError(
            "from test | WHERE CONCAT(\"field\", 1):\"value\"",
            "line 1:37: mismatched input ':' expecting {<EOF>, '|', 'and', '::', 'or', '+', '-', '*', '/', '%'}"
        );
    }

    public void testMatchFunctionFieldCasting() {
        var plan = statement("FROM test | WHERE match(field::int, \"value\")");
        var filter = as(plan, Filter.class);
        var function = (UnresolvedFunction) filter.condition();
        var toInteger = (ToInteger) function.children().get(0);
        var matchField = (UnresolvedAttribute) toInteger.field();
        assertThat(matchField.name(), equalTo("field"));
        assertThat(function.children().get(1).fold(FoldContext.small()), equalTo("value"));
    }

    public void testMatchOperatorFieldCasting() {
        var plan = statement("FROM test | WHERE field::int : \"value\"");
        var filter = as(plan, Filter.class);
        var match = (MatchOperator) filter.condition();
        var toInteger = (ToInteger) match.field();
        var matchField = (UnresolvedAttribute) toInteger.field();
        assertThat(matchField.name(), equalTo("field"));
        assertThat(match.query().fold(FoldContext.small()), equalTo("value"));
    }

    public void testFailingMetadataWithSquareBrackets() {
        expectError(
            "FROM test [METADATA _index] | STATS count(*)",
            "line 1:11: mismatched input '[' expecting {<EOF>, '|', ',', 'metadata'}"
        );
    }

    public void testNamedFunctionArgumentInMap() {
        // functions can be scalar, grouping and aggregation
        // functions can be in eval/where/stats/sort/dissect/grok commands, commands in snapshot are not covered
        // positive
        // In eval and where clause as function arguments
        LinkedHashMap<String, Object> expectedMap1 = new LinkedHashMap<>(4);
        expectedMap1.put("option1", "string");
        expectedMap1.put("option2", 1);
        expectedMap1.put("option3", List.of(2.0, 3.0, 4.0));
        expectedMap1.put("option4", List.of(true, false));
        LinkedHashMap<String, Object> expectedMap2 = new LinkedHashMap<>(4);
        expectedMap2.put("option1", List.of("string1", "string2"));
        expectedMap2.put("option2", List.of(1, 2, 3));
        expectedMap2.put("option3", 2.0);
        expectedMap2.put("option4", true);
        LinkedHashMap<String, Object> expectedMap3 = new LinkedHashMap<>(4);
        expectedMap3.put("option1", "string");
        expectedMap3.put("option2", 2.0);
        expectedMap3.put("option3", List.of(1, 2, 3));
        expectedMap3.put("option4", List.of(true, false));

        assertEquals(
            new Filter(
                EMPTY,
                new Eval(
                    EMPTY,
                    relation("test"),
                    List.of(
                        new Alias(
                            EMPTY,
                            "x",
                            function(
                                "fn1",
                                List.of(attribute("f1"), new Literal(EMPTY, "testString", KEYWORD), mapExpression(expectedMap1))
                            )
                        )
                    )
                ),
                new Equals(
                    EMPTY,
                    attribute("y"),
                    function("fn2", List.of(new Literal(EMPTY, "testString", KEYWORD), mapExpression(expectedMap2)))
                )
            ),
            statement("""
                from test
                | eval x = fn1(f1, "testString", {"option1":"string","option2":1,"option3":[2.0,3.0,4.0],"option4":[true,false]})
                | where y == fn2("testString", {"option1":["string1","string2"],"option2":[1,2,3],"option3":2.0,"option4":true})
                """)
        );

        // In stats, by and sort as function arguments
        assertEquals(
            new OrderBy(
                EMPTY,
                new Aggregate(
                    EMPTY,
                    relation("test"),
                    List.of(
                        new Alias(
                            EMPTY,
                            "fn2(f3, {\"option1\":[\"string1\",\"string2\"],\"option2\":[1,2,3],\"option3\":2.0,\"option4\":true})",
                            function("fn2", List.of(attribute("f3"), mapExpression(expectedMap2)))
                        )
                    ),
                    List.of(
                        new Alias(EMPTY, "x", function("fn1", List.of(attribute("f1"), attribute("f2"), mapExpression(expectedMap1)))),
                        attribute("fn2(f3, {\"option1\":[\"string1\",\"string2\"],\"option2\":[1,2,3],\"option3\":2.0,\"option4\":true})")
                    )
                ),
                List.of(
                    new Order(
                        EMPTY,
                        function("fn3", List.of(attribute("f4"), mapExpression(expectedMap3))),
                        Order.OrderDirection.ASC,
                        Order.NullsPosition.LAST
                    )
                )
            ),
            statement("""
                from test
                | stats x = fn1(f1, f2, {"option1":"string","option2":1,"option3":[2.0,3.0,4.0],"option4":[true,false]})
                  by fn2(f3, {"option1":["string1","string2"],"option2":[1,2,3],"option3":2.0,"option4":true})
                | sort fn3(f4, {"option1":"string","option2":2.0,"option3":[1,2,3],"option4":[true,false]})
                """)
        );

        // In dissect and grok as function arguments
        LogicalPlan plan = statement("""
            from test
            | dissect fn1(f1, f2, {"option1":"string", "option2":1,"option3":[2.0,3.0,4.0],"option4":[true,false]}) "%{bar}"
            | grok fn2(f3, {"option1":["string1","string2"],"option2":[1,2,3],"option3":2.0,"option4":true}) "%{WORD:foo}"
            """);
        Grok grok = as(plan, Grok.class);
        assertEquals(function("fn2", List.of(attribute("f3"), mapExpression(expectedMap2))), grok.input());
        assertEquals("%{WORD:foo}", grok.parser().pattern());
        assertEquals(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
        Dissect dissect = as(grok.child(), Dissect.class);
        assertEquals(function("fn1", List.of(attribute("f1"), attribute("f2"), mapExpression(expectedMap1))), dissect.input());
        assertEquals("%{bar}", dissect.parser().pattern());
        assertEquals("", dissect.parser().appendSeparator());
        assertEquals(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
        UnresolvedRelation ur = as(dissect.child(), UnresolvedRelation.class);
        assertEquals(ur, relation("test"));
    }

    public void testNamedFunctionArgumentInMapWithNamedParameters() {
        // map entry values provided in named parameter, arrays are not supported by named parameters yet
        LinkedHashMap<String, Object> expectedMap1 = new LinkedHashMap<>(4);
        expectedMap1.put("option1", "string");
        expectedMap1.put("option2", 1);
        expectedMap1.put("option3", List.of(2.0, 3.0, 4.0));
        expectedMap1.put("option4", List.of(true, false));
        LinkedHashMap<String, Object> expectedMap2 = new LinkedHashMap<>(4);
        expectedMap2.put("option1", List.of("string1", "string2"));
        expectedMap2.put("option2", List.of(1, 2, 3));
        expectedMap2.put("option3", 2.0);
        expectedMap2.put("option4", true);
        LinkedHashMap<String, Object> expectedMap3 = new LinkedHashMap<>(4);
        expectedMap3.put("option1", "string");
        expectedMap3.put("option2", 2.0);
        expectedMap3.put("option3", List.of(1, 2, 3));
        expectedMap3.put("option4", List.of(true, false));
        assertEquals(
            new Filter(
                EMPTY,
                new Eval(
                    EMPTY,
                    relation("test"),
                    List.of(
                        new Alias(
                            EMPTY,
                            "x",
                            function(
                                "fn1",
                                List.of(attribute("f1"), new Literal(EMPTY, "testString", KEYWORD), mapExpression(expectedMap1))
                            )
                        )
                    )
                ),
                new Equals(
                    EMPTY,
                    attribute("y"),
                    function("fn2", List.of(new Literal(EMPTY, "testString", KEYWORD), mapExpression(expectedMap2)))
                )
            ),
            statement(
                """
                    from test
                    | eval x = ?fn1(?n1, ?n2, {"option1":?n3,"option2":?n4,"option3":[2.0,3.0,4.0],"option4":[true,false]})
                    | where y == ?fn2(?n2, {"option1":["string1","string2"],"option2":[1,2,3],"option3":?n5,"option4":?n6})
                    """,
                new QueryParams(
                    List.of(
                        paramAsIdentifier("fn1", "fn1"),
                        paramAsIdentifier("fn2", "fn2"),
                        paramAsIdentifier("n1", "f1"),
                        paramAsConstant("n2", "testString"),
                        paramAsConstant("n3", "string"),
                        paramAsConstant("n4", 1),
                        paramAsConstant("n5", 2.0),
                        paramAsConstant("n6", true)
                    )
                )
            )
        );

        assertEquals(
            new OrderBy(
                EMPTY,
                new Aggregate(
                    EMPTY,
                    relation("test"),
                    List.of(
                        new Alias(
                            EMPTY,
                            "?fn2(?n7, {\"option1\":[\"string1\",\"string2\"],\"option2\":[1,2,3],\"option3\":?n5,\"option4\":?n6})",
                            function("fn2", List.of(attribute("f3"), mapExpression(expectedMap2)))
                        )
                    ),
                    List.of(
                        new Alias(EMPTY, "x", function("fn1", List.of(attribute("f1"), attribute("f2"), mapExpression(expectedMap1)))),
                        attribute("?fn2(?n7, {\"option1\":[\"string1\",\"string2\"],\"option2\":[1,2,3],\"option3\":?n5,\"option4\":?n6})")
                    )
                ),
                List.of(
                    new Order(
                        EMPTY,
                        function("fn3", List.of(attribute("f4"), mapExpression(expectedMap3))),
                        Order.OrderDirection.ASC,
                        Order.NullsPosition.LAST
                    )
                )
            ),
            statement(
                """
                    from test
                    | stats x = ?fn1(?n1, ?n2, {"option1":?n3,"option2":?n4,"option3":[2.0,3.0,4.0],"option4":[true,false]})
                      by ?fn2(?n7, {"option1":["string1","string2"],"option2":[1,2,3],"option3":?n5,"option4":?n6})
                    | sort ?fn3(?n8, {"option1":?n3,"option2":?n5,"option3":[1,2,3],"option4":[true,false]})
                    """,
                new QueryParams(
                    List.of(
                        paramAsIdentifier("fn1", "fn1"),
                        paramAsIdentifier("fn2", "fn2"),
                        paramAsIdentifier("fn3", "fn3"),
                        paramAsIdentifier("n1", "f1"),
                        paramAsIdentifier("n2", "f2"),
                        paramAsConstant("n3", "string"),
                        paramAsConstant("n4", 1),
                        paramAsConstant("n5", 2.0),
                        paramAsConstant("n6", true),
                        paramAsIdentifier("n7", "f3"),
                        paramAsIdentifier("n8", "f4")
                    )
                )
            )
        );

        LogicalPlan plan = statement(
            """
                from test
                | dissect ?fn1(?n1, ?n2, {"option1":?n3,"option2":?n4,"option3":[2.0,3.0,4.0],"option4":[true,false]}) "%{bar}"
                | grok ?fn2(?n7, {"option1":["string1","string2"],"option2":[1,2,3],"option3":?n5,"option4":?n6}) "%{WORD:foo}"
                """,
            new QueryParams(
                List.of(
                    paramAsIdentifier("fn1", "fn1"),
                    paramAsIdentifier("fn2", "fn2"),
                    paramAsIdentifier("n1", "f1"),
                    paramAsIdentifier("n2", "f2"),
                    paramAsConstant("n3", "string"),
                    paramAsConstant("n4", 1),
                    paramAsConstant("n5", 2.0),
                    paramAsConstant("n6", true),
                    paramAsIdentifier("n7", "f3")
                )
            )
        );
        Grok grok = as(plan, Grok.class);
        assertEquals(function("fn2", List.of(attribute("f3"), mapExpression(expectedMap2))), grok.input());
        assertEquals("%{WORD:foo}", grok.parser().pattern());
        assertEquals(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
        Dissect dissect = as(grok.child(), Dissect.class);
        assertEquals(function("fn1", List.of(attribute("f1"), attribute("f2"), mapExpression(expectedMap1))), dissect.input());
        assertEquals("%{bar}", dissect.parser().pattern());
        assertEquals("", dissect.parser().appendSeparator());
        assertEquals(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
        UnresolvedRelation ur = as(dissect.child(), UnresolvedRelation.class);
        assertEquals(ur, relation("test"));
    }

    public void testNamedFunctionArgumentWithCaseSensitiveKeys() {
        LinkedHashMap<String, Object> expectedMap1 = new LinkedHashMap<>(3);
        expectedMap1.put("option", "string");
        expectedMap1.put("Option", 1);
        expectedMap1.put("oPtion", List.of(2.0, 3.0, 4.0));
        LinkedHashMap<String, Object> expectedMap2 = new LinkedHashMap<>(3);
        expectedMap2.put("option", List.of("string1", "string2"));
        expectedMap2.put("Option", List.of(1, 2, 3));
        expectedMap2.put("oPtion", 2.0);

        assertEquals(
            new Filter(
                EMPTY,
                new Eval(
                    EMPTY,
                    relation("test"),
                    List.of(
                        new Alias(
                            EMPTY,
                            "x",
                            function(
                                "fn1",
                                List.of(attribute("f1"), new Literal(EMPTY, "testString", KEYWORD), mapExpression(expectedMap1))
                            )
                        )
                    )
                ),
                new Equals(
                    EMPTY,
                    attribute("y"),
                    function("fn2", List.of(new Literal(EMPTY, "testString", KEYWORD), mapExpression(expectedMap2)))
                )
            ),
            statement("""
                from test
                | eval x = fn1(f1, "testString", {"option":"string","Option":1,"oPtion":[2.0,3.0,4.0]})
                | where y == fn2("testString", {"option":["string1","string2"],"Option":[1,2,3],"oPtion":2.0})
                """)
        );
    }

    public void testMultipleNamedFunctionArgumentsNotAllowed() {
        Map<String, String> commands = Map.ofEntries(
            Map.entry("eval x = {}", "41"),
            Map.entry("where {}", "38"),
            Map.entry("stats {}", "38"),
            Map.entry("stats agg() by {}", "47"),
            Map.entry("sort {}", "37"),
            Map.entry("dissect {} \"%{bar}\"", "40"),
            Map.entry("grok {} \"%{WORD:foo}\"", "37")
        );

        for (Map.Entry<String, String> command : commands.entrySet()) {
            String cmd = command.getKey();
            String error = command.getValue();
            String errorMessage = cmd.startsWith("dissect") || cmd.startsWith("grok")
                ? "mismatched input ',' expecting ')'"
                : "no viable alternative at input 'fn(f1,";
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {\"option\":1}, {\"option\":2})"),
                LoggerMessageFormat.format(null, "line 1:{}: {}", error, errorMessage)
            );
        }
    }

    public void testNamedFunctionArgumentNotInMap() {
        Map<String, String> commands = Map.ofEntries(
            Map.entry("eval x = {}", "38"),
            Map.entry("where {}", "35"),
            Map.entry("stats {}", "35"),
            Map.entry("stats agg() by {}", "44"),
            Map.entry("sort {}", "34"),
            Map.entry("dissect {} \"%{bar}\"", "37"),
            Map.entry("grok {} \"%{WORD:foo}\"", "34")
        );

        for (Map.Entry<String, String> command : commands.entrySet()) {
            String cmd = command.getKey();
            String error = command.getValue();
            String errorMessage = cmd.startsWith("dissect") || cmd.startsWith("grok")
                ? "extraneous input ':' expecting {',', ')'}"
                : "no viable alternative at input 'fn(f1, \"option1\":'";
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, \"option1\":\"string\")"),
                LoggerMessageFormat.format(null, "line 1:{}: {}", error, errorMessage)
            );
        }
    }

    public void testNamedFunctionArgumentNotConstant() {
        Map<String, String[]> commands = Map.ofEntries(
            Map.entry("eval x = {}", new String[] { "31", "35" }),
            Map.entry("where {}", new String[] { "28", "32" }),
            Map.entry("stats {}", new String[] { "28", "32" }),
            Map.entry("stats agg() by {}", new String[] { "37", "41" }),
            Map.entry("sort {}", new String[] { "27", "31" }),
            Map.entry("dissect {} \"%{bar}\"", new String[] { "30", "34" }),
            Map.entry("grok {} \"%{WORD:foo}\"", new String[] { "27", "31" })
        );

        for (Map.Entry<String, String[]> command : commands.entrySet()) {
            String cmd = command.getKey();
            String error1 = command.getValue()[0];
            String error2 = command.getValue()[1];
            String errorMessage1 = cmd.startsWith("dissect") || cmd.startsWith("grok")
                ? "mismatched input '1' expecting QUOTED_STRING"
                : "no viable alternative at input 'fn(f1, { 1'";
            String errorMessage2 = cmd.startsWith("dissect") || cmd.startsWith("grok")
                ? "mismatched input 'string' expecting {QUOTED_STRING"
                : "no viable alternative at input 'fn(f1, {";
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, { 1:\"string\" })"),
                LoggerMessageFormat.format(null, "line 1:{}: {}", error1, errorMessage1)
            );
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, { \"1\":string })"),
                LoggerMessageFormat.format(null, "line 1:{}: {}", error2, errorMessage2)
            );
        }
    }

    public void testNamedFunctionArgumentEmptyMap() {
        Map<String, String> commands = Map.ofEntries(
            Map.entry("eval x = {}", "30"),
            Map.entry("where {}", "27"),
            Map.entry("stats {}", "27"),
            Map.entry("stats agg() by {}", "36"),
            Map.entry("sort {}", "26"),
            Map.entry("dissect {} \"%{bar}\"", "29"),
            Map.entry("grok {} \"%{WORD:foo}\"", "26")
        );

        for (Map.Entry<String, String> command : commands.entrySet()) {
            String cmd = command.getKey();
            String error = command.getValue();
            String errorMessage = cmd.startsWith("dissect") || cmd.startsWith("grok")
                ? "mismatched input '}' expecting QUOTED_STRING"
                : "no viable alternative at input 'fn(f1, {}'";
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {}})"),
                LoggerMessageFormat.format(null, "line 1:{}: {}", error, errorMessage)
            );
        }
    }

    public void testNamedFunctionArgumentMapWithNULL() {
        Map<String, String> commands = Map.ofEntries(
            Map.entry("eval x = {}", "29"),
            Map.entry("where {}", "26"),
            Map.entry("stats {}", "26"),
            Map.entry("stats agg() by {}", "35"),
            Map.entry("sort {}", "25"),
            Map.entry("dissect {} \"%{bar}\"", "28"),
            Map.entry("grok {} \"%{WORD:foo}\"", "25")
        );

        for (Map.Entry<String, String> command : commands.entrySet()) {
            String cmd = command.getKey();
            String error = command.getValue();
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {\"option\":null})"),
                LoggerMessageFormat.format(
                    null,
                    "line 1:{}: {}",
                    error,
                    "Invalid named function argument [\"option\":null], NULL is not supported"
                )
            );
        }
    }

    public void testNamedFunctionArgumentMapWithEmptyKey() {
        Map<String, String> commands = Map.ofEntries(
            Map.entry("eval x = {}", "29"),
            Map.entry("where {}", "26"),
            Map.entry("stats {}", "26"),
            Map.entry("stats agg() by {}", "35"),
            Map.entry("sort {}", "25"),
            Map.entry("dissect {} \"%{bar}\"", "28"),
            Map.entry("grok {} \"%{WORD:foo}\"", "25")
        );

        for (Map.Entry<String, String> command : commands.entrySet()) {
            String cmd = command.getKey();
            String error = command.getValue();
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {\"\":1})"),
                LoggerMessageFormat.format(
                    null,
                    "line 1:{}: {}",
                    error,
                    "Invalid named function argument [\"\":1], empty key is not supported"
                )
            );
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {\"  \":1})"),
                LoggerMessageFormat.format(
                    null,
                    "line 1:{}: {}",
                    error,
                    "Invalid named function argument [\"  \":1], empty key is not supported"
                )
            );
        }
    }

    public void testNamedFunctionArgumentMapWithDuplicatedKey() {
        Map<String, String> commands = Map.ofEntries(
            Map.entry("eval x = {}", "29"),
            Map.entry("where {}", "26"),
            Map.entry("stats {}", "26"),
            Map.entry("stats agg() by {}", "35"),
            Map.entry("sort {}", "25"),
            Map.entry("dissect {} \"%{bar}\"", "28"),
            Map.entry("grok {} \"%{WORD:foo}\"", "25")
        );

        for (Map.Entry<String, String> command : commands.entrySet()) {
            String cmd = command.getKey();
            String error = command.getValue();
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {\"dup\":1,\"dup\":2})"),
                LoggerMessageFormat.format(
                    null,
                    "line 1:{}: {}",
                    error,
                    "Duplicated function arguments with the same name [dup] is not supported"
                )
            );
        }
    }

    public void testNamedFunctionArgumentInInvalidPositions() {
        // negative, named arguments are not supported outside of a functionExpression where booleanExpression or indexPattern is supported
        String map = "{\"option1\":\"string\", \"option2\":1}";

        Map<String, String> commands = Map.ofEntries(
            Map.entry("from {}", "line 1:7: mismatched input '\"option1\"' expecting {<EOF>, '|', ',', 'metadata'}"),
            Map.entry("row x = {}", "line 1:9: extraneous input '{' expecting {QUOTED_STRING, INTEGER_LITERAL"),
            Map.entry("eval x = {}", "line 1:22: extraneous input '{' expecting {QUOTED_STRING, INTEGER_LITERAL"),
            Map.entry("where x > {}", "line 1:23: no viable alternative at input 'x > {'"),
            Map.entry("stats agg() by {}", "line 1:28: extraneous input '{' expecting {QUOTED_STRING, INTEGER_LITERAL"),
            Map.entry("sort {}", "line 1:18: extraneous input '{' expecting {QUOTED_STRING, INTEGER_LITERAL"),
            Map.entry("keep {}", "line 1:18: token recognition error at: '{'"),
            Map.entry("drop {}", "line 1:18: token recognition error at: '{'"),
            Map.entry("rename a as {}", "line 1:25: token recognition error at: '{'"),
            Map.entry("mv_expand {}", "line 1:23: token recognition error at: '{'"),
            Map.entry("limit {}", "line 1:19: extraneous input '{' expecting {QUOTED_STRING"),
            Map.entry("enrich idx2 on f1 with f2 = {}", "line 1:41: token recognition error at: '{'"),
            Map.entry("dissect {} \"%{bar}\"", "line 1:21: extraneous input '{' expecting {QUOTED_STRING, INTEGER_LITERAL"),
            Map.entry("grok {} \"%{WORD:foo}\"", "line 1:18: extraneous input '{' expecting {QUOTED_STRING, INTEGER_LITERAL")
        );

        for (Map.Entry<String, String> command : commands.entrySet()) {
            String cmd = command.getKey();
            String errorMessage = command.getValue();
            String from = cmd.startsWith("row") || cmd.startsWith("from") ? "" : "from test | ";
            expectError(LoggerMessageFormat.format(null, from + cmd, map), errorMessage);
        }
    }

    public void testNamedFunctionArgumentWithUnsupportedNamedParameterTypes() {
        Map<String, String> commands = Map.ofEntries(
            Map.entry("eval x = {}", "29"),
            Map.entry("where {}", "26"),
            Map.entry("stats {}", "26"),
            Map.entry("stats agg() by {}", "35"),
            Map.entry("sort {}", "25"),
            Map.entry("dissect {} \"%{bar}\"", "28"),
            Map.entry("grok {} \"%{WORD:foo}\"", "25")
        );

        for (Map.Entry<String, String> command : commands.entrySet()) {
            String cmd = command.getKey();
            String error = command.getValue();
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {\"option1\":?n1})"),
                List.of(paramAsIdentifier("n1", "v1")),
                LoggerMessageFormat.format(
                    null,
                    "line 1:{}: {}",
                    error,
                    "Invalid named function argument [\"option1\":?n1], only constant value is supported"
                )
            );
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {\"option1\":?n1})"),
                List.of(paramAsPattern("n1", "v1")),
                LoggerMessageFormat.format(
                    null,
                    "line 1:{}: {}",
                    error,
                    "Invalid named function argument [\"option1\":?n1], only constant value is supported"
                )
            );
        }
    }

    public void testValidFromPattern() {
        var basePattern = randomIndexPatterns();

        var plan = statement("FROM " + basePattern);

        assertThat(as(plan, UnresolvedRelation.class).indexPattern().indexPattern(), equalTo(unquoteIndexPattern(basePattern)));
    }

    public void testValidJoinPattern() {
        assumeTrue("LOOKUP JOIN requires corresponding capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());

        var basePattern = randomIndexPatterns(without(CROSS_CLUSTER));
        var joinPattern = randomIndexPattern(without(WILDCARD_PATTERN), without(CROSS_CLUSTER), without(INDEX_SELECTOR));
        var onField = randomIdentifier();

        var plan = statement("FROM " + basePattern + " | LOOKUP JOIN " + joinPattern + " ON " + onField);

        var join = as(plan, LookupJoin.class);
        assertThat(as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo(unquoteIndexPattern(basePattern)));
        assertThat(as(join.right(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo(unquoteIndexPattern(joinPattern)));

        var joinType = as(join.config().type(), JoinTypes.UsingJoinType.class);
        assertThat(joinType.columns(), hasSize(1));
        assertThat(as(joinType.columns().getFirst(), UnresolvedAttribute.class).name(), equalTo(onField));
        assertThat(joinType.coreJoin().joinName(), equalTo("LEFT OUTER"));
    }

    public void testInvalidJoinPatterns() {
        assumeTrue("LOOKUP JOIN requires corresponding capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());

        {
            // wildcard
            var joinPattern = randomIndexPattern(WILDCARD_PATTERN, without(CROSS_CLUSTER), without(INDEX_SELECTOR));
            expectError(
                "FROM " + randomIndexPatterns() + " | LOOKUP JOIN " + joinPattern + " ON " + randomIdentifier(),
                "invalid index pattern [" + unquoteIndexPattern(joinPattern) + "], * is not allowed in LOOKUP JOIN"
            );
        }
        {
            // remote cluster on the right
            var fromPatterns = randomIndexPatterns(without(CROSS_CLUSTER));
            var joinPattern = randomIndexPattern(CROSS_CLUSTER, without(WILDCARD_PATTERN), without(INDEX_SELECTOR));
            expectError(
                "FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + randomIdentifier(),
                "invalid index pattern [" + unquoteIndexPattern(joinPattern) + "], remote clusters are not supported in LOOKUP JOIN"
            );
        }
        {
            // remote cluster on the left
            var fromPatterns = randomIndexPatterns(CROSS_CLUSTER);
            var joinPattern = randomIndexPattern(without(CROSS_CLUSTER), without(WILDCARD_PATTERN), without(INDEX_SELECTOR));
            expectError(
                "FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + randomIdentifier(),
                "invalid index pattern [" + unquoteIndexPattern(fromPatterns) + "], remote clusters are not supported in LOOKUP JOIN"
            );
        }

        if (EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled()) {
            {
                // Selectors are not supported on the left of the join query if used with cluster ids.
                // Unquoted case: The language specification does not allow mixing `:` and `::` characters in an index expression
                var fromPatterns = randomIndexPatterns(CROSS_CLUSTER, without(DATE_MATH));
                // We do different validation based on the quotation of the pattern
                // Autogenerated patterns will not mix cluster ids with selectors. Unquote it to ensure stable tests
                fromPatterns = unquoteIndexPattern(fromPatterns) + "::data";
                var joinPattern = randomIndexPattern(without(CROSS_CLUSTER), without(WILDCARD_PATTERN), without(INDEX_SELECTOR));
                expectError(
                    "FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + randomIdentifier(),
                    "mismatched input '::' expecting {"
                );
            }
            {
                // Selectors are not supported on the left of the join query if used with cluster ids.
                // Quoted case: The language specification allows mixing `:` and `::` characters in a quoted expression, but this usage
                // must cause a validation exception in the non-generated code.
                var fromPatterns = randomIndexPatterns(CROSS_CLUSTER, without(INDEX_SELECTOR));
                // We do different validation based on the quotation of the pattern
                // Autogenerated patterns will not mix cluster ids with selectors. Unquote, modify, and requote it to ensure stable tests
                fromPatterns = "\"" + unquoteIndexPattern(fromPatterns) + "::data\"";
                var joinPattern = randomIndexPattern(without(CROSS_CLUSTER), without(WILDCARD_PATTERN), without(INDEX_SELECTOR));
                expectError(
                    "FROM " + fromPatterns + " | LOOKUP JOIN " + joinPattern + " ON " + randomIdentifier(),
                    "Selectors are not yet supported on remote cluster patterns"
                );
            }
            {
                // Selectors are not yet supported in join patterns on the right.
                // Unquoted case: The language specification does not allow mixing `:` and `::` characters in an index expression
                var joinPattern = randomIndexPattern(without(CROSS_CLUSTER), without(WILDCARD_PATTERN), without(DATE_MATH), INDEX_SELECTOR);
                // We do different validation based on the quotation of the pattern, so forcefully unquote the expression instead of leaving
                // it to chance.
                joinPattern = unquoteIndexPattern(joinPattern);
                expectError(
                    "FROM " + randomIndexPatterns(without(CROSS_CLUSTER)) + " | LOOKUP JOIN " + joinPattern + " ON " + randomIdentifier(),
                    "extraneous input ':' expecting UNQUOTED_SOURCE"
                );
            }
            {
                // Selectors are not yet supported in join patterns on the right.
                // Quoted case: The language specification allows `::` characters in a quoted expression, but this usage
                // must cause a validation exception in the non-generated code.
                var joinPattern = randomIndexPattern(without(CROSS_CLUSTER), without(WILDCARD_PATTERN), without(DATE_MATH), INDEX_SELECTOR);
                // We do different validation based on the quotation of the pattern, so forcefully quote the expression instead of leaving
                // it to chance.
                joinPattern = "\"" + unquoteIndexPattern(joinPattern) + "\"";
                expectError(
                    "FROM " + randomIndexPatterns(without(CROSS_CLUSTER)) + " | LOOKUP JOIN " + joinPattern + " ON " + randomIdentifier(),
                    "invalid index pattern ["
                        + unquoteIndexPattern(joinPattern)
                        + "], index pattern selectors are not supported in LOOKUP JOIN"
                );
            }
        }
    }

    public void testInvalidInsistAsterisk() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());
        expectError("FROM text | EVAL x = 4 | INSIST_🐔 *", "INSIST doesn't support wildcards, found [*]");
        expectError("FROM text | EVAL x = 4 | INSIST_🐔 foo*", "INSIST doesn't support wildcards, found [foo*]");
    }

    public void testValidFork() {
        assumeTrue("FORK requires corresponding capability", EsqlCapabilities.Cap.FORK.isEnabled());

        var plan = statement("""
            FROM foo*
            | FORK ( WHERE a:"baz" | LIMIT 11 )
                   ( WHERE b:"bar" | SORT b )
                   ( WHERE c:"bat" )
                   ( SORT c )
                   ( LIMIT 5 )
                   ( DISSECT a "%{d} %{e} %{f}" | STATS x = MIN(a), y = MAX(b) WHERE d > 1000 | EVAL xyz = "abc")
            """);
        var fork = as(plan, Fork.class);
        var subPlans = fork.children();

        // first subplan
        var eval = as(subPlans.get(0), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalTo(alias("_fork", literalString("fork1"))));
        var limit = as(eval.child(), Limit.class);
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(11));
        var filter = as(limit.child(), Filter.class);
        var match = (MatchOperator) filter.condition();
        var matchField = (UnresolvedAttribute) match.field();
        assertThat(matchField.name(), equalTo("a"));
        assertThat(match.query().fold(FoldContext.small()), equalTo("baz"));

        // second subplan
        eval = as(subPlans.get(1), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalTo(alias("_fork", literalString("fork2"))));
        var orderBy = as(eval.child(), OrderBy.class);
        assertThat(orderBy.order().size(), equalTo(1));
        Order order = orderBy.order().get(0);
        assertThat(order.child(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) order.child()).name(), equalTo("b"));
        filter = as(orderBy.child(), Filter.class);
        match = (MatchOperator) filter.condition();
        matchField = (UnresolvedAttribute) match.field();
        assertThat(matchField.name(), equalTo("b"));
        assertThat(match.query().fold(FoldContext.small()), equalTo("bar"));

        // third subplan
        eval = as(subPlans.get(2), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalTo(alias("_fork", literalString("fork3"))));
        filter = as(eval.child(), Filter.class);
        match = (MatchOperator) filter.condition();
        matchField = (UnresolvedAttribute) match.field();
        assertThat(matchField.name(), equalTo("c"));
        assertThat(match.query().fold(FoldContext.small()), equalTo("bat"));

        // fourth subplan
        eval = as(subPlans.get(3), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalTo(alias("_fork", literalString("fork4"))));
        orderBy = as(eval.child(), OrderBy.class);
        assertThat(orderBy.order().size(), equalTo(1));
        order = orderBy.order().get(0);
        assertThat(order.child(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) order.child()).name(), equalTo("c"));

        // fifth subplan
        eval = as(subPlans.get(4), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalTo(alias("_fork", literalString("fork5"))));
        limit = as(eval.child(), Limit.class);
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(5));

        // sixth subplan
        eval = as(subPlans.get(5), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalTo(alias("_fork", literalString("fork6"))));
        eval = as(eval.child(), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalTo(alias("xyz", literalString("abc"))));

        Aggregate aggregate = as(eval.child(), Aggregate.class);
        assertThat(aggregate.aggregates().size(), equalTo(2));
        var alias = as(aggregate.aggregates().get(0), Alias.class);
        assertThat(alias.name(), equalTo("x"));
        assertThat(as(alias.child(), UnresolvedFunction.class).name(), equalTo("MIN"));

        alias = as(aggregate.aggregates().get(1), Alias.class);
        assertThat(alias.name(), equalTo("y"));
        var filteredExp = as(alias.child(), FilteredExpression.class);
        assertThat(as(filteredExp.delegate(), UnresolvedFunction.class).name(), equalTo("MAX"));
        var greaterThan = as(filteredExp.filter(), GreaterThan.class);
        assertThat(as(greaterThan.left(), UnresolvedAttribute.class).name(), equalTo("d"));
        assertThat(as(greaterThan.right(), Literal.class).value(), equalTo(1000));

        var dissect = as(aggregate.child(), Dissect.class);
        assertThat(as(dissect.input(), UnresolvedAttribute.class).name(), equalTo("a"));
        assertThat(dissect.parser().pattern(), equalTo("%{d} %{e} %{f}"));
    }

    public void testInvalidFork() {
        assumeTrue("FORK requires corresponding capability", EsqlCapabilities.Cap.FORK.isEnabled());

        expectError("FROM foo* | FORK (WHERE a:\"baz\")", "line 1:13: Fork requires at least two branches");
        expectError("FROM foo* | FORK (LIMIT 10)", "line 1:13: Fork requires at least two branches");
        expectError("FROM foo* | FORK (SORT a)", "line 1:13: Fork requires at least two branches");
        expectError("FROM foo* | FORK (WHERE x>1 | LIMIT 5)", "line 1:13: Fork requires at least two branches");
        expectError("FROM foo* | WHERE x>1 | FORK (WHERE a:\"baz\")", "Fork requires at least two branches");

        expectError("FROM foo* | FORK ( FORK (WHERE x>1) (WHERE y>1)) (WHERE z>1)", "line 1:20: mismatched input 'FORK'");
        expectError("FROM foo* | FORK ( x+1 ) ( WHERE y>2 )", "line 1:20: mismatched input 'x+1'");
        expectError("FROM foo* | FORK ( LIMIT 10 ) ( y+2 )", "line 1:33: mismatched input 'y+2'");
    }

    public void testFieldNamesAsCommands() throws Exception {
        String[] keywords = new String[] {
            "dissect",
            "drop",
            "enrich",
            "eval",
            "explain",
            "from",
            "grok",
            "keep",
            "limit",
            "mv_expand",
            "rename",
            "sort",
            "stats" };
        for (String keyword : keywords) {
            var plan = statement("FROM test | STATS avg(" + keyword + ")");
            var aggregate = as(plan, Aggregate.class);
        }
    }

    // [ and ( are used to trigger a double mode causing their symbol name (instead of text) to be used in error reporting
    // this test checks that their are properly replaced in the error message
    public void testPreserveParanthesis() {
        // test for (
        expectError("row a = 1 not in", "line 1:17: mismatched input '<EOF>' expecting '('");
        expectError("row a = 1 | where a not in", "line 1:27: mismatched input '<EOF>' expecting '('");
        expectError("row a = 1 | where a not in (1", "line 1:30: mismatched input '<EOF>' expecting {',', ')'}");
        expectError("row a = 1 | where a not in [1", "line 1:28: missing '(' at '['");
        expectError("row a = 1 | where a not in 123", "line 1:28: missing '(' at '123'");
        // test for [
        expectError("explain", "line 1:8: mismatched input '<EOF>' expecting '['");
        expectError("explain ]", "line 1:9: token recognition error at: ']'");
        expectError("explain [row x = 1", "line 1:19: missing ']' at '<EOF>'");
    }

    public void testRerankDefaultInferenceId() {
        assumeTrue("RERANK requires corresponding capability", EsqlCapabilities.Cap.RERANK.isEnabled());

        var plan = processingCommand("RERANK \"query text\" ON title");
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.inferenceId(), equalTo(literalString(".rerank-v1-elasticsearch")));
        assertThat(rerank.queryText(), equalTo(literalString("query text")));
        assertThat(rerank.rerankFields(), equalTo(List.of(alias("title", attribute("title")))));
    }

    public void testRerankSingleField() {
        assumeTrue("RERANK requires corresponding capability", EsqlCapabilities.Cap.RERANK.isEnabled());

        var plan = processingCommand("RERANK \"query text\" ON title WITH inferenceID");
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.queryText(), equalTo(literalString("query text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(rerank.rerankFields(), equalTo(List.of(alias("title", attribute("title")))));
    }

    public void testRerankMultipleFields() {
        assumeTrue("RERANK requires corresponding capability", EsqlCapabilities.Cap.RERANK.isEnabled());

        var plan = processingCommand("RERANK \"query text\" ON title, description, authors_renamed=authors WITH inferenceID");
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.queryText(), equalTo(literalString("query text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(
            rerank.rerankFields(),
            equalTo(
                List.of(
                    alias("title", attribute("title")),
                    alias("description", attribute("description")),
                    alias("authors_renamed", attribute("authors"))
                )
            )
        );
    }

    public void testRerankComputedFields() {
        assumeTrue("RERANK requires corresponding capability", EsqlCapabilities.Cap.RERANK.isEnabled());

        var plan = processingCommand("RERANK \"query text\" ON title, short_description = SUBSTRING(description, 0, 100) WITH inferenceID");
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.queryText(), equalTo(literalString("query text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(
            rerank.rerankFields(),
            equalTo(
                List.of(
                    alias("title", attribute("title")),
                    alias("short_description", function("SUBSTRING", List.of(attribute("description"), integer(0), integer(100))))
                )
            )
        );
    }

    public void testRerankWithPositionalParameters() {
        assumeTrue("RERANK requires corresponding capability", EsqlCapabilities.Cap.RERANK.isEnabled());

        var queryParams = new QueryParams(List.of(paramAsConstant(null, "query text"), paramAsConstant(null, "reranker")));
        var rerank = as(parser.createStatement("row a = 1 | RERANK ? ON title WITH ?", queryParams), Rerank.class);

        assertThat(rerank.queryText(), equalTo(literalString("query text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("reranker")));
        assertThat(rerank.rerankFields(), equalTo(List.of(alias("title", attribute("title")))));
    }

    public void testRerankWithNamedParameters() {
        assumeTrue("RERANK requires corresponding capability", EsqlCapabilities.Cap.RERANK.isEnabled());

        var queryParams = new QueryParams(List.of(paramAsConstant("queryText", "query text"), paramAsConstant("inferenceId", "reranker")));
        var rerank = as(parser.createStatement("row a = 1 | RERANK ?queryText ON title WITH ?inferenceId", queryParams), Rerank.class);

        assertThat(rerank.queryText(), equalTo(literalString("query text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("reranker")));
        assertThat(rerank.rerankFields(), equalTo(List.of(alias("title", attribute("title")))));
    }

    public void testInvalidRerank() {
        assumeTrue("RERANK requires corresponding capability", EsqlCapabilities.Cap.RERANK.isEnabled());
        expectError("FROM foo* | RERANK ON title WITH inferenceId", "line 1:20: mismatched input 'ON' expecting {QUOTED_STRING");
        expectError("FROM foo* | RERANK \"query text\" WITH inferenceId", "line 1:33: mismatched input 'WITH' expecting 'on'");
    }

    public void testCompletionUsingFieldAsPrompt() {
        assumeTrue("COMPLETION requires corresponding capability", EsqlCapabilities.Cap.COMPLETION.isEnabled());

        var plan = as(processingCommand("COMPLETION prompt_field WITH inferenceID AS targetField"), Completion.class);

        assertThat(plan.prompt(), equalTo(attribute("prompt_field")));
        assertThat(plan.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(plan.targetField(), equalTo(attribute("targetField")));
    }

    public void testCompletionUsingFunctionAsPrompt() {
        assumeTrue("COMPLETION requires corresponding capability", EsqlCapabilities.Cap.COMPLETION.isEnabled());

        var plan = as(processingCommand("COMPLETION CONCAT(fieldA, fieldB) WITH inferenceID AS targetField"), Completion.class);

        assertThat(plan.prompt(), equalTo(function("CONCAT", List.of(attribute("fieldA"), attribute("fieldB")))));
        assertThat(plan.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(plan.targetField(), equalTo(attribute("targetField")));
    }

    public void testCompletionDefaultFieldName() {
        assumeTrue("COMPLETION requires corresponding capability", EsqlCapabilities.Cap.COMPLETION.isEnabled());

        var plan = as(processingCommand("COMPLETION prompt_field WITH inferenceID"), Completion.class);

        assertThat(plan.prompt(), equalTo(attribute("prompt_field")));
        assertThat(plan.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(plan.targetField(), equalTo(attribute("completion")));
    }

    public void testCompletionWithPositionalParameters() {
        assumeTrue("COMPLETION requires corresponding capability", EsqlCapabilities.Cap.COMPLETION.isEnabled());

        var queryParams = new QueryParams(List.of(paramAsConstant(null, "inferenceId")));
        var plan = as(parser.createStatement("row a = 1 | COMPLETION prompt_field WITH ?", queryParams), Completion.class);

        assertThat(plan.prompt(), equalTo(attribute("prompt_field")));
        assertThat(plan.inferenceId(), equalTo(literalString("inferenceId")));
        assertThat(plan.targetField(), equalTo(attribute("completion")));
    }

    public void testCompletionWithNamedParameters() {
        assumeTrue("COMPLETION requires corresponding capability", EsqlCapabilities.Cap.COMPLETION.isEnabled());

        var queryParams = new QueryParams(List.of(paramAsConstant("inferenceId", "myInference")));
        var plan = as(parser.createStatement("row a = 1 | COMPLETION prompt_field WITH ?inferenceId", queryParams), Completion.class);

        assertThat(plan.prompt(), equalTo(attribute("prompt_field")));
        assertThat(plan.inferenceId(), equalTo(literalString("myInference")));
        assertThat(plan.targetField(), equalTo(attribute("completion")));
    }

    public void testInvalidCompletion() {
        assumeTrue("COMPLETION requires corresponding capability", EsqlCapabilities.Cap.COMPLETION.isEnabled());

        expectError("FROM foo* | COMPLETION WITH inferenceId", "line 1:24: extraneous input 'WITH' expecting {");

        expectError("FROM foo* | COMPLETION prompt WITH", "line 1:35: mismatched input '<EOF>' expecting {");

        expectError("FROM foo* | COMPLETION prompt AS targetField", "line 1:31: mismatched input 'AS' expecting {");
    }

    public void testSample() {
        assumeTrue("SAMPLE requires corresponding capability", EsqlCapabilities.Cap.SAMPLE.isEnabled());
        expectError("FROM test | SAMPLE .1 2 3", "line 1:25: extraneous input '3' expecting <EOF>");
        expectError("FROM test | SAMPLE .1 \"2\"", "line 1:23: extraneous input '\"2\"' expecting <EOF>");
        expectError("FROM test | SAMPLE 1", "line 1:20: mismatched input '1' expecting {DECIMAL_LITERAL, '+', '-'}");
        expectError("FROM test | SAMPLE", "line 1:19: mismatched input '<EOF>' expecting {DECIMAL_LITERAL, '+', '-'}");
        expectError("FROM test | SAMPLE +.1 2147483648", "line 1:24: seed must be an integer, provided [2147483648] of type [LONG]");
    }

    static Alias alias(String name, Expression value) {
        return new Alias(EMPTY, name, value);
    }

    public void testValidRrf() {
        assumeTrue("RRF requires corresponding capability", EsqlCapabilities.Cap.RRF.isEnabled());

        LogicalPlan plan = statement("""
                FROM foo* METADATA _id, _index, _score
                | FORK ( WHERE a:"baz" )
                       ( WHERE b:"bar" )
                | RRF
            """);

        var orderBy = as(plan, OrderBy.class);
        assertThat(orderBy.order().size(), equalTo(3));

        assertThat(orderBy.order().get(0).child(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) orderBy.order().get(0).child()).name(), equalTo("_score"));
        assertThat(orderBy.order().get(1).child(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) orderBy.order().get(1).child()).name(), equalTo("_id"));
        assertThat(orderBy.order().get(2).child(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) orderBy.order().get(2).child()).name(), equalTo("_index"));

        var dedup = as(orderBy.child(), Dedup.class);
        assertThat(dedup.groupings().size(), equalTo(2));
        assertThat(dedup.groupings().get(0), instanceOf(UnresolvedAttribute.class));
        assertThat(dedup.groupings().get(0).name(), equalTo("_id"));
        assertThat(dedup.groupings().get(1), instanceOf(UnresolvedAttribute.class));
        assertThat(dedup.groupings().get(1).name(), equalTo("_index"));
        assertThat(dedup.aggregates().size(), equalTo(1));
        assertThat(dedup.aggregates().get(0), instanceOf(Alias.class));

        var rrfScoreEval = as(dedup.child(), RrfScoreEval.class);
        assertThat(rrfScoreEval.scoreAttribute(), instanceOf(UnresolvedAttribute.class));
        assertThat(rrfScoreEval.scoreAttribute().name(), equalTo("_score"));
        assertThat(rrfScoreEval.forkAttribute(), instanceOf(UnresolvedAttribute.class));
        assertThat(rrfScoreEval.forkAttribute().name(), equalTo("_fork"));

        assertThat(rrfScoreEval.child(), instanceOf(Fork.class));
    }

    public void testDoubleParamsForIdentifier() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        // There are three variations of double parameters - named, positional or anonymous, e.g. ??n, ??1 or ??, covered.
        // Each query is executed three times with the three variations.

        // field names can appear in eval/where/stats/sort/keep/drop/rename/dissect/grok/enrich/mvexpand
        // eval, where
        List<List<String>> doubleParams = new ArrayList<>(3);
        List<String> namedDoubleParams = List.of("??f0", "??fn1", "??f1", "??f2", "??f3");
        List<String> positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5");
        List<String> anonymousDoubleParams = List.of("??", "??", "??", "??", "??");
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | eval {} = {}({})
                | where {} == {}
                | limit 1""", params.get(0), params.get(1), params.get(2), params.get(3), params.get(4));
            assertEquals(
                new Limit(
                    EMPTY,
                    new Literal(EMPTY, 1, INTEGER),
                    new Filter(
                        EMPTY,
                        new Eval(EMPTY, relation("test"), List.of(new Alias(EMPTY, "x", function("toString", List.of(attribute("f1.")))))),
                        new Equals(EMPTY, attribute("f.2"), attribute("f3"))
                    )
                ),
                statement(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("f0", "x"),
                            paramAsConstant("fn1", "toString"),
                            paramAsConstant("f1", "f1."),
                            paramAsConstant("f2", "f.2"),
                            paramAsConstant("f3", "f3")
                        )
                    )
                )
            );
        }

        namedDoubleParams = List.of("??f0", "??fn1", "??f1", "??f2", "??f3", "??f4", "??f5", "??f6");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5", "??6", "??7", "??8");
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                """
                    from test
                    | eval {} = {}({}.{})
                    | where {}.{} == {}.{}
                    | limit 1""",
                params.get(0),
                params.get(1),
                params.get(2),
                params.get(3),
                params.get(4),
                params.get(5),
                params.get(6),
                params.get(7)
            );
            assertEquals(
                new Limit(
                    EMPTY,
                    new Literal(EMPTY, 1, INTEGER),
                    new Filter(
                        EMPTY,
                        new Eval(
                            EMPTY,
                            relation("test"),
                            List.of(new Alias(EMPTY, "x", function("toString", List.of(attribute("f1..f.2")))))
                        ),
                        new Equals(EMPTY, attribute("f3.*.f.4."), attribute("f.5.*.f.*.6"))
                    )
                ),
                statement(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("f0", "x"),
                            paramAsConstant("fn1", "toString"),
                            paramAsConstant("f1", "f1."),
                            paramAsConstant("f2", "f.2"),
                            paramAsConstant("f3", "f3.*"),
                            paramAsConstant("f4", "f.4."),
                            paramAsConstant("f5", "f.5.*"),
                            paramAsConstant("f6", "f.*.6")
                        )
                    )
                )
            );
        }

        // stats, sort, mv_expand
        namedDoubleParams = List.of("??fn2", "??f3", "??f4", "??f5", "??f6");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5");
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats y = {}({}) by {}
                | sort {}
                | mv_expand {}""", params.get(0), params.get(1), params.get(2), params.get(3), params.get(4));
            assertEquals(
                new MvExpand(
                    EMPTY,
                    new OrderBy(
                        EMPTY,
                        new Aggregate(
                            EMPTY,
                            relation("test"),
                            List.of(attribute("f.4.")),
                            List.of(new Alias(EMPTY, "y", function("count", List.of(attribute("f3.*")))), attribute("f.4."))
                        ),
                        List.of(new Order(EMPTY, attribute("f.5.*"), Order.OrderDirection.ASC, Order.NullsPosition.LAST))
                    ),
                    attribute("f.6*"),
                    attribute("f.6*")
                ),
                statement(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("fn2", "count"),
                            paramAsConstant("f3", "f3.*"),
                            paramAsConstant("f4", "f.4."),
                            paramAsConstant("f5", "f.5.*"),
                            paramAsConstant("f6", "f.6*")
                        )
                    )
                )
            );
        }

        namedDoubleParams = List.of("??fn2", "??f7", "??f8", "??f9", "??f10", "??f11", "??f12", "??f13", "??f14");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5", "??6", "??7", "??8", "??9");
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                """
                    from test
                    | stats y = {}({}.{}) by {}.{}
                    | sort {}.{}
                    | mv_expand {}.{}""",
                params.get(0),
                params.get(1),
                params.get(2),
                params.get(3),
                params.get(4),
                params.get(5),
                params.get(6),
                params.get(7),
                params.get(8)
            );
            assertEquals(
                new MvExpand(
                    EMPTY,
                    new OrderBy(
                        EMPTY,
                        new Aggregate(
                            EMPTY,
                            relation("test"),
                            List.of(attribute("f.9.f10.*")),
                            List.of(new Alias(EMPTY, "y", function("count", List.of(attribute("f.7*.f8.")))), attribute("f.9.f10.*"))
                        ),
                        List.of(new Order(EMPTY, attribute("f.11..f.12.*"), Order.OrderDirection.ASC, Order.NullsPosition.LAST))
                    ),
                    attribute("f.*.13.f.14*"),
                    attribute("f.*.13.f.14*")
                ),
                statement(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("fn2", "count"),
                            paramAsConstant("f7", "f.7*"),
                            paramAsConstant("f8", "f8."),
                            paramAsConstant("f9", "f.9"),
                            paramAsConstant("f10", "f10.*"),
                            paramAsConstant("f11", "f.11."),
                            paramAsConstant("f12", "f.12.*"),
                            paramAsConstant("f13", "f.*.13"),
                            paramAsConstant("f14", "f.14*")
                        )
                    )
                )
            );
        }

        // keep, drop, rename, grok, dissect, lookup join
        namedDoubleParams = List.of("??f1", "??f2", "??f3", "??f4", "??f5", "??f6", "??f7", "??f8", "??f9");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5", "??6", "??7", "??8", "??9");
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                """
                    from test
                    | keep {}, {}
                    | drop {}, {}
                    | dissect {} "%{bar}"
                    | grok {} "%{WORD:foo}"
                    | rename {} as {}
                    | lookup join idx on {}
                    | limit 1""",
                params.get(0),
                params.get(1),
                params.get(2),
                params.get(3),
                params.get(4),
                params.get(5),
                params.get(6),
                params.get(7),
                params.get(8)
            );
            LogicalPlan plan = statement(
                query,
                new QueryParams(
                    List.of(
                        paramAsConstant("f1", "f.1.*"),
                        paramAsConstant("f2", "f.2"),
                        paramAsConstant("f3", "f3."),
                        paramAsConstant("f4", "f4.*"),
                        paramAsConstant("f5", "f.5*"),
                        paramAsConstant("f6", "f.6."),
                        paramAsConstant("f7", "f7*."),
                        paramAsConstant("f8", "f.8"),
                        paramAsConstant("f9", "f9")
                    )
                )
            );
            Limit limit = as(plan, Limit.class);
            LookupJoin join = as(limit.child(), LookupJoin.class);
            UnresolvedRelation ur = as(join.right(), UnresolvedRelation.class);
            assertEquals(ur.indexPattern().indexPattern(), "idx");
            JoinTypes.UsingJoinType joinType = as(join.config().type(), JoinTypes.UsingJoinType.class);
            assertEquals(joinType.coreJoin().joinName(), "LEFT OUTER");
            assertEquals(joinType.columns(), List.of(attribute("f9")));
            Rename rename = as(join.left(), Rename.class);
            assertEquals(rename.renamings(), List.of(new Alias(EMPTY, "f.8", attribute("f7*."))));
            Grok grok = as(rename.child(), Grok.class);
            assertEquals(grok.input(), attribute("f.6."));
            assertEquals("%{WORD:foo}", grok.parser().pattern());
            assertEquals(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
            Dissect dissect = as(grok.child(), Dissect.class);
            assertEquals(dissect.input(), attribute("f.5*"));
            assertEquals("%{bar}", dissect.parser().pattern());
            assertEquals("", dissect.parser().appendSeparator());
            assertEquals(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
            Drop drop = as(dissect.child(), Drop.class);
            List<? extends NamedExpression> removals = drop.removals();
            assertEquals(removals, List.of(attribute("f3."), attribute("f4.*")));
            Keep keep = as(drop.child(), Keep.class);
            assertEquals(keep.projections(), List.of(attribute("f.1.*"), attribute("f.2")));
        }

        namedDoubleParams = List.of(
            "??f1",
            "??f2",
            "??f3",
            "??f4",
            "??f5",
            "??f6",
            "??f7",
            "??f8",
            "??f9",
            "??f10",
            "??f11",
            "??f12",
            "??f13",
            "??f14"
        );
        positionalDoubleParams = List.of(
            "??1",
            "??2",
            "??3",
            "??4",
            "??5",
            "??6",
            "??7",
            "??8",
            "??9",
            "??10",
            "??11",
            "??12",
            "??13",
            "??14"
        );
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??", "??", "??", "??", "??", "??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                """
                    from test
                    | keep {}.{}
                    | drop {}.{}
                    | dissect {}.{} "%{bar}"
                    | grok {}.{} "%{WORD:foo}"
                    | rename {}.{} as {}.{}
                    | lookup join idx on {}.{}
                    | limit 1""",
                params.get(0),
                params.get(1),
                params.get(2),
                params.get(3),
                params.get(4),
                params.get(5),
                params.get(6),
                params.get(7),
                params.get(8),
                params.get(9),
                params.get(10),
                params.get(11),
                params.get(12),
                params.get(13)
            );
            LogicalPlan plan = statement(
                query,
                new QueryParams(
                    List.of(
                        paramAsConstant("f1", "f.1.*"),
                        paramAsConstant("f2", "f.2"),
                        paramAsConstant("f3", "f3."),
                        paramAsConstant("f4", "f4.*"),
                        paramAsConstant("f5", "f.5*"),
                        paramAsConstant("f6", "f.6."),
                        paramAsConstant("f7", "f7*."),
                        paramAsConstant("f8", "f.8"),
                        paramAsConstant("f9", "f.9*"),
                        paramAsConstant("f10", "f.10."),
                        paramAsConstant("f11", "f11*."),
                        paramAsConstant("f12", "f.12"),
                        paramAsConstant("f13", "f13"),
                        paramAsConstant("f14", "f14")
                    )
                )
            );
            Limit limit = as(plan, Limit.class);
            LookupJoin join = as(limit.child(), LookupJoin.class);
            UnresolvedRelation ur = as(join.right(), UnresolvedRelation.class);
            assertEquals(ur.indexPattern().indexPattern(), "idx");
            JoinTypes.UsingJoinType joinType = as(join.config().type(), JoinTypes.UsingJoinType.class);
            assertEquals(joinType.coreJoin().joinName(), "LEFT OUTER");
            assertEquals(joinType.columns(), List.of(attribute("f13.f14")));
            Rename rename = as(join.left(), Rename.class);
            assertEquals(rename.renamings(), List.of(new Alias(EMPTY, "f11*..f.12", attribute("f.9*.f.10."))));
            Grok grok = as(rename.child(), Grok.class);
            assertEquals(grok.input(), attribute("f7*..f.8"));
            assertEquals("%{WORD:foo}", grok.parser().pattern());
            assertEquals(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
            Dissect dissect = as(grok.child(), Dissect.class);
            assertEquals(dissect.input(), attribute("f.5*.f.6."));
            assertEquals("%{bar}", dissect.parser().pattern());
            assertEquals("", dissect.parser().appendSeparator());
            assertEquals(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
            Drop drop = as(dissect.child(), Drop.class);
            List<? extends NamedExpression> removals = drop.removals();
            assertEquals(removals, List.of(attribute("f3..f4.*")));
            Keep keep = as(drop.child(), Keep.class);
            assertEquals(keep.projections(), List.of(attribute("f.1.*.f.2")));
        }

        // enrich, lookup join
        namedDoubleParams = List.of("??f1", "??f2", "??f3");
        positionalDoubleParams = List.of("??1", "??2", "??3");
        anonymousDoubleParams = List.of("??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                "from idx1 | ENRICH idx2 ON {} WITH {} = {}",
                params.get(0),
                params.get(1),
                params.get(2)
            );
            assertEquals(
                new Enrich(
                    EMPTY,
                    relation("idx1"),
                    null,
                    new Literal(EMPTY, "idx2", KEYWORD),
                    attribute("f.1.*"),
                    null,
                    Map.of(),
                    List.of(new Alias(EMPTY, "f.2", attribute("f.3*")))
                ),
                statement(
                    query,
                    new QueryParams(List.of(paramAsConstant("f1", "f.1.*"), paramAsConstant("f2", "f.2"), paramAsConstant("f3", "f.3*")))
                )
            );
        }

        namedDoubleParams = List.of("??f1", "??f2", "??f3", "??f4", "??f5", "??f6");
        positionalDoubleParams = List.of("??1", "??2", "??3", "??4", "??5", "??6");
        anonymousDoubleParams = List.of("??", "??", "??", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(
                null,
                "from idx1 | ENRICH idx2 ON {}.{} WITH {}.{} = {}.{}",
                params.get(0),
                params.get(1),
                params.get(2),
                params.get(3),
                params.get(4),
                params.get(5)
            );
            assertEquals(
                new Enrich(
                    EMPTY,
                    relation("idx1"),
                    null,
                    new Literal(EMPTY, "idx2", KEYWORD),
                    attribute("f.1.*.f.2"),
                    null,
                    Map.of(),
                    List.of(new Alias(EMPTY, "f.3*.f.4.*", attribute("f.5.f.6*")))
                ),
                statement(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("f1", "f.1.*"),
                            paramAsConstant("f2", "f.2"),
                            paramAsConstant("f3", "f.3*"),
                            paramAsConstant("f4", "f.4.*"),
                            paramAsConstant("f5", "f.5"),
                            paramAsConstant("f6", "f.6*")
                        )
                    )
                )
            );
        }
    }

    public void testMixedSingleDoubleParams() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        // This is a subset of testDoubleParamsForIdentifier, with single and double parameter markers mixed in the queries
        // Single parameter markers represent a constant value or pattern
        // double parameter markers represent identifiers - field or function names

        // mixed constant and identifier, eval/where
        List<List<String>> doubleParams = new ArrayList<>(3);
        List<String> namedDoubleParams = List.of("??f0", "??fn1", "?v1", "??f2", "?v3");
        List<String> positionalDoubleParams = List.of("??1", "??2", "?3", "??4", "?5");
        List<String> anonymousDoubleParams = List.of("??", "??", "?", "??", "?");
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | eval {} = {}({})
                | where {} == {}
                | limit 1""", params.get(0), params.get(1), params.get(2), params.get(3), params.get(4));
            assertEquals(
                new Limit(
                    EMPTY,
                    new Literal(EMPTY, 1, INTEGER),
                    new Filter(
                        EMPTY,
                        new Eval(
                            EMPTY,
                            relation("test"),
                            List.of(new Alias(EMPTY, "x", function("toString", List.of(new Literal(EMPTY, "constant_value", KEYWORD)))))
                        ),
                        new Equals(EMPTY, attribute("f.2"), new Literal(EMPTY, 100, INTEGER))
                    )
                ),
                statement(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("f0", "x"),
                            paramAsConstant("fn1", "toString"),
                            paramAsConstant("v1", "constant_value"),
                            paramAsConstant("f2", "f.2"),
                            paramAsConstant("v3", 100)
                        )
                    )
                )
            );
        }

        // mixed constant and identifier, stats/sort/mv_expand
        namedDoubleParams = List.of("??fn2", "?v3", "??f4", "??f5", "??f6");
        positionalDoubleParams = List.of("??1", "?2", "??3", "??4", "??5");
        anonymousDoubleParams = List.of("??", "?", "??", "??", "??");
        doubleParams.clear();
        doubleParams.add(namedDoubleParams);
        doubleParams.add(positionalDoubleParams);
        doubleParams.add(anonymousDoubleParams);
        for (List<String> params : doubleParams) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats y = {}({}) by {}
                | sort {}
                | mv_expand {}""", params.get(0), params.get(1), params.get(2), params.get(3), params.get(4));
            assertEquals(
                new MvExpand(
                    EMPTY,
                    new OrderBy(
                        EMPTY,
                        new Aggregate(
                            EMPTY,
                            relation("test"),
                            List.of(attribute("f.4.")),
                            List.of(new Alias(EMPTY, "y", function("count", List.of(new Literal(EMPTY, "*", KEYWORD)))), attribute("f.4."))
                        ),
                        List.of(new Order(EMPTY, attribute("f.5.*"), Order.OrderDirection.ASC, Order.NullsPosition.LAST))
                    ),
                    attribute("f.6*"),
                    attribute("f.6*")
                ),
                statement(
                    query,
                    new QueryParams(
                        List.of(
                            paramAsConstant("fn2", "count"),
                            paramAsConstant("v3", "*"),
                            paramAsConstant("f4", "f.4."),
                            paramAsConstant("f5", "f.5.*"),
                            paramAsConstant("f6", "f.6*")
                        )
                    )
                )
            );
        }

        // mixed field name and field name pattern
        LogicalPlan plan = statement(
            "from test | keep ??f1, ?f2 | drop ?f3, ??f4 | lookup join idx on ??f5",
            new QueryParams(
                List.of(
                    paramAsConstant("f1", "f*1."),
                    paramAsPattern("f2", "f.2*"),
                    paramAsPattern("f3", "f3.*"),
                    paramAsConstant("f4", "f.4.*"),
                    paramAsConstant("f5", "f5")
                )
            )
        );

        LookupJoin join = as(plan, LookupJoin.class);
        UnresolvedRelation ur = as(join.right(), UnresolvedRelation.class);
        assertEquals(ur.indexPattern().indexPattern(), "idx");
        JoinTypes.UsingJoinType joinType = as(join.config().type(), JoinTypes.UsingJoinType.class);
        assertEquals(joinType.coreJoin().joinName(), "LEFT OUTER");
        assertEquals(joinType.columns(), List.of(attribute("f5")));
        Drop drop = as(join.left(), Drop.class);
        List<? extends NamedExpression> removals = drop.removals();
        assertEquals(removals.size(), 2);
        UnresolvedNamePattern up = as(removals.get(0), UnresolvedNamePattern.class);
        assertEquals(up.name(), "f3.*");
        assertEquals(up.pattern(), "f3.*");
        UnresolvedAttribute ua = as(removals.get(1), UnresolvedAttribute.class);
        assertEquals(ua.name(), "f.4.*");
        Keep keep = as(drop.child(), Keep.class);
        assertEquals(keep.projections().size(), 2);
        ua = as(keep.projections().get(0), UnresolvedAttribute.class);
        assertEquals(ua.name(), "f*1.");
        up = as(keep.projections().get(1), UnresolvedNamePattern.class);
        assertEquals(up.name(), "f.2*");
        assertEquals(up.pattern(), "f.2*");
        ur = as(keep.child(), UnresolvedRelation.class);
        assertEquals(ur, relation("test"));

        // test random single and double params
        // commands in group1 take both constants(?) and identifiers(??)
        List<String> commandWithRandomSingleOrDoubleParamsGroup1 = List.of(
            "eval x = {}f1, y = {}f2, z = {}f3",
            "eval x = fn({}f1), y = {}f2 + {}f3",
            "where {}f1 == \"a\" and {}f2 > 1 and {}f3 in (1, 2)",
            "stats x = fn({}f1) by {}f2, {}f3",
            "sort {}f1, {}f2, {}f3",
            "dissect {}f1 \"%{bar}\"",
            "grok {}f1 \"%{WORD:foo}\""
        );
        for (String command : commandWithRandomSingleOrDoubleParamsGroup1) {
            String param1 = randomBoolean() ? "?" : "??";
            String param2 = randomBoolean() ? "?" : "??";
            String param3 = randomBoolean() ? "?" : "??";
            plan = statement(
                LoggerMessageFormat.format(null, "from test | " + command, param1, param2, param3),
                new QueryParams(List.of(paramAsConstant("f1", "f1"), paramAsConstant("f2", "f2"), paramAsConstant("f3", "f3")))
            );
            assertNotNull(plan);
        }
        // commands in group2 only take identifiers(??)
        List<String> commandWithRandomSingleOrDoubleParamsGroup2 = List.of(
            "eval x = {}f1(), y = {}f2(), z = {}f3()",
            "where {}f1 : \"b\" and {}f2() > 0 and {}f3()",
            "stats x = {}f1(), {}f2(), {}f3()",
            "rename {}f1 as {}f2, {}f3 as x",
            "enrich idx2 ON {}f1 WITH {}f2 = {}f3",
            "keep {}f1, {}f2, {}f3",
            "drop {}f1, {}f2, {}f3",
            "mv_expand {}f1 | mv_expand {}f2 | mv_expand {}f3",
            "lookup join idx1 on {}f1 | lookup join idx2 on {}f2 | lookup join idx3 on {}f3"
        );

        for (String command : commandWithRandomSingleOrDoubleParamsGroup2) {
            String param1 = randomBoolean() ? "?" : "??";
            String param2 = randomBoolean() ? "?" : "??";
            String param3 = randomBoolean() ? "?" : "??";
            if (param1.equals("?") || param2.equals("?") || param3.equals("?")) {
                expectError(
                    LoggerMessageFormat.format(null, "from test | " + command, param1, param2, param3),
                    List.of(paramAsConstant("f1", "f1"), paramAsConstant("f2", "f2"), paramAsConstant("f3", "f3")),
                    command.contains("join")
                        ? "JOIN ON clause only supports fields at the moment"
                        : "declared as a constant, cannot be used as an identifier"
                );
            }
        }
    }

    public void testInvalidDoubleParamsNames() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        expectError(
            "from test | where x < ??n1 | eval y = ??n2",
            List.of(paramAsConstant("n1", "f1"), paramAsConstant("n3", "f2")),
            "line 1:39: Unknown query parameter [n2], did you mean any of [n3, n1]?"
        );

        expectError("from test | where x < ??@1", List.of(paramAsConstant("@1", "f1")), "line 1:25: extraneous input '@1' expecting <EOF>");

        expectError("from test | where x < ??#1", List.of(paramAsConstant("#1", "f1")), "line 1:25: token recognition error at: '#'");

        expectError("from test | where x < ??Å", List.of(paramAsConstant("Å", "f1")), "line 1:25: token recognition error at: 'Å'");

        expectError("from test | eval x = ??Å", List.of(paramAsConstant("Å", "f1")), "line 1:24: token recognition error at: 'Å'");
    }

    public void testInvalidDoubleParamsPositions() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        expectError(
            "from test | where x < ??0",
            List.of(paramAsConstant(null, "f1")),
            "line 1:23: No parameter is defined for position 0, did you mean position 1"
        );

        expectError(
            "from test | where x < ??2",
            List.of(paramAsConstant(null, "f1")),
            "line 1:23: No parameter is defined for position 2, did you mean position 1"
        );

        expectError(
            "from test | where x < ??0 and y < ??2",
            List.of(paramAsConstant(null, "f1")),
            "line 1:23: No parameter is defined for position 0, did you mean position 1?; "
                + "line 1:35: No parameter is defined for position 2, did you mean position 1?"
        );

        expectError(
            "from test | where x < ??0",
            List.of(paramAsConstant(null, "f1"), paramAsConstant(null, "f2")),
            "line 1:23: No parameter is defined for position 0, did you mean any position between 1 and 2?"
        );
    }

    public void testInvalidDoubleParamsType() {
        assumeTrue("double parameters markers for identifiers", EsqlCapabilities.Cap.DOUBLE_PARAMETER_MARKERS_FOR_IDENTIFIERS.isEnabled());
        // double parameter markers cannot be declared as identifier patterns
        String error = "Query parameter [??f1][f1] declared as a pattern, cannot be used as an identifier";
        List<String> commandWithDoubleParams = List.of(
            "eval x = ??f1",
            "eval x = ??f1(f1)",
            "where ??f1 == \"a\"",
            "stats x = count(??f1)",
            "sort ??f1",
            "rename ??f1 as ??f2",
            "dissect ??f1 \"%{bar}\"",
            "grok ??f1 \"%{WORD:foo}\"",
            "enrich idx2 ON ??f1 WITH ??f2 = ??f3",
            "keep ??f1",
            "drop ??f1",
            "mv_expand ??f1",
            "lookup join idx on ??f1"
        );
        for (String command : commandWithDoubleParams) {
            expectError(
                "from test | " + command,
                List.of(paramAsPattern("f1", "f1*"), paramAsPattern("f2", "f2*"), paramAsPattern("f3", "f3*")),
                error
            );
        }
    }

    public void testUnclosedParenthesis() {
        String[] queries = { "row a = )", "row ]", "from source | eval x = [1,2,3]]" };
        for (String q : queries) {
            expectError(q, "Invalid query");
        }
    }
}
