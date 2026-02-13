/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FilteredExpression;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDenseVector;
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
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
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
import org.elasticsearch.xpack.esql.plan.logical.MMR;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.assertEqualsIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalToIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsIdentifier;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsPattern;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.CROSS_CLUSTER;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.Features.INDEX_SELECTOR;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.quote;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPattern;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.randomIndexPatterns;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.unquoteIndexPattern;
import static org.elasticsearch.xpack.esql.IdentifierGenerator.without;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.expression.function.FunctionResolutionStrategy.DEFAULT;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerTests.releaseBuildForInlineStats;
import static org.elasticsearch.xpack.esql.parser.ExpressionBuilder.breakIntoFragments;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")

/**
 * Only parses a plan and builds an AST/Logical Plan for it.
 * Analysis is not run, so the plan will contain unresolved references.
 * Use this class to test cases where we throw a Parsing exception
 * especially if it is thrown before we get to the Analysis phase
 */
public class StatementParserTests extends AbstractStatementParserTests {

    private static final LogicalPlan PROCESSING_CMD_INPUT = new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1))));

    public void testRowCommand() {
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1)), new Alias(EMPTY, "b", integer(2)))),
            query("row a = 1, b = 2")
        );
    }

    public void testRowCommandImplicitFieldName() {
        assertEqualsIgnoringIds(
            new Row(
                EMPTY,
                List.of(new Alias(EMPTY, "1", integer(1)), new Alias(EMPTY, "2", integer(2)), new Alias(EMPTY, "c", integer(3)))
            ),
            query("row 1, 2, c = 3")
        );
    }

    public void testRowCommandLong() {
        assertEqualsIgnoringIds(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalLong(2147483648L)))), query("row c = 2147483648"));
    }

    public void testRowCommandHugeInt() {
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalUnsignedLong("9223372036854775808")))),
            query("row c = 9223372036854775808")
        );
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDouble(18446744073709551616.)))),
            query("row c = 18446744073709551616")
        );
    }

    public void testRowCommandHugeNegativeInt() {
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDouble(-92233720368547758080d)))),
            query("row c = -92233720368547758080")
        );
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDouble(-18446744073709551616d)))),
            query("row c = -18446744073709551616")
        );
    }

    public void testRowCommandDouble() {
        assertEqualsIgnoringIds(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDouble(1.0)))), query("row c = 1.0"));
    }

    public void testRowCommandMultivalueInt() {
        assertEqualsIgnoringIds(new Row(EMPTY, List.of(new Alias(EMPTY, "c", integers(1, 2, -5)))), query("row c = [1, 2, -5]"));
    }

    public void testRowCommandMultivalueLong() {
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalLongs(2147483648L, 2147483649L, -434366649L)))),
            query("row c = [2147483648, 2147483649, -434366649]")
        );
    }

    public void testRowCommandMultivalueLongAndInt() {
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalLongs(2147483648L, 1L)))),
            query("row c = [2147483648, 1]")
        );
    }

    public void testRowCommandMultivalueHugeInts() {
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDoubles(18446744073709551616., 18446744073709551617.)))),
            query("row c = [18446744073709551616, 18446744073709551617]")
        );
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalUnsignedLongs("9223372036854775808", "9223372036854775809")))),
            query("row c = [9223372036854775808, 9223372036854775809]")
        );
    }

    public void testRowCommandMultivalueHugeIntAndNormalInt() {
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDoubles(18446744073709551616., 1.0)))),
            query("row c = [18446744073709551616, 1]")
        );
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalUnsignedLongs("9223372036854775808", "1")))),
            query("row c = [9223372036854775808, 1]")
        );
    }

    public void testRowCommandMultivalueDouble() {
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalDoubles(1.0, 2.0, -3.4)))),
            query("row c = [1.0, 2.0, -3.4]")
        );
    }

    public void testRowCommandBoolean() {
        assertEqualsIgnoringIds(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalBoolean(false)))), query("row c = false"));
    }

    public void testRowCommandMultivalueBoolean() {
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalBooleans(false, true)))),
            query("row c = [false, true]")
        );
    }

    public void testRowCommandString() {
        assertEqualsIgnoringIds(new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalString("chicken")))), query("row c = \"chicken\""));
    }

    public void testRowCommandMultivalueString() {
        assertEqualsIgnoringIds(
            new Row(EMPTY, List.of(new Alias(EMPTY, "c", literalStrings("cat", "dog")))),
            query("row c = [\"cat\", \"dog\"]")
        );
    }

    public void testRowCommandWithEscapedFieldName() {
        assertEqualsIgnoringIds(
            new Row(
                EMPTY,
                List.of(
                    new Alias(EMPTY, "a.b.c", integer(1)),
                    new Alias(EMPTY, "b", integer(2)),
                    new Alias(EMPTY, "@timestamp", Literal.keyword(EMPTY, "2022-26-08T00:00:00"))
                )
            ),
            query("row a.b.c = 1, `b` = 2, `@timestamp`=\"2022-26-08T00:00:00\"")
        );
    }

    public void testCompositeCommand() {
        assertEqualsIgnoringIds(
            new Filter(EMPTY, new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1)))), TRUE),
            query("row a = 1 | where true")
        );
    }

    public void testMultipleCompositeCommands() {
        assertEqualsIgnoringIds(
            new Filter(
                EMPTY,
                new Filter(EMPTY, new Filter(EMPTY, new Row(EMPTY, List.of(new Alias(EMPTY, "a", integer(1)))), TRUE), FALSE),
                TRUE
            ),
            query("row a = 1 | where true | where false | where true")
        );
    }

    public void testEval() {
        assertEqualsIgnoringIds(
            new Eval(EMPTY, PROCESSING_CMD_INPUT, List.of(new Alias(EMPTY, "b", attribute("a")))),
            processingCommand("eval b = a")
        );

        assertEqualsIgnoringIds(
            new Eval(
                EMPTY,
                PROCESSING_CMD_INPUT,
                List.of(
                    new Alias(EMPTY, "b", attribute("a")),
                    new Alias(EMPTY, "c", new Add(EMPTY, attribute("a"), integer(1), ConfigurationAware.CONFIGURATION_MARKER))
                )
            ),
            processingCommand("eval b = a, c = a + 1")
        );
    }

    public void testEvalImplicitNames() {
        assertEqualsIgnoringIds(
            new Eval(EMPTY, PROCESSING_CMD_INPUT, List.of(new Alias(EMPTY, "a", attribute("a")))),
            processingCommand("eval a")
        );

        assertEqualsIgnoringIds(
            new Eval(
                EMPTY,
                PROCESSING_CMD_INPUT,
                List.of(
                    new Alias(
                        EMPTY,
                        "fn(a + 1)",
                        new UnresolvedFunction(
                            EMPTY,
                            "fn",
                            DEFAULT,
                            List.of(new Add(EMPTY, attribute("a"), integer(1), ConfigurationAware.CONFIGURATION_MARKER))
                        )
                    )
                )
            ),
            processingCommand("eval fn(a + 1)")
        );
    }

    public void testStatsWithGroups() {
        assertEqualsIgnoringIds(
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
        assertEqualsIgnoringIds(
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
        assertEqualsIgnoringIds(
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
        assertEqualsIgnoringIds(
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

        assertEqualsIgnoringIds(
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
        assertEqualsIgnoringIds(
            new Aggregate(EMPTY, PROCESSING_CMD_INPUT, List.of(), List.of(filter)),
            processingCommand("stats min(a) where a > 1")
        );
    }

    public void testInlineStatsWithGroups() {
        if (releaseBuildForInlineStats(null)) {
            return;
        }
        for (var cmd : List.of("INLINE STATS", "INLINESTATS")) {
            var query = cmd + " b = MIN(a) BY c, d.e";
            assertThat(
                processingCommand(query),
                equalToIgnoringIds(
                    new InlineStats(
                        EMPTY,
                        new Aggregate(
                            EMPTY,
                            PROCESSING_CMD_INPUT,
                            List.of(attribute("c"), attribute("d.e")),
                            List.of(
                                new Alias(EMPTY, "b", new UnresolvedFunction(EMPTY, "MIN", DEFAULT, List.of(attribute("a")))),
                                attribute("c"),
                                attribute("d.e")
                            )
                        )
                    )
                )
            );
        }
    }

    public void testInlineStatsWithoutGroups() {
        if (releaseBuildForInlineStats(null)) {
            return;
        }
        for (var cmd : List.of("INLINE STATS", "INLINESTATS")) {
            var query = cmd + " MIN(a), c = 1";
            assertEqualsIgnoringIds(
                processingCommand(query),
                new InlineStats(
                    EMPTY,
                    new Aggregate(
                        EMPTY,
                        PROCESSING_CMD_INPUT,
                        List.of(),
                        List.of(
                            new Alias(EMPTY, "MIN(a)", new UnresolvedFunction(EMPTY, "MIN", DEFAULT, List.of(attribute("a")))),
                            new Alias(EMPTY, "c", integer(1))
                        )
                    )
                )
            );
        }
    }

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(super.filteredWarnings());
    }

    public void testInlineStatsParsing() {
        if (releaseBuildForInlineStats(null)) {
            return;
        }
        expectThrows(
            ParsingException.class,
            containsString("line 1:19: token recognition error at: 'I'"),
            () -> query("FROM foo | INLINE INLINE STATS COUNT(*)")
        );
        expectThrows(
            ParsingException.class,
            containsString("line 1:19: token recognition error at: 'F'"),
            () -> query("FROM foo | INLINE FOO COUNT(*)")
        );
    }

    /*
     * Fork[[]]
     * |_Eval[[fork1[KEYWORD] AS _fork#3]]
     * | \_Limit[11[INTEGER],false]
     * |   \_Filter[:(?a,baz[KEYWORD])]
     * |     \_UnresolvedRelation[foo*]
     * |_Eval[[fork2[KEYWORD] AS _fork#3]]
     * | \_Aggregate[[],[?COUNT[*] AS COUNT(*)#4]]
     * |   \_UnresolvedRelation[foo*]
     * \_Eval[[fork3[KEYWORD] AS _fork#3]]
     *   \_InlineStats[]
     *     \_Aggregate[[],[?COUNT[*] AS COUNT(*)#5]]
     *       \_UnresolvedRelation[foo*]
     */
    public void testInlineStatsWithinFork() {
        if (releaseBuildForInlineStats(null)) {
            return;
        }
        var query = """
            FROM foo*
            | FORK ( WHERE a:"baz" | LIMIT 11 )
                   ( STATS COUNT(*) )
                   ( INLINE STATS COUNT(*) )
            """;
        var plan = query(query);
        var fork = as(plan, Fork.class);
        var subPlans = fork.children();

        // first subplan
        var eval = as(subPlans.get(0), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", literalString("fork1"))));
        var limit = as(eval.child(), Limit.class);
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(11));
        var filter = as(limit.child(), Filter.class);
        var match = (MatchOperator) filter.condition();
        var matchField = (UnresolvedAttribute) match.field();
        assertThat(matchField.name(), equalTo("a"));
        assertThat(match.query().fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef("baz")));

        // second subplan
        eval = as(subPlans.get(1), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", literalString("fork2"))));
        var aggregate = as(eval.child(), Aggregate.class);
        assertThat(aggregate.aggregates().size(), equalTo(1));
        var alias = as(aggregate.aggregates().get(0), Alias.class);
        assertThat(alias.name(), equalTo("COUNT(*)"));
        var countFn = as(alias.child(), UnresolvedFunction.class);
        assertThat(countFn.children().get(0), instanceOf(Literal.class));
        assertThat(countFn.children().get(0).fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef("*")));

        // third subplan
        eval = as(subPlans.get(2), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", literalString("fork3"))));
        var inlineStats = as(eval.child(), InlineStats.class);
        aggregate = as(inlineStats.child(), Aggregate.class);
        assertThat(aggregate.aggregates().size(), equalTo(1));
        alias = as(aggregate.aggregates().get(0), Alias.class);
        assertThat(alias.name(), equalTo("COUNT(*)"));
        countFn = as(alias.child(), UnresolvedFunction.class);
        assertThat(countFn.children().get(0), instanceOf(Literal.class));
        assertThat(countFn.children().get(0).fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef("*")));
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
            assertStringAsIndexPattern("<logstash-{now/M{yyyy.MM}}>", command + " <logstash-{now/M{yyyy.MM}}>");
            assertStringAsIndexPattern(
                "<logstash-{now/M{yyyy.MM}}>,<logstash-{now/d{yyyy.MM.dd|+12:00}}>",
                command + " <logstash-{now/M{yyyy.MM}}>, \"<logstash-{now/d{yyyy.MM.dd|+12:00}}>\""
            );
            assertStringAsIndexPattern("<logstash-{now/d{yyyy.MM.dd|+12:00}}>", command + " \"<logstash-{now/d{yyyy.MM.dd|+12:00}}>\"");
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
            // Entire index pattern is quoted. So it's not a parse error but a semantic error where the index name
            // is invalid.
            expectError(command + " \"*:index|pattern\"", "Invalid index name [index|pattern], must not contain the following characters");
            clusterAndIndexAsIndexPattern(command, "cluster:index");
            clusterAndIndexAsIndexPattern(command, "cluster:.index");
            clusterAndIndexAsIndexPattern(command, "cluster*:index*");
            clusterAndIndexAsIndexPattern(command, "cluster*:<logstash-{now/d}>*");
            clusterAndIndexAsIndexPattern(command, "cluster*:*");
            clusterAndIndexAsIndexPattern(command, "*:index*");
            clusterAndIndexAsIndexPattern(command, "*:*");
            expectError(
                command + " \"cluster:index|pattern\"",
                "Invalid index name [index|pattern], must not contain the following characters"
            );
            expectError(command + " *:\"index|pattern\"", "expecting UNQUOTED_SOURCE");
            if (EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled()) {
                assertStringAsIndexPattern("foo::data", command + " foo::data");
                assertStringAsIndexPattern("foo::failures", command + " foo::failures");
                expectErrorWithLineNumber(
                    command + " *,\"-foo\"::data",
                    "*,-foo::data",
                    lineNumber,
                    "mismatched input '::' expecting {<EOF>, '|', ',', 'metadata'}"
                );
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

                assertStringAsIndexPattern("cluster:foo::data", command + " cluster:foo::data");
                assertStringAsIndexPattern("cluster:foo::failures", command + " cluster:foo::failures");

                assertStringAsIndexPattern("cluster:foo::data", command + " \"cluster:foo::data\"");
                assertStringAsIndexPattern("cluster:foo::failures", command + " \"cluster:foo::failures\"");

                // Wildcards
                assertStringAsIndexPattern("cluster:*::data", command + " cluster:*::data");
                assertStringAsIndexPattern("cluster:*::failures", command + " cluster:*::failures");
                assertStringAsIndexPattern("*:index::data", command + " *:index::data");
                assertStringAsIndexPattern("*:index::failures", command + " *:index::failures");
                assertStringAsIndexPattern("*:index*::data", command + " *:index*::data");
                assertStringAsIndexPattern("*:index*::failures", command + " *:index*::failures");
                assertStringAsIndexPattern("*:*::data", command + " *:*::data");
                assertStringAsIndexPattern("*:*::failures", command + " *:*::failures");
                assertStringAsIndexPattern("cluster:*::data", command + " \"cluster:*::data\"");
                assertStringAsIndexPattern("cluster:*::failures", command + " \"cluster:*::failures\"");
                assertStringAsIndexPattern("*:index::data", command + " \"*:index::data\"");
                assertStringAsIndexPattern("*:index::failures", command + " \"*:index::failures\"");
                assertStringAsIndexPattern("*:index*::data", command + " \"*:index*::data\"");
                assertStringAsIndexPattern("*:index*::failures", command + " \"*:index*::failures\"");
                assertStringAsIndexPattern("*:*::data", command + " \"*:*::data\"");
                assertStringAsIndexPattern("*:*::failures", command + " \"*:*::failures\"");
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
            commands.put("TS {}", "line 1:4: ");
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

                // Index pattern cannot be quoted if cluster string is present.
                expectErrorWithLineNumber(
                    command,
                    "cluster:\"foo\"::data",
                    command.startsWith("FROM") ? "line 1:14: " : "line 1:12: ",
                    "mismatched input '\"foo\"' expecting UNQUOTED_SOURCE"
                );
                expectErrorWithLineNumber(
                    command,
                    "cluster:\"foo\"::failures",
                    command.startsWith("FROM") ? "line 1:14: " : "line 1:12: ",
                    "mismatched input '\"foo\"' expecting UNQUOTED_SOURCE"
                );

                // Index pattern cannot be clubbed together with cluster string without including selector if present
                int parseLineNumber = 6;
                if (command.startsWith("TS")) {
                    parseLineNumber = 4;
                }

                expectDoubleColonErrorWithLineNumber(command, "\"cluster:foo\"::data", parseLineNumber + 13);
                expectDoubleColonErrorWithLineNumber(command, "\"cluster:foo\"::failures", parseLineNumber + 13);

                // Too many colons
                expectInvalidIndexNameErrorWithLineNumber(command, "\"index:::data\"", lineNumber, "index:", "must not contain ':'");
                expectInvalidIndexNameErrorWithLineNumber(
                    command,
                    "\"index::::data\"",
                    lineNumber,
                    "index::::data",
                    "Invalid usage of :: separator, only one :: separator is allowed per expression"
                );

                expectErrorWithLineNumber(
                    command,
                    "cluster:\"index,index2\"::failures",
                    command.startsWith("FROM") ? "line 1:14: " : "line 1:12: ",
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
            expectInvalidIndexNameErrorWithLineNumber(command, "*, index#pattern", lineNumber, "index#pattern", "must not contain '#'");
            expectInvalidIndexNameErrorWithLineNumber(
                command,
                "index*, index#pattern",
                indexStarLineNumber,
                "index#pattern",
                "must not contain '#'"
            );
            expectDateMathErrorWithLineNumber(command, "cluster*:<logstash-{now/D}*>", commands.get(command), dateMathError);
            expectDateMathErrorWithLineNumber(command, "*, \"-<-logstash-{now/D}>\"", lineNumber, dateMathError);
            expectDateMathErrorWithLineNumber(command, "*, -<-logstash-{now/D}>", lineNumber, dateMathError);
            expectDateMathErrorWithLineNumber(command, "\"*, -<-logstash-{now/D}>\"", commands.get(command), dateMathError);
            expectDateMathErrorWithLineNumber(command, "\"*, -<-logst:ash-{now/D}>\"", commands.get(command), dateMathError);
            if (EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled()) {
                clustersAndIndices(command, "*", "-index::data");
                clustersAndIndices(command, "*", "-index::failures");
                clustersAndIndices(command, "*", "-index*pattern::data");
                clustersAndIndices(command, "*", "-index*pattern::failures");

                // This is by existing design: refer to the comment in IdentifierBuilder#resolveAndValidateIndex() in the last
                // catch clause. If there's an index with a wildcard before an invalid index, we don't error out.
                clustersAndIndices(command, "index*", "-index#pattern::data");
                clustersAndIndices(command, "*", "-<--logstash-{now/M{yyyy.MM}}>::data");
                clustersAndIndices(command, "index*", "-<--logstash#-{now/M{yyyy.MM}}>::data");

                expectError(command + "index1,<logstash-{now+-/d}>", "unit [-] not supported for date math [+-/d]");

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
        assertEqualsIgnoringIds(unresolvedRelation(indexString1 + "," + indexString2), query(command, indexString1 + ", " + indexString2));
        assertEqualsIgnoringIds(
            unresolvedRelation(indexString1 + "," + indexString2),
            query(command, indexString1 + ", \"" + indexString2 + "\"")
        );
        assertEqualsIgnoringIds(
            unresolvedRelation(indexString1 + ", " + indexString2),
            query(command, "\"" + indexString1 + ", " + indexString2 + "\"")
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
                assertThat(filter.child(), equalToIgnoringIds(PROCESSING_CMD_INPUT));
            }
        }
    }

    public void testBooleanLiteralCondition() {
        LogicalPlan where = processingCommand("where true");
        assertThat(where, instanceOf(Filter.class));
        Filter w = (Filter) where;
        assertThat(w.child(), equalToIgnoringIds(PROCESSING_CMD_INPUT));
        assertThat(w.condition(), equalTo(TRUE));
    }

    public void testBasicLimitCommand() {
        LogicalPlan plan = query("from text | where true | limit 5");
        assertThat(plan, instanceOf(Limit.class));
        Limit limit = (Limit) plan;
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(5));
        assertThat(limit.children().size(), equalTo(1));
        assertThat(limit.children().get(0), instanceOf(Filter.class));
        assertThat(limit.children().get(0).children().size(), equalTo(1));
        assertThat(limit.children().get(0).children().get(0), instanceOf(UnresolvedRelation.class));
    }

    public void testBasicSortCommand() {
        LogicalPlan plan = query("from text | where true | sort a+b asc nulls first, x desc nulls last | sort y asc | sort z desc");
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
        assumeTrue("Requires EXPLAIN capability", EsqlCapabilities.Cap.EXPLAIN.isEnabled());
        assertEqualsIgnoringIds(new Explain(EMPTY, PROCESSING_CMD_INPUT), query("explain ( row a = 1 )"));
    }

    public void testBlockComments() {
        assumeTrue("Requires EXPLAIN capability", EsqlCapabilities.Cap.EXPLAIN.isEnabled());
        String query = " explain ( from foo )";
        LogicalPlan expected = query(query);

        int wsIndex = query.indexOf(' ');

        do {
            String queryWithComment = query.substring(0, wsIndex) + "/*explain ( \nfrom bar ) */" + query.substring(wsIndex + 1);

            assertEqualsIgnoringIds(expected, query(queryWithComment));

            wsIndex = query.indexOf(' ', wsIndex + 1);
        } while (wsIndex >= 0);
    }

    public void testSingleLineComments() {
        assumeTrue("Requires EXPLAIN capability", EsqlCapabilities.Cap.EXPLAIN.isEnabled());
        String query = " explain ( from foo ) ";
        LogicalPlan expected = query(query);

        int wsIndex = query.indexOf(' ');

        do {
            String queryWithComment = query.substring(0, wsIndex) + "//explain ( from bar ) \n" + query.substring(wsIndex + 1);

            assertEqualsIgnoringIds(expected, query(queryWithComment));

            wsIndex = query.indexOf(' ', wsIndex + 1);
        } while (wsIndex >= 0);
    }

    public void testNewLines() {
        String[] delims = new String[] { "", "\r", "\n", "\r\n" };
        Function<String, String> queryFun = d -> d + "from " + d + " foo " + d + "| eval " + d + " x = concat(bar, \"baz\")" + d;
        LogicalPlan reference = query(queryFun.apply(delims[0]));
        for (int i = 1; i < delims.length; i++) {
            LogicalPlan candidate = query(queryFun.apply(delims[i]));
            assertThat(candidate, equalToIgnoringIds(reference));
        }
    }

    public void testSuggestAvailableSourceCommandsOnParsingError() {
        var cases = new ArrayList<Tuple<String, String>>();
        cases.add(Tuple.tuple("frm foo", "frm"));
        cases.add(Tuple.tuple("expln[from bar]", "expln"));
        cases.add(Tuple.tuple("not-a-thing logs", "not-a-thing"));
        cases.add(Tuple.tuple("high5 a", "high5"));
        cases.add(Tuple.tuple("a+b = c", "a+b"));
        cases.add(Tuple.tuple("a//hi", "a"));
        cases.add(Tuple.tuple("a/*hi*/", "a"));
        if (EsqlCapabilities.Cap.EXPLAIN.isEnabled()) {
            cases.add(Tuple.tuple("explain ( frm a )", "frm"));
        }

        for (Tuple<String, String> queryWithUnexpectedCmd : cases) {
            expectThrows(
                ParsingException.class,
                allOf(
                    containsString("mismatched input '" + queryWithUnexpectedCmd.v2() + "'"),
                    containsString("'from'"),
                    containsString("'row'")
                ),
                () -> query(queryWithUnexpectedCmd.v1())
            );

        }
    }

    public void testSuggestAvailableProcessingCommandsOnParsingError() {
        for (Tuple<String, String> queryWithUnexpectedCmd : List.of(
            Tuple.tuple("from a | filter b > 1", "filter"),
            Tuple.tuple("from a | explain ( row 1 )", "explain"),
            Tuple.tuple("from a | not-a-thing", "not-a-thing"),
            Tuple.tuple("from a | high5 a", "high5"),
            Tuple.tuple("from a | a+b = c", "a+b"),
            Tuple.tuple("from a | a//hi", "a"),
            Tuple.tuple("from a | a/*hi*/", "a")
        )) {
            expectThrows(
                ParsingException.class,
                allOf(
                    containsString("mismatched input '" + queryWithUnexpectedCmd.v2() + "'"),
                    containsString("'eval'"),
                    containsString("'limit'"),
                    containsString("'where'")
                ),
                () -> query(queryWithUnexpectedCmd.v1())
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
        if (EsqlCapabilities.Cap.EXPLAIN.isEnabled()) {
            expectError("explain ( from foo ) metadata _index", "line 1:22: token recognition error at: 'm'");
        }
    }

    public void testMetadataFieldMultipleDeclarations() {
        expectError("from test metadata _index, _version, _index", "1:38: metadata field [_index] already declared [@1:20]");
    }

    public void testDissectPattern() {
        LogicalPlan cmd = processingCommand("dissect a \"%{foo}\"");
        assertEquals(Dissect.class, cmd.getClass());
        Dissect dissect = (Dissect) cmd;
        assertEquals("%{foo}", dissect.parser().pattern());
        assertEquals("", dissect.parser().appendSeparator());
        assertEqualsIgnoringIds(List.of(referenceAttribute("foo", KEYWORD)), dissect.extractedFields());

        for (String separatorName : List.of("append_separator", "APPEND_SEPARATOR", "AppEnd_SeparAtor")) {
            cmd = processingCommand("dissect a \"%{foo}\" " + separatorName + "=\",\"");
            assertEquals(Dissect.class, cmd.getClass());
            dissect = (Dissect) cmd;
            assertEquals("%{foo}", dissect.parser().pattern());
            assertEquals(",", dissect.parser().appendSeparator());
            assertEqualsIgnoringIds(List.of(referenceAttribute("foo", KEYWORD)), dissect.extractedFields());
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
        assertEqualsIgnoringIds(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());

        expectThrows(
            ParsingException.class,
            containsString("Invalid pattern [%{_invalid_:x}] for grok: Unable to find pattern [_invalid_] in Grok's pattern dictionary"),
            () -> query("row a = \"foo bar\" | grok a \"%{_invalid_:x}\"")
        );

        cmd = processingCommand("grok a \"%{WORD:foo} %{WORD:foo}\"");
        assertEquals(Grok.class, cmd.getClass());
        grok = (Grok) cmd;
        assertEquals("%{WORD:foo} %{WORD:foo}", grok.parser().pattern());
        assertEqualsIgnoringIds(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());

        expectError(
            "row a = \"foo bar\" | GROK a \"%{NUMBER:foo} %{WORD:foo}\"",
            "line 1:21: Invalid GROK pattern [%{NUMBER:foo} %{WORD:foo}]:"
                + " the attribute [foo] is defined multiple times with different types"
        );

        expectError(
            "row a = \"foo\" | GROK a \"(?P<justification>.+)\"",
            "line 1:24: Invalid GROK pattern [(?P<justification>.+)]: [undefined group option]"
        );

        expectError(
            "row a = \"foo bar\" | GROK a \"%{NUMBER:foo}\", \"%{WORD:foo}\"",
            "line 1:21: Invalid GROK patterns [%{NUMBER:foo}, %{WORD:foo}]:"
                + " the attribute [foo] is defined multiple times with different types"
        );

        expectError(
            "row a = \"foo\" | GROK a \"%{WORD:foo}\", \"(?P<justification>.+)\"",
            "line 1:39: Invalid GROK pattern [(?P<justification>.+)]: [undefined group option]"
        );

        // when combining the pattern, the resulting string could be valid, but the single patterns are invalid
        expectError("""
            ROW a = "foo bar"
            | GROK a "(%{WORD:word}", "x)"
            """, "line 2:10: Invalid GROK pattern [(%{WORD:word}]: [end pattern with unmatched parenthesis]");
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

        expectError("from a | where foo like 12", "no viable alternative at input 'foo like 12'");
        expectError("from a | where foo rlike 12", "no viable alternative at input 'foo rlike 12'");

        expectError(
            "from a | where foo like \"(?i)(^|[^a-zA-Z0-9_-])nmap($|\\\\.)\"",
            "line 1:16: Invalid pattern for LIKE [(?i)(^|[^a-zA-Z0-9_-])nmap($|\\.)]: "
                + "[Invalid sequence - escape character is not followed by special wildcard char]"
        );
    }

    public void testIdentifierPatternTooComplex() {
        // It is incredibly unlikely that we will see this limit hit in practice
        // The repetition value 2450 was a ballpark estimate and validated experimentally
        String explodingWildcard = "a*".repeat(2450);
        expectError("FROM a | KEEP " + explodingWildcard, "Pattern was too complex to determinize");
    }

    public void testEnrich() {
        assertEqualsIgnoringIds(
            new Enrich(
                EMPTY,
                PROCESSING_CMD_INPUT,
                null,
                Literal.keyword(EMPTY, "countries"),
                new EmptyAttribute(EMPTY),
                null,
                Map.of(),
                List.of()
            ),
            processingCommand("enrich countries")
        );

        assertEqualsIgnoringIds(
            new Enrich(
                EMPTY,
                PROCESSING_CMD_INPUT,
                null,
                Literal.keyword(EMPTY, "index-policy"),
                new UnresolvedAttribute(EMPTY, "field_underscore"),
                null,
                Map.of(),
                List.of()
            ),
            processingCommand("enrich index-policy ON field_underscore")
        );

        Enrich.Mode mode = randomFrom(Enrich.Mode.values());
        assertEqualsIgnoringIds(
            new Enrich(
                EMPTY,
                PROCESSING_CMD_INPUT,
                mode,
                Literal.keyword(EMPTY, "countries"),
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
            "from a | enrich countries on foo . * ",
            "Using wildcards [*] in ENRICH WITH projections is not allowed, found [foo.*]"
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
        assertThat(expand.target(), equalToIgnoringIds(attribute("a")));
    }

    // see https://github.com/elastic/elasticsearch/issues/103331
    public void testKeepStarMvExpand() {
        try {
            String query = "from test | keep * | mv_expand a";
            var plan = query(query);
        } catch (UnresolvedException e) {
            fail(e, "Regression: https://github.com/elastic/elasticsearch/issues/103331");
        }

    }

    public void testUsageOfProject() {
        String query = "from test | project foo, bar";
        expectThrows(ParsingException.class, containsString("mismatched input 'project' expecting"), () -> query(query));
    }

    public void testInputParams() {
        LogicalPlan stm = query(
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
        assertThat(alias.child().fold(FoldContext.small()), is(BytesRefs.toBytesRef("2")));

        field = row.fields().get(2);
        assertThat(field.name(), is("a"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is(BytesRefs.toBytesRef("2 days")));

        field = row.fields().get(3);
        assertThat(field.name(), is("b"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()), is(BytesRefs.toBytesRef("4 hours")));

        field = row.fields().get(4);
        assertThat(field.name(), is("c"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()).getClass(), is(BytesRef.class));
        assertThat(alias.child().fold(FoldContext.small()), is(BytesRefs.toBytesRef("1.2.3")));

        field = row.fields().get(5);
        assertThat(field.name(), is("d"));
        assertThat(field, instanceOf(Alias.class));
        alias = (Alias) field;
        assertThat(alias.child().fold(FoldContext.small()).getClass(), is(BytesRef.class));
        assertThat(alias.child().fold(FoldContext.small()), is(BytesRefs.toBytesRef("127.0.0.1")));

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
        LogicalPlan stm = query("row x=?name1, y = ?name1", new QueryParams(List.of(paramAsConstant("name1", 1))));
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
        LogicalPlan stm = query("row x=?1, y=?1", new QueryParams(List.of(paramAsConstant(null, 1))));
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
        LogicalPlan plan = query("from test | where x < ? |  limit 10", new QueryParams(List.of(paramAsConstant(null, 5))));
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

        plan = query("from test | where x < ?n1 |  limit 10", new QueryParams(List.of(paramAsConstant("n1", 5))));
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

        plan = query("from test | where x < ?_n1 |  limit 10", new QueryParams(List.of(paramAsConstant("_n1", 5))));
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

        plan = query("from test | where x < ?1 |  limit 10", new QueryParams(List.of(paramAsConstant(null, 5))));
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

        plan = query("from test | where x < ?__1 |  limit 10", new QueryParams(List.of(paramAsConstant("__1", 5))));
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
        LogicalPlan plan = query(
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

        plan = query(
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

        plan = query(
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

        plan = query(
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

        plan = query(
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
        LogicalPlan plan = query(
            "from test | where x < ? | eval y = ? + ? |  stats count(?) by z",
            new QueryParams(
                List.of(paramAsConstant(null, 5), paramAsConstant(null, -1), paramAsConstant(null, 100), paramAsConstant(null, "*"))
            )
        );
        assertThat(plan, instanceOf(Aggregate.class));
        Aggregate agg = (Aggregate) plan;
        assertThat(((Literal) agg.aggregates().get(0).children().get(0).children().get(0)).value(), equalTo(BytesRefs.toBytesRef("*")));
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

        plan = query(
            "from test | where x < ?n1 | eval y = ?n2 + ?n3 |  stats count(?n4) by z",
            new QueryParams(
                List.of(paramAsConstant("n1", 5), paramAsConstant("n2", -1), paramAsConstant("n3", 100), paramAsConstant("n4", "*"))
            )
        );
        assertThat(plan, instanceOf(Aggregate.class));
        agg = (Aggregate) plan;
        assertThat(((Literal) agg.aggregates().get(0).children().get(0).children().get(0)).value(), equalTo(BytesRefs.toBytesRef("*")));
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

        plan = query(
            "from test | where x < ?_n1 | eval y = ?_n2 + ?_n3 |  stats count(?_n4) by z",
            new QueryParams(
                List.of(paramAsConstant("_n1", 5), paramAsConstant("_n2", -1), paramAsConstant("_n3", 100), paramAsConstant("_n4", "*"))
            )
        );
        assertThat(plan, instanceOf(Aggregate.class));
        agg = (Aggregate) plan;
        assertThat(((Literal) agg.aggregates().get(0).children().get(0).children().get(0)).value(), equalTo(BytesRefs.toBytesRef("*")));
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

        plan = query(
            "from test | where x < ?1 | eval y = ?2 + ?1 |  stats count(?3) by z",
            new QueryParams(List.of(paramAsConstant(null, 5), paramAsConstant(null, -1), paramAsConstant(null, "*")))
        );
        assertThat(plan, instanceOf(Aggregate.class));
        agg = (Aggregate) plan;
        assertThat(((Literal) agg.aggregates().get(0).children().get(0).children().get(0)).value(), equalTo(BytesRefs.toBytesRef("*")));
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

        plan = query(
            "from test | where x < ?_1 | eval y = ?_2 + ?_1 |  stats count(?_3) by z",
            new QueryParams(List.of(paramAsConstant("_1", 5), paramAsConstant("_2", -1), paramAsConstant("_3", "*")))
        );
        assertThat(plan, instanceOf(Aggregate.class));
        agg = (Aggregate) plan;
        assertThat(((Literal) agg.aggregates().get(0).children().get(0).children().get(0)).value(), equalTo(BytesRefs.toBytesRef("*")));
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
        LogicalPlan stm = query(
            "row x = ?1::datetime | eval y = ?1::datetime + ?2::date_period",
            new QueryParams(List.of(paramAsConstant("datetime", "2024-01-01"), paramAsConstant("date_period", "3 days")))
        );
        assertThat(stm, instanceOf(Eval.class));
        Eval eval = (Eval) stm;
        assertThat(eval.fields().size(), is(1));

        NamedExpression field = eval.fields().get(0);
        assertThat(field.name(), is("y"));
        assertThat(field, instanceOf(Alias.class));
        assertThat(
            ((Literal) ((Add) eval.fields().get(0).child()).left().children().get(0)).value(),
            equalTo(BytesRefs.toBytesRef("2024-01-01"))
        );
        assertThat(
            ((Literal) ((Add) eval.fields().get(0).child()).right().children().get(0)).value(),
            equalTo(BytesRefs.toBytesRef("3 days"))
        );
    }

    public void testParamForIdentifier() {
        // TODO will be replaced by testDoubleParamsForIdentifier after providing an identifier with a single parameter marker is deprecated
        // field names can appear in eval/where/stats/sort/keep/drop/rename/dissect/grok/enrich/mvexpand
        // eval, where
        assertEqualsIgnoringIds(
            new Limit(
                EMPTY,
                new Literal(EMPTY, 1, INTEGER),
                new Filter(
                    EMPTY,
                    new Eval(EMPTY, relation("test"), List.of(new Alias(EMPTY, "x", function("toString", List.of(attribute("f1.")))))),
                    new Equals(EMPTY, attribute("f1."), attribute("f.2"))
                )
            ),
            query(
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

        assertEqualsIgnoringIds(
            new Limit(
                EMPTY,
                new Literal(EMPTY, 1, INTEGER),
                new Filter(
                    EMPTY,
                    new Eval(EMPTY, relation("test"), List.of(new Alias(EMPTY, "x", function("toString", List.of(attribute("f1..f.2")))))),
                    new Equals(EMPTY, attribute("f3.*.f.4."), attribute("f.5.*.f.*.6"))
                )
            ),
            query(
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
        assertEqualsIgnoringIds(
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
            query(
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

        assertEqualsIgnoringIds(
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
            query(
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
        LogicalPlan plan = query(
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
        assertEqualsIgnoringIds(rename.renamings(), List.of(new Alias(EMPTY, "f.8", attribute("f7*."))));
        Grok grok = as(rename.child(), Grok.class);
        assertEqualsIgnoringIds(grok.input(), attribute("f.6."));
        assertEquals("%{WORD:foo}", grok.parser().pattern());
        assertEqualsIgnoringIds(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
        Dissect dissect = as(grok.child(), Dissect.class);
        assertEqualsIgnoringIds(dissect.input(), attribute("f.5*"));
        assertEquals("%{bar}", dissect.parser().pattern());
        assertEquals("", dissect.parser().appendSeparator());
        assertEqualsIgnoringIds(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
        Drop drop = as(dissect.child(), Drop.class);
        List<? extends NamedExpression> removals = drop.removals();
        assertEqualsIgnoringIds(removals, List.of(attribute("f3."), attribute("f4.*")));
        Keep keep = as(drop.child(), Keep.class);
        assertEqualsIgnoringIds(keep.projections(), List.of(attribute("f.1.*"), attribute("f.2")));

        plan = query(
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
        assertEqualsIgnoringIds(rename.renamings(), List.of(new Alias(EMPTY, "f11*..f.12", attribute("f.9*.f.10."))));
        grok = as(rename.child(), Grok.class);
        assertEqualsIgnoringIds(grok.input(), attribute("f7*..f.8"));
        assertEquals("%{WORD:foo}", grok.parser().pattern());
        assertEqualsIgnoringIds(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
        dissect = as(grok.child(), Dissect.class);
        assertEqualsIgnoringIds(dissect.input(), attribute("f.5*.f.6."));
        assertEquals("%{bar}", dissect.parser().pattern());
        assertEquals("", dissect.parser().appendSeparator());
        assertEqualsIgnoringIds(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
        drop = as(dissect.child(), Drop.class);
        removals = drop.removals();
        assertEqualsIgnoringIds(removals, List.of(attribute("f3..f4.*")));
        keep = as(drop.child(), Keep.class);
        assertEqualsIgnoringIds(keep.projections(), List.of(attribute("f.1.*.f.2")));

        // enrich
        assertEqualsIgnoringIds(
            new Enrich(
                EMPTY,
                relation("idx1"),
                null,
                Literal.keyword(EMPTY, "idx2"),
                attribute("f.1.*"),
                null,
                Map.of(),
                List.of(new Alias(EMPTY, "f.2", attribute("f.3*")))
            ),
            query(
                "from idx1 | ENRICH idx2 ON ?f1 WITH ?f2 = ?f3",
                new QueryParams(List.of(paramAsIdentifier("f1", "f.1.*"), paramAsIdentifier("f2", "f.2"), paramAsIdentifier("f3", "f.3*")))
            )
        );

        assertEqualsIgnoringIds(
            new Enrich(
                EMPTY,
                relation("idx1"),
                null,
                Literal.keyword(EMPTY, "idx2"),
                attribute("f.1.*.f.2"),
                null,
                Map.of(),
                List.of(new Alias(EMPTY, "f.3*.f.4.*", attribute("f.5.f.6*")))
            ),
            query(
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
        LogicalPlan plan = query(
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
        assertEqualsIgnoringIds(ur, relation("test"));

        plan = query(
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
        assertEqualsIgnoringIds(ur, relation("test"));

        // mixed names and patterns
        plan = query(
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
        assertEqualsIgnoringIds(ur, relation("test"));
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
        assertThat(w.child(), equalToIgnoringIds(PROCESSING_CMD_INPUT));
        assertThat(Expressions.name(w.condition()), equalTo("a.b.1m.4321"));
    }

    public void testFieldQualifiedName() {
        LogicalPlan where = processingCommand("where a.b.`1m`.`4321`");
        assertThat(where, instanceOf(Filter.class));
        Filter w = (Filter) where;
        assertThat(w.child(), equalToIgnoringIds(PROCESSING_CMD_INPUT));
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
            expectThrows(ParsingException.class, containsString("mismatched input 'TS' expecting {"), () -> query(statement));
            return;
        }
        LogicalPlan from = query(statement);
        assertThat(from, instanceOf(UnresolvedRelation.class));
        UnresolvedRelation table = (UnresolvedRelation) from;
        assertThat(table.indexPattern().indexPattern(), is(string));
    }

    private void assertStringAsLookupIndexPattern(String string, String statement) {
        if (Build.current().isSnapshot() == false) {
            expectThrows(
                ParsingException.class,
                containsString("line 1:14: LOOKUP_🐔 is in preview and only available in SNAPSHOT build"),
                () -> query(statement)
            );
            return;
        }
        var plan = query(statement);
        var lookup = as(plan, Lookup.class);
        var tableName = as(lookup.tableName(), Literal.class);
        assertThat(tableName.fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef(string)));
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

    public void testEnrichOnMatchField() {
        var plan = query("ROW a = \"1\" | ENRICH languages_policy ON a WITH ```name``* = language_name`");
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
            expectThrows(ParsingException.class, containsString("line 1:13: mismatched input 'LOOKUP_🐔' expecting {"), () -> query(query));
            return;
        }
        var plan = query(query);
        var lookup = as(plan, Lookup.class);
        var tableName = as(lookup.tableName(), Literal.class);
        assertThat(tableName.fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef("t")));
        assertThat(lookup.matchFields(), hasSize(1));
        var matchField = as(lookup.matchFields().get(0), UnresolvedAttribute.class);
        assertThat(matchField.name(), equalTo("j"));
    }

    public void testInlineConvertUnsupportedType() {
        expectError("ROW 3::BYTE", "line 1:5: Unsupported conversion to type [BYTE]");
    }

    public void testMetricsWithoutStats() {
        assertQuery("TS foo", unresolvedTSRelation("foo"));
        assertQuery("TS foo,bar", unresolvedTSRelation("foo,bar"));
        assertQuery("TS foo*,bar", unresolvedTSRelation("foo*,bar"));
        assertQuery("TS foo-*,bar", unresolvedTSRelation("foo-*,bar"));
        assertQuery("TS foo-*,bar+*", unresolvedTSRelation("foo-*,bar+*"));
    }

    public void testMetricsIdentifiers() {
        Map<String, String> patterns = Map.ofEntries(
            Map.entry("ts foo,test-*", "foo,test-*"),
            Map.entry("ts 123-test@foo_bar+baz1", "123-test@foo_bar+baz1"),
            Map.entry("ts foo,   test,xyz", "foo,test,xyz"),
            Map.entry("ts <logstash-{now/M{yyyy.MM}}>", "<logstash-{now/M{yyyy.MM}}>")
        );
        for (Map.Entry<String, String> e : patterns.entrySet()) {
            assertQuery(e.getKey(), unresolvedTSRelation(e.getValue()));
        }
    }

    public void testSimpleMetricsWithStats() {
        assertQuery(
            "TS foo | STATS load=avg(cpu) BY ts",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo"),
                List.of(attribute("ts")),
                List.of(
                    new Alias(EMPTY, "load", new UnresolvedFunction(EMPTY, "avg", DEFAULT, List.of(attribute("cpu")))),
                    attribute("ts")
                ),
                null,
                new UnresolvedTimestamp(new Source(Location.EMPTY, "STATS load=avg(cpu) BY ts"))
            )
        );
        assertQuery(
            "TS foo,bar | STATS load=avg(cpu) BY ts",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo,bar"),
                List.of(attribute("ts")),
                List.of(
                    new Alias(EMPTY, "load", new UnresolvedFunction(EMPTY, "avg", DEFAULT, List.of(attribute("cpu")))),
                    attribute("ts")
                ),
                null,
                new UnresolvedTimestamp(new Source(Location.EMPTY, "STATS load=avg(cpu) BY ts"))
            )
        );
        assertQuery(
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
                null,
                new UnresolvedTimestamp(new Source(Location.EMPTY, "STATS load=avg(cpu),max(rate(requests)) BY ts"))
            )
        );
        assertQuery(
            "TS foo* | STATS count(errors)",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                List.of(),
                List.of(new Alias(EMPTY, "count(errors)", new UnresolvedFunction(EMPTY, "count", DEFAULT, List.of(attribute("errors"))))),
                null,
                new UnresolvedTimestamp(new Source(Location.EMPTY, "STATS count(errors)"))
            )
        );
        assertQuery(
            "TS foo* | STATS a(b)",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                List.of(),
                List.of(new Alias(EMPTY, "a(b)", new UnresolvedFunction(EMPTY, "a", DEFAULT, List.of(attribute("b"))))),
                null,
                new UnresolvedTimestamp(new Source(Location.EMPTY, "STATS a(b)"))
            )
        );
        assertQuery(
            "TS foo* | STATS a(b)",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                List.of(),
                List.of(new Alias(EMPTY, "a(b)", new UnresolvedFunction(EMPTY, "a", DEFAULT, List.of(attribute("b"))))),
                null,
                new UnresolvedTimestamp(new Source(Location.EMPTY, "STATS a(b)"))
            )
        );
        assertQuery(
            "TS foo* | STATS a1(b2)",
            new TimeSeriesAggregate(
                EMPTY,
                unresolvedTSRelation("foo*"),
                List.of(),
                List.of(new Alias(EMPTY, "a1(b2)", new UnresolvedFunction(EMPTY, "a1", DEFAULT, List.of(attribute("b2"))))),
                null,
                new UnresolvedTimestamp(new Source(Location.EMPTY, "STATS a1(b2)"))
            )
        );
        assertQuery(
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
                null,
                new UnresolvedTimestamp(new Source(Location.EMPTY, "STATS b = min(a) by c, d.e"))
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
        var queries = List.of("TS foo | STATS a BY a");
        for (String query : queries) {
            expectVerificationError(query, "grouping key [a] already specified in the STATS BY clause");
        }
    }

    public void testMatchOperatorConstantQueryString() {
        var plan = query("FROM test | WHERE field:\"value\"");
        var filter = as(plan, Filter.class);
        var match = (MatchOperator) filter.condition();
        var matchField = (UnresolvedAttribute) match.field();
        assertThat(matchField.name(), equalTo("field"));
        assertThat(match.query().fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef("value")));
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
        var plan = query("FROM test | WHERE match(field::int, \"value\")");
        var filter = as(plan, Filter.class);
        var function = (UnresolvedFunction) filter.condition();
        var toInteger = (ToInteger) function.children().get(0);
        var matchField = (UnresolvedAttribute) toInteger.field();
        assertThat(matchField.name(), equalTo("field"));
        assertThat(function.children().get(1).fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef("value")));
    }

    public void testMatchOperatorFieldCasting() {
        var plan = query("FROM test | WHERE field::int : \"value\"");
        var filter = as(plan, Filter.class);
        var match = (MatchOperator) filter.condition();
        var toInteger = (ToInteger) match.field();
        var matchField = (UnresolvedAttribute) toInteger.field();
        assertThat(matchField.name(), equalTo("field"));
        assertThat(match.query().fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef("value")));
    }

    public void testFailingMetadataWithSquareBrackets() {
        expectError("FROM test [METADATA _index] | STATS count(*)", "line 1:11: token recognition error at: '['");
    }

    public void testFunctionNamedParameterInMap() {
        // functions can be scalar, grouping and aggregation
        // functions can be in eval/where/stats/sort/dissect/grok commands, commands in snapshot are not covered
        // positive
        // In eval and where clause as function named parameters
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

        assertEqualsIgnoringIds(
            new Filter(
                EMPTY,
                new Eval(
                    EMPTY,
                    relation("test"),
                    List.of(
                        new Alias(
                            EMPTY,
                            "x",
                            function("fn1", List.of(attribute("f1"), Literal.keyword(EMPTY, "testString"), mapExpression(expectedMap1)))
                        )
                    )
                ),
                new Equals(
                    EMPTY,
                    attribute("y"),
                    function("fn2", List.of(Literal.keyword(EMPTY, "testString"), mapExpression(expectedMap2)))
                )
            ),
            query("""
                from test
                | eval x = fn1(f1, "testString", {"option1":"string","option2":1,"option3":[2.0,3.0,4.0],"option4":[true,false]})
                | where y == fn2("testString", {"option1":["string1","string2"],"option2":[1,2,3],"option3":2.0,"option4":true})
                """)
        );

        // In stats, by and sort as function named parameters
        assertEqualsIgnoringIds(
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
            query("""
                from test
                | stats x = fn1(f1, f2, {"option1":"string","option2":1,"option3":[2.0,3.0,4.0],"option4":[true,false]})
                  by fn2(f3, {"option1":["string1","string2"],"option2":[1,2,3],"option3":2.0,"option4":true})
                | sort fn3(f4, {"option1":"string","option2":2.0,"option3":[1,2,3],"option4":[true,false]})
                """)
        );

        // In dissect and grok as function named parameter
        LogicalPlan plan = query("""
            from test
            | dissect fn1(f1, f2, {"option1":"string", "option2":1,"option3":[2.0,3.0,4.0],"option4":[true,false]}) "%{bar}"
            | grok fn2(f3, {"option1":["string1","string2"],"option2":[1,2,3],"option3":2.0,"option4":true}) "%{WORD:foo}"
            """);
        Grok grok = as(plan, Grok.class);
        assertEqualsIgnoringIds(function("fn2", List.of(attribute("f3"), mapExpression(expectedMap2))), grok.input());
        assertEquals("%{WORD:foo}", grok.parser().pattern());
        assertEqualsIgnoringIds(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
        Dissect dissect = as(grok.child(), Dissect.class);
        assertEqualsIgnoringIds(function("fn1", List.of(attribute("f1"), attribute("f2"), mapExpression(expectedMap1))), dissect.input());
        assertEquals("%{bar}", dissect.parser().pattern());
        assertEquals("", dissect.parser().appendSeparator());
        assertEqualsIgnoringIds(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
        UnresolvedRelation ur = as(dissect.child(), UnresolvedRelation.class);
        assertEqualsIgnoringIds(ur, relation("test"));
    }

    public void testFunctionNamedParameterInMapWithNamedParameters() {
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
        assertEqualsIgnoringIds(
            new Filter(
                EMPTY,
                new Eval(
                    EMPTY,
                    relation("test"),
                    List.of(
                        new Alias(
                            EMPTY,
                            "x",
                            function("fn1", List.of(attribute("f1"), Literal.keyword(EMPTY, "testString"), mapExpression(expectedMap1)))
                        )
                    )
                ),
                new Equals(
                    EMPTY,
                    attribute("y"),
                    function("fn2", List.of(Literal.keyword(EMPTY, "testString"), mapExpression(expectedMap2)))
                )
            ),
            query(
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

        assertEqualsIgnoringIds(
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
            query(
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

        LogicalPlan plan = query(
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
        assertEqualsIgnoringIds(function("fn2", List.of(attribute("f3"), mapExpression(expectedMap2))), grok.input());
        assertEquals("%{WORD:foo}", grok.parser().pattern());
        assertEqualsIgnoringIds(List.of(referenceAttribute("foo", KEYWORD)), grok.extractedFields());
        Dissect dissect = as(grok.child(), Dissect.class);
        assertEqualsIgnoringIds(function("fn1", List.of(attribute("f1"), attribute("f2"), mapExpression(expectedMap1))), dissect.input());
        assertEquals("%{bar}", dissect.parser().pattern());
        assertEquals("", dissect.parser().appendSeparator());
        assertEqualsIgnoringIds(List.of(referenceAttribute("bar", KEYWORD)), dissect.extractedFields());
        UnresolvedRelation ur = as(dissect.child(), UnresolvedRelation.class);
        assertEqualsIgnoringIds(ur, relation("test"));
    }

    public void testFunctionNamedParameterWithCaseSensitiveKeys() {
        LinkedHashMap<String, Object> expectedMap1 = new LinkedHashMap<>(3);
        expectedMap1.put("option", "string");
        expectedMap1.put("Option", 1);
        expectedMap1.put("oPtion", List.of(2.0, 3.0, 4.0));
        LinkedHashMap<String, Object> expectedMap2 = new LinkedHashMap<>(3);
        expectedMap2.put("option", List.of("string1", "string2"));
        expectedMap2.put("Option", List.of(1, 2, 3));
        expectedMap2.put("oPtion", 2.0);

        assertEqualsIgnoringIds(
            new Filter(
                EMPTY,
                new Eval(
                    EMPTY,
                    relation("test"),
                    List.of(
                        new Alias(
                            EMPTY,
                            "x",
                            function("fn1", List.of(attribute("f1"), Literal.keyword(EMPTY, "testString"), mapExpression(expectedMap1)))
                        )
                    )
                ),
                new Equals(
                    EMPTY,
                    attribute("y"),
                    function("fn2", List.of(Literal.keyword(EMPTY, "testString"), mapExpression(expectedMap2)))
                )
            ),
            query("""
                from test
                | eval x = fn1(f1, "testString", {"option":"string","Option":1,"oPtion":[2.0,3.0,4.0]})
                | where y == fn2("testString", {"option":["string1","string2"],"Option":[1,2,3],"oPtion":2.0})
                """)
        );
    }

    public void testMultipleFunctionNamedParametersNotAllowed() {
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

    public void testFunctionNamedParameterNotInMap() {
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

    public void testFunctionNamedParameterNotConstant() {
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
                ? "mismatched input '1' expecting {QUOTED_STRING"
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

    public void testNamedFunctionNamedParametersEmptyMap() {
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

            query(LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {})"));
        }
    }

    public void testNamedFunctionNamedParametersMapWithNULL() {
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
                LoggerMessageFormat.format(null, "line 1:{}: {}", error, "Invalid named parameter [\"option\":null], NULL is not supported")
            );
        }
    }

    public void testNamedFunctionNamedParametersMapWithEmptyKey() {
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
                LoggerMessageFormat.format(null, "line 1:{}: {}", error, "Invalid named parameter [\"\":1], empty key is not supported")
            );
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {\"  \":1})"),
                LoggerMessageFormat.format(null, "line 1:{}: {}", error, "Invalid named parameter [\"  \":1], empty key is not supported")
            );
        }
    }

    public void testNamedFunctionNamedParametersMapWithDuplicatedKey() {
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
                    "Duplicated named parameters with the same name [dup] is not supported"
                )
            );
        }
    }

    public void testNamedFunctionNamedParametersInInvalidPositions() {
        // negative, named arguments are not supported outside of a functionExpression where booleanExpression or indexPattern is supported
        String map = "{\"option1\":\"string\", \"option2\":1}";

        Map<String, String> commands = Map.ofEntries(
            Map.entry("from {}", "line 1:7: mismatched input '\"option1\"' expecting {<EOF>, '|', '::', ',', 'metadata'}"),
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

    public void testNamedFunctionNamedParametersWithUnsupportedNamedParameterTypes() {
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
                    "Invalid named parameter [\"option1\":?n1], only constant value is supported"
                )
            );
            expectError(
                LoggerMessageFormat.format(null, "from test | " + cmd, "fn(f1, {\"option1\":?n1})"),
                List.of(paramAsPattern("n1", "v1")),
                LoggerMessageFormat.format(
                    null,
                    "line 1:{}: {}",
                    error,
                    "Invalid named parameter [\"option1\":?n1], only constant value is supported"
                )
            );
        }
    }

    public void testValidFromPattern() {
        var basePattern = randomIndexPatterns();

        var plan = query("FROM " + basePattern);

        assertThat(as(plan, UnresolvedRelation.class).indexPattern().indexPattern(), equalTo(unquoteIndexPattern(basePattern)));
    }

    public void testInvalidFromPatterns() {
        var sourceCommands = new String[] { "FROM", "TS" };
        var indexIsBlank = "Blank index specified in index pattern";
        var remoteIsEmpty = "remote part is empty";
        var invalidDoubleColonUsage = "invalid usage of :: separator";

        expectError(randomFrom(sourceCommands) + " \"\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \" \"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \",,,\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \",,, \"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \", , ,,\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \",,,\",*", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"*,\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"*,,,\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"index1,,,,\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"index1,index2,,\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"index1,<-+^,index2\",*", "must not contain the following characters");
        expectError(randomFrom(sourceCommands) + " \"\",*", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"*: ,*,\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"*: ,*,\",validIndexName", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"\", \" \", \"  \",validIndexName", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"index1\", \"index2\", \"  ,index3,index4\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"index1,index2,,index3\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"index1,index2,  ,index3\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"*, \"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"*\", \"\"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"*\", \" \"", indexIsBlank);
        expectError(randomFrom(sourceCommands) + " \"*\", \":index1\"", remoteIsEmpty);
        expectError(randomFrom(sourceCommands) + " \"index1,*,:index2\"", remoteIsEmpty);
        expectError(randomFrom(sourceCommands) + " \"*\", \"::data\"", remoteIsEmpty);
        expectError(randomFrom(sourceCommands) + " \"*\", \"::failures\"", remoteIsEmpty);
        expectError(randomFrom(sourceCommands) + " \"*,index1::\"", invalidDoubleColonUsage);
        expectError(randomFrom(sourceCommands) + " \"*\", index1, index2, \"index3:: \"", invalidDoubleColonUsage);
        expectError(randomFrom(sourceCommands) + " \"*,index1::*\"", invalidDoubleColonUsage);
    }

    public void testInvalidPatternsWithIntermittentQuotes() {
        // There are 3 ways of crafting invalid index patterns that conforms to the grammar defined through ANTLR.
        // 1. Not quoting the pattern,
        // 2. Quoting individual patterns ("index1", "index2", ...), and,
        // 3. Clubbing all the patterns into a single quoted string ("index1,index2,...).
        //
        // Note that in these tests, we unquote a pattern and then quote it immediately.
        // This is because when randomly generating an index pattern, it may look like: "foo"::data.
        // To convert it into a quoted string like "foo::data", we need to unquote and then re-quote it.

        // Prohibited char in a quoted cross cluster index pattern should result in an error.
        {
            var randomIndex = randomIndexPattern();
            // Select an invalid char to sneak in.
            // Note: some chars like '|' and '"' are excluded to generate a proper invalid name.
            Character[] invalidChars = { ' ', '/', '<', '>', '?' };
            var randomInvalidChar = randomFrom(invalidChars);

            // Construct the new invalid index pattern.
            var invalidIndexName = "foo" + randomInvalidChar + "bar";
            var remoteIndexWithInvalidChar = quote(randomIdentifier() + ":" + invalidIndexName);
            var query = "FROM " + randomIndex + "," + remoteIndexWithInvalidChar;
            expectError(
                query,
                "Invalid index name ["
                    + invalidIndexName
                    + "], must not contain the following characters [' ','\"',',','/','<','>','?','\\','|']"
            );
        }

        // Colon outside a quoted string should result in an ANTLR error: a comma is expected.
        {
            var randomIndex = randomIndexPattern();

            // In the form of: "*|cluster alias:random string".
            var malformedClusterAlias = quote((randomBoolean() ? "*" : randomIdentifier()) + ":" + randomIdentifier());

            // We do not generate a cross cluster pattern or else we'd be getting a different error (which is tested in
            // the next test).
            var remoteIndex = quote(unquoteIndexPattern(randomIndexPattern(without(CROSS_CLUSTER))));
            // Format: FROM <some index>, "<cluster alias: random string>":<remote index>
            var query = "FROM " + randomIndex + "," + malformedClusterAlias + ":" + remoteIndex;
            expectError(query, " mismatched input ':'");
        }

        // If an explicit cluster string is present, then we expect an unquoted string next.
        {
            var randomIndex = randomIndexPattern();
            var remoteClusterAlias = randomBoolean() ? "*" : randomIdentifier();
            // In the form of: random string:random string.
            var malformedRemoteIndex = quote(unquoteIndexPattern(randomIndexPattern(CROSS_CLUSTER)));
            // Format: FROM <some index>, <cluster alias>:"random string:random string"
            var query = "FROM " + randomIndex + "," + remoteClusterAlias + ":" + malformedRemoteIndex;
            // Since "random string:random string" is partially quoted, expect a ANTLR's parse error.
            expectError(query, "expecting UNQUOTED_SOURCE");
        }

        if (EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled()) {
            // If a stream in on a remote and the cluster alias and index pattern are separately quoted, we should
            // still be able to validate it.
            // Note: invalid selector syntax is covered in a different test.
            {
                var fromPattern = randomIndexPattern();
                var malformedIndexSelectorPattern = quote(randomIdentifier())
                    + ":"
                    + quote(unquoteIndexPattern(randomIndexPattern(INDEX_SELECTOR, without(CROSS_CLUSTER))));
                // Format: FROM <some index>, "<cluster alias>":"<some index>::<data|failures>"
                var query = "FROM " + fromPattern + "," + malformedIndexSelectorPattern;
                // Everything after "<cluster alias>" is extraneous input and hence ANTLR's error.
                expectError(query, "mismatched input ':'");
            }
        }
    }

    public void testInvalidInsistAsterisk() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());
        expectError("FROM text | EVAL x = 4 | INSIST_🐔 *", "INSIST doesn't support wildcards, found [*]");
        expectError("FROM text | EVAL x = 4 | INSIST_🐔 foo*", "INSIST doesn't support wildcards, found [foo*]");
    }

    public void testValidFork() {
        var plan = query("""
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
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", literalString("fork1"))));
        var limit = as(eval.child(), Limit.class);
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(11));
        var filter = as(limit.child(), Filter.class);
        var match = (MatchOperator) filter.condition();
        var matchField = (UnresolvedAttribute) match.field();
        assertThat(matchField.name(), equalTo("a"));
        assertThat(match.query().fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef("baz")));

        // second subplan
        eval = as(subPlans.get(1), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", literalString("fork2"))));
        var orderBy = as(eval.child(), OrderBy.class);
        assertThat(orderBy.order().size(), equalTo(1));
        Order order = orderBy.order().get(0);
        assertThat(order.child(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) order.child()).name(), equalTo("b"));
        filter = as(orderBy.child(), Filter.class);
        match = (MatchOperator) filter.condition();
        matchField = (UnresolvedAttribute) match.field();
        assertThat(matchField.name(), equalTo("b"));
        assertThat(match.query().fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef("bar")));

        // third subplan
        eval = as(subPlans.get(2), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", literalString("fork3"))));
        filter = as(eval.child(), Filter.class);
        match = (MatchOperator) filter.condition();
        matchField = (UnresolvedAttribute) match.field();
        assertThat(matchField.name(), equalTo("c"));
        assertThat(match.query().fold(FoldContext.small()), equalTo(BytesRefs.toBytesRef("bat")));

        // fourth subplan
        eval = as(subPlans.get(3), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", literalString("fork4"))));
        orderBy = as(eval.child(), OrderBy.class);
        assertThat(orderBy.order().size(), equalTo(1));
        order = orderBy.order().get(0);
        assertThat(order.child(), instanceOf(UnresolvedAttribute.class));
        assertThat(((UnresolvedAttribute) order.child()).name(), equalTo("c"));

        // fifth subplan
        eval = as(subPlans.get(4), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", literalString("fork5"))));
        limit = as(eval.child(), Limit.class);
        assertThat(limit.limit(), instanceOf(Literal.class));
        assertThat(((Literal) limit.limit()).value(), equalTo(5));

        // sixth subplan
        eval = as(subPlans.get(5), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("_fork", literalString("fork6"))));
        eval = as(eval.child(), Eval.class);
        assertThat(as(eval.fields().get(0), Alias.class), equalToIgnoringIds(alias("xyz", literalString("abc"))));

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

    public void testForkAllReleasedCommands() {
        var query = """
            FROM foo*
            | FORK
               ( SORT c )
               ( LIMIT 5 )
               ( DISSECT a "%{d} %{e} %{f}" )
               ( GROK a "%{WORD:foo}" )
               ( STATS x = MIN(a), y = MAX(b) WHERE d > 1000 )
               ( EVAL xyz = ( (a/b) * (b/a)) )
               ( WHERE a < 1 )
               ( KEEP a )
            | KEEP a
            """;

        var plan = query(query);
        assertThat(plan, instanceOf(Keep.class));

        query = """
            FROM foo*
            | FORK
               ( RENAME a as c )
               ( MV_EXPAND a )
               ( CHANGE_POINT a on b )
               ( LOOKUP JOIN idx2 ON f1 )
               ( ENRICH idx2 on f1 with f2 = f3 )
               ( FORK ( WHERE a:"baz" ) ( EVAL x = [ 1, 2, 3 ] ) )
               ( COMPLETION a=b WITH { "inference_id": "c" } )
            | KEEP a
            """;

        plan = query(query);
        assertThat(plan, instanceOf(Keep.class));
    }

    public void testForkAllCommands() {
        assumeTrue("requires snapshot build", Build.current().isSnapshot());

        var query = """
            FROM foo*
            | FORK
               ( SORT c )
               ( LIMIT 5 )
               ( DISSECT a "%{d} %{e} %{f}" )
               ( GROK a "%{WORD:foo}" )
               ( STATS x = MIN(a), y = MAX(b) WHERE d > 1000 )
               ( EVAL xyz = ( (a/b) * (b/a)) )
               ( WHERE a < 1 )
               ( KEEP a )

            | KEEP a
            """;
        var plan = query(query);
        assertThat(plan, instanceOf(Keep.class));

        query = """
            FROM foo*
            | FORK
               ( RENAME a as c )
               ( MV_EXPAND a )
               ( CHANGE_POINT a on b )
               ( LOOKUP JOIN idx2 ON f1 | LOOKUP JOIN idx3 ON f1 > f3)
               ( ENRICH idx2 on f1 with f2 = f3 )
               ( FORK ( WHERE a:"baz" ) ( EVAL x = [ 1, 2, 3 ] ) )
               ( COMPLETION a=b WITH { "inference_id": "c" } )
               ( SAMPLE 0.99 )
            | KEEP a
            """;
        plan = query(query);
        assertThat(plan, instanceOf(Keep.class));

        query = """
            FROM foo*
            | FORK
               ( INLINE STATS x = MIN(a), y = MAX(b) WHERE d > 1000 )
               ( INSIST_🐔 a )
               ( LOOKUP_🐔 a on b )
            | KEEP a
            """;
        plan = query(query);
        assertThat(plan, instanceOf(Keep.class));
    }

    public void testInvalidFork() {
        expectError("""
            FROM foo* | FORK
            """, "line 2:1: mismatched input '<EOF>' expecting '('");
        expectError("""
            FROM foo* | FORK ()
            """, "line 1:19: mismatched input ')'");

        expectError("""
            FROM foo*
            | FORK (where true) (where true) (where true) (where true)
                   (where true) (where true) (where true) (where true)
                   (where true)
            """, "Fork supports up to 8 branches");

        expectError("FROM foo* | FORK ( x+1 ) ( WHERE y>2 )", "line 1:20: mismatched input 'x+1'");
        expectError("FROM foo* | FORK ( LIMIT 10 ) ( y+2 )", "line 1:33: mismatched input 'y+2'");
        expectError("FROM foo* | FORK (where true) ()", "line 1:32: mismatched input ')'");
        expectError("FROM foo* | FORK () (where true)", "line 1:19: mismatched input ')'");
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
            var plan = query("FROM test | STATS avg(" + keyword + ")");
            var aggregate = as(plan, Aggregate.class);
        }
    }

    // [ and ( are used to trigger a double mode causing their symbol name (instead of text) to be used in error reporting
    // this test checks that they are properly replaced in the error message
    public void testPreserveParentheses() {
        // test for (
        expectError("row a = 1 not in", "line 1:17: mismatched input '<EOF>' expecting '('");
        expectError("row a = 1 | where a not in", "line 1:27: mismatched input '<EOF>' expecting '('");
        expectError("row a = 1 | where a not in (1", "line 1:30: mismatched input '<EOF>' expecting {',', ')'}");
        expectError("row a = 1 | where a not in [1", "line 1:28: missing '(' at '['");
        expectError("row a = 1 | where a not in 123", "line 1:28: missing '(' at '123'");
        // test for [
        if (EsqlCapabilities.Cap.EXPLAIN.isEnabled()) {
            expectError("explain", "line 1:8: mismatched input '<EOF>' expecting '('");
            expectError("explain ]", "line 1:9: token recognition error at: ']'");
            expectError("explain ( row x = 1", "line 1:20: missing ')' at '<EOF>'");
        }
    }

    public void testExplainErrors() {
        assumeTrue("Requires EXPLAIN capability", EsqlCapabilities.Cap.EXPLAIN.isEnabled());
        // TODO this one is incorrect
        expectError("explain ( from test ) | limit 1", "line 1:1: EXPLAIN does not support downstream commands");
        expectError(
            "explain (row x=\"Elastic\" | eval y=concat(x,to_upper(\"search\"))) | mv_expand y",
            "line 1:1: EXPLAIN does not support downstream commands"
        );
    }

    public void testRerankDefaultInferenceIdAndScoreAttribute() {
        var plan = processingCommand("RERANK \"statement text\" ON title");
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.inferenceId(), equalTo(literalString(".rerank-v1-elasticsearch")));
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("_score")));
        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.rerankFields(), equalToIgnoringIds(List.of(alias("title", attribute("title")))));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankEmptyOptions() {
        var plan = processingCommand("RERANK \"statement text\" ON title WITH {}");
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.inferenceId(), equalTo(literalString(".rerank-v1-elasticsearch")));
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("_score")));
        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.rerankFields(), equalToIgnoringIds(List.of(alias("title", attribute("title")))));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankInferenceId() {
        var plan = processingCommand("RERANK \"statement text\" ON title WITH { \"inference_id\" : \"inferenceId\" }");
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.inferenceId(), equalTo(literalString("inferenceId")));
        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.rerankFields(), equalToIgnoringIds(List.of(alias("title", attribute("title")))));
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("_score")));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankScoreAttribute() {
        var plan = processingCommand("RERANK rerank_score=\"statement text\" ON title");
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.inferenceId(), equalTo(literalString(".rerank-v1-elasticsearch")));
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("rerank_score")));
        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.rerankFields(), equalToIgnoringIds(List.of(alias("title", attribute("title")))));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankInferenceIdAnddScoreAttribute() {
        var plan = processingCommand("RERANK rerank_score=\"statement text\" ON title WITH { \"inference_id\" : \"inferenceId\" }");
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.inferenceId(), equalTo(literalString("inferenceId")));
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("rerank_score")));
        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.rerankFields(), equalToIgnoringIds(List.of(alias("title", attribute("title")))));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankSingleField() {
        var plan = processingCommand("RERANK \"statement text\" ON title WITH { \"inference_id\" : \"inferenceID\" }");
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(rerank.rerankFields(), equalToIgnoringIds(List.of(alias("title", attribute("title")))));
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("_score")));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankMultipleFields() {
        var plan = processingCommand(
            "RERANK \"statement text\" ON title, description, authors_renamed=authors WITH { \"inference_id\" : \"inferenceID\" }"
        );
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(
            rerank.rerankFields(),
            equalToIgnoringIds(
                List.of(
                    alias("title", attribute("title")),
                    alias("description", attribute("description")),
                    alias("authors_renamed", attribute("authors"))
                )
            )
        );
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("_score")));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankComputedFields() {
        var plan = processingCommand("""
            RERANK "statement text" ON title, short_description = SUBSTRING(description, 0, 100) WITH { "inference_id": "inferenceID" }
            """);
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(
            rerank.rerankFields(),
            equalToIgnoringIds(
                List.of(
                    alias("title", attribute("title")),
                    alias("short_description", function("SUBSTRING", List.of(attribute("description"), integer(0), integer(100))))
                )
            )
        );
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("_score")));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankComputedFieldsWithoutName() {
        var plan = processingCommand("""
            RERANK "statement text" ON title, SUBSTRING(description, 0, 100), yearRenamed=year WITH { "inference_id": "inferenceID" }
            """);
        var rerank = as(plan, Rerank.class);

        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(
            rerank.rerankFields(),
            equalToIgnoringIds(
                List.of(
                    alias("title", attribute("title")),
                    alias(
                        "SUBSTRING(description, 0, 100)",
                        function("SUBSTRING", List.of(attribute("description"), integer(0), integer(100)))
                    ),
                    alias("yearRenamed", attribute("year"))
                )
            )
        );
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("_score")));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankWithPositionalParameters() {
        var queryParams = new QueryParams(List.of(paramAsConstant(null, "statement text"), paramAsConstant(null, "reranker")));
        var rerank = as(
            parser.parseQuery("row a = 1 | RERANK rerank_score = ? ON title WITH { \"inference_id\" : ? }", queryParams),
            Rerank.class
        );

        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("reranker")));
        assertThat(rerank.rerankFields(), equalToIgnoringIds(List.of(alias("title", attribute("title")))));
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("rerank_score")));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankWithNamedParameters() {
        var queryParams = new QueryParams(
            List.of(paramAsConstant("queryText", "statement text"), paramAsConstant("inferenceId", "reranker"))
        );
        var rerank = as(
            parser.parseQuery("row a = 1 | RERANK rerank_score=?queryText ON title WITH { \"inference_id\": ?inferenceId }", queryParams),
            Rerank.class
        );

        assertThat(rerank.queryText(), equalTo(literalString("statement text")));
        assertThat(rerank.inferenceId(), equalTo(literalString("reranker")));
        assertThat(rerank.rerankFields(), equalToIgnoringIds(List.of(alias("title", attribute("title")))));
        assertThat(rerank.scoreAttribute(), equalToIgnoringIds(attribute("rerank_score")));
        assertThat(rerank.rowLimit(), equalTo(integer(1_000)));
    }

    public void testRerankRowLimitOverride() {
        int customRowLimit = between(1, 10_000);
        Settings settings = Settings.builder().put(InferenceSettings.RERANK_ROW_LIMIT_SETTING.getKey(), customRowLimit).build();

        var plan = as(
            processingCommand("RERANK \"query text\" ON title WITH { \"inference_id\" : \"inferenceID\" }", new QueryParams(), settings),
            Rerank.class
        );

        assertThat(plan.rowLimit(), equalTo(Literal.integer(EMPTY, customRowLimit)));
    }

    public void testCompletionWithTaskSettings() {
        var plan = as(
            processingCommand(
                "COMPLETION prompt_field WITH { \"inference_id\" : \"inferenceID\", \"task_settings\": {\"temperature\": 0.5 } }"
            ),
            Completion.class
        );

        assertThat(plan.prompt(), equalToIgnoringIds(attribute("prompt_field")));
        assertThat(plan.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(plan.targetField(), equalToIgnoringIds(attribute("completion")));
        assertThat(plan.rowLimit(), equalTo(integer(100)));

        MapExpression taskSettings = plan.taskSettings();
        assertThat(taskSettings.get("temperature"), equalTo(Literal.fromDouble(null, 0.5)));
    }

    public void testCompletionWithEmptyTaskSettings() {
        var plan = as(
            processingCommand("COMPLETION prompt_field WITH { \"inference_id\" : \"inferenceID\", \"task_settings\": {} }"),
            Completion.class
        );

        assertThat(plan.prompt(), equalToIgnoringIds(attribute("prompt_field")));
        assertThat(plan.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(plan.taskSettings(), equalTo(new MapExpression(Source.EMPTY, List.of())));
    }

    public void testCompletionWithMultipleTaskSettings() {
        var plan = as(
            processingCommand(
                "COMPLETION prompt_field WITH { \"inference_id\" : \"inferenceID\", "
                    + "\"task_settings\": {\"foo\": \"bar\", \"baz\": \"qux\"} }"
            ),
            Completion.class
        );

        MapExpression taskSettings = plan.taskSettings();
        assertThat(taskSettings.get("foo"), equalTo(literalString("bar")));
        assertThat(taskSettings.get("baz"), equalTo(literalString("qux")));
    }

    public void testCompletionWithNestedTaskSettings() {
        var plan = as(
            processingCommand(
                "COMPLETION prompt_field WITH { \"inference_id\" : \"inferenceID\", "
                    + "\"task_settings\": {\"nested_map\": {\"foo\": \"bar\"}} }"
            ),
            Completion.class
        );

        MapExpression taskSettings = plan.taskSettings();
        MapExpression nestedMap = (MapExpression) taskSettings.get("nested_map");
        assertThat(nestedMap.get("foo"), equalTo(literalString("bar")));
    }

    public void testCompletionInvalidTaskSettingsType() {
        expectError(
            "FROM foo* | COMPLETION prompt WITH { \"inference_id\": \"inferenceId\", \"task_settings\": 3 }",
            "Option [task_settings] must be a map, found [3]"
        );
    }

    public void testCompletionTaskSettingsNull() {
        expectError(
            "FROM foo* | COMPLETION prompt WITH { \"inference_id\": \"inferenceId\", \"task_settings\": null }",
            "Invalid named parameter [\"task_settings\":null], NULL is not supported"
        );
    }

    public void testCompletionTaskSettingsNotAMap() {
        expectError(
            "FROM foo* | COMPLETION prompt WITH { \"inference_id\": \"inferenceId\", \"task_settings\": \"not_a_valid_map\" }",
            "Option [task_settings] must be a map, found [\"not_a_valid_map\"]"
        );
    }

    public void testRerankCommandDisabled() {
        Settings settings = Settings.builder().put(InferenceSettings.RERANK_ENABLED_SETTING.getKey(), false).build();

        ParsingException pe = expectThrows(
            ParsingException.class,
            () -> processingCommand("RERANK \"query text\" ON title", new QueryParams(), settings)
        );
        assertThat(pe.getMessage(), containsString("RERANK command is disabled"));
    }

    public void testInvalidRerank() {
        expectError(
            "FROM foo* | RERANK \"query text\" ON title WITH { \"inference_id\": 3 }",
            "line 1:65: Option [inference_id] must be a valid string, found [3]"
        );
        expectError(
            "FROM foo* | RERANK \"query text\" ON title WITH { \"inference_id\": \"inferenceId\", \"unknown_option\": 3 }",
            "line 1:42: Invalid option [unknown_option] in RERANK, expected one of [[inference_id]]"
        );
        expectError("FROM foo* | RERANK ON title WITH inferenceId", "line 1:20: extraneous input 'ON' expecting {QUOTED_STRING");
        expectError("FROM foo* | RERANK \"query text\" WITH inferenceId", "line 1:33: mismatched input 'WITH' expecting 'on'");

        expectError(
            "FROM foo* | RERANK \"query text\" ON title WITH { \"inference_id\": { \"a\": 123 } }",
            "Option [inference_id] must be a valid string, found [{ \"a\": 123 }]"
        );
    }

    public void testCompletionMissingOptions() {
        expectError("FROM foo* | COMPLETION targetField = prompt", "line 1:13: Missing mandatory option [inference_id] in COMPLETION");
    }

    public void testCompletionEmptyOptions() {
        expectError(
            "FROM foo* | COMPLETION targetField = prompt WITH { }",
            "line 1:13: Missing mandatory option [inference_id] in COMPLETION"
        );
    }

    public void testCompletionUsingFieldAsPrompt() {
        var plan = as(
            processingCommand("COMPLETION targetField=prompt_field WITH{ \"inference_id\" : \"inferenceID\" }"),
            Completion.class
        );

        assertThat(plan.prompt(), equalToIgnoringIds(attribute("prompt_field")));
        assertThat(plan.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(plan.targetField(), equalToIgnoringIds(attribute("targetField")));
        assertThat(plan.rowLimit(), equalTo(integer(100)));

    }

    public void testCompletionUsingFunctionAsPrompt() {
        var plan = as(
            processingCommand("COMPLETION targetField=CONCAT(fieldA, fieldB) WITH { \"inference_id\" : \"inferenceID\" }"),
            Completion.class
        );

        assertThat(plan.prompt(), equalToIgnoringIds(function("CONCAT", List.of(attribute("fieldA"), attribute("fieldB")))));
        assertThat(plan.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(plan.targetField(), equalToIgnoringIds(attribute("targetField")));
        assertThat(plan.rowLimit(), equalTo(integer(100)));
    }

    public void testCompletionDefaultFieldName() {
        var plan = as(processingCommand("COMPLETION prompt_field WITH{ \"inference_id\" : \"inferenceID\" }"), Completion.class);

        assertThat(plan.prompt(), equalToIgnoringIds(attribute("prompt_field")));
        assertThat(plan.inferenceId(), equalTo(literalString("inferenceID")));
        assertThat(plan.targetField(), equalToIgnoringIds(attribute("completion")));
        assertThat(plan.rowLimit(), equalTo(integer(100)));
    }

    public void testCompletionWithPositionalParameters() {
        var queryParams = new QueryParams(List.of(paramAsConstant(null, "inferenceId")));
        var plan = as(
            parser.parseQuery("row a = 1 | COMPLETION prompt_field WITH { \"inference_id\" : ? }", queryParams),
            Completion.class
        );

        assertThat(plan.prompt(), equalToIgnoringIds(attribute("prompt_field")));
        assertThat(plan.inferenceId(), equalTo(literalString("inferenceId")));
        assertThat(plan.targetField(), equalToIgnoringIds(attribute("completion")));
        assertThat(plan.rowLimit(), equalTo(integer(100)));
    }

    public void testCompletionWithNamedParameters() {
        var queryParams = new QueryParams(List.of(paramAsConstant("inferenceId", "myInference")));
        var plan = as(
            parser.parseQuery("row a = 1 | COMPLETION prompt_field WITH { \"inference_id\" : ?inferenceId }", queryParams),
            Completion.class
        );

        assertThat(plan.prompt(), equalToIgnoringIds(attribute("prompt_field")));
        assertThat(plan.inferenceId(), equalTo(literalString("myInference")));
        assertThat(plan.targetField(), equalToIgnoringIds(attribute("completion")));
        assertThat(plan.rowLimit(), equalTo(integer(100)));
    }

    public void testCompletionRowLimitOverride() {
        int customRowLimit = between(1, 10_000);
        Settings settings = Settings.builder().put(InferenceSettings.COMPLETION_ROW_LIMIT_SETTING.getKey(), customRowLimit).build();

        var plan = as(
            processingCommand("COMPLETION prompt_field WITH{ \"inference_id\" : \"inferenceID\" }", new QueryParams(), settings),
            Completion.class
        );

        assertThat(plan.rowLimit(), equalTo(Literal.integer(EMPTY, customRowLimit)));
    }

    public void testCompletionCommandDisabled() {
        Settings settings = Settings.builder().put(InferenceSettings.COMPLETION_ENABLED_SETTING.getKey(), false).build();

        ParsingException pe = expectThrows(
            ParsingException.class,
            () -> processingCommand("COMPLETION prompt_field WITH{ \"inference_id\" : \"inferenceID\" }", new QueryParams(), settings)
        );
        assertThat(pe.getMessage(), containsString("COMPLETION command is disabled"));
    }

    public void testInvalidCompletion() {
        expectError(
            "FROM foo* | COMPLETION prompt WITH { \"inference_id\": 3 }",
            "line 1:54: Option [inference_id] must be a valid string, found [3]"
        );
        expectError(
            "FROM foo* | COMPLETION prompt WITH { \"inference_id\": \"inferenceId\", \"unknown_option\": 3 }",
            "line 1:31: Invalid option [unknown_option] in COMPLETION, expected one of [[inference_id, task_settings]]"
        );

        expectError("FROM foo* | COMPLETION WITH inferenceId", "line 1:24: extraneous input 'WITH' expecting {");

        expectError("FROM foo* | COMPLETION completion=prompt WITH", "ine 1:46: mismatched input '<EOF>' expecting '{'");

        expectError(
            "FROM foo* | COMPLETION prompt WITH { \"inference_id\": { \"a\": 123 } }",
            "line 1:54: Option [inference_id] must be a valid string, found [{ \"a\": 123 }]"
        );
    }

    public void testSample() {
        assumeTrue("SAMPLE requires corresponding capability", EsqlCapabilities.Cap.SAMPLE_V3.isEnabled());
        expectError("FROM test | SAMPLE .1 2", "line 1:23: extraneous input '2' expecting <EOF>");
        expectError("FROM test | SAMPLE .1 \"2\"", "line 1:23: extraneous input '\"2\"' expecting <EOF>");
        expectError(
            "FROM test | SAMPLE 1",
            "line 1:13: invalid value for SAMPLE probability [1], expecting a number between 0 and 1, exclusive"
        );
        expectThrows(
            ParsingException.class,
            startsWith("line 1:19: mismatched input '<EOF>' expecting {"),
            () -> query("FROM test | SAMPLE")
        );
    }

    static Alias alias(String name, Expression value) {
        return new Alias(EMPTY, name, value);
    }

    public void testValidFuse() {
        LogicalPlan plan = query("""
                FROM foo* METADATA _id, _index, _score
                | FORK ( WHERE a:"baz" )
                       ( WHERE b:"bar" )
                | FUSE
            """);

        var fuse = as(plan, Fuse.class);
        assertThat(fuse.keys().size(), equalTo(2));
        assertThat(fuse.keys().get(0), instanceOf(UnresolvedAttribute.class));
        assertThat(fuse.keys().get(0).name(), equalTo("_id"));
        assertThat(fuse.keys().get(1), instanceOf(UnresolvedAttribute.class));
        assertThat(fuse.keys().get(1).name(), equalTo("_index"));
        assertThat(fuse.discriminator().name(), equalTo("_fork"));
        assertThat(fuse.score().name(), equalTo("_score"));
        assertThat(fuse.fuseType(), equalTo(Fuse.FuseType.RRF));

        assertThat(fuse.child(), instanceOf(Fork.class));

        plan = query("""
                FROM foo* METADATA _id, _index, _score
                | FORK ( WHERE a:"baz" )
                       ( WHERE b:"bar" )
                | FUSE RRF
            """);

        fuse = as(plan, Fuse.class);
        assertThat(fuse.fuseType(), equalTo(Fuse.FuseType.RRF));
        assertThat(fuse.child(), instanceOf(Fork.class));

        plan = query("""
                FROM foo* METADATA _id, _index, _score
                | FORK ( WHERE a:"baz" )
                       ( WHERE b:"bar" )
                | FUSE LINEAR
            """);

        fuse = as(plan, Fuse.class);
        assertThat(fuse.fuseType(), equalTo(Fuse.FuseType.LINEAR));

        assertThat(fuse.child(), instanceOf(Fork.class));

        plan = query("""
                FROM foo* METADATA _id, _index, _score
                | FORK ( WHERE a:"baz" )
                       ( WHERE b:"bar" )
                | FUSE WITH {"rank_constant": 15, "weights": {"fork1": 0.33 } }
            """);

        fuse = as(plan, Fuse.class);
        assertThat(fuse.fuseType(), equalTo(Fuse.FuseType.RRF));
        MapExpression options = fuse.options();
        assertThat(options.get("rank_constant"), equalTo(Literal.integer(null, 15)));
        assertThat(options.get("weights"), instanceOf(MapExpression.class));
        assertThat(((MapExpression) options.get("weights")).get("fork1"), equalTo(Literal.fromDouble(null, 0.33)));

        assertThat(fuse.child(), instanceOf(Fork.class));

        plan = query("""
                FROM foo* METADATA _id, _index, _score
                | FORK ( WHERE a:"baz" )
                       ( WHERE b:"bar" )
                | FUSE SCORE BY my_score KEY BY my_key1,my_key2 GROUP BY my_group WITH {"rank_constant": 15 }
            """);

        fuse = as(plan, Fuse.class);
        assertThat(fuse.keys().size(), equalTo(2));
        assertThat(fuse.keys().get(0), instanceOf(UnresolvedAttribute.class));
        assertThat(fuse.keys().get(0).name(), equalTo("my_key1"));
        assertThat(fuse.keys().get(1), instanceOf(UnresolvedAttribute.class));
        assertThat(fuse.keys().get(1).name(), equalTo("my_key2"));
        assertThat(fuse.discriminator().name(), equalTo("my_group"));
        assertThat(fuse.score().name(), equalTo("my_score"));
        assertThat(fuse.fuseType(), equalTo(Fuse.FuseType.RRF));
        options = fuse.options();
        assertThat(options.get("rank_constant"), equalTo(Literal.integer(null, 15)));

        plan = query("""
                FROM foo* METADATA _id, _index, _score
                | FORK ( WHERE a:"baz" )
                       ( WHERE b:"bar" )
                | FUSE GROUP BY my_group KEY BY my_key1,my_key2 SCORE BY my_score WITH {"rank_constant": 15 }
            """);

        fuse = as(plan, Fuse.class);
        assertThat(fuse.keys().size(), equalTo(2));
        assertThat(fuse.keys().get(0), instanceOf(UnresolvedAttribute.class));
        assertThat(fuse.keys().get(0).name(), equalTo("my_key1"));
        assertThat(fuse.keys().get(1), instanceOf(UnresolvedAttribute.class));
        assertThat(fuse.keys().get(1).name(), equalTo("my_key2"));
        assertThat(fuse.discriminator().name(), equalTo("my_group"));
        assertThat(fuse.score().name(), equalTo("my_score"));
        assertThat(fuse.fuseType(), equalTo(Fuse.FuseType.RRF));
        options = fuse.options();
        assertThat(options.get("rank_constant"), equalTo(Literal.integer(null, 15)));

        plan = query("""
                FROM foo* METADATA _id, _index, _score
                | EVAL a.b = my_group
                | FORK ( WHERE a:"baz" )
                       ( WHERE b:"bar" )
                | FUSE GROUP BY a.b KEY BY my_key1,my_key2 SCORE BY my_score WITH {"rank_constant": 15 }
            """);

        fuse = as(plan, Fuse.class);
        assertThat(fuse.keys().size(), equalTo(2));
        assertThat(fuse.keys().get(0), instanceOf(UnresolvedAttribute.class));
        assertThat(fuse.keys().get(0).name(), equalTo("my_key1"));
        assertThat(fuse.keys().get(1), instanceOf(UnresolvedAttribute.class));
        assertThat(fuse.keys().get(1).name(), equalTo("my_key2"));
        assertThat(fuse.discriminator().name(), equalTo("a.b"));
        assertThat(fuse.score().name(), equalTo("my_score"));
        assertThat(fuse.fuseType(), equalTo(Fuse.FuseType.RRF));
        options = fuse.options();
        assertThat(options.get("rank_constant"), equalTo(Literal.integer(null, 15)));

        plan = query("""
                FROM foo* METADATA _id, _index, _score
                | EVAL ??p = my_group
                | FORK ( WHERE a:"baz" )
                       ( WHERE b:"bar" )
                | FUSE GROUP BY ??p KEY BY my_key1,my_key2 SCORE BY my_score WITH {"rank_constant": 15 }
            """, new QueryParams(List.of(paramAsConstant("p", "a.b"))));

        fuse = as(plan, Fuse.class);
        assertThat(fuse.keys().size(), equalTo(2));
        assertThat(fuse.keys().get(0), instanceOf(UnresolvedAttribute.class));
        assertThat(fuse.keys().get(0).name(), equalTo("my_key1"));
        assertThat(fuse.keys().get(1), instanceOf(UnresolvedAttribute.class));
        assertThat(fuse.keys().get(1).name(), equalTo("my_key2"));
        assertThat(fuse.discriminator().name(), equalTo("a.b"));
        assertThat(fuse.score().name(), equalTo("my_score"));
        assertThat(fuse.fuseType(), equalTo(Fuse.FuseType.RRF));
        options = fuse.options();
        assertThat(options.get("rank_constant"), equalTo(Literal.integer(null, 15)));
    }

    public void testInvalidFuse() {
        String queryPrefix = "from test metadata _score, _index, _id | fork (where true) (where true)";

        expectError(queryPrefix + " | FUSE BLA", "line 1:75: Fuse type BLA is not supported");

        expectError(queryPrefix + " | FUSE WITH 1", "line 1:85: mismatched input '1' expecting '{'");

        expectError(
            queryPrefix + " | FUSE  WITH {\"rank_constant\": 15 }  WITH {\"rank_constant\": 15 }",
            "line 1:110: Only one WITH can be specified"
        );

        expectError(queryPrefix + " | FUSE GROUP BY foo SCORE BY my_score GROUP BY bar", "line 1:111: Only one GROUP BY can be specified");

        expectError(
            queryPrefix + " | FUSE SCORE BY my_score GROUP BY bar SCORE BY another_score",
            "line 1:111: Only one SCORE BY can be specified"
        );

        expectError(queryPrefix + " | FUSE KEY BY bar SCORE BY another_score KEY BY bar", "line 1:114: Only one KEY BY can be specified");

        expectError(queryPrefix + " | FUSE GROUP BY foo SCORE BY my_score LINEAR", "line 1:111: extraneous input 'LINEAR' expecting <EOF>");

        expectError(queryPrefix + " | FUSE KEY BY CONCAT(key1, key2)", "line 1:93: token recognition error at: '('");
    }

    public void testUnclosedParenthesis() {
        String[] queries = {
            "row a = )",
            "row ]",
            "from source | eval x = [1,2,3]]",
            "ROW x = 1 | KEEP x )",
            "ROW x = 1 | DROP x )",
            "ROW a = [1, 2] | RENAME a =b)",
            "ROW a = [1, 2] | MV_EXPAND a)",
            "from test | enrich a on b)" };
        for (String q : queries) {
            expectError(q, "Invalid query");
        }
    }

    public void testBracketsInIndexNames() {

        List<String> patterns = List.of(
            "(",
            ")",
            "()",
            "(((",
            ")))",
            "(test",
            "test)",
            "(test)",
            "te()st",
            "concat(foo,bar)",
            "((((()))))",
            "(((abc)))",
            "*()*",
            "*test()*"
        );

        for (String pattern : patterns) {
            expectErrorForBracketsWithoutQuotes(pattern);
            expectSuccessForBracketsWithinQuotes(pattern);
        }

        expectError("from test)", "line -1:-1: Invalid query [from test)]");
        expectError("from te()st", "line 1:8: mismatched input '(' expecting {<EOF>, '|', '::', ',', 'metadata'");
        expectError("from test | enrich foo)", "line -1:-1: Invalid query [from test | enrich foo)]");
        expectError("from test | lookup join foo) on bar", "line 1:28: token recognition error at: ')'");
        if (EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()) {
            expectError("from test | lookup join foo) on bar1 > bar2", "line 1:28: token recognition error at: ')'");
        }
    }

    private void expectErrorForBracketsWithoutQuotes(String pattern) {
        expectThrows(ParsingException.class, () -> processingCommand("from " + pattern));

        expectThrows(ParsingException.class, () -> processingCommand("from *:" + pattern));

        expectThrows(ParsingException.class, () -> processingCommand("from remote1:" + pattern + ",remote2:" + pattern));

        expectThrows(ParsingException.class, () -> processingCommand("from test | lookup join " + pattern + " on bar"));
        expectThrows(ParsingException.class, () -> processingCommand("from test | lookup join " + pattern + " on bar1  < bar2"));

        expectThrows(ParsingException.class, () -> processingCommand("from test | enrich " + pattern));
    }

    private void expectSuccessForBracketsWithinQuotes(String indexName) {
        LogicalPlan plan = query("from \"" + indexName + "\"");
        UnresolvedRelation from = as(plan, UnresolvedRelation.class);
        assertThat(from.indexPattern().indexPattern(), is(indexName));

        plan = query("from \"*:" + indexName + "\"");
        from = as(plan, UnresolvedRelation.class);
        assertThat(from.indexPattern().indexPattern(), is("*:" + indexName));

        plan = query("from \"remote1:" + indexName + ",remote2:" + indexName + "\"");
        from = as(plan, UnresolvedRelation.class);
        assertThat(from.indexPattern().indexPattern(), is("remote1:" + indexName + ",remote2:" + indexName));

        plan = query("from test | enrich \"" + indexName + "\"");
        Enrich enrich = as(plan, Enrich.class);
        assertThat(enrich.policyName().fold(FoldContext.small()), is(BytesRefs.toBytesRef(indexName)));
        as(enrich.child(), UnresolvedRelation.class);

        if (indexName.contains("*")) {
            expectThrows(ParsingException.class, () -> processingCommand("from test | lookup join \"" + indexName + "\" on bar"));
            expectThrows(ParsingException.class, () -> processingCommand("from test | lookup join \"" + indexName + "\" on bar1 > bar2"));
        } else {
            plan = query("from test | lookup join \"" + indexName + "\" on bar");
            LookupJoin lookup = as(plan, LookupJoin.class);
            UnresolvedRelation right = as(lookup.right(), UnresolvedRelation.class);
            assertThat(right.indexPattern().indexPattern(), is(indexName));
            if (EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()) {
                plan = query("from test | lookup join \"" + indexName + "\" on bar1 <= bar2");
                lookup = as(plan, LookupJoin.class);
                right = as(lookup.right(), UnresolvedRelation.class);
                assertThat(right.indexPattern().indexPattern(), is(indexName));
            }
        }
    }

    /**
     * Tests the inline cast syntax {@code <value>::<type>} for all supported types and
     * builds a little json report of the valid types.
     */
    public void testInlineCast() throws IOException {
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry();
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir"))
            .resolve("query-languages")
            .resolve("esql")
            .resolve("kibana")
            .resolve("definition");
        Files.createDirectories(dir);
        Path file = dir.resolve("inline_cast.json");
        try (XContentBuilder report = JsonXContent.contentBuilder().humanReadable(true).prettyPrint().lfAtEnd()) {
            report.startObject();
            List<String> namesAndAliases = new ArrayList<>(DataType.namesAndAliases());
            if (EsqlCapabilities.Cap.DATE_RANGE_FIELD_TYPE.isEnabled() == false) {
                // Some types do not have a converter function if the capability is disabled
                namesAndAliases.removeAll(List.of("date_range"));
            }
            Collections.sort(namesAndAliases);
            for (String nameOrAlias : namesAndAliases) {
                DataType expectedType = DataType.fromNameOrAlias(nameOrAlias);
                if (EsqlDataTypeConverter.converterFunctionFactory(expectedType) == null) {
                    continue;
                }
                LogicalPlan plan = parser.parseQuery("ROW a = 1::" + nameOrAlias);
                Row row = as(plan, Row.class);
                assertThat(row.fields(), hasSize(1));
                org.elasticsearch.xpack.esql.core.expression.function.Function functionCall =
                    (org.elasticsearch.xpack.esql.core.expression.function.Function) row.fields().get(0).child();
                assertThat(functionCall.dataType(), equalTo(expectedType));
                report.field(nameOrAlias, registry.snapshotRegistry().functionName(functionCall.getClass()).toLowerCase(Locale.ROOT));
            }
            report.endObject();
            String rendered = Strings.toString(report);
            (new TestInlineCastDocsSupport(rendered)).renderDocs();
        }
        logger.info("Wrote to file: {}", file);
    }

    private static class TestInlineCastDocsSupport extends DocsV3Support {
        private final String rendered;

        protected TestInlineCastDocsSupport(String rendered) {
            super(null, "inline_cast", StatementParserTests.class, Set::of, new DocsV3Support.WriteCallbacks());
            this.rendered = rendered;
        }

        @Override
        protected void renderDocs() throws IOException {
            this.writeToTempKibanaDir("definition", "json", rendered);
        }
    }

    public void testTooBigQuery() {
        StringBuilder query = new StringBuilder("FROM foo | EVAL a = a");
        while (query.length() < EsqlParser.MAX_LENGTH) {
            query.append(", a = CONCAT(a, a)");
        }
        expectError(query.toString(), "-1:-1: ESQL statement is too large [1000011 characters > 1000000]");
    }

    public void testInvalidLimit() {
        assertLimitWithAndWithoutParams("foo", "\"foo\"", DataType.KEYWORD);
        assertLimitWithAndWithoutParams(1.2, "1.2", DataType.DOUBLE);
        assertLimitWithAndWithoutParams(-1, "-1", DataType.INTEGER);
        assertLimitWithAndWithoutParams(true, "true", DataType.BOOLEAN);
        assertLimitWithAndWithoutParams(false, "false", DataType.BOOLEAN);
        assertLimitWithAndWithoutParams(null, "null", DataType.NULL);
    }

    private void assertLimitWithAndWithoutParams(Object value, String valueText, DataType type) {
        expectError(
            "row a = 1 | limit " + valueText,
            "1:13: value of [limit "
                + valueText
                + "] must be a non negative integer, found value ["
                + valueText
                + "] type ["
                + type.typeName()
                + "]"
        );

        expectError(
            "row a = 1 | limit ?param",
            List.of(new QueryParam("param", value, type, ParserUtils.ParamClassification.VALUE)),
            "1:13: value of [limit ?param] must be a non negative integer, found value [?param] type [" + type.typeName() + "]"
        );

    }

    public void testMMRCommandWithLimitOnly() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        var cmd = processingCommand("mmr on dense_embedding limit 10");
        assertEquals(MMR.class, cmd.getClass());
        MMR mmrCmd = (MMR) cmd;

        assertThat(mmrCmd.diversifyField(), equalToIgnoringIds(attribute("dense_embedding")));
        verifyMMRLimitValue(mmrCmd.limit(), 10);
        assertNull(mmrCmd.queryVector());
        verifyMMRLambdaValue(mmrCmd, null);
    }

    public void testMMRCommandWithLimitAndLambda() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        var cmd = processingCommand("mmr on dense_embedding limit 10 with { \"lambda\": 0.5 }");
        assertEquals(MMR.class, cmd.getClass());
        MMR mmrCmd = (MMR) cmd;

        assertThat(mmrCmd.diversifyField(), equalToIgnoringIds(attribute("dense_embedding")));

        verifyMMRLimitValue(mmrCmd.limit(), 10);
        verifyMMRLambdaValue(mmrCmd, 0.5);

        assertNull(mmrCmd.queryVector());
    }

    public void testMMRCommandWithConstantQueryVector() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        var mmrCmd = as(processingCommand("mmr [0.5, 0.4, 0.3, 0.2] on dense_embedding limit 10 with { \"lambda\": 0.5 }"), MMR.class);
        assertThat(mmrCmd.diversifyField(), equalToIgnoringIds(attribute("dense_embedding")));

        verifyMMRLimitValue(mmrCmd.limit(), 10);
        verifyMMRLambdaValue(mmrCmd, 0.5);
        verifyMMRQueryVectorValue(mmrCmd.queryVector(), List.of(0.5, 0.4, 0.3, 0.2));
    }

    public void testMMRCommandWithByteVectorQuery() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        var mmrByteArray = as(processingCommand("mmr [17, 48, 56] on dense_embedding limit 10 with { \"lambda\": 0.5 }"), MMR.class);
        assertThat(mmrByteArray.diversifyField(), equalToIgnoringIds(attribute("dense_embedding")));
        verifyMMRLimitValue(mmrByteArray.limit(), 10);
        verifyMMRLambdaValue(mmrByteArray, 0.5);
        verifyMMRQueryVectorValue(mmrByteArray.queryVector(), List.of(17, 48, 56));

        var mmrByteArrayString = as(processingCommand("mmr \"113038\" on dense_embedding limit 10 with { \"lambda\": 0.5 }"), MMR.class);
        assertThat(mmrByteArrayString.diversifyField(), equalToIgnoringIds(attribute("dense_embedding")));
        verifyMMRLimitValue(mmrByteArrayString.limit(), 10);
        verifyMMRLambdaValue(mmrByteArrayString, 0.5);
        verifyMMRQueryVectorValue(mmrByteArrayString.queryVector(), List.of(), "113038");
    }

    public void testMMRCommandWithFieldQueryVector() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        var queryParams = new QueryParams(List.of(paramAsConstant("query_vector_field", "[0.5, 0.4, 0.3, 0.2]")));
        var mmrCmd = as(
            parser.parseQuery("row a = 1 | mmr ?query_vector_field on dense_embedding limit 10 with { \"lambda\": 0.5 }", queryParams),
            MMR.class
        );

        assertThat(mmrCmd.diversifyField(), equalToIgnoringIds(attribute("dense_embedding")));

        verifyMMRLimitValue(mmrCmd.limit(), 10);
        verifyMMRLambdaValue(mmrCmd, 0.5);
        verifyMMRQueryVectorValue(mmrCmd.queryVector(), List.of(), "[0.5, 0.4, 0.3, 0.2]");
    }

    public void testMMRCommandWithTextEmbeddingQueryVector() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        var mmrCmd = as(
            processingCommand(
                "mmr TEXT_EMBEDDING(\"test text\", \"test_inference_id\") on dense_embedding limit 10 with { \"lambda\": 0.5 }"
            ),
            MMR.class
        );

        assertThat(mmrCmd.diversifyField(), equalToIgnoringIds(attribute("dense_embedding")));

        verifyMMRLimitValue(mmrCmd.limit(), 10);
        verifyMMRLambdaValue(mmrCmd, 0.5);

        Expression queryVectorExpression = mmrCmd.queryVector();
        if (queryVectorExpression instanceof UnresolvedFunction uaFunctioon) {
            assertThat(uaFunctioon.name(), equalTo("TEXT_EMBEDDING"));
            assertThat(uaFunctioon.source().text(), equalTo("TEXT_EMBEDDING(\"test text\", \"test_inference_id\")"));
        } else {
            fail("query vector expression [" + queryVectorExpression + "] is not a literal double collection");
        }
    }

    private void verifyMMRLimitValue(Expression expression, int limit) {
        assertThat(expression.dataType(), equalTo(INTEGER));
        int limitValue = (Integer) (((Literal) expression).value());
        assertEquals(limit, limitValue);
    }

    private void verifyMMRQueryVectorValue(Expression expression, List<?> expected) {
        verifyMMRQueryVectorValue(expression, expected, null);
    }

    private void verifyMMRQueryVectorValue(Expression expression, List<?> expected, @Nullable String expectedString) {
        if (expression instanceof Literal litExpression) {
            var thisValue = litExpression.value();
            if (thisValue instanceof Collection<?> litCollection) {
                assertEquals(expected, litCollection);
                return;
            }
        } else if (expression instanceof ToDenseVector asDenseVector) {
            var thisValue = ((Literal) asDenseVector.field()).value();
            if (thisValue instanceof Collection<?> litCollection) {
                assertEquals(expected, litCollection);
                return;
            } else if (thisValue instanceof BytesRef bytesRef && expectedString != null) {
                assertEquals(new BytesRef(expectedString), bytesRef);
                return;
            }
        }

        fail("query vector expression [" + expression + "] is not a valid dense vector convertable type");
    }

    private void verifyMMRLambdaValue(MMR mmrCmd, Double value) {
        if (value == null) {
            assertNull(mmrCmd.options());
            return;
        }

        assertTrue(mmrCmd.options() instanceof MapExpression);
        assertEquals(getLambdaFromMMROptions((MapExpression) mmrCmd.options()), value);
    }

    public Double getLambdaFromMMROptions(@Nullable MapExpression options) {
        if (options == null) {
            return null;
        }

        Map<String, Expression> optionsMap = options.keyFoldedMap();

        Expression lambdaValueExpression = optionsMap.remove(MMR.LAMBDA_OPTION_NAME);
        assertNotNull(lambdaValueExpression);
        Literal litLambdaValue = (Literal) lambdaValueExpression;
        assertNotNull(litLambdaValue);
        assertEquals(DOUBLE, litLambdaValue.dataType());
        return (Double) litLambdaValue.value();
    }

    public void testInvalidMMRCommands() {
        assumeTrue("MMR requires corresponding capability", EsqlCapabilities.Cap.MMR.isEnabled());

        expectError("row a = 1 | mmr on some_field", "line 1:30: mismatched input '<EOF>' expecting {'.', MMR_LIMIT}");
        expectError("row a = 1 | mmr on some_field limit", "line 1:36: mismatched input '<EOF>' expecting {INTEGER_LITERAL, '+', '-'}");
        expectError(
            "row a = 1 | mmr on some_field limit 5 {\"unknown\": true}",
            "line 1:39: mismatched input '{' expecting {<EOF>, '|', 'with'}"
        );
    }

    public void testInvalidSample() {
        expectError(
            "row a = 1 | sample \"foo\"",
            "1:13: invalid value for SAMPLE probability [foo], expecting a number between 0 and 1, exclusive"
        );
        expectError(
            "row a = 1 | sample -1.0",
            "1:13: invalid value for SAMPLE probability [-1.0], expecting a number between 0 and 1, exclusive"
        );
        expectError(
            "row a = 1 | sample 0",
            "1:13: invalid value for SAMPLE probability [0], expecting a number between 0 and 1, exclusive"
        );
        expectError(
            "row a = 1 | sample 1",
            "1:13: invalid value for SAMPLE probability [1], expecting a number between 0 and 1, exclusive"
        );
    }

}
