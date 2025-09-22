/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.compute.aggregation.AggregatorMode.FINAL;
import static org.elasticsearch.compute.aggregation.AggregatorMode.INITIAL;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateNanosToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;
import static org.hamcrest.Matchers.is;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class ReplaceRoundToWithQueryAndTagsTests extends LocalPhysicalPlanOptimizerTests {

    public ReplaceRoundToWithQueryAndTagsTests(String name, Configuration config) {
        super(name, config);
    }

    private static final List<String> dateHistograms = List.of(
        "date_trunc(1 day, date)",
        "bucket(date, 1 day)",
        "round_to(date, \"2023-10-20\", \"2023-10-21\", \"2023-10-22\", \"2023-10-23\")"
    );

    private static final Map<String, List<Object>> roundToAllTypes = new HashMap<>(
        Map.ofEntries(
            Map.entry("byte", List.of(2, 1, 3, 4)),
            Map.entry("short", List.of(1, 3, 2, 4)),
            Map.entry("integer", List.of(1, 2, 3, 4)),
            Map.entry("long", List.of(1697760000000L, 1697846400000L, 1697932800000L, 1698019200000L)),
            Map.entry("float", List.of(3.0, 2.0, 1.0, 4.0)),
            Map.entry("half_float", List.of(4.0, 2.0, 3.0, 1.0)),
            Map.entry("scaled_float", List.of(4.0, 3.0, 2.0, 1.0)),
            Map.entry("double", List.of(1.0, 2.0, 3.0, 4.0)),
            Map.entry("date", List.of("\"2023-10-20\"::date", "\"2023-10-21\"::date", "\"2023-10-22\"::date", "\"2023-10-23\"::date")),
            Map.entry(
                "date_nanos",
                List.of(
                    "\"2023-10-20\"::date_nanos",
                    "\"2023-10-21\"::date_nanos",
                    "\"2023-10-22\"::date_nanos",
                    "\"2023-10-23\"::date_nanos"
                )
            )
        )
    );

    private static final Map<String, QueryBuilder> otherPushDownFunctions = new HashMap<>(
        Map.ofEntries(
            Map.entry("keyword == \"keyword\"", termQuery("keyword", "keyword").boost(0)),
            Map.entry(
                "date >= \"2023-10-19\" and date <= \"2023-10-24\"",
                rangeQuery("date").gte("2023-10-19T00:00:00.000Z")
                    .lte("2023-10-24T00:00:00.000Z")
                    .timeZone("Z")
                    .boost(0)
                    .format(DEFAULT_DATE_TIME_FORMATTER.pattern())
            ),
            Map.entry("keyword : \"keyword\"", matchQuery("keyword", "keyword").lenient(true))
        )
    );

    // The date range of SearchStats is from 2023-10-20 to 2023-10-23.
    private static final SearchStats searchStats = searchStats();

    // DateTrunc/Bucket is transformed to RoundTo first and then to QueryAndTags
    public void testDateTruncBucketTransformToQueryAndTags() {
        for (String dateHistogram : dateHistograms) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats count(*) by x = {}
                """, dateHistogram);
            PhysicalPlan plan = plannerOptimizer.plan(query, searchStats, makeAnalyzer("mapping-all-types.json"));

            LimitExec limit = as(plan, LimitExec.class);
            AggregateExec agg = as(limit.child(), AggregateExec.class);
            assertThat(agg.getMode(), is(FINAL));
            List<? extends Expression> groupings = agg.groupings();
            NamedExpression grouping = as(groupings.get(0), NamedExpression.class);
            assertEquals("x", grouping.name());
            assertEquals(DataType.DATETIME, grouping.dataType());
            assertEquals(List.of("count(*)", "x"), Expressions.names(agg.aggregates()));
            ExchangeExec exchange = as(agg.child(), ExchangeExec.class);
            assertThat(exchange.inBetweenAggs(), is(true));
            agg = as(exchange.child(), AggregateExec.class);
            EvalExec eval = as(agg.child(), EvalExec.class);
            List<Alias> aliases = eval.fields();
            assertEquals(1, aliases.size());
            FieldAttribute roundToTag = as(aliases.get(0).child(), FieldAttribute.class);
            assertEquals("$$date$round_to$datetime", roundToTag.name());
            EsQueryExec esQueryExec = as(eval.child(), EsQueryExec.class);
            List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = esQueryExec.queryBuilderAndTags();
            List<EsQueryExec.QueryBuilderAndTags> expectedQueryBuilderAndTags = expectedQueryBuilderAndTags(
                query,
                "date",
                List.of(),
                new Source(2, 24, dateHistogram),
                null
            );
            verifyQueryAndTags(expectedQueryBuilderAndTags, queryBuilderAndTags);
            assertThrows(UnsupportedOperationException.class, esQueryExec::query);
        }
    }

    // DateTrunc is transformed to RoundTo first but cannot be transformed to QueryAndTags, when the TopN is pushed down to EsQueryExec
    public void testDateTruncNotTransformToQueryAndTags() {
        for (String dateHistogram : dateHistograms) {
            if (dateHistogram.contains("bucket")) { // bucket cannot be used out side of stats
                continue;
            }
            String query = LoggerMessageFormat.format(null, """
                from test
                | sort date
                | eval x = {}
                | keep alias_integer, date, x
                | limit 5
                """, dateHistogram);

            PhysicalPlan plan = plannerOptimizer.plan(query, searchStats, makeAnalyzer("mapping-all-types.json"));

            ProjectExec projectExec = as(plan, ProjectExec.class);
            TopNExec topNExec = as(projectExec.child(), TopNExec.class);
            ExchangeExec exchangeExec = as(topNExec.child(), ExchangeExec.class);
            projectExec = as(exchangeExec.child(), ProjectExec.class);
            FieldExtractExec fieldExtractExec = as(projectExec.child(), FieldExtractExec.class);
            EvalExec evalExec = as(fieldExtractExec.child(), EvalExec.class);
            List<Alias> aliases = evalExec.fields();
            assertEquals(1, aliases.size());
            RoundTo roundTo = as(aliases.get(0).child(), RoundTo.class);
            assertEquals(4, roundTo.points().size());
            fieldExtractExec = as(evalExec.child(), FieldExtractExec.class);
            EsQueryExec esQueryExec = as(fieldExtractExec.child(), EsQueryExec.class);
            List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = esQueryExec.queryBuilderAndTags();
            assertEquals(1, queryBuilderAndTags.size());
            EsQueryExec.QueryBuilderAndTags queryBuilder = queryBuilderAndTags.get(0);
            assertNull(queryBuilder.query());
            assertTrue(queryBuilder.tags().isEmpty());
            assertNull(esQueryExec.query());
        }
    }

    // RoundTo(all numeric data types) is transformed to QueryAndTags
    public void testRoundToTransformToQueryAndTags() {
        for (Map.Entry<String, List<Object>> roundTo : roundToAllTypes.entrySet()) {
            String fieldName = roundTo.getKey();
            List<Object> roundingPoints = roundTo.getValue();
            String expression = "round_to("
                + fieldName
                + ", "
                + roundingPoints.stream().map(Object::toString).collect(Collectors.joining(","))
                + ")";
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats count(*) by x = {}
                """, expression);
            PhysicalPlan plan = plannerOptimizer.plan(query, searchStats, makeAnalyzer("mapping-all-types.json"));

            LimitExec limit = as(plan, LimitExec.class);
            AggregateExec agg = as(limit.child(), AggregateExec.class);
            assertThat(agg.getMode(), is(FINAL));
            List<? extends Expression> groupings = agg.groupings();
            NamedExpression grouping = as(groupings.get(0), NamedExpression.class);
            assertEquals("x", grouping.name());
            assertEquals(List.of("count(*)", "x"), Expressions.names(agg.aggregates()));
            ExchangeExec exchange = as(agg.child(), ExchangeExec.class);
            assertThat(exchange.inBetweenAggs(), is(true));
            agg = as(exchange.child(), AggregateExec.class);
            EvalExec eval = as(agg.child(), EvalExec.class);
            List<Alias> aliases = eval.fields();
            assertEquals(1, aliases.size());
            FieldAttribute roundToTag = as(aliases.get(0).child(), FieldAttribute.class);
            assertTrue(roundToTag.name().startsWith("$$" + fieldName + "$round_to$"));
            EsQueryExec esQueryExec = as(eval.child(), EsQueryExec.class);
            List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = esQueryExec.queryBuilderAndTags();
            List<EsQueryExec.QueryBuilderAndTags> expectedQueryBuilderAndTags = expectedQueryBuilderAndTags(
                query,
                fieldName,
                roundingPoints,
                new Source(2, 24, expression),
                null
            );
            verifyQueryAndTags(expectedQueryBuilderAndTags, queryBuilderAndTags);
            assertThrows(UnsupportedOperationException.class, esQueryExec::query);
        }
    }

    // test if the combine query is generated correctly when there are other functions that can be pushed down
    public void testDateTruncBucketTransformToQueryAndTagsWithOtherPushdownFunctions() {
        for (String dateHistogram : dateHistograms) {
            for (Map.Entry<String, QueryBuilder> otherPushDownFunction : otherPushDownFunctions.entrySet()) {
                String predicate = otherPushDownFunction.getKey();
                QueryBuilder qb = otherPushDownFunction.getValue();
                String query = LoggerMessageFormat.format(null, """
                    from test
                    | where {}
                    | stats count(*) by x = {}
                    """, predicate, dateHistogram);
                QueryBuilder mainQueryBuilder = qb instanceof MatchQueryBuilder
                    ? qb
                    : wrapWithSingleQuery(
                        query,
                        qb,
                        predicate.contains("and") ? "date" : "keyword",
                        new Source(2, 8, predicate.contains("and") ? predicate.substring(0, 20) : predicate)
                    );

                PhysicalPlan plan = plannerOptimizer.plan(query, searchStats, makeAnalyzer("mapping-all-types.json"));

                LimitExec limit = as(plan, LimitExec.class);
                AggregateExec agg = as(limit.child(), AggregateExec.class);
                assertThat(agg.getMode(), is(FINAL));
                List<? extends Expression> groupings = agg.groupings();
                NamedExpression grouping = as(groupings.get(0), NamedExpression.class);
                assertEquals("x", grouping.name());
                assertEquals(DataType.DATETIME, grouping.dataType());
                assertEquals(List.of("count(*)", "x"), Expressions.names(agg.aggregates()));
                ExchangeExec exchange = as(agg.child(), ExchangeExec.class);
                assertThat(exchange.inBetweenAggs(), is(true));
                agg = as(exchange.child(), AggregateExec.class);
                EvalExec eval = as(agg.child(), EvalExec.class);
                List<Alias> aliases = eval.fields();
                assertEquals(1, aliases.size());
                FieldAttribute roundToTag = as(aliases.get(0).child(), FieldAttribute.class);
                assertEquals("$$date$round_to$datetime", roundToTag.name());
                EsQueryExec esQueryExec = as(eval.child(), EsQueryExec.class);
                List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = esQueryExec.queryBuilderAndTags();
                List<EsQueryExec.QueryBuilderAndTags> expectedQueryBuilderAndTags = expectedQueryBuilderAndTags(
                    query,
                    "date",
                    List.of(),
                    new Source(3, 24, dateHistogram),
                    mainQueryBuilder
                );
                verifyQueryAndTags(expectedQueryBuilderAndTags, queryBuilderAndTags);
                assertThrows(UnsupportedOperationException.class, esQueryExec::query);
            }
        }
    }

    /**
     * ReplaceRoundToWithQueryAndTags does not support lookup joins yet
     * LimitExec[1000[INTEGER],16]
     * \_AggregateExec[[x{r}#8],[COUNT(*[KEYWORD],true[BOOLEAN]) AS count(*)#9, x{r}#8],FINAL,[x{r}#8, $$count(*)$count{r}#34, $$count(*
     * )$seen{r}#35],16]
     *   \_ExchangeExec[[x{r}#8, $$count(*)$count{r}#34, $$count(*)$seen{r}#35],true]
     *     \_AggregateExec[[x{r}#8],[COUNT(*[KEYWORD],true[BOOLEAN]) AS count(*)#9, x{r}#8],INITIAL,[x{r}#8, $$count(*)$count{r}#36, $$count
     * (*)$seen{r}#37],16]
     *       \_EvalExec[[ROUNDTO(date{f}#15,1697760000000[DATETIME],1697846400000[DATETIME],1697932800000[DATETIME],1698019200000[DATE
     * TIME]) AS x#8]]
     *         \_FieldExtractExec[date{f}#15]
     *           \_LookupJoinExec[[integer{f}#21],[language_code{f}#32],[]]
     *             |_FieldExtractExec[integer{f}#21]
     *             | \_EsQueryExec[test], indexMode[standard], [_doc{f}#38], limit[], sort[] estimatedRowSize[24]
     *             queryBuilderAndTags [[QueryBuilderAndTags{queryBuilder=[null], tags=[]}]]
     *             \_FragmentExec[filter=null, estimatedRowSize=0, reducer=[], fragment=[
     * EsRelation[languages_lookup][LOOKUP][language_code{f}#32]]]
     */
    public void testDateTruncBucketNotTransformToQueryAndTagsWithLookupJoin() {
        for (String dateHistogram : dateHistograms) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | rename integer as language_code
                | lookup join languages_lookup on language_code
                | stats count(*) by x = {}
                """, dateHistogram);
            PhysicalPlan plan = plannerOptimizer.plan(query, searchStats, makeAnalyzer("mapping-all-types.json"));

            LimitExec limit = as(plan, LimitExec.class);
            AggregateExec agg = as(limit.child(), AggregateExec.class);
            assertThat(agg.getMode(), is(FINAL));
            List<? extends Expression> groupings = agg.groupings();
            NamedExpression grouping = as(groupings.get(0), NamedExpression.class);
            assertEquals("x", grouping.name());
            assertEquals(DataType.DATETIME, grouping.dataType());
            assertEquals(List.of("count(*)", "x"), Expressions.names(agg.aggregates()));
            ExchangeExec exchange = as(agg.child(), ExchangeExec.class);
            assertThat(exchange.inBetweenAggs(), is(true));
            agg = as(exchange.child(), AggregateExec.class);
            EvalExec eval = as(agg.child(), EvalExec.class);
            List<Alias> aliases = eval.fields();
            assertEquals(1, aliases.size());
            RoundTo roundTo = as(aliases.get(0).child(), RoundTo.class);
            assertEquals(4, roundTo.points().size());
            FieldExtractExec fieldExtractExec = as(eval.child(), FieldExtractExec.class);
            List<Attribute> attributes = fieldExtractExec.attributesToExtract();
            assertEquals(1, attributes.size());
            assertEquals("date", attributes.get(0).name());
            LookupJoinExec lookupJoinExec = as(fieldExtractExec.child(), LookupJoinExec.class); // this is why the rule doesn't apply
            // lhs of lookup join
            fieldExtractExec = as(lookupJoinExec.left(), FieldExtractExec.class);
            attributes = fieldExtractExec.attributesToExtract();
            assertEquals(1, attributes.size());
            assertEquals("integer", attributes.get(0).name());
            EsQueryExec esQueryExec = as(fieldExtractExec.child(), EsQueryExec.class);
            assertEquals("test", esQueryExec.indexPattern());
            List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = esQueryExec.queryBuilderAndTags();
            assertEquals(1, queryBuilderAndTags.size());
            EsQueryExec.QueryBuilderAndTags queryBuilder = queryBuilderAndTags.get(0);
            assertNull(queryBuilder.query());
            assertTrue(queryBuilder.tags().isEmpty());
            assertNull(esQueryExec.query());
            // rhs of lookup join
            FragmentExec fragmentExec = as(lookupJoinExec.right(), FragmentExec.class);
            EsRelation esRelation = as(fragmentExec.fragment(), EsRelation.class);
            assertTrue(esRelation.toString().contains("EsRelation[languages_lookup][LOOKUP]"));
        }
    }

    // ReplaceRoundToWithQueryAndTags does not support lookup joins yet
    public void testDateTruncBucketNotTransformToQueryAndTagsWithFork() {
        for (String dateHistogram : dateHistograms) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | fork (where integer > 100)
                       (where keyword : "keyword")
                | stats count(*) by x = {}
                """, dateHistogram);
            PhysicalPlan plan = plannerOptimizer.plan(query, searchStats, makeAnalyzer("mapping-all-types.json"));

            LimitExec limit = as(plan, LimitExec.class);
            AggregateExec agg = as(limit.child(), AggregateExec.class);
            assertThat(agg.getMode(), is(FINAL));
            List<? extends Expression> groupings = agg.groupings();
            NamedExpression grouping = as(groupings.get(0), NamedExpression.class);
            assertEquals("x", grouping.name());
            assertEquals(DataType.DATETIME, grouping.dataType());
            assertEquals(List.of("count(*)", "x"), Expressions.names(agg.aggregates()));
            agg = as(agg.child(), AggregateExec.class);
            assertThat(agg.getMode(), is(INITIAL));
            groupings = agg.groupings();
            grouping = as(groupings.get(0), NamedExpression.class);
            assertEquals("x", grouping.name());
            assertEquals(DataType.DATETIME, grouping.dataType());
            assertEquals(List.of("count(*)", "x"), Expressions.names(agg.aggregates()));
            EvalExec eval = as(agg.child(), EvalExec.class);
            List<Alias> aliases = eval.fields();
            assertEquals(1, aliases.size());
            var function = as(aliases.get(0).child(), Function.class);
            ReferenceAttribute fa = null; // if merge returns FieldAttribute instead of ReferenceAttribute, the rule might apply
            if (function instanceof DateTrunc dateTrunc) {
                fa = as(dateTrunc.field(), ReferenceAttribute.class);
            } else if (function instanceof Bucket bucket) {
                fa = as(bucket.field(), ReferenceAttribute.class);
            } else if (function instanceof RoundTo roundTo) {
                fa = as(roundTo.field(), ReferenceAttribute.class);
            }
            assertNotNull(fa);
            assertEquals("date", fa.name());
            assertEquals(DataType.DATETIME, fa.dataType());
            MergeExec mergeExec = as(eval.child(), MergeExec.class);
        }
    }

    /**
     * If the number of rounding points is 127 or less, the query is rewritten to QueryAndTags.
     * If the number of rounding points is 128 or more, the query is not rewritten.
     */
    public void testRoundToTransformToQueryAndTagsWithDefaultUpperLimit() {
        for (int numOfPoints : List.of(127, 128)) {
            StringBuilder points = new StringBuilder();
            for (int i = 0; i < numOfPoints; i++) {
                if (i > 0) {
                    points.append(", ");
                }
                points.append(i);
            }
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats count(*) by x = round_to(integer, {})
                """, points.toString());

            PhysicalPlan plan = plannerOptimizer.plan(query, searchStats, makeAnalyzer("mapping-all-types.json"));

            LimitExec limit = as(plan, LimitExec.class);
            AggregateExec agg = as(limit.child(), AggregateExec.class);
            assertThat(agg.getMode(), is(FINAL));
            List<? extends Expression> groupings = agg.groupings();
            NamedExpression grouping = as(groupings.get(0), NamedExpression.class);
            assertEquals("x", grouping.name());
            assertEquals(DataType.INTEGER, grouping.dataType());
            assertEquals(List.of("count(*)", "x"), Expressions.names(agg.aggregates()));
            ExchangeExec exchange = as(agg.child(), ExchangeExec.class);
            assertThat(exchange.inBetweenAggs(), is(true));
            agg = as(exchange.child(), AggregateExec.class);
            EvalExec evalExec = as(agg.child(), EvalExec.class);
            List<Alias> aliases = evalExec.fields();
            assertEquals(1, aliases.size());
            if (numOfPoints == 127) {
                FieldAttribute roundToTag = as(aliases.get(0).child(), FieldAttribute.class);
                assertTrue(roundToTag.name().startsWith("$$integer$round_to$"));
                EsQueryExec esQueryExec = as(evalExec.child(), EsQueryExec.class);
                List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = esQueryExec.queryBuilderAndTags();
                assertEquals(128, queryBuilderAndTags.size()); // 127 + nullBucket
                assertThrows(UnsupportedOperationException.class, esQueryExec::query);
            } else { // numOfPoints == 128, query rewrite does not happen
                RoundTo roundTo = as(aliases.get(0).child(), RoundTo.class);
                assertEquals(128, roundTo.points().size());
                FieldExtractExec fieldExtractExec = as(evalExec.child(), FieldExtractExec.class);
                EsQueryExec esQueryExec = as(fieldExtractExec.child(), EsQueryExec.class);
                List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = esQueryExec.queryBuilderAndTags();
                assertEquals(1, queryBuilderAndTags.size());
                EsQueryExec.QueryBuilderAndTags queryBuilder = queryBuilderAndTags.get(0);
                assertNull(queryBuilder.query());
                assertTrue(queryBuilder.tags().isEmpty());
                assertNull(esQueryExec.query());
            }
        }
    }

    /**
     * Query level threshold(if greater than -1) set in QueryPragmas overrides the cluster level threshold set in EsqlFlags.
     */
    public void testRoundToTransformToQueryAndTagsWithCustomizedUpperLimit() {
        for (int clusterLevelThreshold : List.of(-1, 0, 60, 126, 128, 256)) {
            for (int queryLevelThreshold : List.of(-1, 0, 60, 126, 128, 256)) {
                StringBuilder points = new StringBuilder(); // there are 127 rounding points
                for (int i = 0; i < 127; i++) {
                    if (i > 0) {
                        points.append(", ");
                    }
                    points.append(i);
                }
                String query = LoggerMessageFormat.format(null, """
                    from test
                    | stats count(*) by x = round_to(integer, {})
                    """, points.toString());

                TestPlannerOptimizer plannerOptimizerWithPragmas = new TestPlannerOptimizer(
                    configuration(
                        new QueryPragmas(
                            Settings.builder()
                                .put(QueryPragmas.ROUNDTO_PUSHDOWN_THRESHOLD.getKey().toLowerCase(Locale.ROOT), queryLevelThreshold)
                                .build()
                        ),
                        query
                    ),
                    makeAnalyzer("mapping-all-types.json")
                );
                EsqlFlags esqlFlags = new EsqlFlags(clusterLevelThreshold);
                assertEquals(clusterLevelThreshold, esqlFlags.roundToPushdownThreshold());
                assertTrue(esqlFlags.stringLikeOnIndex());
                PhysicalPlan plan = plannerOptimizerWithPragmas.plan(query, searchStats, esqlFlags);
                boolean pushdown = false;
                if (queryLevelThreshold > -1) {
                    pushdown = queryLevelThreshold >= 127;
                } else {
                    pushdown = clusterLevelThreshold >= 127;
                }

                LimitExec limit = as(plan, LimitExec.class);
                AggregateExec agg = as(limit.child(), AggregateExec.class);
                assertThat(agg.getMode(), is(FINAL));
                List<? extends Expression> groupings = agg.groupings();
                NamedExpression grouping = as(groupings.get(0), NamedExpression.class);
                assertEquals("x", grouping.name());
                assertEquals(DataType.INTEGER, grouping.dataType());
                assertEquals(List.of("count(*)", "x"), Expressions.names(agg.aggregates()));
                ExchangeExec exchange = as(agg.child(), ExchangeExec.class);
                assertThat(exchange.inBetweenAggs(), is(true));
                agg = as(exchange.child(), AggregateExec.class);
                EvalExec evalExec = as(agg.child(), EvalExec.class);
                List<Alias> aliases = evalExec.fields();
                assertEquals(1, aliases.size());
                if (pushdown) {
                    FieldAttribute roundToTag = as(aliases.get(0).child(), FieldAttribute.class);
                    assertTrue(roundToTag.name().startsWith("$$integer$round_to$"));
                    EsQueryExec esQueryExec = as(evalExec.child(), EsQueryExec.class);
                    List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = esQueryExec.queryBuilderAndTags();
                    assertEquals(128, queryBuilderAndTags.size()); // 127 + nullBucket
                    assertThrows(UnsupportedOperationException.class, esQueryExec::query);
                } else { // query rewrite does not happen
                    RoundTo roundTo = as(aliases.get(0).child(), RoundTo.class);
                    assertEquals(127, roundTo.points().size());
                    FieldExtractExec fieldExtractExec = as(evalExec.child(), FieldExtractExec.class);
                    EsQueryExec esQueryExec = as(fieldExtractExec.child(), EsQueryExec.class);
                    List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags = esQueryExec.queryBuilderAndTags();
                    assertEquals(1, queryBuilderAndTags.size());
                    EsQueryExec.QueryBuilderAndTags queryBuilder = queryBuilderAndTags.get(0);
                    assertNull(queryBuilder.query());
                    assertTrue(queryBuilder.tags().isEmpty());
                    assertNull(esQueryExec.query());
                }
            }
        }
    }

    private static void verifyQueryAndTags(List<EsQueryExec.QueryBuilderAndTags> expected, List<EsQueryExec.QueryBuilderAndTags> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            EsQueryExec.QueryBuilderAndTags expectedItem = expected.get(i);
            EsQueryExec.QueryBuilderAndTags actualItem = actual.get(i);
            assertEquals(expectedItem.query().toString(), actualItem.query().toString());
            assertEquals(expectedItem.tags().get(0), actualItem.tags().get(0));
        }
    }

    private static List<EsQueryExec.QueryBuilderAndTags> expectedQueryBuilderAndTags(
        String query,
        String fieldName,
        List<Object> roundingPoints,
        Source source,
        QueryBuilder mainQueryBuilder
    ) {
        List<EsQueryExec.QueryBuilderAndTags> expected = new ArrayList<>(5);
        boolean isDateField = fieldName.equals("date");
        boolean isDateNanosField = fieldName.equals("date_nanos");
        boolean isNumericField = (isDateField || isDateNanosField) == false;
        List<List<Object>> rangeAndTags = isNumericField ? numericBuckets(roundingPoints) : dateBuckets(isDateField);
        for (List<Object> rangeAndTag : rangeAndTags) {
            Object lower = rangeAndTag.get(0);
            Object upper = rangeAndTag.get(1);
            Object tag = rangeAndTag.get(2);
            RangeQueryBuilder rangeQueryBuilder;
            if (isNumericField) {
                rangeQueryBuilder = rangeQuery(fieldName).boost(0);
            } else if (isDateField) { // date
                rangeQueryBuilder = rangeQuery(fieldName).boost(0).timeZone("Z").format(DEFAULT_DATE_TIME_FORMATTER.pattern());
            } else { // date_nanos
                rangeQueryBuilder = rangeQuery(fieldName).boost(0).timeZone("Z").format(DEFAULT_DATE_NANOS_FORMATTER.pattern());
            }
            if (upper != null) {
                rangeQueryBuilder = rangeQueryBuilder.lt(upper);
            }
            if (lower != null) {
                rangeQueryBuilder = rangeQueryBuilder.gte(lower);
            }

            QueryBuilder qb = wrapWithSingleQuery(query, rangeQueryBuilder, fieldName, source);
            if (mainQueryBuilder != null) {
                qb = boolQuery().filter(mainQueryBuilder).filter(qb);
            }
            expected.add(new EsQueryExec.QueryBuilderAndTags(qb, List.of(tag)));
        }
        // add null bucket
        BoolQueryBuilder isNullQueryBuilder = boolQuery().mustNot(existsQuery(fieldName).boost(0));
        List<Object> nullTags = new ArrayList<>(1);
        nullTags.add(null);
        if (mainQueryBuilder != null) {
            isNullQueryBuilder = boolQuery().filter(mainQueryBuilder).filter(isNullQueryBuilder.boost(0));
        }
        expected.add(new EsQueryExec.QueryBuilderAndTags(isNullQueryBuilder, nullTags));
        return expected;
    }

    private static List<List<Object>> dateBuckets(boolean isDate) {
        // Date rounding points
        String[] dates = { "2023-10-20T00:00:00.000Z", "2023-10-21T00:00:00.000Z", "2023-10-22T00:00:00.000Z", "2023-10-23T00:00:00.000Z" };

        // first bucket has no lower bound
        List<Object> firstBucket = new ArrayList<>(3);
        firstBucket.add(null);
        firstBucket.add(dates[1]);
        firstBucket.add(isDate ? dateTimeToLong(dates[0]) : dateNanosToLong(dates[0]));

        // last bucket has no upper bound
        List<Object> lastBucket = new ArrayList<>(3);
        lastBucket.add(dates[3]);
        lastBucket.add(null);
        lastBucket.add(isDate ? dateTimeToLong(dates[3]) : dateNanosToLong(dates[3]));

        return List.of(
            firstBucket,
            List.of(dates[1], dates[2], isDate ? dateTimeToLong(dates[1]) : dateNanosToLong(dates[1])),
            List.of(dates[2], dates[3], isDate ? dateTimeToLong(dates[2]) : dateNanosToLong(dates[2])),
            lastBucket
        );
    }

    private static List<List<Object>> numericBuckets(List<Object> roundingPoints) {
        // sort the rounding points in ascending order
        roundingPoints = roundingPoints.stream().sorted().collect(Collectors.toList());
        Object p1 = roundingPoints.get(0);
        Object p2 = roundingPoints.get(1);
        Object p3 = roundingPoints.get(2);
        Object p4 = roundingPoints.get(3);
        // first bucket has no lower bound
        List<Object> firstBucket = new ArrayList<>(3);
        firstBucket.add(null);
        firstBucket.add(p2);
        firstBucket.add(p1);
        // last bucket has no upper bound
        List<Object> lastBucket = new ArrayList<>(3);
        lastBucket.add(p4);
        lastBucket.add(null);
        lastBucket.add(p4);
        return List.of(firstBucket, List.of(p2, p3, p2), List.of(p3, p4, p3), lastBucket);
    }

    private static SearchStats searchStats() {
        // create a SearchStats with min and max in milliseconds
        Map<String, Object> minValue = Map.of("date", 1697804103360L); // 2023-10-20T12:15:03.360Z
        Map<String, Object> maxValue = Map.of("date", 1698069301543L); // 2023-10-23T13:55:01.543Z
        return new EsqlTestUtils.TestSearchStatsWithMinMax(minValue, maxValue);
    }
}
