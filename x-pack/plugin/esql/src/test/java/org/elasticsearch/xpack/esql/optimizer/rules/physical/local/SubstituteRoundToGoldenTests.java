/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class SubstituteRoundToGoldenTests extends GoldenTestCase {
    private record QueryAndName(String query, String name) {}

    private static final List<QueryAndName> dateHistograms = List.of(
        new QueryAndName("date_trunc(1 day, date)", "date_trunc"),
        new QueryAndName("bucket(date, 1 day)", "bucket"),
        new QueryAndName("round_to(date, \"2023-10-20\", \"2023-10-21\", \"2023-10-22\", \"2023-10-23\")", "round_to")
    );

    // DateTrunc/Bucket is transformed to RoundTo first and then to QueryAndTags
    public void testDateTruncBucketTransformToQueryAndTags() {
        for (var queryAndName : dateHistograms) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats count(*) by x = {}
                """, queryAndName.query());
            runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS, queryAndName.name());
        }
    }

    // Pushing count to source isn't supported when there is a filter on the count at the moment.
    public void testDateTruncBucketTransformToQueryAndTagsWithWhereInsideAggregation() {
        for (var queryAndName : dateHistograms) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats count(*) where long > 10 by x = {}
                """, queryAndName.query());
            runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS, queryAndName.name());
        }
    }

    // We do not support count pushdown when there is an ES filter at the moment, even if it's on the same field.
    public void testDateTruncBucketTransformToQueryAndTagsWithEsFilter() {
        for (var queryAndName : dateHistograms) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats count(*) by x = {}
                """, queryAndName.query());
            runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS, queryAndName.name());
        }
    }

    // Pushing count to source isn't supported when there are multiple aggregates.
    public void testDateTruncBucketTransformToQueryAndTagsWithMultipleAggregates() {
        for (var queryAndName : dateHistograms) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | stats sum(long), count(*) by x = {}
                """, queryAndName.query());
            runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS, queryAndName.name());
        }
    }

    // DateTrunc is transformed to RoundTo first but cannot be transformed to QueryAndTags, when the TopN is pushed down to EsQueryExec
    public void testDateTruncNotTransformToQueryAndTags() {
        for (var queryAndName : dateHistograms) {
            var dateHistogram = queryAndName.query();
            if (dateHistogram.contains("bucket")) { // bucket cannot be used outside of stats
                continue;
            }
            String query = LoggerMessageFormat.format(null, """
                from test
                | sort date
                | eval x = {}
                | keep alias_integer, date, x
                | limit 5
                """, dateHistogram);
            runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS, queryAndName.name);
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
            runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS, fieldName);
        }
    }

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

    // test if the combine query is generated correctly when there are other functions that can be pushed down
    public void testDateTruncBucketTransformToQueryAndTagsWithOtherPushdownFunctions() {
        for (var queryAndName : dateHistograms) {
            for (var otherPushDownFunction : otherPushDownFunctions) {
                String query = LoggerMessageFormat.format(null, """
                    from test
                    | where {}
                    | stats count(*) by x = {}
                    """, otherPushDownFunction.query(), queryAndName.query());
                runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS, queryAndName.name(), otherPushDownFunction.name());
            }
        }
    }

    private static final List<QueryAndName> otherPushDownFunctions = List.of(
        new QueryAndName("keyword == \"keyword\"", "keyword_equality"),
        new QueryAndName("date >= \"2023-10-19\" and date <= \"2023-10-24\"", "date_range"),
        new QueryAndName("keyword : \"keyword\"", "keyword_match")
    );

    // ReplaceRoundToWithQueryAndTags does not support lookup joins yet
    public void testDateTruncBucketNotTransformToQueryAndTagsWithLookupJoin() {
        for (var queryAndName : dateHistograms) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | rename integer as language_code
                | lookup join languages_lookup on language_code
                | stats count(*) by x = {}
                """, queryAndName.query());
            runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS, queryAndName.name());
        }
    }

    // ReplaceRoundToWithQueryAndTags does not support lookup joins yet
    public void testDateTruncBucketNotTransformToQueryAndTagsWithFork() {
        for (var queryAndName : dateHistograms) {
            String query = LoggerMessageFormat.format(null, """
                from test
                | fork (where integer > 100)
                (where keyword : "keyword")
                | stats count(*) by x = {}
                """, queryAndName.query());
            runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS, queryAndName.name());
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

            runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS, Integer.toString(numOfPoints));
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

                runGoldenTest(
                    query,
                    EnumSet.of(Stage.LOCAL_PHYSICAL),
                    STATS,
                    "cluster_" + clusterLevelThreshold,
                    "query_" + queryLevelThreshold
                );
            }
        }
    }

    public void testForkWithStatsCountStarDateTrunc() {
        String query = """
            from test
            | fork (stats x = count(*), y = max(long) by hd = date_trunc(1 day, date))
            (stats x = count(*), y = min(long) by hd = date_trunc(2 day, date))
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS);
    }

    public void testSubqueryWithCountStarAndDateTrunc() {
        assumeTrue("requires subqueries in from", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            from test, (from test | stats cnt = count(*) by x = date_trunc(1 day, date))
            | keep x, cnt, date
            """;
        runGoldenTest(query, EnumSet.of(Stage.LOCAL_PHYSICAL), STATS);
    }

    private static final EsqlTestUtils.TestSearchStatsWithMinMax STATS = new EsqlTestUtils.TestSearchStatsWithMinMax(
        Map.of("date", dateTimeToLong("2023-10-20T12:15:03.360Z")),
        Map.of("date", dateTimeToLong("2023-10-23T13:55:01.543Z"))
    );
}
