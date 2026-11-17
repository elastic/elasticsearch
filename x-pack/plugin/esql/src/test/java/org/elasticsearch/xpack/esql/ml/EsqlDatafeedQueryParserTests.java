/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.spi.SPIClassIterator;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryWithParams;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucket;
import org.junit.After;
import org.junit.Before;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class EsqlDatafeedQueryParserTests extends ESTestCase {

    private final EsqlDatafeedQueryParser parser = new EsqlDatafeedQueryParser();
    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void createClusterService() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = clusterService(Settings.EMPTY);
    }

    @After
    public void closeClusterService() {
        clusterService.close();
        terminate(threadPool);
    }

    private ClusterService clusterService(Settings settings) {
        Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.addAll(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS);
        registered.addAll(QuerySettings.clusterSettings());
        return ClusterServiceUtils.createClusterService(
            threadPool,
            DiscoveryNodeUtils.create("node", "node"),
            settings,
            new ClusterSettings(settings, registered)
        );
    }

    private ParsedEsqlQuery parse(String query) {
        return parser.parse(query, clusterService);
    }

    public void testServicesFileShouldRegisterParser() {
        SPIClassIterator<EsqlQueryParser> providers = SPIClassIterator.get(EsqlQueryParser.class, getClass().getClassLoader());
        List<Class<? extends EsqlQueryParser>> classes = new ArrayList<>();
        providers.forEachRemaining(classes::add);
        assertThat(classes, contains(EsqlDatafeedQueryParser.class));
    }

    public void testNestedCommentsShouldNotHideSource() {
        ParsedEsqlQuery query = parse("/* outer /* inner */ still outer */ FROM t | LIMIT 5");
        assertThat(query.indexPatterns(), contains("t"));
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(5)));
    }

    public void testQueryWithoutSettingsShouldRunInUtcAndNotSetProjectRouting() {
        ParsedEsqlQuery query = parse("FROM idx");
        assertThat(query.timeZone(), equalTo(ZoneOffset.UTC));
        assertThat(query.setsProjectRoutingInQuery(), is(false));
    }

    public void testTimeZoneSetInQueryShouldWinOverClusterDefault() {
        try (ClusterService berlin = clusterService(Settings.builder().put("esql.query.settings.time_zone", "Europe/Berlin").build())) {
            assertThat(parser.parse("FROM idx", berlin).timeZone(), equalTo(ZoneId.of("Europe/Berlin")));
            assertThat(parser.parse("SET time_zone=\"Asia/Tokyo\"; FROM idx", berlin).timeZone(), equalTo(ZoneId.of("Asia/Tokyo")));
        }
    }

    public void testProjectRoutingSetInQueryShouldBeReportedWhenCrossProjectIsEnabled() {
        try (ClusterService cps = clusterService(Settings.builder().put("serverless.cross_project.enabled", true).build())) {
            assertThat(parser.parse("SET project_routing=\"_alias:*\"; FROM idx", cps).setsProjectRoutingInQuery(), is(true));
            assertThat(parser.parse("FROM idx", cps).setsProjectRoutingInQuery(), is(false));
        }
    }

    public void testProjectRoutingWithoutCrossProjectShouldBeRejectedByEsql() {
        expectThrows(ParsingException.class, () -> parse("SET project_routing=\"_alias:*\"; FROM idx"));
    }

    public void testUnknownSettingShouldFailWithBadRequest() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse("SET not_a_setting=\"x\"; FROM idx"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testCommandCountShouldFollowClusterSetting() {
        // 600 commands in subqueries exceed the default of 500, which counts the whole statement.
        String evals = "| EVAL x = 1 ".repeat(300);
        String query = "FROM (FROM a " + evals + "), (FROM b " + evals + ")";
        ParsingException tooMany = expectThrows(ParsingException.class, () -> parse(query));
        assertThat(tooMany.getMessage(), not(containsString("not supported in ES|QL datafeeds")));
        try (ClusterService raised = clusterService(Settings.builder().put("esql.query.max_command_count", 1000).build())) {
            ParsingException unsupported = expectThrows(ParsingException.class, () -> parser.parse(query, raised));
            assertThat(unsupported.getMessage(), containsString("[FROM with several subqueries] is not supported in ES|QL datafeeds"));
        }
    }

    public void testSetStatementsShouldPrefixSourceQueries() {
        ParsedEsqlQuery query = parse("SET time_zone=\"UTC\"; FROM a | LIMIT 5");
        assertThat(query.sourceTimeFieldProbeQuery("t").query(), equalTo("SET time_zone=\"UTC\";\nFROM a | KEEP ??timeField | LIMIT 0"));
    }

    public void testSourceQueryShouldKeepMetadataOfPlainFrom() {
        ParsedEsqlQuery query = parse("/* c */ FROM a /* mid */ , remote:b METADATA _id, _index | LIMIT 5");
        assertThat(
            query.sourceTimeFieldProbeQuery("t").query(),
            equalTo("FROM a /* mid */ , remote:b METADATA _id, _index | KEEP ??timeField | LIMIT 0")
        );
    }

    public void testSourceQueryShouldNotBeSwallowedByTrailingComment() {
        ParsedEsqlQuery query = parse("FROM t // trailing\n| LIMIT 1");
        assertThat(query.sourceRangeSummaryQuery("t").query(), containsString("FROM t | STATS"));
    }

    public void testSourceQueryShouldFailWithoutIndexSource() {
        ParsedEsqlQuery query = parse("ROW a = 1 | LIMIT 1");
        assertThat(query.indexPatterns(), empty());
        expectThrows(IllegalStateException.class, () -> query.sourceTimeFieldProbeQuery("t"));
        expectThrows(IllegalStateException.class, () -> query.sourceRangeSummaryQuery("t"));
    }

    public void testFullQueryExtensionShouldNotBeSwallowedByTrailingComment() {
        ParsedEsqlQuery query = parse("FROM t // trailing");
        assertThat(query.limitZeroQuery().query(), equalTo("FROM t // trailing\n| LIMIT 0"));
        assertThat(parse(query.limitZeroQuery().query()).outputLimit(), equalTo(OptionalInt.of(0)));
    }

    public void testOrderedQueryShouldSortOnTimeFieldAndCapRowsOnlyWhenAsked() {
        ParsedEsqlQuery query = parse("FROM t | WHERE a > 1");
        EsqlQueryWithParams capped = query.orderedByTimeQuery("@timestamp", OptionalLong.of(10_000));
        assertThat(capped.query(), equalTo("FROM t | WHERE a > 1\n| SORT ??timeField ASC\n| LIMIT 10000"));
        EsqlQueryWithParams uncapped = query.orderedByTimeQuery("@timestamp", OptionalLong.empty());
        assertThat(uncapped.query(), equalTo("FROM t | WHERE a > 1\n| SORT ??timeField ASC"));
        assertThat(uncapped.params().size(), equalTo(1));
        assertThat(uncapped.params().getFirst().name(), equalTo("timeField"));
        assertThat(uncapped.params().getFirst().value(), equalTo("@timestamp"));
        assertThat(uncapped.params().getFirst().classification(), equalTo(IDENTIFIER));
    }

    public void testProbeQueriesShouldReadTheColumnsTheInterfaceNames() {
        ParsedEsqlQuery query = parse("FROM t | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)");
        assertThat(
            query.sourceRangeSummaryQuery("@timestamp").query(),
            equalTo(
                "FROM t | STATS "
                    + ParsedEsqlQuery.EARLIEST_TIME_COLUMN
                    + " = MIN(??timeField), "
                    + ParsedEsqlQuery.LATEST_TIME_COLUMN
                    + " = MAX(??timeField), "
                    + ParsedEsqlQuery.TOTAL_HITS_COLUMN
                    + " = COUNT(*)"
            )
        );
        assertThat(
            query.outputRowCountQuery().query(),
            equalTo(
                "FROM t | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)\n| STATS "
                    + ParsedEsqlQuery.OUTPUT_ROWS_COLUMN
                    + " = COUNT(*)"
            )
        );
        assertThat(query.outputRowCountQuery().params(), empty());
    }

    public void testColumnMetadataProbeShouldEnableColumnMetadataBeforeTheQuery() {
        assertThat(
            parse("FROM t | STATS c = COUNT(*) BY b = TBUCKET(1 hour)").columnMetadataProbeQuery().query(),
            equalTo("SET column_metadata=true;\nFROM t | STATS c = COUNT(*) BY b = TBUCKET(1 hour)")
        );
        assertThat(
            parse("SET time_zone=\"UTC\"; FROM t").columnMetadataProbeQuery().query(),
            startsWith("SET column_metadata=true;\nSET time_zone")
        );
    }

    public void testTimeBucketShouldBeReadFromColumnMetadataOfTheTimeColumn() {
        List<ColumnInfoImpl> columns = List.of(
            new ColumnInfoImpl("c", "long", null),
            new ColumnInfoImpl("b", "date", null, Map.of("bucket", Map.of("interval", 15L, "unit", "minute")))
        );
        ParsedEsqlQuery query = parse("FROM t");
        assertThat(query.timeBucket(columns, "b"), equalTo(Optional.of(new TimeBucket(15, "minute"))));
        assertThat(query.timeBucket(columns, "c"), equalTo(Optional.empty()));
        assertThat(query.timeBucket(columns, "missing"), equalTo(Optional.empty()));
    }

    public void testNumericBucketMetadataShouldNotBeATimeBucket() {
        List<ColumnInfoImpl> columns = List.of(new ColumnInfoImpl("b", "double", null, Map.of("bucket", Map.of("interval", 10.0))));
        assertThat(parse("FROM t").timeBucket(columns, "b"), equalTo(Optional.empty()));
    }

    public void testSubqueriesWithSeveralBranchesShouldBeRejected() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse("FROM (FROM a | WHERE x > 1), b | LIMIT 5"));
        assertThat(e.getMessage(), containsString("[FROM with several subqueries] is not supported in ES|QL datafeeds"));
        expectThrows(ParsingException.class, () -> parse("FROM (FROM a | WHERE x > 1), (FROM b)"));
        expectThrows(ParsingException.class, () -> parse("FROM (ROW a = 1), (ROW b = 2)"));
    }

    public void testSingleSubqueryShouldBeReadAsOnePipeline() {
        ParsedEsqlQuery query = parse("FROM (FROM remote:logs | WHERE a > 1) | LIMIT 5");
        assertThat(query.indexPatterns(), contains("remote:logs"));
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(5)));
        assertThat(query.filtersOn("a"), is(true));
    }

    public void testForkShouldBeRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parse("FROM t | FORK (STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)) (WHERE false)")
        );
        assertThat(e.getMessage(), containsString("[FORK] is not supported in ES|QL datafeeds"));
        expectThrows(ParsingException.class, () -> parse("FROM t | FORK (LIMIT 100) (LIMIT 42)"));
    }

    public void testRenameShouldBeRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parse("FROM t | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour) | RENAME b AS t")
        );
        assertThat(e.getMessage(), containsString("[RENAME] is not supported in ES|QL datafeeds"));
    }

    public void testMvExpandShouldBeRejected() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse("FROM t | MV_EXPAND x"));
        assertThat(e.getMessage(), containsString("[MV_EXPAND] is not supported in ES|QL datafeeds"));
    }

    public void testLookupJoinShouldBeRejected() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse("FROM t | LOOKUP JOIN l ON k"));
        assertThat(e.getMessage(), containsString("[LOOKUP JOIN] is not supported in ES|QL datafeeds"));
    }

    public void testLetBindingShouldBeRejected() {
        assumeTrue("requires NAMED_SUBQUERY_LET capability", EsqlCapabilities.Cap.NAMED_SUBQUERY_LET.isEnabled());
        ParsingException e = expectThrows(ParsingException.class, () -> parse("LET top3 = (FROM logs | LIMIT 3); FROM top3"));
        assertThat(e.getMessage(), containsString("[LET] is not supported in ES|QL datafeeds"));
    }

    public void testLimitBeforeStatsShouldBeRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parse("FROM x | LIMIT 500 | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)")
        );
        assertThat(e.getMessage(), containsString("[LIMIT] before [STATS] is not supported in ES|QL datafeeds"));
    }

    public void testLimitBeforeStatsWithFiltersInBetweenShouldBeRejected() {
        expectThrows(ParsingException.class, () -> parse("FROM x | LIMIT 500 | WHERE a > 1 | EVAL y = 1 | STATS c = COUNT(*)"));
    }

    public void testLimitAfterStatsShouldCapOutput() {
        ParsedEsqlQuery query = parse("FROM x | STATS c = COUNT(*) | LIMIT 10");
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(10)));
        assertThat(query.isAggregating(), is(true));
    }

    public void testLimitBetweenTwoStatsShouldNotCapOutput() {
        ParsedEsqlQuery query = parse("FROM x | STATS c = COUNT(*) BY h | LIMIT 5 | STATS m = MAX(c)");
        assertThat(query.outputLimit(), equalTo(OptionalInt.empty()));
    }

    public void testSmallestOfSeveralOutputLimitsShouldBeReported() {
        ParsedEsqlQuery query = parse("FROM x | LIMIT 100 | EVAL y = 1 | LIMIT 50");
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(50)));
        assertThat(query.isAggregating(), is(false));
    }

    public void testNoLimitShouldReportNone() {
        assertThat(parse("FROM x | STATS c = COUNT(*)").outputLimit(), equalTo(OptionalInt.empty()));
    }

    public void testStatsBelowOtherCommandsShouldCountAsAggregation() {
        assertThat(parse("FROM x | STATS c = COUNT(*) BY h | EVAL d = c * 2 | WHERE d > 1 | KEEP h, d").isAggregating(), is(true));
        assertThat(parse("FROM x | WHERE a > 1 | KEEP a, b").isAggregating(), is(false));
    }

    public void testInlineStatsShouldNotCountAsAggregation() {
        ParsedEsqlQuery query = parse("FROM x | INLINE STATS c = COUNT(*) BY h | LIMIT 5");
        assertThat(query.isAggregating(), is(false));
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(5)));
        assertThat(query.indexPatterns(), contains("x"));
    }

    public void testBacktickFieldShouldBeUnquoted() {
        ParsedEsqlQuery query = parse("FROM x | WHERE `@timestamp` > NOW() - 1 day | SORT `@timestamp` DESC");
        assertThat(query.filtersOn("@timestamp"), is(true));
        assertThat(query.filtersOn("other"), is(false));
    }

    public void testFilterOnOtherFieldShouldNotReportTimeField() {
        ParsedEsqlQuery query = parse(
            "FROM x | WHERE k8s.event.reason IS NOT NULL | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 minute)"
        );
        assertThat(query.filtersOn("k8s.event.reason"), is(true));
        assertThat(query.filtersOn("@timestamp"), is(false));
    }

    public void testIndexPatternsShouldBeSplitAndReportedAsWritten() {
        ParsedEsqlQuery query = parse("FROM logs-*, -logs-secret, remote:b, \"quoted:idx\"");
        assertThat(query.indexPatterns(), containsInAnyOrder("logs-*", "-logs-secret", "remote:b", "quoted:idx"));
    }

    public void testTimeSeriesSourceShouldBeProbedWithFromScopedToTimeSeriesIndices() {
        ParsedEsqlQuery query = parse("TS metrics METADATA _index | STATS max(cpu) BY TBUCKET(1 hour)");
        String scoped = "FROM metrics METADATA _index, _index_mode | WHERE _index_mode == \"time_series\" | ";
        assertThat(query.sourceRangeSummaryQuery("@timestamp").query(), containsString(scoped + "STATS"));
        assertThat(query.sourceTimeFieldProbeQuery("@timestamp").query(), containsString(scoped + "KEEP"));
    }

    public void testTimeSeriesSourceWithoutMetadataShouldGetIndexModeMetadata() {
        ParsedEsqlQuery query = parse("TS metrics | STATS max(cpu) BY TBUCKET(1 hour)");
        assertThat(
            query.sourceTimeFieldProbeQuery("@timestamp").query(),
            containsString("FROM metrics METADATA _index_mode | WHERE _index_mode == \"time_series\" | KEEP")
        );
    }

    public void testTimeSeriesSourceWithIndexModeMetadataShouldNotRepeatIt() {
        ParsedEsqlQuery query = parse("TS metrics METADATA _index_mode | STATS max(cpu) BY TBUCKET(1 hour)");
        assertThat(
            query.sourceTimeFieldProbeQuery("@timestamp").query(),
            containsString("FROM metrics METADATA _index_mode | WHERE _index_mode == \"time_series\" | KEEP")
        );
    }

    public void testPromqlShouldBeRejected() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse("PROMQL index=metrics step=5m avg(foo)"));
        assertThat(e.getMessage(), containsString("[PROMQL] is not supported in ES|QL datafeeds"));
    }

    public void testColumnMetadataSettingShouldBeRejected() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse("SET column_metadata=false; FROM logs | LIMIT 1"));
        assertThat(e.getMessage(), containsString("[SET column_metadata] is not supported in ES|QL datafeeds"));
    }

    public void testInSubqueryShouldBeRejected() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse("FROM logs | WHERE host IN (FROM remote:hosts | KEEP host)"));
        assertThat(e.getMessage(), containsString("[IN (subquery)] is not supported in ES|QL datafeeds"));
    }

    public void testDynamicInferenceSettingShouldOverrideNodeSetting() {
        String completion = "FROM t | COMPLETION prompt_field WITH { \"inference_id\" : \"id\" }";
        try (ClusterService disabled = clusterService(Settings.builder().put("esql.command.completion.enabled", false).build())) {
            expectThrows(ParsingException.class, () -> parser.parse(completion, disabled));
            ClusterServiceUtils.setState(
                disabled,
                ClusterState.builder(disabled.state())
                    .metadata(
                        Metadata.builder(disabled.state().metadata())
                            .persistentSettings(Settings.builder().put("esql.command.completion.enabled", true).build())
                    )
                    .build()
            );
            assertThat(parser.parse(completion, disabled).indexPatterns(), contains("t"));
        }
    }

    public void testTimeSeriesSourceShouldReadIndices() {
        ParsedEsqlQuery query = parse("TS metrics | STATS max(cpu) BY TBUCKET(1 hour)");
        assertThat(query.isAggregating(), is(true));
        assertThat(query.indexPatterns(), contains("metrics"));
    }

    public void testRowSourceShouldNotReadIndices() {
        assertThat(parse("ROW a = 1 | LIMIT 1").indexPatterns(), empty());
    }

    public void testParsedCollectionsShouldBeUnmodifiable() {
        ParsedEsqlQuery query = parse("FROM t | WHERE a > 1");
        expectThrows(UnsupportedOperationException.class, () -> query.indexPatterns().add("x"));
    }

    public void testInvalidQueryShouldFailWithBadRequest() {
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> parse("FROM t | NOT_A_COMMAND x"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testConcurrentParsingShouldMatchSequentialResults() throws Exception {
        String[] queries = {
            "FROM a | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour) | LIMIT 500",
            "SET unmapped_fields=\"nullify\"; FROM (FROM r:a | WHERE x > 1) | SORT @timestamp",
            "FROM t | WHERE a > 1 | LIMIT 3" };
        List<ParsedEsqlQuery> expected = new ArrayList<>();
        for (String query : queries) {
            expected.add(parse(query));
        }
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                futures.add(executor.submit(() -> {
                    int mismatches = 0;
                    for (int i = 0; i < 100; i++) {
                        int k = i % queries.length;
                        if (parse(queries[k]).equals(expected.get(k)) == false) {
                            mismatches++;
                        }
                    }
                    return mismatches;
                }));
            }
            for (Future<Integer> future : futures) {
                assertThat(future.get(60, TimeUnit.SECONDS), equalTo(0));
            }
        } finally {
            terminate(executor);
        }
    }
}
