/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.spi.SPIClassIterator;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryAnalyzer;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryStructure;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryStructure.SourceKind;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.PipelineStep;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.PipelineStep.Kind;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucket;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class EsqlParserQueryAnalyzerTests extends ESTestCase {

    private final EsqlParserQueryAnalyzer analyzer = new EsqlParserQueryAnalyzer();

    private static List<Kind> kinds(EsqlQueryStructure structure) {
        return structure.outerPipeline().stream().map(PipelineStep::kind).toList();
    }

    private static PipelineStep stats(EsqlQueryStructure structure) {
        return structure.outerPipeline().stream().filter(s -> s.kind() == Kind.STATS).findFirst().orElseThrow();
    }

    public void testServicesFileShouldRegisterAnalyzer() {
        SPIClassIterator<EsqlQueryAnalyzer> providers = SPIClassIterator.get(EsqlQueryAnalyzer.class, getClass().getClassLoader());
        List<Class<? extends EsqlQueryAnalyzer>> classes = new ArrayList<>();
        providers.forEachRemaining(classes::add);
        assertThat(classes, contains(EsqlParserQueryAnalyzer.class));
    }

    public void testNestedCommentsShouldNotHideSource() {
        EsqlQueryStructure structure = analyzer.analyze("/* outer /* inner */ still outer */ FROM t | LIMIT 5");
        assertThat(structure.sourcePatterns(), contains("t"));
        assertThat(structure.sourceCommand(), equalTo("FROM t"));
        assertThat(kinds(structure), contains(Kind.LIMIT));
    }

    public void testSetStatementsShouldBeReportedAsSettings() {
        String settingsText = "SET unmapped_fields=\"nullify\"; SET project_routing=\"_alias:*\"; SET time_zone=\"Europe/Berlin\";";
        EsqlQueryStructure structure = analyzer.analyze(settingsText + "\nFROM idx | STATS c = COUNT(*)");
        assertThat(
            structure.settings(),
            equalTo(Map.of("unmapped_fields", "nullify", "project_routing", "_alias:*", "time_zone", "Europe/Berlin"))
        );
        assertThat(structure.settingsText(), equalTo(settingsText));
        assertThat(structure.sourcePatterns(), contains("idx"));
    }

    public void testSupplementaryCharacterBeforeSetShouldNotShiftSettingsText() {
        String settingsText = "/* \uD83D\uDE00 */ SET time_zone=\"UTC\";";
        EsqlQueryStructure structure = analyzer.analyze(settingsText + " FROM idx");
        assertThat(structure.settingsText(), equalTo(settingsText));
    }

    public void testNoSetStatementsShouldGiveEmptySettings() {
        EsqlQueryStructure structure = analyzer.analyze("FROM idx");
        assertThat(structure.settings().isEmpty(), is(true));
        assertThat(structure.settingsText(), equalTo(""));
    }

    public void testSourceCommandShouldReparseWithAppendedPipeline() {
        String[] queries = {
            "FROM a, b | STATS c = COUNT(*)",
            "SET unmapped_fields=\"nullify\";\nFROM idx | WHERE x > 1",
            "/* c */ FROM a /* mid */ , remote:b METADATA _id, _index | LIMIT 5",
            "FROM (FROM remote:logs | WHERE a > 1), local | STATS c = COUNT(*)",
            "FROM (FROM a | WHERE a > 1), b METADATA _index | KEEP a",
            "FROM t // trailing\n| LIMIT 1",
            "TS metrics | STATS max(cpu) BY TBUCKET(1 hour)",
            "SET unmapped_fields=\"nullify\"; FROM (FROM remote:logs | WHERE a > 1) | STATS c = COUNT(*)" };
        for (String query : queries) {
            EsqlQueryStructure structure = analyzer.analyze(query);
            String probe = structure.settingsText() + "\n" + structure.sourceCommand() + "\n| KEEP a | LIMIT 0";
            EsqlQueryStructure reparsed = analyzer.analyze(probe);
            assertThat(query, reparsed.sourcePatterns(), equalTo(structure.sourcePatterns()));
            assertThat(query, reparsed.settings(), equalTo(structure.settings()));
        }
    }

    public void testSourceCommandShouldKeepMetadata() {
        EsqlQueryStructure structure = analyzer.analyze("/* c */ FROM a /* mid */ , remote:b METADATA _id, _index | LIMIT 5");
        assertThat(structure.sourceCommand(), equalTo("FROM a /* mid */ , remote:b METADATA _id, _index"));
        assertThat(structure.sourceKind(), equalTo(SourceKind.FROM));
    }

    public void testSubqueriesShouldExposeInnerPatterns() {
        EsqlQueryStructure structure = analyzer.analyze("FROM (FROM remote:logs | WHERE a > 1), local, *:wild, \"quoted:idx\" | LIMIT 5");
        assertThat(structure.sourcePatterns(), containsInAnyOrder("remote:logs", "local", "*:wild", "quoted:idx"));
        assertThat(structure.sourceCommand(), equalTo("FROM (FROM remote:logs | WHERE a > 1), local, *:wild, \"quoted:idx\""));
        assertThat(kinds(structure), contains(Kind.LIMIT));
    }

    public void testSingleSubqueryShouldUseInnerRelation() {
        EsqlQueryStructure structure = analyzer.analyze("FROM (FROM remote:logs | WHERE a > 1) | LIMIT 5");
        assertThat(structure.sourceCommand(), equalTo("FROM remote:logs"));
        assertThat(structure.sourcePatterns(), contains("remote:logs"));
        assertThat(kinds(structure), contains(Kind.LIMIT, Kind.WHERE));
    }

    public void testForkShouldEndOuterPipelineWithMerge() {
        EsqlQueryStructure structure = analyzer.analyze("FROM t | FORK (WHERE a > 1 | LIMIT 5) (WHERE a < 1) | LIMIT 3");
        assertThat(kinds(structure), contains(Kind.LIMIT, Kind.MERGE));
        assertThat(structure.sourceCommand(), equalTo("FROM t"));
        assertThat(structure.sourcePatterns(), contains("t"));
    }

    public void testLimitBeforeStatsShouldBeOrderedRootFirst() {
        EsqlQueryStructure structure = analyzer.analyze("FROM x | LIMIT 500 | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)");
        assertThat(kinds(structure), contains(Kind.STATS, Kind.LIMIT));
        assertThat(kinds(analyzer.analyze("FROM x | STATS c = COUNT(*) | LIMIT 10")), contains(Kind.LIMIT, Kind.STATS));
    }

    public void testBacktickFieldShouldBeUnquoted() {
        EsqlQueryStructure structure = analyzer.analyze("FROM x | WHERE `@timestamp` > NOW() - 1 day | SORT `@timestamp` DESC");
        assertThat(kinds(structure), contains(Kind.SORT, Kind.WHERE));
        for (PipelineStep step : structure.outerPipeline()) {
            assertThat(step.referencedFields(), equalTo(Set.of("@timestamp")));
        }
    }

    public void testFixedSpanBucketShouldBeReported() {
        TimeValue hour = TimeValue.timeValueHours(1);
        assertBucket("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)", "@timestamp", hour);
        assertBucket("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1h)", "@timestamp", hour);
        assertBucket("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, \"1 hour\")", "@timestamp", hour);
        assertBucket("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, \"30 minutes\")", "@timestamp", TimeValue.timeValueMinutes(30));
        assertBucket("FROM x | STATS c = COUNT(*) BY BUCKET(`@timestamp`, 15 minutes)", "@timestamp", TimeValue.timeValueMinutes(15));
        assertBucket("FROM x | STATS c = COUNT(*) BY d = DATE_TRUNC(1 hour, @timestamp)", "@timestamp", hour);
        assertBucket("TS m | STATS max(cpu) BY TBUCKET(1 hour)", null, hour);
    }

    public void testAutoOrCalendarBucketShouldNotBeReported() {
        assertNoBucket("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 20, \"2024-01-01\", \"2024-01-02\")");
        assertNoBucket("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 month)");
        assertNoBucket("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, \"1d\")");
        // ES|QL only converts a string span with a space between value and unit.
        assertNoBucket("FROM x | STATS c = COUNT(*) BY b = BUCKET(@timestamp, \"1h\")");
        assertNoBucket("FROM x | STATS c = COUNT(*) BY h");
        assertNoBucket("FROM x | STATS c = COUNT(*) BY a = BUCKET(@timestamp, 1 hour), b = BUCKET(event.ingested, 1 hour)");
    }

    public void testInlineStatsShouldNotBeReportedAsStats() {
        EsqlQueryStructure structure = analyzer.analyze("FROM x | INLINE STATS c = COUNT(*) BY h | LIMIT 5");
        assertThat(kinds(structure), contains(Kind.LIMIT, Kind.OTHER));
        assertThat(structure.sourcePatterns(), contains("x"));
    }

    public void testLookupJoinShouldWalkLeftAndListLookupIndex() {
        EsqlQueryStructure structure = analyzer.analyze("FROM x | LOOKUP JOIN l ON k | STATS c = COUNT(*)");
        assertThat(kinds(structure), contains(Kind.STATS, Kind.OTHER));
        assertThat(structure.sourceCommand(), equalTo("FROM x"));
        assertThat(structure.sourcePatterns(), contains("x"));
        assertThat(structure.lookupIndices(), contains("l"));
    }

    public void testTimeSeriesSourceShouldReportTsKind() {
        assertThat(analyzer.analyze("TS metrics | STATS max(cpu) BY TBUCKET(1 hour)").sourceKind(), equalTo(SourceKind.TS));
        assertThat(analyzer.analyze("FROM metrics | LIMIT 1").sourceKind(), equalTo(SourceKind.FROM));
    }

    public void testRowSourceShouldReportOtherKind() {
        EsqlQueryStructure structure = analyzer.analyze("ROW a = 1 | LIMIT 1");
        assertThat(structure.sourceKind(), equalTo(SourceKind.OTHER));
        assertThat(structure.sourceCommand(), equalTo(""));
        assertThat(structure.sourcePatterns(), empty());
    }

    public void testInvalidQueryShouldFailWithBadRequest() {
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> analyzer.analyze("FROM t | NOT_A_COMMAND x"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testOutputTypeShouldBeDateOnlyForDateTypes() {
        assertThat(analyzer.isDateType("date"), is(true));
        assertThat(analyzer.isDateType("date_nanos"), is(true));
        assertThat(analyzer.isDateType("long"), is(false));
        assertThat(analyzer.isDateType("keyword"), is(false));
        assertThat(analyzer.isDateType("unknown_type"), is(false));
    }

    public void testConcurrentAnalysisShouldMatchSequentialResults() throws Exception {
        String[] queries = {
            "FROM a | LIMIT 500 | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)",
            "SET unmapped_fields=\"nullify\"; FROM (FROM r:a | WHERE x > 1), b | SORT @timestamp",
            "FROM t | FORK (WHERE a > 1 | LIMIT 5) (WHERE a < 1) | LIMIT 3" };
        List<EsqlQueryStructure> expected = new ArrayList<>();
        for (String query : queries) {
            expected.add(analyzer.analyze(query));
        }
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                futures.add(executor.submit(() -> {
                    int mismatches = 0;
                    for (int i = 0; i < 100; i++) {
                        int k = i % queries.length;
                        if (analyzer.analyze(queries[k]).equals(expected.get(k)) == false) {
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

    private void assertBucket(String query, String field, TimeValue span) {
        TimeBucket bucket = stats(analyzer.analyze(query)).bucket();
        assertThat(query, bucket, instanceOf(TimeBucket.class));
        assertThat(query, bucket.field(), equalTo(field));
        assertThat(query, bucket.span(), equalTo(span));
    }

    private void assertNoBucket(String query) {
        assertThat(query, stats(analyzer.analyze(query)).bucket(), nullValue());
    }
}
