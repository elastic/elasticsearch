/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.plugins.spi.SPIClassIterator;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EsqlDatafeedQueryParserTests extends ESTestCase {

    private final EsqlDatafeedQueryParser parser = new EsqlDatafeedQueryParser();

    public void testServicesFileShouldRegisterParser() {
        SPIClassIterator<EsqlQueryParser> providers = SPIClassIterator.get(EsqlQueryParser.class, getClass().getClassLoader());
        List<Class<? extends EsqlQueryParser>> classes = new ArrayList<>();
        providers.forEachRemaining(classes::add);
        assertThat(classes, contains(EsqlDatafeedQueryParser.class));
    }

    public void testNestedCommentsShouldNotHideSource() {
        ParsedEsqlQuery query = parser.parse("/* outer /* inner */ still outer */ FROM t | LIMIT 5");
        assertThat(query.indexPatterns(), contains("t"));
        assertThat(query.readsIndices(), is(true));
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(5)));
    }

    public void testSetStatementsShouldBeReportedAsSettings() {
        ParsedEsqlQuery query = parser.parse(
            "SET unmapped_fields=\"nullify\"; SET project_routing=\"_alias:*\"; SET time_zone=\"Europe/Berlin\";"
                + "\nFROM idx | STATS c = COUNT(*)"
        );
        assertThat(query.setting("unmapped_fields"), equalTo(Optional.of("nullify")));
        assertThat(query.setting("project_routing"), equalTo(Optional.of("_alias:*")));
        assertThat(query.setting("time_zone"), equalTo(Optional.of("Europe/Berlin")));
        assertThat(query.setting("missing"), equalTo(Optional.empty()));
        assertThat(query.indexPatterns(), contains("idx"));
    }

    public void testNoSetStatementsShouldGiveNoSettings() {
        assertThat(parser.parse("FROM idx").setting("time_zone"), equalTo(Optional.empty()));
    }

    public void testSourceWithPipelineShouldReparseToSameSourceAndSettings() {
        String[] queries = {
            "FROM a, b | STATS c = COUNT(*)",
            "SET unmapped_fields=\"nullify\";\nFROM idx | WHERE x > 1",
            "/* c */ FROM a /* mid */ , remote:b METADATA _id, _index | LIMIT 5",
            "FROM (FROM remote:logs | WHERE a > 1), local | STATS c = COUNT(*)",
            "FROM (FROM a | WHERE a > 1), b METADATA _index | KEEP a",
            "FROM t // trailing\n| LIMIT 1",
            "TS metrics | STATS max(cpu) BY TBUCKET(1 hour)",
            "/* \uD83D\uDE00 */ SET time_zone=\"UTC\"; FROM idx",
            "SET unmapped_fields=\"nullify\"; FROM (FROM remote:logs | WHERE a > 1) | STATS c = COUNT(*)" };
        for (String text : queries) {
            ParsedEsqlQuery query = parser.parse(text);
            ParsedEsqlQuery reparsed = parser.parse(query.sourceWithPipeline("KEEP a | LIMIT 0"));
            assertThat(text, reparsed.indexPatterns(), equalTo(query.indexPatterns()));
            assertThat(text, reparsed.setting("unmapped_fields"), equalTo(query.setting("unmapped_fields")));
            assertThat(text, reparsed.setting("time_zone"), equalTo(query.setting("time_zone")));
        }
    }

    public void testSourceWithPipelineShouldKeepMetadataOfPlainFrom() {
        ParsedEsqlQuery query = parser.parse("/* c */ FROM a /* mid */ , remote:b METADATA _id, _index | LIMIT 5");
        assertThat(query.sourceWithPipeline("LIMIT 0"), equalTo("FROM a /* mid */ , remote:b METADATA _id, _index\n| LIMIT 0"));
    }

    public void testSourceWithPipelineShouldPrefixSettings() {
        ParsedEsqlQuery query = parser.parse("SET time_zone=\"UTC\"; FROM a | LIMIT 5");
        assertThat(query.sourceWithPipeline("LIMIT 0"), equalTo("SET time_zone=\"UTC\";\nFROM a\n| LIMIT 0"));
    }

    public void testWithPipelineShouldNotBeSwallowedByTrailingComment() {
        ParsedEsqlQuery query = parser.parse("FROM t // trailing");
        ParsedEsqlQuery extended = parser.parse(query.withPipeline("LIMIT 3"));
        assertThat(extended.outputLimit(), equalTo(OptionalInt.of(3)));
    }

    public void testSourceWithPipelineShouldFailWithoutIndexSource() {
        ParsedEsqlQuery query = parser.parse("ROW a = 1 | LIMIT 1");
        expectThrows(IllegalStateException.class, () -> query.sourceWithPipeline("LIMIT 0"));
    }

    public void testSubqueriesShouldExposeInnerPatterns() {
        ParsedEsqlQuery query = parser.parse("FROM (FROM remote:logs | WHERE a > 1), local, *:wild, \"quoted:idx\" | LIMIT 5");
        assertThat(query.indexPatterns(), containsInAnyOrder("remote:logs", "local", "*:wild", "quoted:idx"));
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(5)));
        assertThat(query.filtersOn("a"), is(false));
    }

    public void testSingleSubqueryShouldBelongToTopLevelPipeline() {
        ParsedEsqlQuery query = parser.parse("FROM (FROM remote:logs | WHERE a > 1) | LIMIT 5");
        assertThat(query.indexPatterns(), contains("remote:logs"));
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(5)));
        assertThat(query.filtersOn("a"), is(true));
    }

    public void testForkShouldEndTopLevelPipeline() {
        ParsedEsqlQuery query = parser.parse("FROM t | WHERE b > 1 | FORK (WHERE a > 1 | LIMIT 5) (WHERE a < 1) | LIMIT 3");
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(3)));
        assertThat(query.filtersOn("a"), is(false));
        assertThat(query.filtersOn("b"), is(false));
        assertThat(query.indexPatterns(), contains("t"));
        assertThat(query.sourceWithPipeline("LIMIT 0"), equalTo("FROM t\n| LIMIT 0"));
    }

    public void testLimitBeforeStatsShouldNotCapOutput() {
        ParsedEsqlQuery query = parser.parse("FROM x | LIMIT 500 | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)");
        assertThat(query.aggregates(), is(true));
        assertThat(query.outputLimit(), equalTo(OptionalInt.empty()));
        assertThat(query.limitBeforeAggregation(), equalTo(OptionalInt.of(500)));
    }

    public void testLimitAfterStatsShouldCapOutput() {
        ParsedEsqlQuery query = parser.parse("FROM x | STATS c = COUNT(*) | LIMIT 10");
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(10)));
        assertThat(query.limitBeforeAggregation(), equalTo(OptionalInt.empty()));
    }

    public void testLimitOnBothSidesOfStatsShouldBeReportedSeparately() {
        ParsedEsqlQuery query = parser.parse("FROM x | LIMIT 500 | STATS c = COUNT(*) | LIMIT 10");
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(10)));
        assertThat(query.limitBeforeAggregation(), equalTo(OptionalInt.of(500)));
    }

    public void testSmallestOfSeveralOutputLimitsShouldBeReported() {
        ParsedEsqlQuery query = parser.parse("FROM x | LIMIT 100 | EVAL y = 1 | LIMIT 50");
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(50)));
        assertThat(query.aggregates(), is(false));
    }

    public void testNoLimitShouldReportNone() {
        ParsedEsqlQuery query = parser.parse("FROM x | STATS c = COUNT(*)");
        assertThat(query.outputLimit(), equalTo(OptionalInt.empty()));
        assertThat(query.limitBeforeAggregation(), equalTo(OptionalInt.empty()));
    }

    public void testBacktickFieldShouldBeUnquoted() {
        ParsedEsqlQuery query = parser.parse("FROM x | WHERE `@timestamp` > NOW() - 1 day | SORT `@timestamp` DESC");
        assertThat(query.filtersOn("@timestamp"), is(true));
        assertThat(query.sortsOn("@timestamp"), is(true));
        assertThat(query.sortsOn("other"), is(false));
    }

    public void testInlineStatsShouldNotCountAsAggregation() {
        ParsedEsqlQuery query = parser.parse("FROM x | INLINE STATS c = COUNT(*) BY h | LIMIT 5");
        assertThat(query.aggregates(), is(false));
        assertThat(query.outputLimit(), equalTo(OptionalInt.of(5)));
        assertThat(query.indexPatterns(), contains("x"));
    }

    public void testLookupJoinShouldWalkLeftAndExcludeLookupIndex() {
        ParsedEsqlQuery query = parser.parse("FROM x | LOOKUP JOIN l ON k | STATS c = COUNT(*)");
        assertThat(query.aggregates(), is(true));
        assertThat(query.indexPatterns(), contains("x"));
        assertThat(query.sourceWithPipeline("LIMIT 0"), equalTo("FROM x\n| LIMIT 0"));
    }

    public void testTimeSeriesSourceShouldReadIndices() {
        ParsedEsqlQuery query = parser.parse("TS metrics | STATS max(cpu) BY TBUCKET(1 hour)");
        assertThat(query.readsIndices(), is(true));
        assertThat(query.aggregates(), is(true));
        assertThat(query.indexPatterns(), contains("metrics"));
    }

    public void testRowSourceShouldNotReadIndices() {
        ParsedEsqlQuery query = parser.parse("ROW a = 1 | LIMIT 1");
        assertThat(query.readsIndices(), is(false));
        assertThat(query.indexPatterns(), empty());
    }

    public void testInvalidQueryShouldFailWithBadRequest() {
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> parser.parse("FROM t | NOT_A_COMMAND x"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testConcurrentParsingShouldMatchSequentialResults() throws Exception {
        String[] queries = {
            "FROM a | LIMIT 500 | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)",
            "SET unmapped_fields=\"nullify\"; FROM (FROM r:a | WHERE x > 1), b | SORT @timestamp",
            "FROM t | FORK (WHERE a > 1 | LIMIT 5) (WHERE a < 1) | LIMIT 3" };
        List<ParsedEsqlQuery> expected = new ArrayList<>();
        for (String query : queries) {
            expected.add(parser.parse(query));
        }
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                futures.add(executor.submit(() -> {
                    int mismatches = 0;
                    for (int i = 0; i < 100; i++) {
                        int k = i % queries.length;
                        if (parser.parse(queries[k]).equals(expected.get(k)) == false) {
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
