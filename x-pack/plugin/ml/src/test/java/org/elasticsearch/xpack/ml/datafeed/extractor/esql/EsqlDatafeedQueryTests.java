/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.ml.EsqlParserQueryAnalyzer;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryAnalyzer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EsqlDatafeedQueryTests extends ESTestCase {

    private static final EsqlQueryAnalyzer ANALYZER = new EsqlParserQueryAnalyzer();
    private static final String SORT = "\n| SORT ??timeField ASC";
    private static final String CAP = "\n| LIMIT 10000";

    private static EsqlDatafeedQuery parse(String query) {
        return EsqlDatafeedQuery.parse(ANALYZER, query);
    }

    /** Generated queries take {@code ??} parameters that the parser cannot resolve without a request. */
    private static void assertParses(String generatedQuery) {
        parse(generatedQuery.replace("??sourceTimeField", "ts").replace("??timeField", "ts"));
    }

    public void testTopLevelStatsShouldBeAnAggregation() {
        assertThat(parse("FROM logs-* | STATS COUNT(*)").hasAggregation(), is(true));
        assertThat(parse("FROM logs-* | stats c = COUNT(*) BY BUCKET(@timestamp, 1h)").hasAggregation(), is(true));
    }

    public void testStatsOutsideTheTopLevelPipelineShouldNotBeAnAggregation() {
        assertThat(parse("FROM logs-* | KEEP @timestamp, bytes").hasAggregation(), is(false));
        assertThat(parse("FROM logs-* | KEEP statsField").hasAggregation(), is(false));
        assertThat(parse("FROM logs-* | WHERE message == \"STATS COUNT(*)\"").hasAggregation(), is(false));
        assertThat(parse("FROM logs-* | INLINE STATS c = COUNT(*) BY host").hasAggregation(), is(false));
    }

    public void testQueryWithoutUserLimitShouldBeSortedThenCapped() {
        String[] queries = {
            "FROM logs-* | KEEP @timestamp, bytes",
            "FROM logs-* | WHERE message == \"LIMIT 20\"",
            "FROM logs-* | WHERE message == \"\"\"| LIMIT 20\"\"\"",
            "FROM logs-* // | LIMIT 20\n| KEEP @timestamp",
            "FROM logs-* /* outer /* | LIMIT 20 */ comment */ | KEEP @timestamp",
            "FROM logs-* | KEEP limit_value, `LIMIT``_value`",
            "FROM logs-* | FORK (WHERE level == \"warn\" | LIMIT 3) (WHERE level == \"error\" | LIMIT 4)",
            "FROM logs-* | STATS c = COUNT(*) BY bucket = BUCKET(@timestamp, 1h)" };
        for (String query : queries) {
            assertThat(query, parse(query).orderedQuery(), equalTo(query + SORT + CAP));
        }
    }

    public void testQueryWithUserLimitOnOutputShouldBeSortedWithoutCap() {
        String[] queries = {
            "FROM logs-* | LIMIT 20",
            "FROM logs-* | SORT value DESC | LIMIT 20",
            "FROM logs-*\n| KEEP @timestamp\n| lImIt 42",
            "FROM logs-* | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1h) | LIMIT 10",
            "FROM logs-* | FORK (WHERE level == \"warn\") (WHERE level == \"error\") | LIMIT 5" };
        for (String query : queries) {
            assertThat(query, parse(query).orderedQuery(), equalTo(query + SORT));
        }
    }

    public void testQueryEndingInLineCommentShouldKeepGeneratedPipelineOnItsOwnLine() {
        String query = "FROM logs-* // no user limit";
        assertThat(parse(query).orderedQuery(), equalTo(query + SORT + CAP));
        assertParses(parse(query).orderedQuery());
    }

    public void testLimitBeforeStatsShouldCapOutputAndBeReported() {
        EsqlDatafeedQuery query = parse("FROM logs-* | LIMIT 500 | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1h)");
        assertThat(query.injectsRowLimit(), is(true));
        assertThat(query.hasLimitBeforeAggregation(), is(true));
        assertThat(query.hasOuterLimit(), is(true));
    }

    public void testLimitAfterStatsShouldNotBeReportedAsLimitBeforeAggregation() {
        assertThat(parse("FROM logs-* | STATS c = COUNT(*) | LIMIT 5").hasLimitBeforeAggregation(), is(false));
        assertThat(parse("FROM logs-* | STATS c = COUNT(*) | LIMIT 5 | STATS d = COUNT(*)").hasLimitBeforeAggregation(), is(false));
        assertThat(parse("FROM logs-* | SAMPLE 0.1 | STATS c = COUNT(*)").hasLimitBeforeAggregation(), is(false));
        assertThat(parse("FROM logs-* | LIMIT 5").hasLimitBeforeAggregation(), is(false));
    }

    public void testLimitInsideSubqueryShouldNotBeAnOuterLimit() {
        assertThat(parse("FROM logs | WHERE x IN (FROM other | LIMIT 1)").hasOuterLimit(), is(false));
        assertThat(parse("FROM logs | /* | LIMIT 5 */ KEEP a").hasOuterLimit(), is(false));
    }

    public void testTimeFieldClausesShouldBeDetectedByFieldName() {
        assertThat(parse("FROM logs | WHERE ts > 0").hasOuterTimeFilter("ts"), is(true));
        assertThat(parse("FROM logs | SORT ts DESC").hasOuterTimeSort("ts"), is(true));
        assertThat(parse("FROM logs | WHERE `@timestamp` > NOW() - 1 day").hasOuterTimeFilter("@timestamp"), is(true));
        assertThat(parse("FROM logs | WHERE message == \"WHERE ts\"").hasOuterTimeFilter("ts"), is(false));
        assertThat(parse("FROM logs | KEEP `WHERE ts`, value | SORT value").hasOuterTimeSort("ts"), is(false));
    }

    public void testSourceProbesShouldKeepSettingsAndSourceCommandOnly() {
        EsqlDatafeedQuery query = parse("SET unmapped_fields=\"nullify\";\nFROM logs-* METADATA _index | STATS c = COUNT(*)");
        assertThat(
            query.sourceRangeSummaryQuery(),
            equalTo(
                "SET unmapped_fields=\"nullify\";\nFROM logs-* METADATA _index"
                    + "\n| STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)"
            )
        );
        assertThat(
            query.sourceTimeFieldProbeQuery(),
            equalTo("SET unmapped_fields=\"nullify\";\nFROM logs-* METADATA _index\n| KEEP ??sourceTimeField | LIMIT 0")
        );
        assertParses(query.sourceRangeSummaryQuery());
        assertParses(query.sourceTimeFieldProbeQuery());
    }

    public void testSourceProbesShouldKeepSubqueries() {
        EsqlDatafeedQuery query = parse("FROM (FROM a | WHERE x > 1), b | STATS c = COUNT(*)");
        assertThat(query.sourceTimeFieldProbeQuery(), equalTo("FROM (FROM a | WHERE x > 1), b\n| KEEP ??sourceTimeField | LIMIT 0"));
        assertParses(query.sourceTimeFieldProbeQuery());
    }

    public void testOutputRowsProbeShouldCountTheFullPipeline() {
        String query = "FROM logs-* | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1h) // trailing";
        assertThat(parse(query).outputRowsProbeQuery(), equalTo(query + "\n| STATS probe_output_rows = COUNT(*)"));
        assertParses(parse(query).outputRowsProbeQuery());
    }

    public void testQueryWithoutIndexSourceShouldBeRejected() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> parse("ROW a = 1"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testLimitZeroQueryShouldAppendOnNewLine() {
        assertThat(EsqlDatafeedQuery.limitZeroQuery("FROM logs // c"), equalTo("FROM logs // c\n| LIMIT 0"));
    }
}
