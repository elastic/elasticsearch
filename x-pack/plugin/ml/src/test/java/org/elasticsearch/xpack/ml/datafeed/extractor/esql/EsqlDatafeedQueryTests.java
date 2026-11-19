/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.esql.EsqlParserTestSupport;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryWithParams;

import java.util.OptionalInt;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EsqlDatafeedQueryTests extends ESTestCase {

    private static final EsqlQueryParser PARSER = EsqlParserTestSupport.parser();
    private static final String SORT = "\n| SORT ??timeField ASC";
    private static final String CAP = "\n| LIMIT 10000";

    private static EsqlDatafeedQuery parse(String query) {
        return EsqlDatafeedQuery.parse(PARSER, query);
    }

    /** Generated queries take a {@code ??timeField} parameter that the parser cannot resolve without a request. */
    private static void assertParses(EsqlQueryWithParams generated) {
        parse(generated.query().replace("??timeField", "ts"));
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
            "FROM logs-* | STATS c = COUNT(*) BY bucket = BUCKET(@timestamp, 1h)" };
        for (String query : queries) {
            EsqlQueryWithParams ordered = parse(query).orderedQuery("@timestamp");
            assertThat(query, ordered.query(), equalTo(query + SORT + CAP));
            assertThat(query, ordered.params().getFirst().value(), equalTo("@timestamp"));
        }
    }

    public void testQueryWithUserLimitOnOutputShouldBeSortedWithoutCap() {
        String[] queries = {
            "FROM logs-* | LIMIT 20",
            "FROM logs-* | SORT value DESC | LIMIT 20",
            "FROM logs-*\n| KEEP @timestamp\n| lImIt 42",
            "FROM logs-* | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1h) | LIMIT 10" };
        for (String query : queries) {
            assertThat(query, parse(query).orderedQuery("@timestamp").query(), equalTo(query + SORT));
        }
    }

    public void testQueryEndingInLineCommentShouldKeepGeneratedPipelineOnItsOwnLine() {
        String query = "FROM logs-* // no user limit";
        assertThat(parse(query).orderedQuery("@timestamp").query(), equalTo(query + SORT + CAP));
        assertParses(parse(query).orderedQuery("@timestamp"));
    }

    public void testLimitBeforeStatsShouldBeRejected() {
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> parse("FROM logs-* | LIMIT 500 | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1h)")
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testCommandsThatDependOnTheTimeWindowShouldBeRejected() {
        String[] queries = {
            "FROM logs-* | STATS c = COUNT(*) | LIMIT 5 | STATS d = COUNT(*)",
            "FROM logs-* | SAMPLE 0.1 | STATS c = COUNT(*)" };
        for (String query : queries) {
            ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> parse(query));
            assertThat(query, e.status(), equalTo(RestStatus.BAD_REQUEST));
        }
    }

    public void testLimitAfterStatsShouldCapOutput() {
        assertThat(parse("FROM logs-* | STATS c = COUNT(*) | LIMIT 5").outputLimit(), equalTo(OptionalInt.of(5)));
        assertThat(parse("FROM logs-* | LIMIT 5").injectsRowLimit(), is(false));
    }

    public void testOutputLimitShouldExposeTheLimitValue() {
        assertThat(parse("FROM logs-* | LIMIT 20").outputLimit(), equalTo(OptionalInt.of(20)));
        assertThat(parse("FROM logs-* | LIMIT 20 | LIMIT 5").outputLimit(), equalTo(OptionalInt.of(5)));
    }

    public void testUnsupportedConstructsShouldBeRejectedWithBadRequest() {
        String[] queries = {
            "FROM logs-* | FORK (WHERE level == \"warn\" | LIMIT 3) (WHERE level == \"error\" | LIMIT 4)",
            "FROM (FROM a | WHERE x > 1), b | STATS c = COUNT(*)",
            "FROM logs | WHERE x IN (FROM other | LIMIT 1)",
            "FROM logs | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1h) | RENAME b AS t",
            "FROM logs | MV_EXPAND x" };
        for (String query : queries) {
            ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> parse(query));
            assertThat(query, e.status(), equalTo(RestStatus.BAD_REQUEST));
        }
    }

    public void testSourceProbesShouldKeepSettingsAndSourceCommandOnly() {
        EsqlDatafeedQuery query = parse("SET unmapped_fields=\"nullify\";\nFROM logs-* METADATA _index | STATS c = COUNT(*)");
        EsqlQueryWithParams summary = query.sourceRangeSummaryQuery("@timestamp");
        assertThat(
            summary.query(),
            equalTo(
                "SET unmapped_fields=\"nullify\";\nFROM logs-* METADATA _index"
                    + " | STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)"
            )
        );
        assertThat(summary.params().getFirst().value(), equalTo("@timestamp"));
        EsqlQueryWithParams probe = query.sourceTimeFieldProbeQuery("event.created");
        assertThat(probe.query(), equalTo("SET unmapped_fields=\"nullify\";\nFROM logs-* METADATA _index | KEEP ??timeField | LIMIT 0"));
        assertThat(probe.params().getFirst().value(), equalTo("event.created"));
        assertParses(summary);
        assertParses(probe);
    }

    public void testOutputRowsProbeShouldCountTheFullPipeline() {
        String query = "FROM logs-* | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1h) // trailing";
        EsqlQueryWithParams probe = parse(query).outputRowsProbeQuery();
        assertThat(probe.query(), equalTo(query + "\n| STATS probe_output_rows = COUNT(*)"));
        assertThat(probe.params().isEmpty(), is(true));
        assertParses(probe);
    }

    public void testQueryWithoutIndexSourceShouldBeRejected() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> parse("ROW a = 1"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testLimitZeroQueryShouldAppendOnNewLine() {
        assertThat(parse("FROM logs // c").limitZeroQuery().query(), equalTo("FROM logs // c\n| LIMIT 0"));
    }
}
