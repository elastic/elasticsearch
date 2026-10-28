/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EsqlQueryClauseScannerTests extends ESTestCase {

    public void testHasAggregationGivenNoStatsCommandIsFalse() {
        assertThat(EsqlQueryClauseScanner.hasAggregation("FROM logs-* | KEEP @timestamp, bytes"), is(false));
        assertThat(EsqlQueryClauseScanner.hasAggregation("FROM logs-*"), is(false));
    }

    public void testHasAggregationGivenTopLevelStatsIsTrue() {
        assertThat(EsqlQueryClauseScanner.hasAggregation("FROM logs-* | STATS COUNT(*)"), is(true));
        assertThat(EsqlQueryClauseScanner.hasAggregation("FROM logs-* | STATS doc_count = COUNT(*) BY BUCKET(@timestamp, 1h)"), is(true));
    }

    public void testHasAggregationGivenMixedCaseStatsIsTrue() {
        assertThat(EsqlQueryClauseScanner.hasAggregation("FROM logs-* | stats COUNT(*)"), is(true));
    }

    public void testHasAggregationGivenStatsOnlyInSubqueryIsFalse() {
        assertThat(EsqlQueryClauseScanner.hasAggregation("FROM logs-* | WHERE id IN (FROM other | STATS COUNT(*))"), is(false));
    }

    public void testHasAggregationGivenStatsSubstringIsFalse() {
        assertThat(EsqlQueryClauseScanner.hasAggregation("FROM logs-* | KEEP statsField"), is(false));
        assertThat(EsqlQueryClauseScanner.hasAggregation("FROM logs-* | WHERE message == \"STATS COUNT(*)\""), is(false));
    }

    public void testScanDetectsOuterLimitAtDepthZero() {
        EsqlQueryClauseScanner.ScanResult scan = EsqlQueryClauseScanner.scan("FROM logs | LIMIT 5", "ts");
        assertThat(scan.hasOuterLimit(), is(true));
        assertThat(EsqlQueryClauseScanner.scan("FROM logs | WHERE x IN (FROM other | LIMIT 1)", "ts").hasOuterLimit(), is(false));
    }

    public void testScanDetectsOuterTimeWhereAndSort() {
        assertThat(EsqlQueryClauseScanner.scan("FROM logs | WHERE ts > 0", "ts").hasOuterTimeWhere(), is(true));
        assertThat(EsqlQueryClauseScanner.scan("FROM logs | SORT ts DESC", "ts").hasOuterTimeSort(), is(true));
        assertThat(EsqlQueryClauseScanner.scan("FROM logs | WHERE message == \"WHERE ts\"", "ts").hasOuterTimeWhere(), is(false));
    }

    public void testScanIgnoresClausesInsideStringsCommentsAndBackticks() {
        String query = "FROM logs | KEEP `WHERE ts`, value /* WHERE ts | SORT ts | LIMIT 20 */ "
            + "| EVAL note = \"WHERE ts | SORT ts | LIMIT 20\" "
            + "| EVAL triple = \"\"\"WHERE ts | SORT ts | LIMIT 20\"\"\" "
            + "// WHERE ts | SORT ts | LIMIT 20\n";
        EsqlQueryClauseScanner.ScanResult scan = EsqlQueryClauseScanner.scan(query, "ts");
        assertThat(scan.hasOuterLimit(), is(false));
        assertThat(scan.hasOuterTimeWhere(), is(false));
        assertThat(scan.hasOuterTimeSort(), is(false));
    }

    public void testEndsInLineComment() {
        assertThat(EsqlQueryClauseScanner.endsInLineComment("FROM logs // note"), is(true));
        assertThat(EsqlQueryClauseScanner.endsInLineComment("FROM logs"), is(false));
    }

    public void testExtractLeadingCommandStopsAtDepthZeroPipe() {
        assertThat(EsqlQueryClauseScanner.extractLeadingCommand("FROM logs | STATS c = COUNT(*)"), equalTo("FROM logs "));
        assertThat(EsqlQueryClauseScanner.extractLeadingCommand("FROM logs | WHERE x IN (FROM o | LIMIT 1)"), equalTo("FROM logs "));
    }
}
