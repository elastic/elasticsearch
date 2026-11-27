/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class EsqlDataExtractorQueryUtilsTests extends ESTestCase {

    public void testAppendPipeShouldPutThePipeOnANewLine() {
        assertThat(EsqlDataExtractorQueryUtils.appendPipe("FROM logs", "LIMIT 0"), equalTo("FROM logs\n| LIMIT 0"));
    }

    public void testAppendPipeAfterTrailingLineCommentShouldNotBeSwallowedByTheComment() {
        assertThat(
            EsqlDataExtractorQueryUtils.appendPipe("FROM logs | LIMIT 20 // explanation", "DESCRIBE"),
            equalTo("FROM logs | LIMIT 20 // explanation\n| DESCRIBE")
        );
    }

    public void testOrderedQueryShouldAlwaysAppendTimeSortThenCap() {
        for (String query : new String[] {
            "FROM logs-* | KEEP @timestamp, bytes",
            "FROM logs-* | LIMIT 20",
            "FROM logs-* | SORT value DESC | LIMIT 20",
            "FROM logs-* | WHERE id IN (FROM other-logs | LIMIT 3 | KEEP id)",
            "FROM logs-* // no user limit" }) {
            assertThat(EsqlDataExtractorQueryUtils.buildOrderedQuery(query), equalTo(query + "\n| SORT ??timeField ASC\n| LIMIT 10000"));
        }
    }

    public void testInjectedCapShouldFollowTimeSortSoTheEarliestRowsSurviveTruncation() {
        String ordered = EsqlDataExtractorQueryUtils.buildOrderedQuery(
            "FROM logs-* | STATS c = COUNT(*) BY bucket = BUCKET(@timestamp, 1h)"
        );

        int sortIdx = ordered.indexOf("| SORT ??timeField ASC");
        int limitIdx = ordered.indexOf("| LIMIT 10000");
        assertThat(sortIdx, greaterThan(-1));
        assertThat(limitIdx, greaterThan(-1));
        assertThat("time sort must precede the injected cap", sortIdx, lessThan(limitIdx));
        assertThat(ordered, endsWith("\n| LIMIT 10000"));
    }

    public void testSummaryQueryShouldAppendOutputLevelStats() {
        assertThat(
            EsqlDataExtractorQueryUtils.summaryQuery("FROM logs // c"),
            equalTo("FROM logs // c\n| STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)")
        );
    }
}
