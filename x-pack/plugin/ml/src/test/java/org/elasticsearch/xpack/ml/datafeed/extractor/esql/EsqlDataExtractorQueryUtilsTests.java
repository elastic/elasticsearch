/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class EsqlDataExtractorQueryUtilsTests extends ESTestCase {

    public void testQueryWithoutOuterLimitShouldAppendTimeSortAndDefaultLimit() {
        List<String[]> cases = List.of(
            new String[] {
                "FROM logs-* | KEEP @timestamp, bytes",
                "FROM logs-* | KEEP @timestamp, bytes | SORT ??timeField ASC | LIMIT 10000" },
            new String[] {
                "FROM logs-* | WHERE message == \"LIMIT 20\"",
                "FROM logs-* | WHERE message == \"LIMIT 20\" | SORT ??timeField ASC | LIMIT 10000" },
            new String[] {
                "FROM logs-* | WHERE message == \"\"\"| LIMIT 20\"\"\"",
                "FROM logs-* | WHERE message == \"\"\"| LIMIT 20\"\"\" | SORT ??timeField ASC | LIMIT 10000" },
            new String[] {
                "FROM logs-* // | LIMIT 20\n| KEEP @timestamp",
                "FROM logs-* // | LIMIT 20\n| KEEP @timestamp | SORT ??timeField ASC | LIMIT 10000" },
            new String[] {
                "FROM logs-* /* | LIMIT 20 */ | KEEP @timestamp",
                "FROM logs-* /* | LIMIT 20 */ | KEEP @timestamp | SORT ??timeField ASC | LIMIT 10000" },
            new String[] {
                "FROM logs-* /* outer /* | LIMIT 20 */ comment */ | KEEP @timestamp",
                "FROM logs-* /* outer /* | LIMIT 20 */ comment */ | KEEP @timestamp | SORT ??timeField ASC | LIMIT 10000" },
            new String[] {
                "FROM logs-* | KEEP limit_value, `LIMIT``_value`",
                "FROM logs-* | KEEP limit_value, `LIMIT``_value` | SORT ??timeField ASC | LIMIT 10000" },
            new String[] {
                "FROM logs-* | WHERE id IN (FROM other-logs | LIMIT 3 | KEEP id)",
                "FROM logs-* | WHERE id IN (FROM other-logs | LIMIT 3 | KEEP id) | SORT ??timeField ASC | LIMIT 10000" },
            new String[] {
                "FROM logs-* | FORK (WHERE level == \"warn\" | LIMIT 3) (WHERE level == \"error\" | LIMIT 4)",
                "FROM logs-* | FORK (WHERE level == \"warn\" | LIMIT 3) (WHERE level == \"error\" | LIMIT 4)"
                    + " | SORT ??timeField ASC | LIMIT 10000" },
            new String[] { "FROM logs-* // no user limit", "FROM logs-* // no user limit\n | SORT ??timeField ASC | LIMIT 10000" }
        );
        for (String[] c : cases) {
            assertThat(EsqlDataExtractorQueryUtils.buildOrderedQuery(c[0]), equalTo(c[1]));
        }
    }

    public void testQueryWithOuterLimitShouldAppendTimeSortAndKeepUserLimit() {
        List<String[]> cases = List.of(
            new String[] { "FROM logs-* | LIMIT 20", "FROM logs-* | LIMIT 20 | SORT ??timeField ASC" },
            new String[] {
                "FROM logs-* | WHERE id IN (FROM other-logs | LIMIT 3 | KEEP id) | LIMIT 20",
                "FROM logs-* | WHERE id IN (FROM other-logs | LIMIT 3 | KEEP id) | LIMIT 20 | SORT ??timeField ASC" },
            new String[] {
                "FROM logs-* | WHERE message == \"\"\"literal\"\"\"\"\" | LIMIT 20",
                "FROM logs-* | WHERE message == \"\"\"literal\"\"\"\"\" | LIMIT 20 | SORT ??timeField ASC" },
            new String[] {
                "FROM logs-*\n| KEEP @timestamp\n| lImIt 42",
                "FROM logs-*\n| KEEP @timestamp\n| lImIt 42 | SORT ??timeField ASC" }
        );
        for (String[] c : cases) {
            assertThat(EsqlDataExtractorQueryUtils.buildOrderedQuery(c[0]), equalTo(c[1]));
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
        assertThat(ordered, endsWith("| LIMIT 10000"));
    }

    public void testUserLimitShouldStayBeforeTimeSortAndNoCapIsInjected() {
        String ordered = EsqlDataExtractorQueryUtils.buildOrderedQuery("FROM logs-* | SORT value DESC | LIMIT 20");

        assertThat(ordered, equalTo("FROM logs-* | SORT value DESC | LIMIT 20 | SORT ??timeField ASC"));
    }
}
