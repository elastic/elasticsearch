/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.rest;

import org.elasticsearch.core.Tuple;

import java.io.IOException;

import static org.elasticsearch.xpack.ql.TestUtils.assertNoSearchContexts;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class RestSqlPaginationTestCase extends BaseRestSqlTestCase {

    public void testPaginationIsConsistentWithCompositeAggQueries() throws Exception {
        testPaginationIsConsistent("SELECT foo FROM test GROUP BY foo ORDER BY foo");
    }

    public void testPaginationIsConsistentWithSearchHitQueries() throws Exception {
        testPaginationIsConsistent("SELECT foo FROM test ORDER BY foo");
    }

    public void testPaginationIsConsistent(String query) throws Exception {
        index("{\"foo\": 1}", "{\"foo\": 2}", "{\"foo\": 3}");

        String format = randomFrom("text/csv", "text/tab-separated-values", "text/plain");

        Tuple<String, String> response = runSqlAsText(query(query).fetchSize(2), format);

        String lateValue = Integer.toString(randomIntBetween(4, Integer.MAX_VALUE));
        index("{\"foo\": " + lateValue + "}");

        assertBusy(() -> assertThat(runSqlAsText(query("SELECT foo FROM test"), format).v1(), containsString(lateValue)));

        assertThat(fetchRemainingPages(response.v2(), format), not(containsString(lateValue)));
        assertNoSearchContexts(client());

        assertThat(runSqlAsText(query(query), format).v1(), containsString(lateValue));
    }

    public void testPaginationIsConsistentWithPivotQueries() throws Exception {
        index("{\"foo\": 1, \"bar\": 1}", "{\"foo\": 2, \"bar\": 1}", "{\"foo\": 3, \"bar\": 2}");

        String format = randomFrom("text/csv", "text/tab-separated-values", "text/plain");

        Tuple<String, String> response = runSqlAsText(
            query("SELECT * FROM (SELECT foo, bar FROM test) PIVOT (COUNT(*) FOR bar IN (1, 2))").fetchSize(2),
            format
        );

        String lateValue = Integer.toString(randomIntBetween(4, Integer.MAX_VALUE));
        index("{\"foo\": " + lateValue + ", \"bar\": 2}");

        assertBusy(() -> assertThat(runSqlAsText(query("SELECT foo FROM test"), format).v1(), containsString(lateValue)));

        assertThat(fetchRemainingPages(response.v2(), format), not(containsString("4")));
        assertNoSearchContexts(client());
    }

    private static String fetchRemainingPages(String cursor, String format) throws IOException {
        StringBuilder result = new StringBuilder();
        while (cursor != null) {
            Tuple<String, String> response = runSqlAsText(cursor(cursor), format);
            result.append("\n").append(response.v1());
            cursor = response.v2();
        }
        return result.toString();
    }

}
