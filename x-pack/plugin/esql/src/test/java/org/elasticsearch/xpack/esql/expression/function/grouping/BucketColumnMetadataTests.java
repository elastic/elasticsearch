/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class BucketColumnMetadataTests extends ESTestCase {

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testNoBuckets() {
        assertBucketColumnMetadata("FROM test | STATS count(*) BY emp_no");
    }

    public void testNumericWithoutRange() {
        assertBucketColumnMetadata("FROM test | STATS count(*) BY BUCKET(salary, 1000.0)", Map.of("bucket", Map.of("interval", 1000.0)));
    }

    public void testNumericWithRange() {
        assertBucketColumnMetadata(
            "FROM test | STATS count(*) BY BUCKET(salary, 10, 0, 10000)",
            Map.of("bucket", Map.of("interval", 1000.0))
        );
    }

    public void testDateAutoModeMonth() {
        // 20 buckets over 1 year -> monthly (12 <= 20, weekly would be ~52)
        assertBucketColumnMetadata(
            "FROM test | STATS count(*) BY BUCKET(hire_date, 20, \"1985-01-01\", \"1986-01-01\")",
            Map.of("bucket", Map.of("interval", 1L, "unit", "month"))
        );
    }

    public void testDateAutoModeWeek() {
        // 100 buckets over 1 year -> weekly (52 <= 100, monthly also fits but weekly is finer)
        assertBucketColumnMetadata(
            "FROM test | STATS count(*) BY BUCKET(hire_date, 100, \"1985-01-01\", \"1986-01-01\")",
            Map.of("bucket", Map.of("interval", 1L, "unit", "week"))
        );
    }

    public void testDateAutoModeYear() {
        // 5 buckets over 10 years -> yearly
        assertBucketColumnMetadata(
            "FROM test | STATS count(*) BY BUCKET(hire_date, 5, \"1980-01-01\", \"1990-01-01\")",
            Map.of("bucket", Map.of("interval", 1L, "unit", "year"))
        );
    }

    public void testDateExplicitPeriodYear() {
        assertBucketColumnMetadata(
            "FROM test | STATS count(*) BY BUCKET(hire_date, 1 year)",
            Map.of("bucket", Map.of("interval", 1L, "unit", "year"))
        );
    }

    public void testDateExplicitPeriodMonth() {
        assertBucketColumnMetadata(
            "FROM test | STATS count(*) BY BUCKET(hire_date, 1 month)",
            Map.of("bucket", Map.of("interval", 1L, "unit", "month"))
        );
    }

    public void testDateExplicitDurationHours() {
        assertBucketColumnMetadata(
            "FROM test | STATS count(*) BY BUCKET(hire_date, 1 hour)",
            Map.of("bucket", Map.of("interval", 1L, "unit", "hour"))
        );
    }

    public void testDateExplicitDurationMinutes() {
        assertBucketColumnMetadata(
            "FROM test | STATS count(*) BY BUCKET(hire_date, 30 minutes)",
            Map.of("bucket", Map.of("interval", 30L, "unit", "minute"))
        );
    }

    public void testBucketInRow() {
        assertBucketColumnMetadata(
            "ROW date=TO_DATETIME(\"1985-07-09T00:00:00.000Z\") "
                + "| STATS date=VALUES(date) BY bucket=BUCKET(date, 20, \"1985-01-01T00:00:00Z\", \"1986-01-01T00:00:00Z\")",
            Map.of("bucket", Map.of("interval", 1L, "unit", "month"))
        );
    }

    public void testMultipleBuckets() {
        assertBucketColumnMetadata(
            "FROM test | STATS count(*) BY BUCKET(hire_date, 1 year), BUCKET(salary, 5000.0)",
            Map.of("bucket", Map.of("interval", 1L, "unit", "year")),
            Map.of("bucket", Map.of("interval", 5000.0))
        );
    }

    @SafeVarargs
    private static void assertBucketColumnMetadata(String query, Map<String, Object>... expectedMetadata) {
        assertThat(
            BucketColumnMetadata.createColumnMetadata(analyzedPlan(query), FoldContext.small()).values(),
            containsInAnyOrder(expectedMetadata)
        );
    }

    private static LogicalPlan analyzedPlan(String query) {
        LogicalPlan plan = TEST_PARSER.createStatement(query, new QueryParams()).plan();
        return analyzer().addEmployees("test").buildAnalyzer().analyze(plan);
    }
}
