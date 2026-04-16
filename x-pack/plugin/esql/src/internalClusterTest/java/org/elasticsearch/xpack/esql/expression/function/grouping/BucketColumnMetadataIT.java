/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class BucketColumnMetadataIT extends AbstractEsqlIntegTestCase {

    public void testBucketColumnMetadata() {
        try (var response = run(syncEsqlQueryRequest("""
            ROW date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "bucket").meta(), equalTo(Map.of("bucket", Map.of("date_range", "1 month"))));
        }
    }

    public void testUnnamedBucketColumnMetadata() {
        try (var response = run(syncEsqlQueryRequest("""
            ROW date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | STATS date=VALUES(date) BY BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(response.columns().get(1).meta(), equalTo(Map.of("bucket", Map.of("date_range", "1 month"))));
        }
    }

    public void testBucketColumnMetadataRetainedOnKeep() {
        try (var response = run(syncEsqlQueryRequest("""
            ROW date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | KEEP date, bucket
            """))) {
            assertThat(response.columns().get(1).meta(), equalTo(Map.of("bucket", Map.of("date_range", "1 month"))));
        }
    }

    public void testRenameEvalBucketColumnHasNoMetadata() {
        try (var response = run(syncEsqlQueryRequest("""
            ROW date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | EVAL renamed = bucket
            """))) {
            assertThat(response.columns().get(1).meta(), equalTo(Map.of("bucket", Map.of("date_range", "1 month"))));
            assertThat(findColumn(response, "renamed").meta(), nullValue());
        }
    }

    public void testRenameBucketColumnHasNoMetadata() {
        try (var response = run(syncEsqlQueryRequest("""
            ROW date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | RENAME bucket AS renamed
            """))) {
            assertThat(findColumn(response, "renamed").meta(), nullValue());
        }
    }

    public void testMvExpandedBucketColumnHasNoMetadata() {
        try (var response = run(syncEsqlQueryRequest("""
            ROW points=[10, 20, 30, 40, 50]
            | STATS count=SUM(MV_COUNT(points)) BY bucket=BUCKET(points, 5)
            | MV_EXPAND bucket
            """))) {
            assertThat(findColumn(response, "bucket").meta(), nullValue());
        }
    }

    public void testSubqueryBucketMetadata() {
        client().prepareBulk()
            .add(client().prepareIndex("dates").setSource("date", "1985-07-09T00:00:00.000Z"))
            .add(client().prepareIndex("numbers").setSource("number", 100))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM
                (FROM dates | STATS date=VALUES(date) BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")),
                (FROM numbers | STATS number=VALUES(number) BY number_bucket=BUCKET(number, 5))
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), nullValue());
            assertThat(findColumn(response, "number_bucket").meta(), nullValue());
        }
        try (var response = run(syncEsqlQueryRequest("""
            FROM
                (FROM dates | STATS date=VALUES(date) BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z"))
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("date_range", "1 month"))));
        }
    }

    public void testMultipleStats() {
        // dropping bucket in subsequent aggregation
        try (var response = run(syncEsqlQueryRequest("""
            ROW number=1, date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | STATS number=VALUES(number), date=VALUES(date) BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | SORT number
            | STATS count() BY date
            """))) {
            assertThat(response.columns().stream().allMatch(c -> c.meta() == null), equalTo(true));
        }
        // retaining bucket in subsequent aggregation
        try (var response = run(syncEsqlQueryRequest("""
            ROW number=1, date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | STATS number=VALUES(number), date=VALUES(date) BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | SORT number
            | STATS count() BY date_bucket
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("date_range", "1 month"))));
        }
        // introducing bucket in subsequent aggregation
        try (var response = run(syncEsqlQueryRequest("""
            ROW number=1, date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | STATS number=VALUES(number) BY date
            | SORT number
            | STATS count() BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("date_range", "1 month"))));
        }
    }

    public void testStatsWithMultiBucket() {
        try (var response = run(syncEsqlQueryRequest("""
            ROW number=1, date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | STATS count() BY
                date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z"),
                number_bucket=BUCKET(number, 10)
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("date_range", "1 month"))));
            assertThat(findColumn(response, "number_bucket").meta(), equalTo(Map.of("bucket", Map.of("numeric_range", 10.0))));
        }
    }

    public void testMultipleStatsWithBucket() {
        try (var response = run(syncEsqlQueryRequest("""
            ROW number=1, date=TO_DATETIME("1985-07-09T00:00:00.000Z")
             | STATS number=VALUES(number), date=VALUES(date)
                BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
             | STATS count() BY date_bucket, number_bucket=BUCKET(number, 10)
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("date_range", "1 month"))));
            assertThat(findColumn(response, "number_bucket").meta(), equalTo(Map.of("bucket", Map.of("numeric_range", 10.0))));
        }
    }

    public void testBucketColumnMetadataWithNonFoldableRanges() {
        try (var response = run(syncEsqlQueryRequest("""
            ROW number=1, date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | EVAL bucket_start = TO_DATETIME("1999-01-01T00:00:00.000Z")
            | EVAL bucket_end = NOW()
            | STATS COUNT(*) BY bucket = BUCKET(date, 5, bucket_start, bucket_end)
            | SORT bucket
            """))) {
            assertThat(findColumn(response, "bucket").meta(), nullValue());
        }
    }

    private static ColumnInfoImpl findColumn(EsqlQueryResponse response, String name) {
        return response.columns().stream().filter(c -> Objects.equals(c.name(), name)).findFirst().orElseThrow();
    }
}
