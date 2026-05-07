/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class BucketColumnMetadataIT extends AbstractEsqlIntegTestCase {

    public void testBucketColumnMetadata() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
        }
    }

    public void testUnnamedBucketColumnMetadata() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates | STATS date=VALUES(date) BY BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(response.columns().get(1).meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
        }
    }

    public void testBucketColumnMetadataRetainedOnKeep() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | KEEP date, bucket
            """))) {
            assertThat(response.columns().get(1).meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
        }
    }

    public void testRenameEvalBucketColumnHasNoMetadata() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | EVAL bucket_renamed = bucket
            """))) {
            assertThat(response.columns().get(1).meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
            assertThat(findColumn(response, "bucket_renamed").meta(), nullValue());
        }
    }

    public void testRenameBucketColumnHasNoMetadata() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | RENAME bucket AS bucket_renamed
            """))) {
            assertThat(findColumn(response, "bucket_renamed").meta(), nullValue());
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
                (FROM dates
                | WHERE date <= "1985-06-01"
                | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")),
                (FROM dates
                | WHERE date > "1985-06-01"
                | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z"))
            """))) {
            assertThat(findColumn(response, "bucket").meta(), nullValue());
        }
        try (var response = run(syncEsqlQueryRequest("""
            FROM
                (FROM dates | STATS date=VALUES(date) BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z"))
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
        }
    }

    public void testViewBucketMetadata() {
        client().prepareBulk()
            .add(client().prepareIndex("dates").setSource("date", "1985-07-09T00:00:00.000Z"))
            .add(client().prepareIndex("numbers").setSource("number", 100))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View("date-stats", """
                    FROM dates | STATS date=VALUES(date) BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
                    """))
            )
        );
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View("number-stats", """
                    FROM numbers | STATS number=VALUES(number) BY number_bucket=BUCKET(number, 5)
                    """))
            )
        );

        try (var response = run(syncEsqlQueryRequest("FROM date-stats, number-stats"))) {
            assertThat(findColumn(response, "date_bucket").meta(), nullValue());
            assertThat(findColumn(response, "number_bucket").meta(), nullValue());
        }
        try (var response = run(syncEsqlQueryRequest("FROM date-stats"))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
        }

        client().execute(
            DeleteViewAction.INSTANCE,
            new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { "date-stats", "number-stats" })
        );
    }

    public void testMultipleStats() {
        client().prepareIndex("test")
            .setSource("number", 1, "date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // dropping bucket in subsequent aggregation
        try (var response = run(syncEsqlQueryRequest("""
            FROM test
            | STATS number=VALUES(number), date=VALUES(date) BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | SORT number
            | STATS count() BY date
            """))) {
            assertThat(response.columns().stream().allMatch(c -> c.meta() == null), equalTo(true));
        }
        // retaining bucket in subsequent aggregation
        try (var response = run(syncEsqlQueryRequest("""
            FROM test
            | STATS number=VALUES(number), date=VALUES(date) BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            | SORT number
            | STATS count() BY date_bucket
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
        }
        // introducing bucket in subsequent aggregation
        try (var response = run(syncEsqlQueryRequest("""
            FROM test
            | STATS number=VALUES(number) BY date
            | SORT number
            | STATS count() BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
        }
    }

    public void testStatsWithMultiBucket() {
        client().prepareIndex("test")
            .setSource("number", 1, "date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM test
            | STATS count() BY
                date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z"),
                number_bucket=BUCKET(number, 10)
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
            assertThat(findColumn(response, "number_bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 10.0))));
        }
    }

    public void testMultipleStatsWithBucket() {
        client().prepareIndex("test")
            .setSource("number", 1, "date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM test
             | STATS number=VALUES(number), date=VALUES(date)
                BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
             | STATS count() BY date_bucket, number_bucket=BUCKET(number, 10)
            """))) {
            assertThat(findColumn(response, "date_bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
            assertThat(findColumn(response, "number_bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 10.0))));
        }
        try (var response = run(syncEsqlQueryRequest("""
            FROM test
             | STATS number=VALUES(number), date=VALUES(date)
                BY date_bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
             | STATS count() BY date_bucket_renamed = date_bucket
            """))) {
            assertThat(findColumn(response, "date_bucket_renamed").meta(), nullValue());
        }

    }

    public void testBucketColumnMetadataRetainedAfterLookupJoin() {
        client().prepareIndex("events")
            .setSource("value", 150)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("bucket_descriptions")
                .setSettings(
                    Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                )
                .setMapping("bucket", "type=double", "label", "type=keyword")
        );
        client().prepareIndex("bucket_descriptions")
            .setSource("bucket", 100.0, "label", "low")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM events
            | STATS c=COUNT(*) BY bucket=BUCKET(value, 100.0)
            | LOOKUP JOIN bucket_descriptions ON bucket
            """))) {
            assertThat(findColumn(response, "bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 100.0))));
        }
    }

    public void testForkBucketColumnHasNoMetadata() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | FORK ( STATS c=COUNT(*) BY bucket=BUCKET(date, 1 month) )
                   ( STATS c=COUNT(*) BY bucket=BUCKET(date, 1 year) )
            | KEEP bucket
            """))) {
            assertThat(findColumn(response, "bucket").meta(), nullValue());
        }
    }

    public void testInlineStatsBucketColumnMetadata() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | INLINE STATS c=COUNT(*) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
        }
    }

    public void testInlineStatsUnnamedBucketColumnMetadata() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | INLINE STATS c=COUNT(*) BY BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(
                findColumn(response, "BUCKET(date, 20, \"1985-01-01T00:00:00Z\", \"1986-01-01T00:00:00Z\")").meta(),
                equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month")))
            );
        }
    }

    public void testBucketColumnMetadataWithNonFoldableRanges() {
        client().prepareIndex("test")
            .setSource("number", 1, "date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM test
            | EVAL bucket_start = TO_DATETIME("1999-01-01T00:00:00.000Z")
            | EVAL bucket_end = NOW()
            | STATS COUNT(*) BY bucket = BUCKET(date, 5, bucket_start, bucket_end)
            | SORT bucket
            """))) {
            assertThat(findColumn(response, "bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "year"))));
        }
    }

    public void testTBucketGroupingColumnMetadata() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("ts-index")
                .setSettings(Settings.builder().put("mode", "time_series").putList("routing_path", List.of("host")))
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "metric",
                    "type=long,time_series_metric=gauge"
                )
                .get()
        );
        client().prepareIndex("ts-index")
            .setSource("@timestamp", "2024-06-01T12:00:00.000Z", "host", "host-1", "metric", 42)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("TS ts-index | STATS max(metric) BY tb = TBUCKET(1 hour)"))) {
            assertThat(findColumn(response, "tb").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "hour"))));
        }
    }

    private static ColumnInfoImpl findColumn(EsqlQueryResponse response, String name) {
        return response.columns().stream().filter(c -> Objects.equals(c.name(), name)).findFirst().orElseThrow();
    }
}
