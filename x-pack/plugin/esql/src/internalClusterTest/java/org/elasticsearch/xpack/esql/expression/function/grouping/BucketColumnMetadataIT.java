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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class BucketColumnMetadataIT extends AbstractEsqlIntegTestCase {

    @Before
    public void requireBucketMetadataCapability() {
        // The bucket metadata feature is snapshot-only until finalized; non-snapshot builds skip these tests.
        assumeTrue("requires column_metadata_bucket capability", EsqlCapabilities.Cap.COLUMN_METADATA_BUCKET.isEnabled());
    }

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
            assertThat(
                findColumn(response, "BUCKET(date, 20, \"1985-01-01T00:00:00Z\", \"1986-01-01T00:00:00Z\")").meta(),
                equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month")))
            );
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
            assertThat(findColumn(response, "bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "month"))));
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
        client().prepareIndex("events").setSource("value", 150).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

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

    public void testBucketDayPeriodVs24HourDurationProduceSameMetadata() {
        // `1 day` parses as Period.ofDays(1) (calendar DAY_OF_MONTH path); `24 hours` parses as Duration.ofHours(24)
        // (TimeIntervalRounding 86400000ms). The two go through different Rounding subtypes but must surface the
        // same metadata so users see consistent (interval, unit) regardless of which literal form they wrote.
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        Map<String, Object> expected = Map.of("bucket", Map.of("interval", 1L, "unit", "day"));
        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS c=COUNT(*) BY bucket=BUCKET(date, 1 day)
            """))) {
            assertThat(findColumn(response, "bucket").meta(), equalTo(expected));
        }
        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS c=COUNT(*) BY bucket=BUCKET(date, 24 hours)
            """))) {
            assertThat(findColumn(response, "bucket").meta(), equalTo(expected));
        }
    }

    public void testBucketWithZeroSpan() {
        // BUCKET(field, 0, 0, 0) is accepted by the analyzer but fails at evaluation (NaN/divide-by-zero).
        // Such impossible bucket definitions must not surface any metadata.
        client().prepareIndex("numbers").setSource("number", 1).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM numbers
            | STATS max=max(number) BY b = BUCKET(number, 0, 0, 0)
            """))) {
            assertThat(findColumn(response, "b").meta(), nullValue());
        }
    }

    public void testBucketWithNegativeSpan() {
        // Mirrors testBucketWithZeroSpan for a negative bucket count.
        client().prepareIndex("numbers").setSource("number", 1).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM numbers
            | STATS max=max(number) BY b = BUCKET(number, -1, 0, 0)
            """))) {
            assertThat(findColumn(response, "b").meta(), nullValue());
        }
    }

    public void testBucketDateWithZeroBucketsHasNoMetadata() {
        // BUCKET(date, 0, from, to) is nonsensical: a zero bucket count means "give me no histogram". The
        // metadata path must not surface any interval/unit for it.
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS c=COUNT(*) BY b = BUCKET(date, 0, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "b").meta(), nullValue());
        }
    }

    public void testBucketDateWithNegativeBucketsHasNoMetadata() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS c=COUNT(*) BY b = BUCKET(date, -1, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "b").meta(), nullValue());
        }
    }

    public void testBucketDateNanosWithZeroBucketsHasNoMetadata() {
        assertAcked(client().admin().indices().prepareCreate("date_nanos_idx").setMapping("date", "type=date_nanos"));
        client().prepareIndex("date_nanos_idx")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM date_nanos_idx
            | STATS c=COUNT(*) BY b = BUCKET(date, 0, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "b").meta(), nullValue());
        }
    }

    public void testBucketDateNanosWithNegativeBucketsHasNoMetadata() {
        assertAcked(client().admin().indices().prepareCreate("date_nanos_idx").setMapping("date", "type=date_nanos"));
        client().prepareIndex("date_nanos_idx")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM date_nanos_idx
            | STATS c=COUNT(*) BY b = BUCKET(date, -1, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "b").meta(), nullValue());
        }
    }

    public void testBucketWithNullFieldLiteralHasNoMetadata() {
        // A null literal in the field position: after constant folding the alias' child is no longer a Bucket,
        // so no metadata is attached to the column.
        try (var response = run(syncEsqlQueryRequest("""
            ROW d=null::DATETIME
            | STATS c=COUNT(*) BY bucket=BUCKET(d, 1 month)
            """))) {
            assertThat(findColumn(response, "bucket").meta(), nullValue());
        }
    }

    public void testBucketWithNullSpanLiteralHasNoMetadata() {
        // A null literal in the span position folds the BUCKET expression away; the alias' child is no longer
        // a Bucket so no metadata is attached. Using FROM (not ROW) so the field stays a real Attribute.
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS c=COUNT(*) BY bucket=BUCKET(date, null)
            """))) {
            assertThat(findColumn(response, "bucket").meta(), nullValue());
        }
    }

    public void testBucketLongFormWithNullFieldLiteralHasNoMetadata() {
        // Null literal in the field position of the four-arg form folds BUCKET away; no metadata.
        try (var response = run(syncEsqlQueryRequest("""
            ROW d=null::DATETIME
            | STATS c=COUNT(*) BY bucket=BUCKET(d, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "bucket").meta(), nullValue());
        }
    }

    public void testBucketLongFormWithNullBucketsCountHasNoMetadata() {
        // Null literal in the buckets-count position folds BUCKET away; no metadata. Using FROM so the field
        // stays a real Attribute (ROW would let constant folding remove the whole pipeline).
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS c=COUNT(*) BY bucket=BUCKET(date, null, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "bucket").meta(), nullValue());
        }
    }

    public void testBucketLongFormWithNullFromHasNoMetadata() {
        // Null literal in the "from" position folds BUCKET away; no metadata.
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS c=COUNT(*) BY bucket=BUCKET(date, 20, null, "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "bucket").meta(), nullValue());
        }
    }

    public void testBucketLongFormWithNullToHasNoMetadata() {
        // Null literal in the "to" position folds BUCKET away; no metadata.
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS c=COUNT(*) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", null)
            """))) {
            assertThat(findColumn(response, "bucket").meta(), nullValue());
        }
    }

    public void testBucketWrappedInArithmeticHasNoMetadata() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS c=COUNT(*) BY b = BUCKET(date + 1 HOUR, 1 YEAR) - 1 HOUR
            """))) {
            assertThat(findColumn(response, "b").meta(), nullValue());
        }
    }

    public void testBucketWrappedInRoundHasNoMetadata() {
        client().prepareIndex("test").setSource("value", 150).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM test
            | STATS c=COUNT(*) BY bh = ROUND(BUCKET(value, 100.0), 0)
            """))) {
            assertThat(findColumn(response, "bh").meta(), nullValue());
        }
    }

    public void testBucketInsideAggregateHasNoMetadata() {
        client().prepareIndex("test").setSource("value", 150).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM test
            | STATS avg_b = AVG(BUCKET(value, 100.0)) BY bucket = BUCKET(value, 100.0)
            """))) {
            assertThat(findColumn(response, "avg_b").meta(), nullValue());
            assertThat(findColumn(response, "bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 100.0))));
        }
    }

    public void testBucketColumnCastHasNoMetadata() {
        client().prepareIndex("test").setSource("value", 150).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM test
            | STATS c=COUNT(*) BY bucket=BUCKET(value, 100.0)
            | EVAL bucket_int = bucket::INTEGER
            """))) {
            assertThat(findColumn(response, "bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 100.0))));
            assertThat(findColumn(response, "bucket_int").meta(), nullValue());
        }
    }

    public void testBucketWithFieldArithmetic() {
        client().prepareIndex("dates")
            .setSource("date", "1985-07-09T00:00:00.000Z")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM dates
            | STATS c=COUNT(*) BY bucket = BUCKET(date + 1 HOUR, 1 YEAR)
            """))) {
            assertThat(findColumn(response, "bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 1L, "unit", "year"))));
        }
    }

    public void testBucketWithFoldableBucketsArg() {
        client().prepareIndex("test").setSource("value", 150).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (var response = run(syncEsqlQueryRequest("""
            FROM test
            | STATS c=COUNT(*) BY bucket = BUCKET(value, 2.0 + 3.0)
            """))) {
            assertThat(findColumn(response, "bucket").meta(), equalTo(Map.of("bucket", Map.of("interval", 5.0))));
        }
    }

    private static ColumnInfoImpl findColumn(EsqlQueryResponse response, String name) {
        return response.columns().stream().filter(c -> Objects.equals(c.name(), name)).findFirst().orElseThrow();
    }
}
