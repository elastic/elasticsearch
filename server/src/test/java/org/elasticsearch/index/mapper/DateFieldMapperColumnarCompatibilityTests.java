/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;

/**
 * Parity tests for {@link DateFieldMapper#mapColumnBatch} against the row path.
 * Only single-valued columnar date fields are tested; multi-valued and data stream
 * timestamp fields are out of scope and covered elsewhere.
 */
public class DateFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .build();
    }

    public void testStringValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").endObject()),
            columnarSettings(),
            batch("string value", 1L, doc("d1", 1L, "{\"f\":\"2024-01-15T12:00:00.000Z\"}"))
        );
    }

    public void testStringValueDateOnly() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").endObject()),
            columnarSettings(),
            batch("string date-only value", 1L, doc("d1", 1L, "{\"f\":\"2024-06-01\"}"))
        );
    }

    public void testAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").endObject()),
            columnarSettings(),
            batch("absent doc", 1L, doc("d1", 1L, "{}"))
        );
    }

    public void testMixedAbsentPresent() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").endObject()),
            columnarSettings(),
            batch(
                "mixed absent/present strings",
                1L,
                doc("d1", 1L, "{\"f\":\"2024-01-01T00:00:00.000Z\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"2024-03-15T08:30:00.000Z\"}")
            )
        );
    }

    public void testMultipleStringDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").endObject()),
            columnarSettings(),
            batch(
                "multiple string docs",
                1L,
                doc("d1", 1L, "{\"f\":\"2020-01-01T00:00:00.000Z\"}"),
                doc("d2", 2L, "{\"f\":\"2021-06-15T12:00:00.000Z\"}"),
                doc("d3", 3L, "{\"f\":\"2022-12-31T23:59:59.999Z\"}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testLongEpochMillis() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").endObject()),
            columnarSettings(),
            batch("long epoch millis", 1L, doc("d1", 1L, "{\"f\":1705320000000}"))
        );
    }

    public void testLongEpochMillisZero() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").endObject()),
            columnarSettings(),
            batch("long epoch millis zero", 1L, doc("d1", 1L, "{\"f\":0}"))
        );
    }

    public void testMixedAbsentPresentLong() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").endObject()),
            columnarSettings(),
            batch(
                "mixed absent/present longs",
                1L,
                doc("d1", 1L, "{\"f\":1700000000000}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":1710000000000}")
            )
        );
    }

    public void testMultipleLongDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").endObject()),
            columnarSettings(),
            batch(
                "multiple long docs",
                1L,
                doc("d1", 1L, "{\"f\":1000000000000}"),
                doc("d2", 2L, "{\"f\":1500000000000}"),
                doc("d3", 3L, "{\"f\":1700000000000}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testCustomFormatString() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").field("format", "yyyy-MM-dd").endObject()),
            columnarSettings(),
            batch(
                "custom format string values",
                1L,
                doc("d1", 1L, "{\"f\":\"2024-03-21\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"2024-12-31\"}")
            )
        );
    }

    public void testEpochMillisFormatWithLong() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").field("format", "epoch_millis").endObject()),
            columnarSettings(),
            batch(
                "epoch_millis format with longs",
                1L,
                doc("d1", 1L, "{\"f\":1705320000000}"),
                doc("d2", 2L, "{\"f\":0}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testIndexedStringValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed string value", 1L, doc("d1", 1L, "{\"f\":\"2024-01-15T12:00:00.000Z\"}"))
        );
    }

    public void testIndexedLongValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed long value", 1L, doc("d1", 1L, "{\"f\":1705320000000}"))
        );
    }

    public void testIndexedWithAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").field("index", true).endObject()),
            columnarSettings(),
            batch(
                "indexed with absent doc",
                1L,
                doc("d1", 1L, "{\"f\":\"2024-01-01T00:00:00.000Z\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"2024-03-15T08:30:00.000Z\"}")
            )
        );
    }

    public void testIndexedMultipleDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").field("index", true).endObject()),
            columnarSettings(),
            batch(
                "indexed multiple docs",
                1L,
                doc("d1", 1L, "{\"f\":\"2020-01-01T00:00:00.000Z\"}"),
                doc("d2", 2L, "{\"f\":\"2021-06-15T12:00:00.000Z\"}"),
                doc("d3", 3L, "{\"f\":\"2022-12-31T23:59:59.999Z\"}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    /**
     * Columnar-mode settings leaving {@code doc_values.multi_value} at its default of {@code true},
     * so array values reach the mapper instead of being rejected at parse time.
     */
    private static Settings multiValueColumnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    /**
     * {@link DateFieldMapper#supportsColumnarParse} accepts {@code doc_values.multi_value=true} —
     * the setting defaults to {@code true}, so rejecting it would take every date field in a
     * columnar index off the columnar path. Multi-valued documents themselves are not implemented:
     * they arrive as an ESCF {@code ARRAY} column and the kind switch in
     * {@link DateFieldMapper#mapColumnBatch} throws, which makes {@code ShardBatchMapper} fall the
     * chunk back to the row path. This test pins the gap that fallback papers over.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement multi-valued date fields; ARRAY columns fall back to the row path")
    public void testMultiValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").endObject()),
            multiValueColumnarSettings(),
            // Every present value is an array so the column is a plain ARRAY; mixing in a scalar
            // would make it a UNION and trip the same switch for a different reason.
            batch(
                "multi-value dates",
                1L,
                doc("d1", 1L, "{\"f\":[\"2024-01-01T00:00:00.000Z\",\"2024-02-01T00:00:00.000Z\"]}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[\"2024-03-01T00:00:00.000Z\"]}")
            )
        );
    }

    /**
     * As {@link #testMultiValue}, for a null value. A null makes the column a UNION rather than a
     * plain STRING, which the same kind switch rejects.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement null date values; UNION columns fall back to the row path")
    public void testNullValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").field("null_value", "2024-01-01T00:00:00.000Z").endObject()),
            columnarSettings(),
            batch(
                "null date value",
                1L,
                doc("d1", 1L, "{\"f\":\"2024-05-01T00:00:00.000Z\"}"),
                doc("d2", 2L, "{\"f\":null}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    /**
     * {@link DateFieldMapper#supportsColumnarParse} accepts {@code ignore_malformed=true} — the
     * logsdb index modes default it to {@code true}. Per-value error handling
     * ({@code addIgnoredField} plus the ignored-source stored copy) is not implemented in
     * {@code mapColumnBatch}, so an unparseable value throws out of {@code fieldType().parse} in
     * {@code datesFromStrings} and the chunk falls back to the row path, which applies
     * {@code ignore_malformed} properly.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement ignore_malformed; malformed dates fall back to the row path")
    public void testIgnoreMalformed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "date").field("ignore_malformed", true).endObject()),
            columnarSettings(),
            batch(
                "ignore_malformed dates",
                1L,
                doc("d1", 1L, "{\"f\":\"2024-01-15T12:00:00.000Z\"}"),
                doc("d2", 2L, "{\"f\":\"not-a-date\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    // ---- store=true (TIME_SERIES / TSDB mode) ------------------------------------------------
    //
    // strict-columnar index modes (COLUMNAR, LOGSDB_COLUMNAR) reject store=true at mapping
    // validation time. TIME_SERIES is the only columnar-eligible mode that permits it.
    // These tests use the coordinator-tsid path (index.dimensions) so that metadata fields
    // are computed columnarally via pre-supplied tsid bytes. The keyword dimension field (dim)
    // is declared in the mapping but absent from sources to keep it out of the ESCF schema.

    private static final BytesRef ST_TSID = new BytesRef(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
    private static final int ST_ROUTING_HASH = 42;
    private static final String ST_ROUTING = TimeSeriesRoutingHashFieldMapper.encode(ST_ROUTING_HASH);
    // epoch millis: 2024-01-15T12:00:00.000Z, 2024-06-01T00:00:00.000Z, 2024-03-15T08:30:00.000Z
    private static final long ST_TS_A = 1705320000000L;
    private static final long ST_TS_B = 1717200000000L;
    private static final long ST_TS_C = 1710491400000L;

    private static Settings tsdbSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.SYNTHETIC_ID.getKey(), false)
            .build();
    }

    public void testStoredStringValue() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "date").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch(
                "stored string value",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":\"2024-01-15T12:00:00.000Z\"}")
            )
        );
    }

    public void testStoredLongValue() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "date").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch("stored long value", 1L, doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":" + ST_TS_A + "}"))
        );
    }

    public void testStoredWithAbsentDoc() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_B);
        final String idC = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_C);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "date").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch(
                "stored with absent doc",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":\"2024-01-15T12:00:00.000Z\"}"),
                doc(idB, ST_ROUTING, ST_TSID, 2L, "{\"@timestamp\":" + ST_TS_B + "}"),
                doc(idC, ST_ROUTING, ST_TSID, 3L, "{\"@timestamp\":" + ST_TS_C + ",\"f\":\"2024-03-15T08:30:00.000Z\"}")
            )
        );
    }

    public void testStoredIndexed() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "date").field("store", true).field("index", true).endObject();
        }),
            tsdbSettings(),
            batch(
                "stored+indexed",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":\"2024-01-15T12:00:00.000Z\"}")
            )
        );
    }

    public void testStoredNotIndexed() throws IOException {
        // index=false → columnFieldType is SORTED_NUMERIC_DV_FIELD_TYPE (plain doc values, no BKD).
        // Exercises the else-branch of the columnFieldType selection together with the stored column.
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "date").field("store", true).field("index", false).endObject();
        }),
            tsdbSettings(),
            batch(
                "stored not indexed",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":\"2024-01-15T12:00:00.000Z\"}")
            )
        );
    }
}
