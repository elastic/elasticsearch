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
 * Parity tests for {@link NumberFieldMapper#mapColumnBatch} against the row path.
 * One test per numeric type and ESCF source kind combination; absent (sparse) docs are exercised
 * in every scenario to confirm validity-bitset handling.
 */
public class NumberFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    /**
     * Columnar-mode settings that satisfy {@link NumberFieldMapper#supportsColumnarParse}:
     * single-value doc-values ({@code multi_value=false}).
     */
    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .build();
    }

    public void testLongField_singleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long single-value", 1L, doc("d1", 1L, "{\"f\":42}"))
        );
    }

    public void testLongField_absentDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long absent", 1L, doc("d1", 1L, "{\"f\":1}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":3}"))
        );
    }

    public void testLongField_negative() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long negative", 1L, doc("d1", 1L, "{\"f\":-9223372036854775808}"), doc("d2", 2L, "{\"f\":9223372036854775807}"))
        );
    }

    public void testIntegerField_singleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "integer").endObject()),
            columnarSettings(),
            batch("integer single-value", 1L, doc("d1", 1L, "{\"f\":100}"))
        );
    }

    public void testIntegerField_absentDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "integer").endObject()),
            columnarSettings(),
            batch("integer absent", 1L, doc("d1", 1L, "{\"f\":10}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":20}"))
        );
    }

    public void testShortField_singleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "short").endObject()),
            columnarSettings(),
            batch("short single-value", 1L, doc("d1", 1L, "{\"f\":32767}"))
        );
    }

    public void testShortField_absentDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "short").endObject()),
            columnarSettings(),
            batch("short absent", 1L, doc("d1", 1L, "{\"f\":1}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-1}"))
        );
    }

    public void testByteField_singleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "byte").endObject()),
            columnarSettings(),
            batch("byte single-value", 1L, doc("d1", 1L, "{\"f\":127}"))
        );
    }

    public void testByteField_absentDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "byte").endObject()),
            columnarSettings(),
            batch("byte absent", 1L, doc("d1", 1L, "{\"f\":-128}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":0}"))
        );
    }

    public void testLongField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").field("index", true).endObject()),
            columnarSettings(),
            batch("long indexed", 1L, doc("d1", 1L, "{\"f\":42}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-7}"))
        );
    }

    public void testIntegerField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "integer").field("index", true).endObject()),
            columnarSettings(),
            batch("integer indexed", 1L, doc("d1", 1L, "{\"f\":100}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-50}"))
        );
    }

    public void testShortField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "short").field("index", true).endObject()),
            columnarSettings(),
            batch("short indexed", 1L, doc("d1", 1L, "{\"f\":32767}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-1}"))
        );
    }

    public void testByteField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "byte").field("index", true).endObject()),
            columnarSettings(),
            batch("byte indexed", 1L, doc("d1", 1L, "{\"f\":127}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-128}"))
        );
    }

    public void testFloatField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "float").field("index", true).endObject()),
            columnarSettings(),
            batch("float indexed", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}"))
        );
    }

    public void testDoubleField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").field("index", true).endObject()),
            columnarSettings(),
            batch("double indexed", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}"))
        );
    }

    public void testHalfFloatField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "half_float").field("index", true).endObject()),
            columnarSettings(),
            batch("half_float indexed", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}"))
        );
    }

    public void testFloatField_doubleColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "float").endObject()),
            columnarSettings(),
            batch("float from double", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}"))
        );
    }

    /** JSON integer values encode as LONG in ESCF; the mapper converts via {@code floatToSortableInt}. */
    public void testFloatField_longColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "float").endObject()),
            columnarSettings(),
            batch("float from long", 1L, doc("d1", 1L, "{\"f\":5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-100}"))
        );
    }

    public void testDoubleField_doubleColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").endObject()),
            columnarSettings(),
            batch("double from double", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}"))
        );
    }

    /** JSON integer values encode as LONG in ESCF; the mapper converts via {@code doubleToSortableLong}. */
    public void testDoubleField_longColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").endObject()),
            columnarSettings(),
            batch("double from long", 1L, doc("d1", 1L, "{\"f\":5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-100}"))
        );
    }

    public void testLongField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long string", 1L, doc("d1", 1L, "{\"f\":\"42\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-7\"}"))
        );
    }

    /** Quoted Long.MIN_VALUE and Long.MAX_VALUE exercise the ASCII fast path at boundary values. */
    public void testLongField_stringColumn_boundaries() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch(
                "long string boundaries",
                1L,
                doc("d1", 1L, "{\"f\":\"-9223372036854775808\"}"),
                doc("d2", 2L, "{\"f\":\"9223372036854775807\"}")
            )
        );
    }

    /**
     * A batch mixing a plain integer string (ASCII fast path) and scientific notation (slow path)
     * must produce the same doc values as the row path for both.
     */
    public void testLongField_stringColumn_fastPathAndFallbackInOneBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long string fast+slow", 1L, doc("d1", 1L, "{\"f\":\"1000\"}"), doc("d2", 2L, "{\"f\":\"1e3\"}"))
        );
    }

    /** A decimal string with coerce=true (default) is truncated to a long, matching the row path. */
    public void testLongField_stringColumn_decimalTruncated() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long string decimal coerce", 1L, doc("d1", 1L, "{\"f\":\"1.9\"}"), doc("d2", 2L, "{\"f\":\"42\"}"))
        );
    }

    public void testLongField_stringColumn_emptyStringMissing() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long empty string missing", 1L, doc("d1", 1L, "{\"f\":\"10\"}"), doc("d2", 2L, "{\"f\":\"\"}"), doc("d3", 3L, "{}"))
        );
    }

    public void testLongField_stringColumn_emptyStringUsesNullValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").field("null_value", 7).endObject()),
            columnarSettings(),
            batch("long empty string null_value", 1L, doc("d1", 1L, "{\"f\":\"10\"}"), doc("d2", 2L, "{\"f\":\"\"}"), doc("d3", 3L, "{}"))
        );
    }

    public void testLongField_stringColumn_coerceFalseRejectsNumericString() throws IOException {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> assertColumnarMatchesXContent(
                mapping(b -> b.startObject(FIELD).field("type", "long").field("coerce", false).endObject()),
                columnarSettings(),
                batch("long coerce false string", 1L, doc("d1", 1L, "{\"f\":\"42\"}"))
            )
        );
        assertTrue("expected coerce message but got: " + ex.getMessage(), ex.getMessage().contains("Long value passed as String"));
    }

    public void testLongField_stringColumn_coerceFalseRejectsEmptyString() throws IOException {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> assertColumnarMatchesXContent(
                mapping(b -> b.startObject(FIELD).field("type", "long").field("coerce", false).endObject()),
                columnarSettings(),
                batch("long coerce false empty string", 1L, doc("d1", 1L, "{\"f\":\"\"}"))
            )
        );
        assertTrue("expected coerce message but got: " + ex.getMessage(), ex.getMessage().contains("Long value passed as String"));
    }

    public void testDoubleField_bigIntegerNumberTokenStoredAsStringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").endObject()),
            columnarSettings(),
            batch("double big integer token", 1L, doc("d1", 1L, "{\"f\":9223372036854775808}"))
        );
    }

    public void testDoubleField_bigDecimalNumberTokenStoredAsStringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").endObject()),
            columnarSettings(),
            batch("double big decimal token", 1L, doc("d1", 1L, "{\"f\":1.2345678901234567890123456789}"))
        );
    }

    public void testIntegerField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "integer").endObject()),
            columnarSettings(),
            batch("integer string", 1L, doc("d1", 1L, "{\"f\":\"100\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-50\"}"))
        );
    }

    public void testIntegerField_stringColumn_decimalTruncated() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "integer").endObject()),
            columnarSettings(),
            batch("integer string decimal coerce", 1L, doc("d1", 1L, "{\"f\":\"123.9\"}"), doc("d2", 2L, "{\"f\":\"-123.9\"}"))
        );
    }

    public void testShortField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "short").endObject()),
            columnarSettings(),
            batch("short string", 1L, doc("d1", 1L, "{\"f\":\"32767\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-1\"}"))
        );
    }

    public void testByteField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "byte").endObject()),
            columnarSettings(),
            batch("byte string", 1L, doc("d1", 1L, "{\"f\":\"127\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-128\"}"))
        );
    }

    public void testFloatField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "float").endObject()),
            columnarSettings(),
            batch("float string", 1L, doc("d1", 1L, "{\"f\":\"1.5\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-2.25\"}"))
        );
    }

    public void testDoubleField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").endObject()),
            columnarSettings(),
            batch("double string", 1L, doc("d1", 1L, "{\"f\":\"1.5\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-2.25\"}"))
        );
    }

    public void testHalfFloatField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "half_float").endObject()),
            columnarSettings(),
            batch("half_float string", 1L, doc("d1", 1L, "{\"f\":\"1.5\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-2.25\"}"))
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
     * {@link NumberFieldMapper#supportsColumnarParse} accepts {@code doc_values.multi_value=true} —
     * the setting defaults to {@code true}, so rejecting it would take every numeric field in a
     * columnar index off the columnar path. Multi-valued documents themselves are not implemented:
     * they arrive as an ESCF {@code ARRAY} column and the kind switch in
     * {@link NumberFieldMapper#mapColumnBatch} throws, which makes {@code ShardBatchMapper} fall the
     * chunk back to the row path. This test pins the gap that fallback papers over.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement multi-valued numeric fields; ARRAY columns fall back to the row path")
    public void testLongField_multiValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            multiValueColumnarSettings(),
            // Every present value is an array so the column is a plain ARRAY; mixing in a scalar
            // would make it a UNION and trip the same switch for a different reason.
            batch("long multi-value", 1L, doc("d1", 1L, "{\"f\":[1,2,3]}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":[7]}"))
        );
    }

    /**
     * As {@link #testLongField_multiValue}, for a null value. A null makes the column a UNION rather
     * than a plain LONG, which the same kind switch rejects.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement null numeric values; UNION columns fall back to the row path")
    public void testLongField_nullValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").field("null_value", 9).endObject()),
            columnarSettings(),
            batch("long null value", 1L, doc("d1", 1L, "{\"f\":1}"), doc("d2", 2L, "{\"f\":null}"), doc("d3", 3L, "{\"f\":3}"))
        );
    }

    /**
     * {@link NumberFieldMapper#supportsColumnarParse} accepts {@code ignore_malformed=true} — the
     * logsdb index modes default it to {@code true}. Per-value error handling
     * ({@code addIgnoredField} plus the ignored-source stored copy) is not implemented in
     * {@code mapColumnBatch}, so an unparseable value throws out of {@code NumberColumnTransform}
     * and the chunk falls back to the row path, which applies {@code ignore_malformed} properly.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement ignore_malformed; malformed values fall back to the row path")
    public void testLongField_ignoreMalformed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").field("ignore_malformed", true).endObject()),
            columnarSettings(),
            // All values are strings so the column is a plain STRING and the malformed value reaches
            // the numeric parser; mixing in a JSON number would make it a UNION and fail earlier.
            batch(
                "long ignore_malformed",
                1L,
                doc("d1", 1L, "{\"f\":\"1\"}"),
                doc("d2", 2L, "{\"f\":\"not-a-number\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    /**
     * As {@link #testLongField_ignoreMalformed}, for a value that parses but falls outside the
     * type's range — rejected by {@code NumberColumnTransform#validateLongRange} rather than by the
     * string parser.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement ignore_malformed; out-of-range values fall back to the row path")
    public void testByteField_ignoreMalformedOutOfRange() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "byte").field("ignore_malformed", true).endObject()),
            columnarSettings(),
            batch("byte ignore_malformed out of range", 1L, doc("d1", 1L, "{\"f\":1}"), doc("d2", 2L, "{\"f\":300}"))
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
    // epoch millis: 2024-01-15T12:00:00.000Z, 2024-06-01T00:00:00.000Z
    private static final long ST_TS_A = 1705320000000L;
    private static final long ST_TS_B = 1717200000000L;

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

    public void testLongField_stored() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_B);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "long").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch(
                "long stored",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":42}"),
                doc(idB, ST_ROUTING, ST_TSID, 2L, "{\"@timestamp\":" + ST_TS_B + "}")
            )
        );
    }

    public void testIntegerField_stored() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "integer").field("store", true).endObject();
        }), tsdbSettings(), batch("integer stored", 1L, doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":7}")));
    }

    public void testFloatField_stored() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "float").field("store", true).endObject();
        }), tsdbSettings(), batch("float stored", 1L, doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":3.14}")));
    }

    public void testDoubleField_stored() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "double").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch("double stored", 1L, doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":2.718281828}"))
        );
    }

    public void testHalfFloatField_stored() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "half_float").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch("half_float stored", 1L, doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":1.5}"))
        );
    }
}
