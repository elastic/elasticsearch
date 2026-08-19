/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;

/**
 * Parity tests for {@link BooleanFieldMapper#mapColumnBatch} against the row path.
 * Each test passes the same documents through both the columnar and x-content parse paths
 * and asserts that the resulting Lucene fields are identical.
 */
public class BooleanFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .build();
    }

    // ---- basic boolean values ------------------------------------------------------------------

    public void testTrueValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").endObject()),
            columnarSettings(),
            batch("true value", 1L, doc("d1", 1L, "{\"f\":true}"))
        );
    }

    public void testFalseValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").endObject()),
            columnarSettings(),
            batch("false value", 1L, doc("d1", 1L, "{\"f\":false}"))
        );
    }

    public void testAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").endObject()),
            columnarSettings(),
            batch("absent doc", 1L, doc("d1", 1L, "{}"))
        );
    }

    public void testMixedAbsentPresent() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").endObject()),
            columnarSettings(),
            batch("mixed absent/present", 1L, doc("d1", 1L, "{\"f\":true}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":false}"))
        );
    }

    public void testMultipleDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").endObject()),
            columnarSettings(),
            batch(
                "multiple docs",
                1L,
                doc("d1", 1L, "{\"f\":true}"),
                doc("d2", 2L, "{\"f\":false}"),
                doc("d3", 3L, "{\"f\":true}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testAllFalse() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").endObject()),
            columnarSettings(),
            batch("all false", 1L, doc("d1", 1L, "{\"f\":false}"), doc("d2", 2L, "{\"f\":false}"), doc("d3", 3L, "{\"f\":false}"))
        );
    }

    // ---- null_value ---------------------------------------------------------------------------

    /**
     * When {@code null_value} is set, absent fields are substituted with the configured boolean.
     * This exercises the validity-bit hole path in {@link BooleanFieldMapper#booleansToLongs}.
     */
    public void testNullValueSubstitutedForAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").field("null_value", true).endObject()),
            columnarSettings(),
            batch("null_value for absent docs", 1L, doc("d1", 1L, "{\"f\":true}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":false}"))
        );
    }

    public void testNullValueFalseSubstitutedForAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").field("null_value", false).endObject()),
            columnarSettings(),
            batch(
                "null_value=false for absent docs",
                1L,
                doc("d1", 1L, "{\"f\":true}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":false}")
            )
        );
    }

    // ---- indexed=true -------------------------------------------------------------------------

    /**
     * Explicit {@code index=true} emits both a {@code StringField} ("T"/"F") and a
     * {@code SortedNumericDocValuesField} (1/0). The columnar path must emit both a
     * {@link org.elasticsearch.escf.LuceneBinaryColumn} and a {@link org.elasticsearch.escf.LuceneLongColumn}.
     */
    public void testIndexedTrueValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed true value", 1L, doc("d1", 1L, "{\"f\":true}"))
        );
    }

    public void testIndexedFalseValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed false value", 1L, doc("d1", 1L, "{\"f\":false}"))
        );
    }

    public void testIndexedWithAbsentDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").field("index", true).endObject()),
            columnarSettings(),
            batch("indexed with absent doc", 1L, doc("d1", 1L, "{\"f\":true}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":false}"))
        );
    }

    public void testIndexedMultipleDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").field("index", true).endObject()),
            columnarSettings(),
            batch(
                "indexed multiple docs",
                1L,
                doc("d1", 1L, "{\"f\":true}"),
                doc("d2", 2L, "{\"f\":false}"),
                doc("d3", 3L, "{\"f\":true}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    // ---- explicit index=false -----------------------------------------------------------------

    /**
     * Explicit {@code index=false} mirrors the default in columnar mode (where
     * {@code index.mapping.index_disabled_by_default=true}), but is stated explicitly so the test
     * remains meaningful if the default ever changes. Emits only a {@code SortedNumericDocValuesField}.
     */
    public void testExplicitlyNotIndexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").field("index", false).endObject()),
            columnarSettings(),
            batch("explicitly not-indexed", 1L, doc("d1", 1L, "{\"f\":true}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":false}"))
        );
    }

    // ---- multi-value / null / ignore_malformed (awaiting full implementation) -----------------

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
     * {@link BooleanFieldMapper#supportsColumnarParse} accepts {@code doc_values.multi_value=true}.
     * Multi-valued boolean documents arrive as an ESCF {@code ARRAY} column; the kind switch in
     * {@link BooleanFieldMapper#mapColumnBatch} throws, causing the chunk to fall back to the row
     * path. This test pins the gap that fallback papers over.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement multi-valued boolean fields; ARRAY columns fall back to the row path")
    public void testMultiValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").endObject()),
            multiValueColumnarSettings(),
            batch("multi-value booleans", 1L, doc("d1", 1L, "{\"f\":[true,false]}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":[true]}"))
        );
    }

    /**
     * A JSON-null ({@code "f":null}) with a {@code null_value} configured arrives as a UNION column,
     * which the kind switch in {@link BooleanFieldMapper#mapColumnBatch} rejects, falling back to the
     * row path.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement null boolean values; UNION columns fall back to the row path")
    public void testNullValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").field("null_value", true).endObject()),
            columnarSettings(),
            batch("null boolean value", 1L, doc("d1", 1L, "{\"f\":true}"), doc("d2", 2L, "{\"f\":null}"), doc("d3", 3L, "{}"))
        );
    }

    /**
     * {@link BooleanFieldMapper#supportsColumnarParse} accepts {@code ignore_malformed=true}. A
     * malformed value (non-boolean string) throws out of the string parser in
     * {@link BooleanFieldMapper#booleansToLongs}, causing the chunk to fall back to the row path
     * which applies {@code ignore_malformed} properly.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement ignore_malformed; malformed booleans fall back to the row path")
    public void testIgnoreMalformed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "boolean").field("ignore_malformed", true).endObject()),
            columnarSettings(),
            batch(
                "ignore_malformed booleans",
                1L,
                doc("d1", 1L, "{\"f\":true}"),
                doc("d2", 2L, "{\"f\":\"not-a-boolean\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }
}
