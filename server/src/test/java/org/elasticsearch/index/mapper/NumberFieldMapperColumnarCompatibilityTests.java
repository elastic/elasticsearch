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
}
