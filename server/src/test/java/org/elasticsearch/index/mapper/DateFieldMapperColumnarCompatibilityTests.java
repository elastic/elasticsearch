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
            // DateFieldMapper.supportsColumnarParse requires multiValue == false.
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
}
