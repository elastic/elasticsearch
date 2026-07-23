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
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;

/**
 * Parity tests for {@link KeywordFieldMapper#mapColumnBatch} against the row path.
 * The {@link AbstractColumnarMapperCompatibilityTestCase} harness drives leaf mappers automatically via
 * {@link EscfEncoder}; no subclass override is needed.
 */
public class KeywordFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("single value", 1L, doc("d1", 1L, "{\"f\":\"hello\"}"))
        );
    }

    public void testMultiValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("multi-value", 1L, doc("d1", 1L, "{\"f\":[\"hello\",\"world\"]}"))
        );
    }

    public void testAbsentDocsMixed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("absent docs mixed", 1L, doc("d1", 1L, "{\"f\":\"alpha\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"gamma\"}"))
        );
    }

    public void testArrayValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch(
                "array values",
                1L,
                doc("d1", 1L, "{\"f\":[\"solo\"]}"),
                doc("d2", 2L, "{\"f\":[\"alpha\",\"beta\",\"gamma\"]}"),
                doc("d3", 3L, "{\"f\":[]}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    @AwaitsFix(bugUrl = "Uses null. Needs union column mapping support")
    public void testArrayValuesWithNull() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("array values", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{}"))
        );
    }

    /**
     * Mixing scalar and array values in the same batch causes {@link EscfEncoder} to promote the column
     * to {@link EscfColumnKind#UNION}; deferred until UNION support is added to
     * {@link KeywordFieldMapper#mapColumnBatch}.
     */
    @AwaitsFix(bugUrl = "Mixes arrays and scalars. Needs union column mapping support")
    public void testMixedBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch(
                "mixed batch",
                1L,
                doc("d1", 1L, "{\"f\":\"a\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[\"b\",\"c\"]}"),
                doc("d4", 4L, "{\"f\":\"d\"}")
            )
        );
    }

    public void testNoIndexTermsAbsent() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").field("index", false).endObject()),
            columnarSettings(),
            batch("no-index single value", 1L, doc("d1", 1L, "{\"f\":\"only_dv\"}"))
        );
    }

    public void testIgnoreAbove() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").field("ignore_above", 8191).endObject()),
            columnarSettings(),
            batch("ignore_above value", 1L, doc("d1", 1L, "{\"f\":\"" + "x".repeat(8192) + "\"}"))
        );
    }
}
