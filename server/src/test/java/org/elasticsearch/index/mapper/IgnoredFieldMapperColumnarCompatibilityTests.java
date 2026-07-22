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
 * Compatibility tests for {@link IgnoredFieldMapper}'s columnar batch path
 * ({@link IgnoredFieldMapper#postColumnarParse}) against its row-major path
 * ({@link IgnoredFieldMapper#postParse}).
 *
 * <p>Uses keyword fields with {@code ignore_above} to naturally populate {@code _ignored}
 * entries via {@link KeywordFieldMapper#mapColumnBatch}, which also emits the single-valued
 * synthetic-source fallback column ({@code f._original} + {@code f._original.counts}).
 */
public class IgnoredFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    /** Single doc where the keyword value exceeds {@code ignore_above}: {@code _ignored} and the fallback column are emitted. */
    public void testSingleIgnoredField() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject("f").field("type", "keyword").field("ignore_above", 5).endObject()),
            columnarSettings(),
            batch("single ignored", 1L, doc("d1", 1L, "{\"f\":\"toolong\"}"))
        );
    }

    /** Two docs where no values are ignored: the {@code _ignored} accumulator stays empty. */
    public void testNoIgnoredFields() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject("f").field("type", "keyword").field("ignore_above", 5).endObject()),
            columnarSettings(),
            batch("no ignored", 1L, doc("d1", 1L, "{\"f\":\"ok\"}"), doc("d2", 2L, "{\"f\":\"fine\"}"))
        );
    }

    /**
     * Four docs with three keyword fields at {@code ignore_above=5}: per-doc ignored sets vary,
     * one doc is absent entirely, and doc 4 has all three fields ignored — exercising value
     * interning and the multi-valued {@code _ignored} array column.
     */
    public void testOverlappingAndMultiValued() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("field_a").field("type", "keyword").field("ignore_above", 5).endObject();
            b.startObject("field_b").field("type", "keyword").field("ignore_above", 5).endObject();
            b.startObject("field_c").field("type", "keyword").field("ignore_above", 5).endObject();
        }),
            columnarSettings(),
            batch(
                "overlapping multi-valued",
                1L,
                doc("d1", 1L, "{\"field_a\":\"toolong1\",\"field_b\":\"toolong2\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"field_a\":\"toolong3\"}"),
                doc("d4", 4L, "{\"field_b\":\"toolong4\",\"field_c\":\"toolong5\",\"field_a\":\"toolong6\"}")
            )
        );
    }
}
