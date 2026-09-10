/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractColumnarMapperCompatibilityTestCase;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Parity tests for {@link MatchOnlyTextFieldMapper#mapColumnBatch} against the row path.
 * The {@link AbstractColumnarMapperCompatibilityTestCase} harness drives leaf mappers automatically
 * via {@code EscfEncoder}; no subclass override is needed.
 */
public class MatchOnlyTextFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "match_only_text").endObject()),
            columnarSettings(),
            batch("single value", 1L, doc("d1", 1L, "{\"f\":\"hello\"}"))
        );
    }

    public void testMultiValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "match_only_text").endObject()),
            columnarSettings(),
            batch("multi-value", 1L, doc("d1", 1L, "{\"f\":[\"hello\",\"world\"]}"))
        );
    }

    public void testAbsentDocsMixed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "match_only_text").endObject()),
            columnarSettings(),
            batch("absent docs mixed", 1L, doc("d1", 1L, "{\"f\":\"alpha\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"gamma\"}"))
        );
    }

    public void testArrayValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "match_only_text").endObject()),
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

    public void testArrayValuesWithNull() throws IOException {
        // An array containing an explicit null element produces a null slot in the doc-values blob.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "match_only_text").endObject()),
            columnarSettings(),
            batch("array values with null", 1L, doc("d1", 1L, "{\"f\":[\"a\",null,\"b\"]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testNullValue() throws IOException {
        // match_only_text has no null_value: an explicit JSON null records a null slot only.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "match_only_text").endObject()),
            columnarSettings(),
            batch("null value", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{\"f\":\"a\"}"), doc("d3", 3L, "{}"))
        );
    }

    public void testMixedBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "match_only_text").endObject()),
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

    public void testNestedArray() throws IOException {
        // Nested arrays are flattened, matching the row-path behaviour in DocumentParser.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "match_only_text").endObject()),
            columnarSettings(),
            batch("nested array", 1L, doc("d1", 1L, "{\"f\":[[\"a\",\"b\"],[\"c\"]]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testNoIndexDocValuesOnly() throws IOException {
        // index:false — only the binary DV column is emitted, no terms column.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "match_only_text").field("index", false).endObject()),
            columnarSettings(),
            batch("no-index single value", 1L, doc("d1", 1L, "{\"f\":\"only_dv\"}"))
        );
    }

    public void testSingleValueMultiValueFalse() throws IOException {
        // One string value, one absent doc, and an empty string.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "match_only_text");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "single value multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"hello\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"\"}")
            )
        );
    }

    public void testAbsentAndNullMultiValueFalse() throws IOException {
        // Present value, absent doc ({}), and explicit JSON null -> absent (no null_value).
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "match_only_text");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "absent and null multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"alpha\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":null}")
            )
        );
    }

    public void testIndexedAndDocValuesMultiValueFalse() throws IOException {
        // Default index:true — both a terms column and a binary DV column are emitted.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "match_only_text");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "indexed and dv multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"indexed\"}"),
                doc("d2", 2L, "{\"f\":\"also_indexed\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testMultiValueViolationBailsOutOfColumnarPath() throws IOException {
        // Two values for a multi_value=false field: mapColumnBatch must throw so that
        // ShardBatchMapper falls back to the row path, which raises the correct
        // on_failure=FAIL document-level error instead.
        final var mapperService = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD).field("type", "match_only_text");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }));
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":[\"a\",\"b\"]}"));
    }

    public void testAllPresentDenseMultiValueFalse() throws IOException {
        // Every doc has a string value; no absent docs. Exercises the dense (validity==null) wrap in
        // the fast path.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "match_only_text");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "all present dense multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"a\"}"),
                doc("d2", 2L, "{\"f\":\"b\"}"),
                doc("d3", 3L, "{\"f\":\"c\"}")
            )
        );
    }
}
