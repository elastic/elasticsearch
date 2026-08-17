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
 * Parity tests for {@link KeywordFieldMapper#mapColumnBatch} against the row path.
 * The {@link AbstractColumnarMapperCompatibilityTestCase} harness drives leaf mappers automatically
 * via {@code EscfEncoder}; no subclass override is needed.
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

    public void testArrayValuesWithNull() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("array values with null", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{}"))
        );
    }

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

    public void testLongValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch(
                "long values",
                1L,
                doc("d1", 1L, "{\"f\":42}"),
                doc("d2", 2L, "{\"f\":-7}"),
                doc("d3", 3L, "{\"f\":9876543210}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testDoubleValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch(
                "double values",
                1L,
                doc("d1", 1L, "{\"f\":3.14}"),
                doc("d2", 2L, "{\"f\":1.5}"),
                doc("d3", 3L, "{\"f\":-2.5}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testBooleanValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("boolean values", 1L, doc("d1", 1L, "{\"f\":true}"), doc("d2", 2L, "{\"f\":false}"), doc("d3", 3L, "{}"))
        );
    }

    public void testLongArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("long array", 1L, doc("d1", 1L, "{\"f\":[1,2,3]}"), doc("d2", 2L, "{\"f\":[]}"), doc("d3", 3L, "{}"))
        );
    }

    public void testDoubleArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("double array", 1L, doc("d1", 1L, "{\"f\":[1.5,2.5]}"), doc("d2", 2L, "{\"f\":[]}"), doc("d3", 3L, "{}"))
        );
    }

    public void testBooleanArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("boolean array", 1L, doc("d1", 1L, "{\"f\":[true,false]}"), doc("d2", 2L, "{\"f\":[]}"), doc("d3", 3L, "{}"))
        );
    }

    public void testMixedLongDouble() throws IOException {
        // A batch with one long and one double value promotes the column to UNION.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("mixed long and double", 1L, doc("d1", 1L, "{\"f\":1}"), doc("d2", 2L, "{\"f\":2.5}"))
        );
    }

    public void testNullValueSubstitution() throws IOException {
        // An explicit JSON null is substituted with the configured null_value.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").field("null_value", "NULL").endObject()),
            columnarSettings(),
            batch("null_value substitution", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{\"f\":\"a\"}"), doc("d3", 3L, "{}"))
        );
    }

    public void testArrayWithNull() throws IOException {
        // An array containing an explicit null element produces a null slot in the doc-values blob.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("array with null element", 1L, doc("d1", 1L, "{\"f\":[\"a\",null,\"b\"]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testNoIndexTermsAbsent() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").field("index", false).endObject()),
            columnarSettings(),
            batch("no-index single value", 1L, doc("d1", 1L, "{\"f\":\"only_dv\"}"))
        );
    }

    public void testNestedArray() throws IOException {
        // Nested arrays are flattened, matching the row-path behaviour in DocumentParser.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("nested array", 1L, doc("d1", 1L, "{\"f\":[[1,2],[3]]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testIgnoreAbove() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").field("ignore_above", 8191).endObject()),
            columnarSettings(),
            batch("ignore_above value", 1L, doc("d1", 1L, "{\"f\":\"" + "x".repeat(8192) + "\"}"))
        );
    }

    public void testSingleValueMultiValueFalse() throws IOException {
        // One string value, one absent doc, and an empty string.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
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
        // Present value, absent doc ({}), and explicit JSON null without null_value -> absent.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
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

    public void testNullValueSubstitutionMultiValueFalse() throws IOException {
        // Explicit JSON null is substituted with null_value.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword").field("null_value", "NULL");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "null_value substitution multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":null}"),
                doc("d2", 2L, "{\"f\":\"a\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testNoIndexDocValuesOnlyMultiValueFalse() throws IOException {
        // index:false — only the binary DV column is emitted, no terms column.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword").field("index", false);
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }), columnarSettings(), batch("no-index dv-only multi_value=false", 1L, doc("d1", 1L, "{\"f\":\"only_dv\"}"), doc("d2", 2L, "{}")));
    }

    public void testIndexedAndDocValuesMultiValueFalse() throws IOException {
        // Default index:true — both a terms column and a binary DV column are emitted.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
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

    public void testScalarCoercionsMultiValueFalse() throws IOException {
        // Numeric and boolean scalars are stringified by utf8Cursor, matching the row path.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "scalar coercions multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":42}"),
                doc("d2", 2L, "{\"f\":3.14}"),
                doc("d3", 3L, "{\"f\":true}")
            )
        );
    }

    public void testIgnoreAboveMultiValueFalse() throws IOException {
        // ignore_above: the too-long value is recorded in _ignored and stored as a plain
        // BinaryDocValuesField synthetic-source fallback (no counts sidecar).
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword").field("ignore_above", 8);
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch("ignore_above multi_value=false", 1L, doc("d1", 1L, "{\"f\":\"toolongvalue\"}"), doc("d2", 2L, "{\"f\":\"short\"}"))
        );
    }

    public void testNullValueConfiguredNoNullsMultiValueFalse() throws IOException {
        // null_value is configured but the batch contains only real string values plus an absent doc.
        // The fast path must not be disabled by a configured null_value; no substitution should occur
        // because the source STRING column contains no null slots (explicit JSON nulls promote to UNION).
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword").field("null_value", "NULL");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "null_value configured no nulls multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"alpha\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"beta\"}")
            )
        );
    }

    public void testAllPresentDenseMultiValueFalse() throws IOException {
        // Every doc has a string value; no absent docs. Exercises the dense (validity==null) wrap in
        // the fast path.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
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

    public void testManyMixedPresentAbsentMultiValueFalse() throws IOException {
        // Larger interleaved present/absent batch to stress the SPARSE wrap and the
        // length-validation scan across many rows.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "many mixed present absent multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"v1\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"v3\"}"),
                doc("d4", 4L, "{}"),
                doc("d5", 5L, "{\"f\":\"v5\"}"),
                doc("d6", 6L, "{\"f\":\"v6\"}"),
                doc("d7", 7L, "{}")
            )
        );
    }

    public void testSingleElementArrayMultiValueFalse() throws IOException {
        // A single-element array {"f":["a"]} is a legal value for a multi_value=false field.
        // The ESCF encoder produces an ARRAY-of-STRING column; the fast path must wrap it
        // zero-copy, matching the row path which extracts the sole element.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "single element array multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":[\"a\"]}"),
                doc("d2", 2L, "{\"f\":[]}"),
                doc("d3", 3L, "{}")
            )
        );
    }
}
