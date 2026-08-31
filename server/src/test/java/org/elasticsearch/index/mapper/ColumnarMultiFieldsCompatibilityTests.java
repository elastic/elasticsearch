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
 * Parity tests for multi-field dispatch on {@link FieldMapper#mapColumnBatch} against the row path.
 * <p>
 * A multi-field consumes exactly the same source value as its parent, so the columnar path hands each sub-mapper the parent's own
 * {@code EscfColumn}. These scenarios pin down that the resulting Lucene fields — including the {@code _ignored} entries and the
 * synthetic-source fallback, which a sub-field suppresses via {@code isWithinMultiField()} — match what the row path's
 * {@link FieldMapper#doParseMultiFields} produces for the same documents.
 * <p>
 * Numeric sources are written as canonical literals throughout. The columnar path stringifies the <em>parsed</em> value, so a
 * non-canonical literal such as {@code 1.50} reaches a keyword sub-field as {@code "1.5"} where the row path preserves the source
 * characters. That divergence is pre-existing and documented on {@code KeywordFieldMapper#mapColumnBatch}; a number parent with a
 * keyword sub-field is simply a new way to reach it.
 */
public class ColumnarMultiFieldsCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    /** {@code keyword} parent with a plain {@code keyword} sub-field. */
    public void testKeywordSubField() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch("keyword sub-field", 1L, doc("d1", 1L, "{\"f\":\"hello\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"world\"}"))
        );
    }

    /** Arrays and explicit nulls must produce the same array-order slots on the parent and on the sub-field. */
    public void testKeywordSubFieldArraysAndNulls() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "keyword sub-field arrays and nulls",
                1L,
                doc("d1", 1L, "{\"f\":[\"a\",\"b\",\"c\"]}"),
                doc("d2", 2L, "{\"f\":[\"a\",null,\"b\"]}"),
                doc("d3", 3L, "{\"f\":null}"),
                doc("d4", 4L, "{\"f\":[]}"),
                doc("d5", 5L, "{}")
            )
        );
    }

    /** {@code null_value} is resolved independently by the parent and the sub-field. */
    public void testKeywordSubFieldWithOwnNullValue() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword").field("null_value", "PARENT_NULL");
            b.startObject("fields");
            b.startObject("raw").field("type", "keyword").field("null_value", "SUB_NULL").endObject();
            b.endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch("sub-field null_value", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{\"f\":\"present\"}"), doc("d3", 3L, "{}"))
        );
    }

    /**
     * {@code ignore_above} is evaluated per mapper, so a value can be ignored by the parent, by the sub-field, by both, or by
     * neither. Each combination must yield the same {@code _ignored} entries on both paths, and the sub-field must never write a
     * synthetic-source fallback column.
     */
    public void testIgnoreAboveAcrossParentAndSubField() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword").field("ignore_above", 10);
            b.startObject("fields").startObject("raw").field("type", "keyword").field("ignore_above", 4).endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "ignore_above parent and sub-field",
                1L,
                doc("d1", 1L, "{\"f\":\"tiny\"}"),                  // neither ignores
                doc("d2", 2L, "{\"f\":\"medium_len\"}"),            // sub-field ignores
                doc("d3", 3L, "{\"f\":\"way_too_long_value\"}"),    // both ignore
                doc("d4", 4L, "{}")
            )
        );
    }

    /** Several sub-fields under one parent are all driven from the same source column. */
    public void testMultipleSubFields() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("fields");
            b.startObject("raw").field("type", "keyword").endObject();
            b.startObject("trimmed").field("type", "keyword").field("ignore_above", 3).endObject();
            b.endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch("multiple sub-fields", 1L, doc("d1", 1L, "{\"f\":\"ab\"}"), doc("d2", 2L, "{\"f\":\"abcdef\"}"), doc("d3", 3L, "{}"))
        );
    }

    /** A {@code multi_value=false} sub-field under a multi-valued-capable parent. */
    public void testSubFieldMultiValueFalse() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("fields").startObject("raw").field("type", "keyword");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "sub-field multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"single\"}"),
                doc("d2", 2L, "{\"f\":[\"solo\"]}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    /** {@code index:false} on the sub-field only: it emits doc values but no terms column. */
    public void testSubFieldNotIndexed() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("fields").startObject("raw").field("type", "keyword").field("index", false).endObject().endObject();
            b.endObject();
        }), columnarSettings(), batch("sub-field index=false", 1L, doc("d1", 1L, "{\"f\":\"only_dv\"}"), doc("d2", 2L, "{}")));
    }

    /**
     * A {@code long} parent stringifies into its keyword sub-field, on both paths. Kept to scalar values: mixing a scalar and an
     * array promotes the ESCF column to UNION, which {@code NumberFieldMapper#mapColumnBatch} does not handle yet and rejects by
     * throwing so the batch falls back — a pre-existing limitation of that mapper, unrelated to multi-fields.
     */
    public void testLongParentWithKeywordSubField() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "long");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "long parent, keyword sub-field",
                1L,
                doc("d1", 1L, "{\"f\":42}"),
                doc("d2", 2L, "{\"f\":-7}"),
                doc("d3", 3L, "{\"f\":9876543210}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testBooleanParentWithKeywordSubField() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "boolean");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "boolean parent, keyword sub-field",
                1L,
                doc("d1", 1L, "{\"f\":true}"),
                doc("d2", 2L, "{\"f\":false}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testDateParentWithKeywordSubField() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "date");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "date parent, keyword sub-field",
                1L,
                doc("d1", 1L, "{\"f\":\"2024-01-01T00:00:00.000Z\"}"),
                doc("d2", 2L, "{\"f\":\"2024-06-30T23:59:59.999Z\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testIpParentWithKeywordSubField() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "ip");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "ip parent, keyword sub-field",
                1L,
                doc("d1", 1L, "{\"f\":\"192.168.0.1\"}"),
                doc("d2", 2L, "{\"f\":[\"10.0.0.1\",\"10.0.0.2\"]}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    /**
     * Two sub-fields of different types under one keyword parent, both fed from the same source column. Values stay strings so the
     * numeric sub-field sees a STRING column rather than a UNION one (see {@link #testLongParentWithKeywordSubField}).
     */
    public void testMixedTypeSubFields() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("fields");
            b.startObject("as_long").field("type", "long").endObject();
            b.startObject("as_keyword").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch("mixed-type sub-fields", 1L, doc("d1", 1L, "{\"f\":\"123\"}"), doc("d2", 2L, "{\"f\":\"456\"}"), doc("d3", 3L, "{}"))
        );
    }
}
