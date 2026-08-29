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
 * Parity tests for {@link TextFieldMapper#mapColumnBatch} against the row path.
 * The {@link AbstractColumnarMapperCompatibilityTestCase} harness drives leaf mappers automatically
 * via {@code EscfEncoder}; no subclass override is needed.
 */
public class TextFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "text").endObject()),
            columnarSettings(),
            batch("single value", 1L, doc("d1", 1L, "{\"f\":\"hello world\"}"))
        );
    }

    public void testMultiValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "text").endObject()),
            columnarSettings(),
            batch("multi-value", 1L, doc("d1", 1L, "{\"f\":[\"hello\",\"world\"]}"))
        );
    }

    public void testAbsentDocsMixed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "text").endObject()),
            columnarSettings(),
            batch("absent docs mixed", 1L, doc("d1", 1L, "{\"f\":\"alpha\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"gamma\"}"))
        );
    }

    public void testArrayValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "text").endObject()),
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
            mapping(b -> b.startObject(FIELD).field("type", "text").endObject()),
            columnarSettings(),
            batch("array values with null", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{}"))
        );
    }

    public void testMixedBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "text").endObject()),
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
            mapping(b -> b.startObject(FIELD).field("type", "text").endObject()),
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
            mapping(b -> b.startObject(FIELD).field("type", "text").endObject()),
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
            mapping(b -> b.startObject(FIELD).field("type", "text").endObject()),
            columnarSettings(),
            batch("boolean values", 1L, doc("d1", 1L, "{\"f\":true}"), doc("d2", 2L, "{\"f\":false}"), doc("d3", 3L, "{}"))
        );
    }

    public void testNestedArray() throws IOException {
        // Nested arrays are flattened, matching the row-path behaviour in DocumentParser.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "text").endObject()),
            columnarSettings(),
            batch("nested array", 1L, doc("d1", 1L, "{\"f\":[[\"a\",\"b\"],[\"c\"]]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testNoIndexDocValuesOnly() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "text").field("index", false).endObject()),
            columnarSettings(),
            batch("no-index single value", 1L, doc("d1", 1L, "{\"f\":\"only_dv\"}"))
        );
    }

    public void testTermVectors() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "text").field("term_vector", "with_positions_offsets").endObject()),
            columnarSettings(),
            batch("term vectors", 1L, doc("d1", 1L, "{\"f\":\"hello world\"}"), doc("d2", 2L, "{}"))
        );
    }

    public void testNormsEnabled() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "text").field("norms", true).endObject()),
            columnarSettings(),
            batch("norms enabled", 1L, doc("d1", 1L, "{\"f\":\"hello world\"}"), doc("d2", 2L, "{}"))
        );
    }

    public void testSingleValueMultiValueFalse() throws IOException {
        // One string value, one absent doc, and an empty string.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text");
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
        // Present value, absent doc ({}), and explicit JSON null -> absent (text has no null_value).
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text");
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
        // Default index:true — both an indexed (tokenized) column and a binary DV column are emitted.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "indexed and dv multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"indexed\"}"),
                doc("d2", 2L, "{\"f\":\"also indexed\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testScalarCoercionsMultiValueFalse() throws IOException {
        // Numeric and boolean scalars are stringified by utf8Cursor, matching the row path.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text");
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

    public void testAllPresentDenseMultiValueFalse() throws IOException {
        // Every doc has a string value; no absent docs. Exercises the dense (validity==null) wrap in the fast path.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text");
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
        // Larger interleaved present/absent batch to stress the SPARSE wrap.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text");
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
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text");
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

    // ---- multi-fields ---------------------------------------------------------------------------

    public void testMultiFieldSingleValue() throws IOException {
        // text with a keyword multi-field: the most common real-world mapping pattern (dynamic mapping's
        // default "text" + ".keyword").
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text");
            b.startObject("fields");
            b.startObject("keyword").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "multi-field single value",
                1L,
                doc("d1", 1L, "{\"f\":\"Hello World\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"Another Value\"}")
            )
        );
    }

    public void testMultiFieldArrayValues() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text");
            b.startObject("fields");
            b.startObject("keyword").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "multi-field array values",
                1L,
                doc("d1", 1L, "{\"f\":[\"alpha beta\",\"gamma\"]}"),
                doc("d2", 2L, "{\"f\":[]}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testMultiFieldTwoMultiFields() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text");
            b.startObject("fields");
            b.startObject("keyword").field("type", "keyword").endObject();
            b.startObject("other").field("type", "keyword").field("ignore_above", 3).endObject();
            b.endObject();
            b.endObject();
        }), columnarSettings(), batch("multi-field two multi-fields", 1L, doc("d1", 1L, "{\"f\":\"abcd\"}"), doc("d2", 2L, "{}")));
    }

    public void testMultiFieldNotIndexedParent() throws IOException {
        // The parent is not indexed (doc values only) while its multi-field is fully indexed.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "text").field("index", false);
            b.startObject("fields");
            b.startObject("keyword").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch("multi-field not-indexed parent", 1L, doc("d1", 1L, "{\"f\":\"only_dv_and_multifield\"}"), doc("d2", 2L, "{}"))
        );
    }

    // ---- fields not yet supported: verify correct fallback ---------------------------------------

    public void testIndexPhrasesFallsBack() throws IOException {
        MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "text").field("index_phrases", true).endObject())
        );
        Mapper mapper = ms.mappingLookup().getMapper(FIELD);
        assertTrue(mapper instanceof TextFieldMapper);
        assertFalse(((TextFieldMapper) mapper).supportsColumnarParse(ms.getIndexSettings()));
    }

    public void testIndexPrefixesFallsBack() throws IOException {
        MapperService ms = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD).field("type", "text");
            b.startObject("index_prefixes").endObject();
            b.endObject();
        }));
        Mapper mapper = ms.mappingLookup().getMapper(FIELD);
        assertTrue(mapper instanceof TextFieldMapper);
        assertFalse(((TextFieldMapper) mapper).supportsColumnarParse(ms.getIndexSettings()));
    }

    public void testMultiFieldWithNonColumnarMultiFieldFallsBack() throws IOException {
        // A multi-field that is itself disqualified from columnar parse (here, a "text" multi-field with
        // index_phrases) must disqualify the parent too - recursion via multiFieldsSupportColumnarParse.
        MapperService ms = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD).field("type", "text");
            b.startObject("fields");
            b.startObject("phrases").field("type", "text").field("index_phrases", true).endObject();
            b.endObject();
            b.endObject();
        }));
        Mapper mapper = ms.mappingLookup().getMapper(FIELD);
        assertTrue(mapper instanceof TextFieldMapper);
        assertFalse(((TextFieldMapper) mapper).supportsColumnarParse(ms.getIndexSettings()));
    }
}
