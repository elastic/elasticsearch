/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractColumnarMapperCompatibilityTestCase;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Parity tests for {@link WildcardFieldMapper#doMapColumnBatch} against the row path.
 * The {@link AbstractColumnarMapperCompatibilityTestCase} harness drives leaf mappers automatically
 * via {@code EscfEncoder}; no subclass override is needed.
 */
public class WildcardFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singleton(new Wildcard());
    }

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    public void testSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("single value", 1L, doc("d1", 1L, "{\"f\":\"hello\"}"))
        );
    }

    public void testMultiValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("multi-value", 1L, doc("d1", 1L, "{\"f\":[\"hello\",\"world\"]}"))
        );
    }

    public void testAbsentDocsMixed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("absent docs mixed", 1L, doc("d1", 1L, "{\"f\":\"alpha\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"gamma\"}"))
        );
    }

    public void testArrayValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
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
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("array values with null", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{}"))
        );
    }

    public void testMixedBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
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
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
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
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
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
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("boolean values", 1L, doc("d1", 1L, "{\"f\":true}"), doc("d2", 2L, "{\"f\":false}"), doc("d3", 3L, "{}"))
        );
    }

    public void testNestedArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("nested array", 1L, doc("d1", 1L, "{\"f\":[[\"a\",\"b\"],[\"c\"]]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testNullValueSubstitution() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").field("null_value", "NULL").endObject()),
            columnarSettings(),
            batch("null_value substitution", 1L, doc("d1", 1L, "{\"f\":null}"), doc("d2", 2L, "{\"f\":\"a\"}"), doc("d3", 3L, "{}"))
        );
    }

    public void testArrayWithNull() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("array with null element", 1L, doc("d1", 1L, "{\"f\":[\"a\",null,\"b\"]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testIgnoreAbove() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").field("ignore_above", 8191).endObject()),
            columnarSettings(),
            batch("ignore_above value", 1L, doc("d1", 1L, "{\"f\":\"" + "x".repeat(8192) + "\"}"))
        );
    }

    public void testIgnoreAboveWithOtherValuesInDoc() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").field("ignore_above", 4).endObject()),
            columnarSettings(),
            batch(
                "ignore_above with other values",
                1L,
                doc("d1", 1L, "{\"f\":[\"hi\",\"toolong\",\"ok\"]}"),
                doc("d2", 2L, "{\"f\":\"short\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testIgnoreAboveTwoValuesInDocFallsBack() throws IOException {
        final MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").field("ignore_above", 4).endObject())
        );
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(ms, FIELD, "{\"f\":[\"toolong1\",\"toolong2\"]}"));
    }

    public void testDuplicateValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("duplicate values", 1L, doc("d1", 1L, "{\"f\":[\"x\",\"x\",\"y\",\"x\"]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testAllNullArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("all-null array", 1L, doc("d1", 1L, "{\"f\":[null,null]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testNullValueCombinedWithIgnoreAbove() throws IOException {
        assertColumnarMatchesXContent(
            mapping(
                b -> b.startObject(FIELD).field("type", "wildcard").field("null_value", "toolong").field("ignore_above", 4).endObject()
            ),
            columnarSettings(),
            batch(
                "null_value combined with ignore_above",
                1L,
                doc("d1", 1L, "{\"f\":null}"),
                doc("d2", 2L, "{\"f\":\"ok\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testEmptyStringValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("empty string", 1L, doc("d1", 1L, "{\"f\":\"\"}"), doc("d2", 2L, "{\"f\":\"nonempty\"}"), doc("d3", 3L, "{}"))
        );
    }

    public void testMultiByteUnicodeValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()),
            columnarSettings(),
            batch("multi-byte unicode", 1L, doc("d1", 1L, "{\"f\":\"héllo wörld\"}"), doc("d2", 2L, "{\"f\":\"日本語\"}"))
        );
    }

    public void testMultiFieldSingleValue() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "wildcard");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "multi-field single value",
                1L,
                doc("d1", 1L, "{\"f\":\"hello\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"world\"}")
            )
        );
    }

    public void testMultiFieldArrayValues() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "wildcard");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "multi-field array values",
                1L,
                doc("d1", 1L, "{\"f\":[\"a\",\"b\",\"c\"]}"),
                doc("d2", 2L, "{\"f\":[\"a\",null,\"b\"]}"),
                doc("d3", 3L, "{\"f\":null}"),
                doc("d4", 4L, "{\"f\":[]}"),
                doc("d5", 5L, "{}")
            )
        );
    }

    public void testMultiFieldDivergingIgnoreAbove() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "wildcard").field("ignore_above", 10);
            b.startObject("fields").startObject("raw").field("type", "keyword").field("ignore_above", 4).endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "multi-field diverging ignore_above",
                1L,
                doc("d1", 1L, "{\"f\":\"tiny\"}"),
                doc("d2", 2L, "{\"f\":\"medium_len\"}"),
                doc("d3", 3L, "{\"f\":\"way_too_long_value\"}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    public void testKeywordParentWithWildcardMultiField() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("fields").startObject("wc").field("type", "wildcard").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "keyword parent with wildcard multi-field",
                1L,
                doc("d1", 1L, "{\"f\":\"hello\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"world\"}")
            )
        );
    }

    public void testNonColumnarModeFallsBack() throws IOException {
        final MapperService ms = createMapperService(mapping(b -> b.startObject(FIELD).field("type", "wildcard").endObject()));
        final var mapper = (WildcardFieldMapper) ms.mappingLookup().getMapper(FIELD);
        assertFalse(
            "wildcard must not take the columnar path outside strict-columnar index modes",
            mapper.supportsColumnarParse(ms.getIndexSettings())
        );
    }
}
