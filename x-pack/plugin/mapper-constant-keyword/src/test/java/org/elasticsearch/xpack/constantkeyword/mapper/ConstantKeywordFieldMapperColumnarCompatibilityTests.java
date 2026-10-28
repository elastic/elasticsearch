/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractColumnarMapperCompatibilityTestCase;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Parity tests for {@link ConstantKeywordFieldMapper#mapColumnBatch} against the row path.
 */
public class ConstantKeywordFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new ConstantKeywordMapperPlugin());
    }

    private static Settings columnarSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
    }

    public void testSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject()),
            columnarSettings(),
            batch("single value", 1L, doc("d1", 1L, "{\"f\":\"hello\"}"))
        );
    }

    public void testAbsentDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject()),
            columnarSettings(),
            batch("absent docs", 1L, doc("d1", 1L, "{\"f\":\"hello\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"hello\"}"))
        );
    }

    public void testAllPresent() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject()),
            columnarSettings(),
            batch(
                "all present",
                1L,
                doc("d1", 1L, "{\"f\":\"hello\"}"),
                doc("d2", 2L, "{\"f\":\"hello\"}"),
                doc("d3", 3L, "{\"f\":\"hello\"}")
            )
        );
    }

    public void testSingleElementArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject()),
            columnarSettings(),
            batch("single-element array", 1L, doc("d1", 1L, "{\"f\":[\"hello\"]}"))
        );
    }

    public void testMultiValueArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject()),
            columnarSettings(),
            batch("multi-value array", 1L, doc("d1", 1L, "{\"f\":[\"hello\",\"hello\",\"hello\"]}"))
        );
    }

    public void testArrayWithDuplicates() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "v").endObject()),
            columnarSettings(),
            batch("array with duplicates", 1L, doc("d1", 1L, "{\"f\":[\"v\",\"v\",\"v\",\"v\"]}"))
        );
    }

    public void testNestedArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject()),
            columnarSettings(),
            batch("nested array", 1L, doc("d1", 1L, "{\"f\":[[\"hello\"],[\"hello\"]]}"))
        );
    }

    public void testEmptyArray() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject()),
            columnarSettings(),
            batch("empty array", 1L, doc("d1", 1L, "{\"f\":[]}"))
        );
    }

    public void testEmptyArrayMixedWithValues() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject()),
            columnarSettings(),
            batch(
                "empty array mixed with values",
                1L,
                doc("d1", 1L, "{\"f\":\"hello\"}"),
                doc("d2", 2L, "{\"f\":[]}"),
                doc("d3", 3L, "{\"f\":\"hello\"}")
            )
        );
    }

    public void testLargerMixedBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "v").endObject()),
            columnarSettings(),
            batch(
                "larger mixed batch",
                2L,  // non-trivial primary term
                doc("d1", 1L, "{\"f\":\"v\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[\"v\",\"v\"]}"),
                doc("d4", 4L, "{}"),
                doc("d5", 5L, "{\"f\":\"v\"}"),
                doc("d6", 6L, "{\"f\":[]}"),
                doc("d7", 7L, "{\"f\":\"v\"}")
            )
        );
    }

    public void testNumericConstant() throws IOException {
        // ESCF stringifies LONG values canonically, so "42" matches the constant.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "42").endObject()),
            columnarSettings(),
            batch("numeric constant", 1L, doc("d1", 1L, "{\"f\":42}"))
        );
    }

    public void testBooleanConstant() throws IOException {
        // ESCF stringifies booleans as "true"/"false", so these match.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "true").endObject()),
            columnarSettings(),
            batch("boolean constant", 1L, doc("d1", 1L, "{\"f\":true}"))
        );
    }

    public void testNonAsciiConstant() throws IOException {
        // Non-ASCII UTF-8 round-trips through ESCF unchanged.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "café").endObject()),
            columnarSettings(),
            batch("non-ascii constant", 1L, doc("d1", 1L, "{\"f\":\"café\"}"))
        );
    }

    /**
     * Stored source is not supported by the columnar batch path yet: {@code SourceFieldMapper} refuses it, so every batch for such an
     * index falls back to the row path even though the {@code constant_keyword} mapper itself would accept it. The index is a TSDB one
     * because a data field only takes part in the batch path in a strict-columnar or TSDB index mode, and strict-columnar modes do not
     * allow stored source.
     *
     * <p>The parity check below is the one the field needs under stored source: {@code isSourceSynthetic()} is false, so
     * {@code mapColumnBatch} validates values but emits no presence-marker column. When stored source support lands, the check passes
     * and the surrounding {@code expectThrows} fails; remove it then.
     */
    public void testGate_storedSourceFallsBackToRowPath() {
        final BytesRef tsid = new BytesRef(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
        final int routingHash = 42;
        final long timestamp = 1622505600000L; // 2021-06-01T00:00:00.000Z
        final String id = TsidExtractingIdFieldMapper.createId(routingHash, tsid, timestamp);
        final String routing = TimeSeriesRoutingHashFieldMapper.encode(routingHash);

        final AssertionError e = expectThrows(AssertionError.class, () -> assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject();
        }),
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
                .put(IndexSettings.SYNTHETIC_ID.getKey(), false)
                .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED.toString())
                .build(),
            batch(
                "stored source",
                1L,
                doc(id, routing, tsid, 1L, "{\"@timestamp\":\"2021-06-01T00:00:00.000Z\",\"dim\":\"a\",\"f\":\"hello\"}")
            )
        ));
        assertThat(e.getMessage(), containsString("metadata mapper [_source] does not support columnar parse"));
    }

    public void testBailOutOnNull() throws IOException {
        MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject())
        );
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(ms, FIELD, "{\"f\":null}"));
    }

    public void testBailOutOnMismatch() throws IOException {
        MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject())
        );
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(ms, FIELD, "{\"f\":\"other\"}"));
    }

    public void testBailOutOnMixedBatch() throws IOException {
        // A matching value followed by a mismatching one in the same batch must bail out.
        MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject())
        );
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(ms, FIELD, "{\"f\":\"hello\"}", "{\"f\":\"other\"}"));
    }

    public void testBailOutOnArrayWithNull() throws IOException {
        MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject())
        );
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(ms, FIELD, "{\"f\":[\"hello\",null]}"));
    }

    public void testBailOutOnArrayWithMismatch() throws IOException {
        MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject())
        );
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(ms, FIELD, "{\"f\":[\"hello\",\"other\"]}"));
    }

    public void testBailOutUnderStoredSource() throws IOException {
        // Validation runs regardless of isSourceSynthetic(): the bail-out is independent of whether
        // the presence-marker column is being built. When stored source is eventually supported in
        // the columnar batch path, this verifies the mapper's own behaviour rather than any
        // ShardBatchMapper gate.
        MapperService ms = createMapperService(
            Settings.EMPTY,
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject())
        );
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(ms, FIELD, "{\"f\":\"other\"}"));
    }

    // -------------------------------------------------------------------------
    // Gate tests — supportsColumnarParse must gate correctly.
    // -------------------------------------------------------------------------

    public void testGate_unconfiguredValue() throws IOException {
        // A constant_keyword with no value set pins its value on the first document via a dynamic
        // mapping update; the columnar batch path cannot carry mapping updates.
        MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").endObject())
        );
        FieldMapper mapper = (FieldMapper) ms.mappingLookup().getMapper(FIELD);
        assertFalse(
            "supportsColumnarParse must be false when value is not configured",
            mapper.supportsColumnarParse(ms.getIndexSettings())
        );
    }

    public void testGate_configuredValue_columnarMode() throws IOException {
        // A configured constant_keyword supports columnar parse in columnar index mode.
        MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject())
        );
        FieldMapper mapper = (FieldMapper) ms.mappingLookup().getMapper(FIELD);
        assertTrue("supportsColumnarParse must be true when value is configured", mapper.supportsColumnarParse(ms.getIndexSettings()));
    }

    /**
     * A {@code constant_keyword} sub-field is driven from its parent's source column like any other multi-field. It accepts the
     * documents whose value matches its configured constant, and its {@code UnsupportedOperationException} bail-out for a
     * mismatch still fires from the sub-field position, forcing the batch back to the row path.
     */
    public void testAsSubFieldOfKeyword() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("fields");
            b.startObject("kind").field("type", "constant_keyword").field("value", "hello").endObject();
            b.endObject();
            b.endObject();
        }), columnarSettings(), batch("constant_keyword sub-field", 1L, doc("d1", 1L, "{\"f\":\"hello\"}"), doc("d2", 2L, "{}")));
    }

    public void testSubFieldValueMismatchBailsOut() throws IOException {
        MapperService ms = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("fields");
            b.startObject("kind").field("type", "constant_keyword").field("value", "hello").endObject();
            b.endObject();
            b.endObject();
        }));
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(ms, FIELD, "{\"f\":\"not-hello\"}"));
    }

    /**
     * Under {@code index.mapping.synthetic_source_keep: arrays} the row path keeps an array in {@code _ignored_source}, which the
     * columnar path does not write, so an array must fall back.
     */
    public void testTsdbKeepArraysArrayBailsOut() throws IOException {
        final MapperService ms = createMapperService(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
                .put(IndexSettings.SYNTHETIC_ID.getKey(), false)
                .put("index.mapping.synthetic_source_keep", "arrays")
                .build(),
            mapping(b -> {
                b.startObject("@timestamp").field("type", "date").endObject();
                b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
                b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").endObject();
            })
        );
        final UnsupportedOperationException ex = expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(ms, FIELD, "{\"@timestamp\":1705320000000,\"f\":[\"hello\",\"hello\"]}")
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("may keep array source outside a strict-columnar index mode"));
    }

    public void testGate_copyTo() throws IOException {
        // copy_to is not supported by the columnar path. Use standard mode because columnar mode already
        // rejects copy_to at mapping-parse time (before the mapper is ever consulted).
        MapperService ms = createMapperService(Settings.EMPTY, mapping(b -> {
            b.startObject(FIELD).field("type", "constant_keyword").field("value", "hello").field("copy_to", "other").endObject();
            b.startObject("other").field("type", "keyword").endObject();
        }));
        FieldMapper mapper = (FieldMapper) ms.mappingLookup().getMapper(FIELD);
        assertFalse("supportsColumnarParse must be false with copy_to", mapper.supportsColumnarParse(ms.getIndexSettings()));
    }
}
