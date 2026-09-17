/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;
import java.util.List;

/**
 * Parity tests for {@link KeywordFieldMapper#mapColumnBatch} against the row path.
 * The {@link AbstractColumnarMapperCompatibilityTestCase} harness drives leaf mappers automatically
 * via {@code EscfEncoder}; no subclass override is needed.
 */
public class KeywordFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    /**
     * Extends the base equality check with:
     * <ul>
     *   <li>An explicit doc-values-type assertion for {@code FIELD}: when {@code expected} produces a
     *       non-NONE doc-values type, this asserts that {@code actual} matches it as a named assertion
     *       rather than buried in the full set diff.</li>
     *   <li>Exclusion of {@code _id} from comparison in TSDB scenarios (detected by the presence of
     *       {@code _tsid} in {@code expected}): the columnar path splits {@code _id} into a DV column
     *       and a separate TokenStreamColumn while the row path combines them — a known divergence
     *       covered by {@link TsidExtractingIdFieldMapperColumnarCompatibilityTests}.</li>
     * </ul>
     */
    @Override
    protected void assertFieldSetsEqual(List<FieldDescriptor> expected, List<FieldDescriptor> actual, String message) {
        for (FieldDescriptor fd : expected) {
            if (fd.name().equals(FIELD) && fd.fieldType().docValuesType() != DocValuesType.NONE) {
                final DocValuesType dvType = fd.fieldType().docValuesType();
                assertTrue(
                    message + ": field [" + FIELD + "] expected docValuesType=" + dvType + " but columnar path did not produce it",
                    actual.stream().anyMatch(a -> a.name().equals(FIELD) && a.fieldType().docValuesType() == dvType)
                );
            }
        }
        final boolean isTsdb = expected.stream().anyMatch(fd -> fd.name().equals("_tsid"));
        if (isTsdb) {
            expected = expected.stream().filter(fd -> fd.name().equals("_id") == false).toList();
            actual = actual.stream().filter(fd -> fd.name().equals("_id") == false).toList();
        }
        super.assertFieldSetsEqual(expected, actual, message);
    }

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

    public void testIgnoreAboveIsNoOpMultiValueFalse() throws IOException {
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

    public void testColumnarDimensionSingleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).endObject()),
            columnarSettings(),
            batch("columnar keyword dimension single value", 1L, doc("d1", 1L, "{\"f\":\"host-0\"}"))
        );
    }

    public void testColumnarDimensionAbsentDocsMixed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).endObject()),
            columnarSettings(),
            batch(
                "columnar keyword dimension absent docs mixed",
                1L,
                doc("d1", 1L, "{\"f\":\"host-0\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"host-1\"}")
            )
        );
    }

    public void testColumnarDimensionExplicitNull() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).endObject()),
            columnarSettings(),
            batch(
                "columnar keyword dimension explicit null",
                1L,
                doc("d1", 1L, "{\"f\":null}"),
                doc("d2", 2L, "{\"f\":\"host-1\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testDimensionRoutingPathIsNotColumnar() throws IOException {
        // index.routing_path resolves to ForRoutingPath, whose extractDimensionsWhileMapping() is true, so
        // the row path writes the dimension to the routing fields and the columnar path must refuse.
        final MapperService mapperService = createMapperService(tsdbSettings(IndexMetadata.INDEX_ROUTING_PATH.getKey()), mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        final FieldMapper mapper = (FieldMapper) mapperService.mappingLookup().getMapper(FIELD);
        assertFalse(
            "a dimension whose routing is extracted while mapping must not be columnar",
            mapper.supportsColumnarParse(mapperService.getIndexSettings())
        );
    }

    public void testTsdbDimensionIsColumnar() throws IOException {
        // TSDB keyword dimensions resolve to DocValuesDiskFormat.SORTED_SET. The columnar batch path now
        // emits native SORTED_SET doc values, so these fields take the columnar path. Routing-path
        // dimensions are still excluded (testDimensionRoutingPathIsNotColumnar covers that).
        final MapperService mapperService = createMapperService(tsdbSettings(IndexMetadata.INDEX_DIMENSIONS.getKey()), mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        final FieldMapper mapper = (FieldMapper) mapperService.mappingLookup().getMapper(FIELD);
        assertEquals(
            "precondition: TSDB keyword dimension must use SORTED_SET doc values",
            KeywordFieldMapper.KeywordFieldType.DocValuesDiskFormat.SORTED_SET,
            ((KeywordFieldMapper.KeywordFieldType) mapper.fieldType()).diskFormat()
        );
        assertTrue(
            "TSDB keyword dimensions now take the columnar path via SORTED_SET emission",
            mapper.supportsColumnarParse(mapperService.getIndexSettings())
        );
    }

    /** TIME_SERIES settings listing {@code f} under the given dimension-source setting. */
    private static Settings tsdbSettings(String dimensionSettingKey) {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .putList(dimensionSettingKey, FIELD)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .build();
    }

    // =========================================================================
    // ColumNAR codec: the doc-values blob is a payload rather than the bare value
    //
    // Under the codec a field's doc values are a ColumnarBinaryDocValuesField payload, carrying their
    // slot count in the blob, whatever the field's cardinality. That is the one output the batch path
    // cannot take straight from the source column, so these pin it against the row path — the check
    // that catches a bare value written where a payload is read.
    // =========================================================================

    private static Settings columnarCodecSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    private void assumeColumnarCodecEnabled() {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
    }

    public void testColumnarCodecSingleValueMultiValueFalse() throws IOException {
        assumeColumnarCodecEnabled();
        // A lone value still travels with its count, so the blob is never the bare term.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarCodecSettings(),
            batch(
                "columnar codec single value multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"hello\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"\"}"),
                doc("d4", 4L, "{\"f\":null}")
            )
        );
    }

    public void testColumnarCodecNoIndexDocValuesOnlyMultiValueFalse() throws IOException {
        assumeColumnarCodecEnabled();
        // index:false — the payload column is the only output, so it cannot ride along with a terms column.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword").field("index", false);
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarCodecSettings(),
            batch("columnar codec no-index dv-only multi_value=false", 1L, doc("d1", 1L, "{\"f\":\"only_dv\"}"), doc("d2", 2L, "{}"))
        );
    }

    public void testColumnarCodecIgnoreAboveMultiValueFalse() throws IOException {
        assumeColumnarCodecEnabled();
        // A value that trips ignore_above writes no payload, and deoptimizes the shared terms column it would have ridden on.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword").field("ignore_above", 8);
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarCodecSettings(),
            batch(
                "columnar codec ignore_above multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":\"toolongvalue\"}"),
                doc("d2", 2L, "{\"f\":\"short\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testColumnarCodecNullValueSubstitutionMultiValueFalse() throws IOException {
        assumeColumnarCodecEnabled();
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword").field("null_value", "NULL");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarCodecSettings(),
            batch(
                "columnar codec null_value substitution multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":null}"),
                doc("d2", 2L, "{\"f\":\"a\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    public void testColumnarCodecScalarCoercionsMultiValueFalse() throws IOException {
        assumeColumnarCodecEnabled();
        // A non-STRING source column is built through the shared builder; the payload column is built beside it either way.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }),
            columnarCodecSettings(),
            batch(
                "columnar codec scalar coercions multi_value=false",
                1L,
                doc("d1", 1L, "{\"f\":42}"),
                doc("d2", 2L, "{\"f\":3.14}"),
                doc("d3", 3L, "{\"f\":true}")
            )
        );
    }

    public void testColumnarCodecArrayOrder() throws IOException {
        assumeColumnarCodecEnabled();
        // The multi-valued arm, where the payload replaces both the inline-null blob and its .counts companion.
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarCodecSettings(),
            batch(
                "columnar codec array order",
                1L,
                doc("d1", 1L, "{\"f\":[\"a\",null,\"b\"]}"),
                doc("d2", 2L, "{\"f\":\"solo\"}"),
                doc("d3", 3L, "{\"f\":[null]}"),
                doc("d4", 4L, "{\"f\":[]}"),
                doc("d5", 5L, "{}")
            )
        );
    }

    // ---- multi-fields -------------------------------------------------------------------------

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
     * Two sub-fields of different types under one keyword parent, both fed from the same source column. Values stay strings so the
     * numeric sub-field sees a STRING column rather than a UNION one.
     */
    public void testMultiValueViolationBailsOutOfColumnarPath() throws IOException {
        // Two values for a multi_value=false field: mapColumnBatch must throw so that
        // ShardBatchMapper falls back to the row path, which raises the correct
        // on_failure=FAIL document-level error instead.
        final var mapperService = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("doc_values").field("multi_value", false).endObject();
            b.endObject();
        }));
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":[\"a\",\"b\"]}"));
    }

    public void testNullabilityViolationBailsOutOfColumnarPath() throws IOException {
        // A null value for a nullability=false field: mapColumnBatch must throw so that
        // ShardBatchMapper falls back to the row path, which raises the correct
        // on_failure=FAIL document-level error instead.
        final var mapperService = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD).field("type", "keyword");
            b.startObject("doc_values").field("nullability", false).endObject();
            b.endObject();
        }));
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":\"value\"}", "{\"f\":null}"));
    }

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

    // =========================================================================
    // TSDB / SORTED_SET: keyword fields in TIME_SERIES index mode
    //
    // Every keyword field in a TSDB index resolves to DocValuesDiskFormat.SORTED_SET (Lucene-native
    // sorted and deduplicated doc values). The columnar batch path now emits SORTED_SET directly via
    // LuceneBinaryColumn.of with the frozen fieldType (which carries DocValuesType.SORTED_SET).
    //
    // The tested keyword field "f" is also the TSDB dimension. Routing is computed by the coordinating
    // node in production; in tests we supply it as TimeSeriesRoutingHashFieldMapper.encode(routingHash)
    // and use TsidExtractingIdFieldMapper.createSyntheticId to produce the correct document IDs.
    // The assertFieldSetsEqual override excludes _id from TSDB comparisons (detected via _tsid presence)
    // since the columnar path splits _id differently — covered by TsidExtractingIdFieldMapperColumnarCompatibilityTests.
    // =========================================================================

    private static final BytesRef ST_TSID = new BytesRef(new byte[] { 0x10, 0x20, 0x30, 0x40, 0x50 });
    private static final int ST_ROUTING_HASH = 17;
    private static final String ST_ROUTING = TimeSeriesRoutingHashFieldMapper.encode(ST_ROUTING_HASH);
    // epoch millis: 2025-01-01T00:00:00.000Z
    private static final long ST_TS_A = 1735689600000L;

    /**
     * TIME_SERIES settings with {@code f} as the keyword dimension (via {@code index.time_series.dimensions}).
     */
    private static Settings tsdbDimensionSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), FIELD)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .build();
    }

    private static String tsdbId(long tsMillis) {
        return TsidExtractingIdFieldMapper.createSyntheticId(ST_TSID, tsMillis, ST_ROUTING_HASH);
    }

    public void testTsdbSingleValue() throws IOException {
        // One value per document; the source column stays scalar (STRING), so zero-copy applies.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).endObject();
        }),
            tsdbDimensionSettings(),
            batch(
                "TSDB keyword single value",
                1L,
                doc(tsdbId(ST_TS_A), ST_ROUTING, ST_TSID, 1L, "{\"f\":\"host-a\",\"@timestamp\":" + ST_TS_A + "}"),
                doc(tsdbId(ST_TS_A + 1000L), ST_ROUTING, ST_TSID, 2L, "{\"f\":\"host-b\",\"@timestamp\":" + (ST_TS_A + 1000L) + "}"),
                doc(tsdbId(ST_TS_A + 2000L), ST_ROUTING, ST_TSID, 3L, "{\"@timestamp\":" + (ST_TS_A + 2000L) + "}")
            )
        );
    }

    public void testTsdbArrayValues() throws IOException {
        // Multi-valued document: the source column is promoted to ARRAY. Lucene's SORTED_SET writer
        // adds one value per element and deduplicates. Both paths must produce an identical multiset.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).endObject();
        }),
            tsdbDimensionSettings(),
            batch(
                "TSDB keyword array values",
                1L,
                doc(tsdbId(ST_TS_A), ST_ROUTING, ST_TSID, 1L, "{\"f\":[\"tag-a\",\"tag-b\"],\"@timestamp\":" + ST_TS_A + "}"),
                doc(tsdbId(ST_TS_A + 1000L), ST_ROUTING, ST_TSID, 2L, "{\"f\":\"solo\",\"@timestamp\":" + (ST_TS_A + 1000L) + "}"),
                doc(tsdbId(ST_TS_A + 2000L), ST_ROUTING, ST_TSID, 3L, "{\"@timestamp\":" + (ST_TS_A + 2000L) + "}")
            )
        );
    }

    public void testTsdbArrayWithDuplicates() throws IOException {
        // Duplicate values in an array: Lucene's SORTED_SET writer deduplicates per doc on both paths.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).endObject();
        }),
            tsdbDimensionSettings(),
            batch(
                "TSDB keyword array duplicates",
                1L,
                doc(tsdbId(ST_TS_A), ST_ROUTING, ST_TSID, 1L, "{\"f\":[\"dup\",\"dup\",\"other\"],\"@timestamp\":" + ST_TS_A + "}"),
                doc(tsdbId(ST_TS_A + 1000L), ST_ROUTING, ST_TSID, 2L, "{\"f\":[\"x\",\"x\"],\"@timestamp\":" + (ST_TS_A + 1000L) + "}")
            )
        );
    }

    public void testTsdbExplicitNull() throws IOException {
        // Explicit JSON null with no null_value configured: absent on both paths (no slot written).
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).endObject();
        }),
            tsdbDimensionSettings(),
            batch(
                "TSDB keyword explicit null",
                1L,
                doc(tsdbId(ST_TS_A), ST_ROUTING, ST_TSID, 1L, "{\"f\":null,\"@timestamp\":" + ST_TS_A + "}"),
                doc(tsdbId(ST_TS_A + 1000L), ST_ROUTING, ST_TSID, 2L, "{\"f\":\"val\",\"@timestamp\":" + (ST_TS_A + 1000L) + "}")
            )
        );
    }

    public void testTsdbNullValue() throws IOException {
        // null_value substitution: explicit null becomes the configured null_value on both paths.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).field("null_value", "NULL").endObject();
        }),
            tsdbDimensionSettings(),
            batch(
                "TSDB keyword null_value",
                1L,
                doc(tsdbId(ST_TS_A), ST_ROUTING, ST_TSID, 1L, "{\"f\":null,\"@timestamp\":" + ST_TS_A + "}"),
                doc(tsdbId(ST_TS_A + 1000L), ST_ROUTING, ST_TSID, 2L, "{\"f\":\"real\",\"@timestamp\":" + (ST_TS_A + 1000L) + "}")
            )
        );
    }

    public void testTsdbIgnoreAboveSingleValuePerDoc() throws IOException {
        // One ignore_above-exceeded value per document is stored as a synthetic-source fallback blob.
        // The batch path must emit the SeparateCount blob + .counts sidecar to match the row path.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).field("ignore_above", 4).endObject();
        }),
            tsdbDimensionSettings(),
            batch(
                "TSDB keyword ignore_above single per doc",
                1L,
                doc(tsdbId(ST_TS_A), ST_ROUTING, ST_TSID, 1L, "{\"f\":\"TOOLONG\",\"@timestamp\":" + ST_TS_A + "}"),
                doc(tsdbId(ST_TS_A + 1000L), ST_ROUTING, ST_TSID, 2L, "{\"f\":\"ok\",\"@timestamp\":" + (ST_TS_A + 1000L) + "}"),
                doc(tsdbId(ST_TS_A + 2000L), ST_ROUTING, ST_TSID, 3L, "{\"@timestamp\":" + (ST_TS_A + 2000L) + "}")
            )
        );
    }

    public void testTsdbIgnoreAboveTwoValuesPerDocBailsOut() throws IOException {
        // More than one ignore_above-exceeded value in a single document: the batch path throws
        // UnsupportedOperationException so ShardBatchMapper falls back to the row path.
        final MapperService mapperService = createMapperService(tsdbDimensionSettings(), mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).field("ignore_above", 3).endObject();
        }));
        expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":[\"TOOLONG1\",\"TOOLONG2\"],\"@timestamp\":" + ST_TS_A + "}")
        );
    }

    public void testTsdbIgnoreAboveBackfillFix() throws IOException {
        // ["short", "TOOLONG", "short2"] — the first element is accepted, the second triggers
        // deoptimization via ignore_above, and the third is accepted again. This exercises the
        // backfillUtf8Before fix: without passing elementsThisDoc=1, "short" would be silently lost.
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).field("ignore_above", 6).endObject();
        }),
            tsdbDimensionSettings(),
            batch(
                "TSDB keyword ignore_above backfill fix",
                1L,
                doc(tsdbId(ST_TS_A), ST_ROUTING, ST_TSID, 1L, "{\"f\":[\"short\",\"TOOLONG\",\"short2\"],\"@timestamp\":" + ST_TS_A + "}"),
                doc(tsdbId(ST_TS_A + 1000L), ST_ROUTING, ST_TSID, 2L, "{\"f\":\"other\",\"@timestamp\":" + (ST_TS_A + 1000L) + "}")
            )
        );
    }

    public void testTsdbSortedSetDocValuesWithLargeTermThrows() throws IOException {
        // A non-dimension keyword field with index: false in a TSDB index uses SORTED_SET doc values.
        // SORTED_SET carries the 32766-byte ceiling via writesIndexableField (docValuesType != NONE)
        // even when emitTerms is false (no Lucene terms written). Without this fix, the over-long value
        // would be silently accepted on the batch path but rejected on the row path — a row/batch split.
        final MapperService mapperService = createMapperService(tsdbDimensionSettings(), mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject(FIELD).field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("f_plain").field("type", "keyword").field("index", false).endObject();
        }));
        final String longValue = "a".repeat(IndexWriter.MAX_TERM_LENGTH + 1);
        expectThrows(
            IllegalArgumentException.class,
            () -> mapColumnarLeaf(mapperService, "f_plain", "{\"f_plain\":\"" + longValue + "\"}")
        );
    }
}
