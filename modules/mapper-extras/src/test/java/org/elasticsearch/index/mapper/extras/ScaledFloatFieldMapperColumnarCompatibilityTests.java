/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractColumnarMapperCompatibilityTestCase;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

/**
 * Parity tests for {@link ScaledFloatFieldMapper#mapColumnBatch} against the row path, and the shapes that must stay on the row path.
 */
public class ScaledFloatFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    private static final BytesRef TSID = new BytesRef(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
    private static final int ROUTING_HASH = 42;
    private static final String ROUTING = TimeSeriesRoutingHashFieldMapper.encode(ROUTING_HASH);
    private static final long TS_A = 1705320000000L;
    private static final long TS_B = 1717200000000L;

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    private static Settings columnarSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
    }

    private static Settings singleValueColumnarSettings() {
        return Settings.builder().put(columnarSettings()).put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false).build();
    }

    // The keyword dimension is in the mapping only, so it stays out of the ESCF schema and the tsid comes from the request.
    private static Settings tsdbSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(IndexSettings.SYNTHETIC_ID.getKey(), false)
            .build();
    }

    private static XContentBuilder scaledFloat(double scalingFactor) throws IOException {
        return scaledFloat(scalingFactor, b -> {});
    }

    private static XContentBuilder scaledFloat(double scalingFactor, CheckedConsumer<XContentBuilder, IOException> extras)
        throws IOException {
        return mapping(b -> {
            b.startObject(FIELD).field("type", "scaled_float").field("scaling_factor", scalingFactor);
            extras.accept(b);
            b.endObject();
        });
    }

    private static XContentBuilder tsdbScaledFloat(CheckedConsumer<XContentBuilder, IOException> extras) throws IOException {
        return mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "scaled_float").field("scaling_factor", 100);
            extras.accept(b);
            b.endObject();
        });
    }

    private static Batch tsdbBatch(String name, String valueA, String valueB) {
        return batch(
            name,
            1L,
            doc(
                TsidExtractingIdFieldMapper.createId(ROUTING_HASH, TSID, TS_A),
                ROUTING,
                TSID,
                1L,
                "{\"@timestamp\":" + TS_A + ",\"f\":" + valueA + "}"
            ),
            doc(
                TsidExtractingIdFieldMapper.createId(ROUTING_HASH, TSID, TS_B),
                ROUTING,
                TSID,
                2L,
                "{\"@timestamp\":" + TS_B + ",\"f\":" + valueB + "}"
            ),
            doc(
                TsidExtractingIdFieldMapper.createId(ROUTING_HASH, TSID, TS_B + 1),
                ROUTING,
                TSID,
                3L,
                "{\"@timestamp\":" + (TS_B + 1) + "}"
            )
        );
    }

    public void testDoubleColumn() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(100),
            randomFrom(columnarSettings(), singleValueColumnarSettings()),
            batch("double", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}")),
            batch("double dense", 1L, doc("d1", 1L, "{\"f\":3.14}"), doc("d2", 2L, "{\"f\":1e3}"), doc("d3", 3L, "{\"f\":-0.0}"))
        );
    }

    public void testLongColumn() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(100),
            randomFrom(columnarSettings(), singleValueColumnarSettings()),
            batch("long", 1L, doc("d1", 1L, "{\"f\":42}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-7}")),
            batch("long saturating", 1L, doc("d1", 1L, "{\"f\":9223372036854775807}"), doc("d2", 2L, "{\"f\":-9223372036854775808}"))
        );
    }

    // One field with JSON integers and JSON doubles is a UNION column.
    public void testMixedLongAndDoubleColumn() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(1000),
            randomFrom(columnarSettings(), singleValueColumnarSettings()),
            batch(
                "long then double",
                1L,
                doc("d1", 1L, "{\"f\":0}"),
                doc("d2", 2L, "{\"f\":0.0394}"),
                doc("d3", 3L, "{}"),
                doc("d4", 4L, "{\"f\":1}")
            ),
            batch("double then long", 1L, doc("d1", 1L, "{\"f\":0.9606}"), doc("d2", 2L, "{\"f\":4}"), doc("d3", 3L, "{\"f\":-1.9212}")),
            batch(
                "large values",
                1L,
                doc("d1", 1L, "{\"f\":9223372036854775807}"),
                doc("d2", 2L, "{\"f\":1e300}"),
                doc("d3", 3L, "{\"f\":-7}")
            )
        );
    }

    public void testMixedNumberAndStringColumn() throws IOException {
        assertColumnarMatchesXContent(
            randomBoolean() ? scaledFloat(100) : scaledFloat(100, b -> b.field("null_value", 2.5)),
            randomFrom(columnarSettings(), singleValueColumnarSettings()),
            batch("double then string", 1L, doc("d1", 1L, "{\"f\":0.2}"), doc("d2", 2L, "{\"f\":\"0.2\"}"), doc("d3", 3L, "{}")),
            batch(
                "string then numbers",
                1L,
                doc("d1", 1L, "{\"f\":\"-2.25\"}"),
                doc("d2", 2L, "{\"f\":7}"),
                doc("d3", 3L, "{\"f\":79.99}"),
                doc("d4", 4L, "{\"f\":\"7\"}")
            ),
            batch("empty string", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{\"f\":\"\"}"), doc("d3", 3L, "{\"f\":3}"))
        );
    }

    public void testStringColumn() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(100),
            randomFrom(columnarSettings(), singleValueColumnarSettings()),
            batch(
                "string",
                1L,
                doc("d1", 1L, "{\"f\":\"1.5\"}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":\"-2.25\"}"),
                doc("d4", 4L, "{\"f\":\"7\"}"),
                doc("d5", 5L, "{\"f\":\"\"}")
            )
        );
    }

    public void testRounding() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(100),
            columnarSettings(),
            // 79.99 * 100 is 7998.999999999999 in double arithmetic.
            batch("inexact products", 1L, doc("d1", 1L, "{\"f\":79.99}"), doc("d2", 2L, "{\"f\":0.29}"), doc("d3", 3L, "{\"f\":-79.99}")),
            batch(
                "half values",
                1L,
                doc("d1", 1L, "{\"f\":0.125}"),
                doc("d2", 2L, "{\"f\":-0.125}"),
                doc("d3", 3L, "{\"f\":0.005}"),
                doc("d4", 4L, "{\"f\":-0.005}")
            ),
            batch(
                "saturating",
                1L,
                doc("d1", 1L, "{\"f\":1e300}"),
                doc("d2", 2L, "{\"f\":-1e300}"),
                doc("d3", 3L, "{\"f\":1.7976931348623157E308}"),
                doc("d4", 4L, "{\"f\":4.9E-324}")
            )
        );
    }

    public void testRandomValuesAndFactors() throws IOException {
        final double scalingFactor = randomFrom(0.001, 0.5, 1.0, 3.0, 10.0, 100.0, 1000.0, randomDoubleBetween(0.001, 100_000, true));
        final Doc[] doubles = new Doc[between(1, 20)];
        final Doc[] strings = new Doc[doubles.length];
        for (int i = 0; i < doubles.length; i++) {
            if (rarely()) {
                doubles[i] = doc("d" + i, i, "{}");
                strings[i] = doubles[i];
            } else {
                final double value = randomBoolean() ? randomDoubleBetween(-1_000_000, 1_000_000, true) : randomDouble() * 100;
                doubles[i] = doc("d" + i, i, "{\"f\":" + value + "}");
                strings[i] = doc("d" + i, i, "{\"f\":\"" + value + "\"}");
            }
        }
        assertColumnarMatchesXContent(
            scaledFloat(scalingFactor),
            columnarSettings(),
            batch("random doubles, factor " + scalingFactor, 1L, doubles),
            batch("random strings, factor " + scalingFactor, 1L, strings)
        );
    }

    public void testNotIndexed() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(10, b -> b.field("index", false)),
            columnarSettings(),
            batch("not indexed", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":79.99}")),
            batch("not indexed arrays", 1L, doc("d1", 1L, "{\"f\":[2.5,0.5,2.5]}"), doc("d2", 2L, "{}"))
        );
    }

    public void testKeywordSubField() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(100, b -> b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject()),
            singleValueColumnarSettings(),
            batch(
                "keyword sub-field",
                1L,
                doc("d1", 1L, "{\"f\":-1.90}"),
                doc("d2", 2L, "{\"f\":3.14}"),
                doc("d3", 3L, "{}"),
                doc("d4", 4L, "{\"f\":12.40}")
            )
        );
    }

    public void testNullValueReplacesEmptyString() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(100, b -> b.field("null_value", 2.5)),
            columnarSettings(),
            batch("scalar", 1L, doc("d1", 1L, "{\"f\":\"1.5\"}"), doc("d2", 2L, "{\"f\":\"\"}"), doc("d3", 3L, "{}")),
            batch(
                "array",
                1L,
                doc("d1", 1L, "{\"f\":[\"\",\"5\",\"\"]}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[\"9.75\",\"\",\"1\"]}"),
                doc("d4", 4L, "{\"f\":[\"\"]}")
            )
        );
    }

    // Each array holds elements of one width class, because the encoder packs mixed classes as a UNION.
    public void testArraysRecordOffsets() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(10),
            columnarSettings(),
            randomFrom(SourceEncoder.available()),
            batch(
                "double arrays",
                1L,
                doc("d1", 1L, "{\"f\":[2.5,1.5,2.5]}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[0.5,0.5,0.5,0.5]}"),
                doc("d4", 4L, "{\"f\":[-1.25,7.5]}"),
                doc("d5", 5L, "{\"f\":[]}")
            ),
            batch("long arrays", 1L, doc("d1", 1L, "{\"f\":[2,1,2]}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":[-1,7]}")),
            batch(
                "string arrays",
                1L,
                doc("d1", 1L, "{\"f\":[\"3.14\",\"-2.25\",\"3.14\"]}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[\"0.5\"]}")
            ),
            // Values that differ before scaling and are equal after it share one ordinal.
            batch("equal after scaling", 1L, doc("d1", 1L, "{\"f\":[\"1.01\",\"0.5\",\"1.04\"]}"), doc("d2", 2L, "{\"f\":[\"2\",\"1\"]}")),
            batch("single elements", 1L, doc("d1", 1L, "{\"f\":[1.5]}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":[2.5]}"))
        );
    }

    public void testSingleElementArraysWithSingleValueField() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(100),
            singleValueColumnarSettings(),
            batch("single elements", 1L, doc("d1", 1L, "{\"f\":[1.5]}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":[2.5]}"))
        );
    }

    public void testRequiredFieldWithoutValueIsIgnored() throws IOException {
        assertColumnarMatchesXContent(
            scaledFloat(100, b -> b.field("nullability", false)),
            Settings.builder().put(columnarSettings()).put(FieldMapper.DOC_VALUES_ON_FAILURE_SETTING.getKey(), "ignore").build(),
            batch("missing", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}")),
            batch("empty string", 1L, doc("d1", 1L, "{\"f\":\"1.5\"}"), doc("d2", 2L, "{\"f\":\"\"}")),
            batch("empty array", 1L, doc("d1", 1L, "{\"f\":[1.5,2.5]}"), doc("d2", 2L, "{\"f\":[]}"))
        );
    }

    public void testTsdbGauge() throws IOException {
        assertColumnarMatchesXContent(
            tsdbScaledFloat(b -> b.field("time_series_metric", "gauge")),
            tsdbSettings(),
            tsdbBatch("gauge doubles", "79.99", "0.5"),
            tsdbBatch("gauge longs", "3", "-12")
        );
    }

    public void testTsdbStored() throws IOException {
        assertColumnarMatchesXContent(
            tsdbScaledFloat(b -> b.field("store", true).field("index", randomBoolean())),
            tsdbSettings(),
            tsdbBatch("stored doubles", "79.99", "0.5"),
            tsdbBatch("stored strings", "\"1.5\"", "\"-2.25\"")
        );
    }

    public void testCoerceFalseRejectsString() throws IOException {
        final MapperService mapperService = createMapperService(columnarSettings(), scaledFloat(100, b -> b.field("coerce", false)));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":\"" + randomFrom("1.5", "") + "\"}")
        );
        assertThat(e.getMessage(), containsString("Double value passed as String"));
        final IllegalArgumentException mixed = expectThrows(
            IllegalArgumentException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":1.5}", "{\"f\":\"2.5\"}")
        );
        assertThat(mixed.getMessage(), containsString("Double value passed as String"));
        assertEquals(BatchRefusal.MAP_COLUMN_BATCH, assertBatchPathRefuses(mapperService, SourceEncoder.JACKSON, "{\"f\":\"1.5\"}"));
    }

    public void testMalformedValueFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(
            columnarSettings(),
            scaledFloat(100, b -> b.field("ignore_malformed", randomBoolean()))
        );
        final IllegalArgumentException nonFinite = expectThrows(
            IllegalArgumentException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":\"1.5\"}", "{\"f\":\"" + randomFrom("NaN", "Infinity", "-Infinity") + "\"}")
        );
        assertThat(nonFinite.getMessage(), containsString("[scaled_float] only supports finite values"));
        expectThrows(
            NumberFormatException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":" + randomFrom("\"1.5\"", "1.5") + "}", "{\"f\":\"abc\"}")
        );
        assertEquals(
            BatchRefusal.MAP_COLUMN_BATCH,
            assertBatchPathRefuses(mapperService, SourceEncoder.JACKSON, "{\"f\":\"1.5\"}", "{\"f\":\"NaN\"}")
        );
    }

    public void testExplicitNullFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(
            columnarSettings(),
            randomBoolean() ? scaledFloat(100) : scaledFloat(100, b -> b.field("null_value", 2.5))
        );
        final UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, randomFrom(SourceEncoder.available()), "{\"f\":1.5}", "{\"f\":null}")
        );
        assertThat(e.getMessage(), containsString("ESCF value type [NULL] is not yet supported for field [f]"));
    }

    public void testEmptyStringArrayElementWithOffsetsFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(columnarSettings(), scaledFloat(100));
        final UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":[\"\",\"1.5\"]}")
        );
        assertThat(e.getMessage(), containsString("an empty string in an array of field [f] records a null offsets slot"));
    }

    public void testArrayWithMixedElementsFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(columnarSettings(), scaledFloat(100));
        final String array = randomFrom("[1.5,null]", "[1,\"2.5\"]", "[1.5,true]", "[[1.5,2.5],[3.5]]");
        final UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":" + array + "}", "{}")
        );
        assertThat(e.getMessage(), containsString("is not yet supported for field [f]"));
    }

    public void testUnionWithOtherValueTypesFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(columnarSettings(), scaledFloat(100));
        final String other = randomFrom("[1.5,2.5]", "true");
        final UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":1.5}", "{\"f\":" + other + "}")
        );
        assertThat(e.getMessage(), containsString("is not yet supported for field [f]"));
    }

    public void testMultiValueViolationFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(singleValueColumnarSettings(), scaledFloat(100));
        final UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":[1.5,2.5]}")
        );
        assertThat(e.getMessage(), containsString("multi_value=false field [f] has more than one value per document"));
    }

    public void testNullabilityViolationFallsBack() throws IOException {
        final MapperService mapperService = createMapperService(columnarSettings(), scaledFloat(100, b -> b.field("nullability", false)));
        final UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":1.5}", "{}")
        );
        assertThat(e.getMessage(), containsString("nullability=false field [f] has a null or absent value"));
    }

    public void testWithoutDocValuesIsNotColumnar() throws IOException {
        final MapperService mapperService = createMapperService(tsdbSettings(), tsdbScaledFloat(b -> b.field("doc_values", false)));
        assertFalse(mapper(mapperService).doSupportsColumnarParse(mapperService.getIndexSettings(), Set.of()));
    }

    public void testBooleanColumnIsRefusedAtResolve() throws IOException {
        final MapperService mapperService = createMapperService(columnarSettings(), scaledFloat(100));
        assertEquals(
            BatchRefusal.RESOLVE_MAPPERS,
            assertBatchPathRefuses(mapperService, randomFrom(SourceEncoder.available()), "{\"f\":true}", "{\"f\":false}")
        );
        assertEquals(
            BatchRefusal.MAP_COLUMN_BATCH,
            assertBatchPathRefuses(mapperService, randomFrom(SourceEncoder.available()), "{\"f\":1.5}", "{\"f\":true}")
        );
    }

    public void testRawBinaryColumnIsRefusedAtResolve() throws IOException {
        final MapperService mapperService = createMapperService(columnarSettings(), scaledFloat(100));
        final BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (EscfBatchBuilder builder = new EscfBatchBuilder(recycler)) {
            final EscfRowBuffer row = builder.beginRow();
            row.binaryField(FIELD, new BytesRef(new byte[] { 1, 2, 3 }));
            row.finishRow();
            builder.commit(0);
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertEquals(EscfColumnKind.BINARY, batch.column(0).kind());
                assertNull(ShardBatchMapper.resolveMappers(batch, mapperService.mappingLookup(), mapperService.getIndexSettings()));
            }
        }
    }

    public void testNonFiniteNullValueIsNotColumnar() throws IOException {
        final MapperService nonFinite = createMapperService(
            columnarSettings(),
            scaledFloat(100, b -> b.field("null_value", randomFrom("NaN", "Infinity", "-Infinity")))
        );
        assertFalse(mapper(nonFinite).supportsColumnarParse(nonFinite.getIndexSettings(), Set.of()));

        final MapperService finite = createMapperService(columnarSettings(), scaledFloat(100, b -> b.field("null_value", 2.5)));
        assertTrue(mapper(finite).supportsColumnarParse(finite.getIndexSettings(), Set.of()));
    }

    private static ScaledFloatFieldMapper mapper(MapperService mapperService) {
        return (ScaledFloatFieldMapper) mapperService.mappingLookup().getMapper(FIELD);
    }
}
