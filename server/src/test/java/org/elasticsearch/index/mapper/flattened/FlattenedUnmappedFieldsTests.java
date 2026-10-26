/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Write-path coverage for the implicit flattened {@code _unmapped} sink that absorbs unmapped fields on strict columnar indices.
 */
public class FlattenedUnmappedFieldsTests extends MapperServiceTestCase {

    private static final String KEYED = FlattenedFieldMapper.UNMAPPED_SINK_NAME + FlattenedFieldMapper.KEYED_FIELD_SUFFIX;

    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.FLATTENED_UNMAPPED_FIELDS_ENABLED.getKey(), true)
            .build();
    }

    private MapperService columnarService(CheckedConsumer<XContentBuilder, IOException> mapping) throws IOException {
        return createMapperService(columnarSettings(), mapping(mapping));
    }

    private MapperService columnarServiceTop(CheckedConsumer<XContentBuilder, IOException> mapping) throws IOException {
        return createMapperService(columnarSettings(), topMapping(mapping));
    }

    private static boolean isUnmappedSink(MapperService mapperService) {
        return mapperService.mappingLookup().getMapper(FlattenedFieldMapper.UNMAPPED_SINK_NAME) instanceof UnmappedSinkFieldMapper;
    }

    /**
     * In columnar mode the sink is doc-values-only: absorbed values land in the keyed {@link
     * org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull} column, whose blob stores each
     * {@code key\0value} slot inline. This checks that byte sequence is present, which is the write-path signal that a value was absorbed.
     */
    private static boolean hasKeyedSlot(ParsedDocument doc, String keyedSlot) {
        byte[] term = keyedSlot.getBytes(StandardCharsets.UTF_8);
        for (IndexableField field : doc.rootDoc().getFields(KEYED)) {
            BytesRef blob = field.binaryValue();
            if (blob != null && ESVectorUtil.contains(blob.bytes, blob.offset, blob.length, term, 0, term.length)) {
                return true;
            }
        }
        return false;
    }

    @Before
    public void testFeatureFlag() {
        assumeTrue("flattened_unmapped_fields is enabled", FlattenedFieldMapper.UNMAPPED_FIELDS_FEATURE_FLAG.isEnabled());
    }

    public void testSettingRejectedOutsideColumnarMode() {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.getName())
            .put(IndexSettings.FLATTENED_UNMAPPED_FIELDS_ENABLED.getKey(), true)
            .build();
        var scoped = new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        var e = expectThrows(IllegalArgumentException.class, () -> scoped.validate(settings, true));
        assertThat(e.getMessage(), containsString("only permitted in strict columnar index modes"));
    }

    public void testSettingAcceptedInColumnarMode() {
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR).getName())
            .put(IndexSettings.FLATTENED_UNMAPPED_FIELDS_ENABLED.getKey(), true)
            .build();
        var scoped = new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        scoped.validate(settings, true); // no throw
    }

    public void testSinkPresentButNotSerialized() throws IOException {
        MapperService mapperService = columnarService(b -> {});
        assertTrue(isUnmappedSink(mapperService));
        assertTrue(mapperService.isMetadataField(FlattenedFieldMapper.UNMAPPED_SINK_NAME));
        assertThat(mapperService.documentMapper().mappingSource().toString(), not(containsString(FlattenedFieldMapper.UNMAPPED_SINK_NAME)));
    }

    /**
     * The sink resolves like {@code _ignored_source}: {@code _unmapped} is a metadata field that cannot be searched, aggregated or fetched,
     * and no absorbed key resolves under it, so {@code _search} and field-level security never reach absorbed values through the sink.
     */
    public void testSinkResolvesAsInternalMetadataField() throws IOException {
        MapperService mapperService = columnarService(b -> {});
        String key = randomAlphaOfLengthBetween(1, 10);
        String value = randomAlphanumericOfLength(6);
        MappedFieldType sinkType = mapperService.fieldType(FlattenedFieldMapper.UNMAPPED_SINK_NAME);
        assertFalse(sinkType.isSearchable());
        assertFalse(sinkType.isAggregatable());
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        var termError = expectThrows(IllegalArgumentException.class, () -> sinkType.termQuery(value, context));
        assertThat(termError.getMessage(), startsWith("Cannot query field [_unmapped]: "));
        var fetchError = expectThrows(IllegalArgumentException.class, () -> sinkType.valueFetcher(context, null));
        assertThat(fetchError.getMessage(), startsWith("Cannot fetch values for field [_unmapped]: "));

        String absorbedName = FlattenedFieldMapper.UNMAPPED_SINK_NAME + "." + key;
        assertNull(mapperService.fieldType(absorbedName));
        assertFalse(context.isFieldMapped(absorbedName));
        assertFalse(context.isMappedField(absorbedName));
    }

    public void testUserFieldWithSameNameAsSinkRejected() {
        String type = randomFrom("keyword", "flattened", "long");
        var e = expectThrows(
            MapperParsingException.class,
            () -> columnarService(b -> b.startObject(FlattenedFieldMapper.UNMAPPED_SINK_NAME).field("type", type).endObject())
        );
        assertThat(e.getMessage(), containsString("Field [_unmapped] is defined more than once"));
    }

    /**
     * A user {@code nested} object named {@code _unmapped} keeps the name and collides with the sink. A plain {@code object} is flattened
     * by {@code subobjects: false} into dotted leaves, which {@link #testUserFieldUnderSinkPrefixRejected} covers.
     */
    public void testUserObjectWithSameNameAsSinkRejected() throws IOException {
        var e = expectThrows(
            MapperParsingException.class,
            () -> columnarService(b -> b.startObject(FlattenedFieldMapper.UNMAPPED_SINK_NAME).field("type", "nested").endObject())
        );
        assertThat(e.getMessage(), containsString("Field [_unmapped] is defined both as an object and a field"));
    }

    /**
     * The sink reserves {@code _unmapped.} as a prefix in mappings: any user field or object under it is rejected, whether it is a plain
     * leaf, a deeper dotted leaf, a nested object, or one of the sink's own internal {@code _keyed} fields.
     */
    public void testUserFieldUnderSinkPrefixRejected() {
        String sink = FlattenedFieldMapper.UNMAPPED_SINK_NAME;
        String name = randomFrom(
            sink + "." + randomAlphaOfLength(5),
            sink + "." + randomAlphaOfLength(5) + "." + randomAlphaOfLength(5),
            KEYED,
            KEYED + ".counts",
            KEYED + "._ignored"
        );
        String type = randomFrom("keyword", "long", "flattened", "nested");
        var e = expectThrows(MapperParsingException.class, () -> columnarService(b -> b.startObject(name).field("type", type).endObject()));
        assertThat(e.getMessage(), containsString("Field [" + name + "] is reserved by the metadata field [_unmapped]"));
    }

    /**
     * The sink answers metadata columnar support through its wrapped flattened mapper, so the batch path falls back for a shape that mapper
     * cannot batch, such as the inverted index being enabled through {@code index.mapping.index_disabled_by_default: false}. The sink never
     * maps a column of its own, so its field-level columnar support is always {@code false}.
     */
    public void testSinkColumnarSupportFollowsDelegate() throws IOException {
        boolean indexDisabled = randomBoolean();
        Settings settings = Settings.builder()
            .put(columnarSettings())
            .put(IndexSettings.INDEX_DISABLED_BY_DEFAULT.getKey(), indexDisabled)
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        var sink = (UnmappedSinkFieldMapper) mapperService.mappingLookup().getMapper(FlattenedFieldMapper.UNMAPPED_SINK_NAME);
        assertEquals(indexDisabled, sink.supportsColumnarMetadataParse(mapperService.getIndexSettings()));
        assertFalse(sink.supportsColumnarParse(mapperService.getIndexSettings()));
    }

    /**
     * The sink reserves {@code _unmapped} and the prefix {@code _unmapped.} as root document keys. Every value shape is rejected the same
     * way: scalar, null, object (including empty), array (including empty), and the dotted spellings that {@code subobjects: false}
     * would otherwise treat as leaves of their own.
     */
    public void testDocumentKeyUnderSinkRejected() throws IOException {
        DocumentMapper mapper = columnarService(b -> {}).documentMapper();
        String sink = FlattenedFieldMapper.UNMAPPED_SINK_NAME;
        String dotted = sink + "." + randomAlphaOfLength(5);
        String deeper = dotted + "." + randomAlphaOfLength(5);
        String key = randomAlphaOfLength(5);
        String value = randomAlphanumericOfLength(6);
        Map<String, CheckedConsumer<XContentBuilder, IOException>> shapes = new LinkedHashMap<>();
        shapes.put("_unmapped: value", b -> b.field(sink, value));
        shapes.put("_unmapped: null", b -> b.nullField(sink));
        shapes.put("_unmapped: {}", b -> b.startObject(sink).endObject());
        shapes.put("_unmapped: {a: value}", b -> b.startObject(sink).field(key, value).endObject());
        shapes.put("_unmapped: []", b -> b.startArray(sink).endArray());
        shapes.put("_unmapped: [value]", b -> b.array(sink, value));
        shapes.put("_unmapped.a: value", b -> b.field(dotted, value));
        shapes.put("_unmapped.a: null", b -> b.nullField(dotted));
        shapes.put("_unmapped.a: {}", b -> b.startObject(dotted).endObject());
        shapes.put("_unmapped.a: []", b -> b.startArray(dotted).endArray());
        shapes.put("_unmapped.a.b: value", b -> b.field(deeper, value));
        for (var shape : shapes.entrySet()) {
            var e = expectThrows(DocumentParsingException.class, shape.getKey(), () -> mapper.parse(source(shape.getValue())));
            assertThat(shape.getKey(), e.getMessage(), containsString("failed to parse field [_unmapped]"));
            assertThat(
                shape.getKey(),
                e.getCause().getMessage(),
                containsString("Field [_unmapped] is a metadata field and cannot be added inside a document")
            );
        }
    }

    /**
     * The reservation is on the full path, so {@code _unmapped} inside another object (flattened to {@code x._unmapped} under
     * {@code subobjects: false}) is an ordinary unmapped name and is absorbed.
     */
    public void testSinkNameInsideObjectIsNotReserved() throws IOException {
        DocumentMapper mapper = columnarService(b -> {}).documentMapper();
        String outer = randomAlphaOfLength(5);
        String value = randomAlphanumericOfLength(6);
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject(outer).field(FlattenedFieldMapper.UNMAPPED_SINK_NAME, value).endObject())
        );
        assertNull(doc.dynamicMappingsUpdate());
        assertTrue(hasKeyedSlot(doc, outer + "." + FlattenedFieldMapper.UNMAPPED_SINK_NAME + "\0" + value));
    }

    /** Arrays keep document order, duplicates and nulls; the sink stores keywords, so numbers and booleans come back as strings. */
    public void testSyntheticSourceKeepsAbsorbedArraysAndStringifiesValues() throws IOException {
        DocumentMapper mapper = columnarService(b -> {}).documentMapper();
        long n = randomLongBetween(0, 1000);

        // Keys are written in sorted order: the sink's blob keeps slots in document order across keys, so the harness's re-index of the
        // (key-sorted) synthetic source only reproduces the same blob when the input order is already sorted.
        String source = syntheticSource(mapper, b -> {
            b.array("flags", true, false, true);
            b.nullField("nothing");
            b.startArray("nums").value(n).value(n).nullValue().value(n + 1).endArray();
        });

        assertEquals(
            "{\"flags\":[\"true\",\"false\",\"true\"],\"nothing\":null,\"nums\":[\"" + n + "\",\"" + n + "\",null,\"" + (n + 1) + "\"]}",
            source
        );
    }

    /**
     * Fields absorbed inside a nested object are stored on that object's own Lucene document, so they render inside the nested element they
     * came from, relative to it, including in an element that holds nothing mapped. They are filtered by their full path, like the mapped
     * fields beside them.
     */
    public void testSyntheticSourceRendersAbsorbedFieldsInsideNestedObjects() throws IOException {
        DocumentMapper mapper = columnarService(
            b -> b.startObject("n")
                .field("type", "nested")
                .startObject("properties")
                .startObject("k")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
        ).documentMapper();
        String k = randomAlphanumericOfLength(6);
        String u1 = randomAlphanumericOfLength(6);
        String u2 = randomAlphanumericOfLength(6);
        String top = randomAlphanumericOfLength(6);
        CheckedConsumer<XContentBuilder, IOException> doc = b -> {
            b.startArray("n");
            b.startObject().field("k", k).field("u", u1).endObject();
            b.startObject().field("u", u2).endObject();
            b.endArray();
            b.field("top", top);
        };

        assertEquals(
            "{\"n\":[{\"k\":\"" + k + "\",\"u\":\"" + u1 + "\"},{\"u\":\"" + u2 + "\"}],\"top\":\"" + top + "\"}",
            syntheticSource(mapper, doc)
        );
        assertEquals(
            "{\"n\":[{\"u\":\"" + u1 + "\"},{\"u\":\"" + u2 + "\"}]}",
            syntheticSource(mapper, new SourceFilter(new String[] { "n.u" }, null), doc)
        );
        assertEquals("{\"top\":\"" + top + "\"}", syntheticSource(mapper, new SourceFilter(new String[] { "top" }, null), doc));
    }

    /**
     * Synthetic source renders absorbed fields as if they had never been absorbed: at the root, under their full dotted paths (the same
     * flat shape subobjects: false gives mapped fields), sorted among the mapped fields, with no {@code _unmapped} object. Source filtering
     * applies to each absorbed key's own path, not to the sink's name. ES|QL's source fallback for an unmapped field relies on this: it
     * loads source with only that field's name included.
     */
    public void testSyntheticSourceRendersAndFiltersAbsorbedFieldsPerKey() throws IOException {
        DocumentMapper mapper = columnarService(b -> b.startObject("m").field("type", "keyword").endObject()).documentMapper();
        String a = randomAlphanumericOfLength(6);
        String leaf = randomAlphanumericOfLength(6);
        String m = randomAlphanumericOfLength(6);
        CheckedConsumer<XContentBuilder, IOException> doc = b -> {
            b.field("a", a);
            b.field("m", m);
            b.startObject("outer").startObject("inner").field("leaf", leaf).endObject().endObject();
        };

        assertEquals("{\"a\":\"" + a + "\",\"m\":\"" + m + "\",\"outer.inner.leaf\":\"" + leaf + "\"}", syntheticSource(mapper, doc));
        assertEquals("{\"a\":\"" + a + "\"}", syntheticSource(mapper, new SourceFilter(new String[] { "a" }, null), doc));
        assertEquals(
            "{\"outer.inner.leaf\":\"" + leaf + "\"}",
            syntheticSource(mapper, new SourceFilter(new String[] { "outer" }, null), doc)
        );
        assertEquals(
            "{\"m\":\"" + m + "\",\"outer.inner.leaf\":\"" + leaf + "\"}",
            syntheticSource(mapper, new SourceFilter(null, new String[] { "a" }), doc)
        );
        assertEquals("{\"m\":\"" + m + "\"}", syntheticSource(mapper, new SourceFilter(new String[] { "m" }, null), doc));
        // The sink's own name is not a path in the rendered source, so filtering on it selects nothing.
        assertEquals("{}", syntheticSource(mapper, new SourceFilter(new String[] { FlattenedFieldMapper.UNMAPPED_SINK_NAME }, null), doc));
    }

    public void testAbsentWhenSettingOff() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        assertNull(mapperService.mappingLookup().getMapper(FlattenedFieldMapper.UNMAPPED_SINK_NAME));

        // With no sink, an unmapped field is dynamically mapped as usual, emitting a mapping update rather than being absorbed.
        String field = randomAlphanumericOfLength(8);
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field(field, randomAlphanumericOfLength(6))));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertTrue(doc.rootDoc().getFields(KEYED).isEmpty());
    }

    public void testAbsorbsUnmappedLeafWithFullDottedKey() throws IOException {
        DocumentMapper mapper = columnarService(b -> {}).documentMapper();
        String value = randomAlphanumericOfLength(6);
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", value)));

        assertNull("absorbed fields create no mapper, so no dynamic update is emitted", doc.dynamicMappingsUpdate());
        assertTrue(hasKeyedSlot(doc, "foo\0" + value));
        // Nothing is captured into generic _ignored_source in columnar mode.
        assertTrue(doc.rootDoc().getFields(IgnoredSourceFieldMapper.NAME).isEmpty());
    }

    public void testAbsorbsNestedUnmappedObjectAsDottedPaths() throws IOException {
        DocumentMapper mapper = columnarService(b -> {}).documentMapper();
        String v1 = randomAlphanumericOfLength(5);
        String v2 = randomAlphanumericOfLength(5);
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("outer");
            b.startObject("inner").field("leaf", v1).endObject();
            b.field("other", v2);
            b.endObject();
        }));

        assertNull(doc.dynamicMappingsUpdate());
        assertTrue(hasKeyedSlot(doc, "outer.inner.leaf\0" + v1));
        assertTrue(hasKeyedSlot(doc, "outer.other\0" + v2));
    }

    public void testArraysAbsorbPerElementIncludingNull() throws IOException {
        DocumentMapper mapper = columnarService(b -> {}).documentMapper();
        ParsedDocument doc = mapper.parse(source(b -> { b.startArray("arr").value(1).nullValue().value(3).endArray(); }));

        assertNull(doc.dynamicMappingsUpdate());
        assertTrue(hasKeyedSlot(doc, "arr\0" + "1"));
        assertTrue(hasKeyedSlot(doc, "arr\0" + "3"));
        // The null slot is recorded inline as key\0 (no value); exact ordering is verified in the synthetic-source round-trip PR.
        assertTrue(hasKeyedSlot(doc, "arr\0"));
        assertFalse(doc.rootDoc().getFields(KEYED).isEmpty());
    }

    public void testDynamicTemplateWinsOverAbsorptionWithPathMatch() throws IOException {
        DocumentMapper mapper = columnarServiceTop(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("as_long");
            b.field("path_match", "metrics.*");
            b.startObject("mapping").field("type", "long").endObject();
            b.endObject();
            b.endObject();
            b.endArray();
        }).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.startObject("metrics").field("count", 5).endObject()));

        // The template matched, so a real long mapper is created (mapping update emitted) and the value is NOT absorbed.
        assertNotNull(doc.dynamicMappingsUpdate());
        assertFalse(hasKeyedSlot(doc, "metrics.count\0" + "5"));
        assertThat(doc.dynamicMappingsUpdate().toString(), containsString("metrics"));
    }

    public void testExplicitSubObjectDynamicOverridesAbsorption() throws IOException {
        // A sub-object declared dynamic:false becomes a prefix property resolving to FALSE, so unmapped fields under it are
        // dropped rather than absorbed; unmapped fields elsewhere still fall back to the FLATTENED sink.
        DocumentMapper mapper = columnarService(b -> {
            b.startObject("attributes");
            b.field("dynamic", "false");
            b.startObject("properties").startObject("host").field("type", "keyword").endObject().endObject();
            b.endObject();
        }).documentMapper();

        String top = randomAlphanumericOfLength(5);
        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("toplevel", top);
            b.startObject("attributes").field("unmapped", randomAlphanumericOfLength(5)).endObject();
        }));

        assertTrue("root-level unmapped field is absorbed", hasKeyedSlot(doc, "toplevel\0" + top));
        assertFalse("dynamic:false sub-object drops its unmapped field", hasKeyedSlot(doc, "attributes.unmapped\0"));
    }

    public void testSinkSurvivesMerge() throws IOException {
        MapperService mapperService = columnarService(b -> {});
        merge(mapperService, mapping(b -> b.startObject("mapped").field("type", "keyword").endObject()));

        assertTrue(isUnmappedSink(mapperService));
        String serialized = mapperService.documentMapper().mappingSource().toString();
        assertThat(serialized, not(containsString(FlattenedFieldMapper.UNMAPPED_SINK_NAME)));
        assertThat(serialized, containsString("mapped"));

        // Absorption still works after the merge.
        String value = randomAlphanumericOfLength(6);
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("afterMerge", value)));
        assertTrue(hasKeyedSlot(doc, "afterMerge\0" + value));
    }

    public void testUserUnmappedFieldNotSinkWhenFeatureOff() throws IOException {
        // A flattened field literally named _unmapped on an index without the feature is an ordinary field, not the sink.
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.getName()).build();
        MapperService mapperService = createMapperService(
            settings,
            mapping(b -> b.startObject(FlattenedFieldMapper.UNMAPPED_SINK_NAME).field("type", "flattened").endObject())
        );
        assertFalse(isUnmappedSink(mapperService));
        // It serializes normally (not skipped), and unmapped fields are dynamically mapped rather than absorbed.
        assertThat(mapperService.documentMapper().mappingSource().toString(), containsString(FlattenedFieldMapper.UNMAPPED_SINK_NAME));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("other", randomAlphanumericOfLength(5))));
        assertNotNull(doc.dynamicMappingsUpdate());
    }

    public void testDynamicFlattenedNotUserSettableInPrefixProperties() {
        // Dynamic.FLATTENED is an internal resolved value; prefix_properties uses Dynamic.valueOf, so it must be rejected explicitly.
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        var e = expectThrows(Exception.class, () -> createMapperService(settings, topMapping(b -> {
            b.startObject("prefix_properties");
            b.startObject("attributes").field("dynamic", "flattened").endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("does not support [flattened]"));
    }
}
