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
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

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
        return ((FlattenedFieldMapper) mapperService.mappingLookup().getMapper(FlattenedFieldMapper.UNMAPPED_SINK_NAME)).isUnmappedSink();
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
        assertThat(mapperService.documentMapper().mappingSource().toString(), not(containsString(FlattenedFieldMapper.UNMAPPED_SINK_NAME)));
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
