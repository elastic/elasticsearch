/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public abstract class OffsetDocValuesLoaderTestCase extends MapperServiceTestCase {

    @Override
    protected Settings getIndexSettings() {
        return Settings.builder()
            .put("index.mapping.source.mode", "synthetic")
            .put("index.mapping.synthetic_source_keep", "arrays")
            .build();
    }

    public void testOffsetArrayNoDocValues() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.field("doc_values", false);
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(mapping)) {
            var fieldMapper = mapperService.mappingLookup().getMapper("field");
            assertThat(fieldMapper.getOffsetFieldName(), nullValue());
        }
    }

    public void testOffsetArrayStored() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.field("store", true);
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(mapping)) {
            var fieldMapper = mapperService.mappingLookup().getMapper("field");
            assertThat(fieldMapper.getOffsetFieldName(), nullValue());
        }
    }

    public void testOffsetMultiFields() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.startObject("fields").startObject("sub").field("type", "text").endObject().endObject();
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(mapping)) {
            var fieldMapper = mapperService.mappingLookup().getMapper("field");
            assertThat(fieldMapper.getOffsetFieldName(), nullValue());
        }
    }

    public void testOffsetArrayNoSyntheticSource() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(Settings.EMPTY, mapping)) {
            var fieldMapper = mapperService.mappingLookup().getMapper("field");
            assertThat(fieldMapper.getOffsetFieldName(), nullValue());
        }
    }

    public void testOffsetArrayNoSourceArrayKeep() throws Exception {
        var settingsBuilder = Settings.builder().put("index.mapping.source.mode", "synthetic");
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        if (randomBoolean()) {
            mapping.field("synthetic_source_keep", randomBoolean() ? "none" : "all");
        } else if (randomBoolean()) {
            settingsBuilder.put("index.mapping.synthetic_source_keep", "none");
        }
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(settingsBuilder.build(), mapping)) {
            var fieldMapper = mapperService.mappingLookup().getMapper("field");
            assertThat(fieldMapper.getOffsetFieldName(), nullValue());
        }
    }

    public void testOffsetEmptyArray() throws Exception {
        verifyOffsets("{\"field\":[]}");
    }

    public void testOffsetArrayWithNulls() throws Exception {
        verifyOffsets("{\"field\":[null,null,null]}");
        verifyOffsets("{\"field\":[null,[null],null]}", "{\"field\":[null,null,null]}");
    }

    public void testOffsetArrayRandom() throws Exception {
        String values;
        int numValues = randomIntBetween(0, 256);

        var previousValues = new HashSet<>();
        try (XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent());) {
            b.startArray();
            for (int i = 0; i < numValues; i++) {
                if (randomInt(10) == 1) {
                    b.nullValue();
                } else if (randomInt(10) == 1 && previousValues.size() > 0) {
                    b.value(randomFrom(previousValues));
                } else {
                    Object value = randomValue();
                    previousValues.add(value);
                    b.value(value);
                }
            }
            b.endArray();
            values = Strings.toString(b);
        }
        verifyOffsets("{\"field\":" + values + "}");
    }

    /**
     * An array with so many elements that its encoded offsets exceed {@link IndexWriter#MAX_TERM_LENGTH} must not fail the
     * document. Instead the array is stored in {@code _ignored_source}, so synthetic source still reproduces element order,
     * duplicates and nulls exactly, and no {@code field.offsets} doc value is written.
     */
    public void testOversizedOffsetArrayFallsBackToIgnoredSource() throws Exception {
        // One more slot than IndexWriter.MAX_TERM_LENGTH guarantees the encoded offsets exceed the limit, since every slot
        // takes at least one byte on top of the length prefix.
        int numSlots = IndexWriter.MAX_TERM_LENGTH + between(1, 500);
        List<Object> values = new ArrayList<>(numSlots);
        List<Object> pool = new ArrayList<>();
        for (int i = 0; i < numSlots; i++) {
            if (randomInt(10) == 1) {
                values.add(null);
            } else if (randomInt(10) == 1 && pool.isEmpty() == false) {
                values.add(randomFrom(pool));
            } else {
                Object value = randomValue();
                pool.add(value);
                values.add(value);
            }
        }

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(mapping)) {
            var mapper = mapperService.documentMapper();

            var doc = mapper.parse(source(b -> b.field("field", values)));
            assertThat(doc.rootDoc().getFields("field.offsets"), Matchers.empty());

            String expected;
            try (XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent())) {
                b.startObject().field("field", values).endObject();
                expected = Strings.toString(b);
            }
            assertThat(syntheticSource(mapper, b -> b.field("field", values)), Matchers.equalTo(expected));
        }
    }

    /**
     * Like {@link #testOversizedOffsetArrayFallsBackToIgnoredSource()}, but some of the array's values are malformed and ignored
     * via {@code ignore_malformed}. Such values get no offsets slot and are stored in the field's own fallback storage, which the
     * {@code _ignored_source} entry suppresses at load time. So the entry carries the malformed values itself, at their original
     * position in the array.
     */
    public void testOversizedOffsetArrayWithMalformedValuesFallsBackToIgnoredSource() throws Exception {
        assumeTrue("field type has no ignore_malformed parameter", randomMalformedValue() != null);
        int numSlots = IndexWriter.MAX_TERM_LENGTH + between(1, 500);
        List<Object> values = new ArrayList<>(numSlots);
        for (int i = 0; i < numSlots; i++) {
            values.add(randomBoolean() && randomBoolean() ? null : randomValue());
        }
        // Cover the first and last position, several in between, and two adjacent ones.
        values.addFirst(randomMalformedValue());
        values.addLast(randomMalformedValue());
        int adjacent = between(1, values.size() - 1);
        values.add(adjacent, randomMalformedValue());
        values.add(adjacent, randomMalformedValue());
        for (int i = 0; i < between(1, 10); i++) {
            values.add(between(1, values.size() - 1), randomMalformedValue());
        }

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.field("ignore_malformed", true);
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(mapping)) {
            var mapper = mapperService.documentMapper();

            var doc = mapper.parse(source(b -> b.field("field", values)));
            assertThat(doc.rootDoc().getFields("field.offsets"), Matchers.empty());

            String expected;
            try (XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent())) {
                b.startObject().field("field", values).endObject();
                expected = Strings.toString(b);
            }
            assertThat(syntheticSource(mapper, b -> b.field("field", values)), Matchers.equalTo(expected));
        }
    }

    /**
     * An array whose values were all ignored via {@code ignore_malformed} records ignored values but no offsets slot. No offsets
     * doc value may be written for it: one would read back as an empty-array marker and alter the synthetic source, which must
     * come from the field's own storage of ignored values alone, like before the oversized-offsets fallback existed. Covers both
     * a small array and one with more elements than a sorted doc value could hold offsets for.
     */
    public void testAllIgnoredArrayWritesNoOffsets() throws Exception {
        assumeTrue("field type has no ignore_malformed parameter", randomMalformedValue() != null);
        int numValues = randomBoolean() ? between(1, 10) : IndexWriter.MAX_TERM_LENGTH + between(1, 500);
        List<Object> values = new ArrayList<>(numValues);
        for (int i = 0; i < numValues; i++) {
            values.add(randomMalformedValue());
        }

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.field("ignore_malformed", true);
        mapping.endObject().endObject().endObject().endObject();
        try (var mapperService = createMapperService(mapping)) {
            var mapper = mapperService.documentMapper();

            var doc = mapper.parse(source(b -> b.field("field", values)));
            assertThat(doc.rootDoc().getFields("field.offsets"), Matchers.empty());
            assertSyntheticSourceHasIgnoredValues(syntheticSource(mapper, b -> b.field("field", values)), values);
        }
    }

    /**
     * Asserts that the synthetic source of a document holds exactly the given ignored values of {@code field}, in any order: the
     * field's own storage of ignored values does not preserve their order, and without offsets a single value is synthesized as
     * a scalar, like any single value.
     */
    protected static void assertSyntheticSourceHasIgnoredValues(String syntheticSource, List<Object> values) {
        var source = XContentHelper.convertToMap(new BytesArray(syntheticSource), false, XContentType.JSON).v2();
        assertThat(source.keySet(), Matchers.contains("field"));
        if (values.size() == 1) {
            assertThat(source.get("field"), Matchers.equalTo(values.getFirst()));
        } else {
            assertThat(source.get("field"), Matchers.instanceOf(List.class));
            assertThat((List<?>) source.get("field"), Matchers.containsInAnyOrder(values.toArray()));
        }
    }

    /**
     * Like {@link #testOversizedOffsetArrayFallsBackToIgnoredSource()}, but the field is not mapped at the root. The
     * {@code _ignored_source} entry records where the leaf name starts within the full path, which is only non-zero for such
     * fields. With {@code subobjects: false} on the enclosing object the leaf name itself contains a dot, so it cannot be derived
     * from the full path alone.
     */
    public void testOversizedOffsetArrayOnNonRootFieldFallsBackToIgnoredSource() throws Exception {
        List<Object> values = randomOversizedValues();
        boolean subobjectsFalse = randomBoolean();

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("obj");
        if (subobjectsFalse) {
            mapping.field("subobjects", false);
            mapping.startObject("properties").startObject("inner.field");
            minimalMapping(mapping);
            mapping.endObject().endObject();
        } else {
            mapping.startObject("properties").startObject("inner").startObject("properties").startObject("field");
            minimalMapping(mapping);
            mapping.endObject().endObject().endObject().endObject();
        }
        mapping.endObject().endObject().endObject().endObject();

        CheckedConsumer<XContentBuilder, IOException> document = b -> {
            b.startObject("obj");
            if (subobjectsFalse) {
                b.field("inner.field", values);
            } else {
                b.startObject("inner").field("field", values).endObject();
            }
            b.endObject();
        };
        try (var mapperService = createMapperService(mapping)) {
            var mapper = mapperService.documentMapper();

            var doc = mapper.parse(source(document));
            assertThat(doc.rootDoc().getFields("obj.inner.field.offsets"), Matchers.empty());
            assertThat(syntheticSource(mapper, document), Matchers.equalTo(toJson(document)));
        }
    }

    /**
     * Like {@link #testOversizedOffsetArrayFallsBackToIgnoredSource()}, but the oversized array has regular siblings in the same
     * object: a single value sorting before it and a small array, with regular offsets, sorting after it. The
     * {@code _ignored_source} entry for the oversized field must not hide a sibling, and the keys keep the sorted order that
     * synthetic source uses, regardless of the order in the document.
     */
    public void testOversizedOffsetArrayWithSiblingsFallsBackToIgnoredSource() throws Exception {
        List<Object> values = randomOversizedValues();
        Object before = randomValue();
        List<Object> after = List.of(randomValue(), randomValue(), randomValue());
        List<CheckedConsumer<XContentBuilder, IOException>> fields = new ArrayList<>(
            List.of(b -> b.field("a_before", before), b -> b.field("field", values), b -> b.field("z_after", after))
        );
        Collections.shuffle(fields, random());
        CheckedConsumer<XContentBuilder, IOException> expected = b -> {
            b.field("a_before", before);
            b.field("field", values);
            b.field("z_after", after);
        };
        assertOversizedArraysWithSiblingsFallBackToIgnoredSource(List.of("a_before", "field", "z_after"), fields, expected, "field");
    }

    /**
     * Like {@link #testOversizedOffsetArrayWithSiblingsFallsBackToIgnoredSource()}, but with two oversized arrays in the same
     * object, so two {@code _ignored_source} entries with the same parent.
     */
    public void testTwoOversizedOffsetArraysInSameObjectFallBackToIgnoredSource() throws Exception {
        List<Object> values = randomOversizedValues();
        List<Object> otherValues = randomOversizedValues();
        List<CheckedConsumer<XContentBuilder, IOException>> fields = new ArrayList<>(
            List.of(b -> b.field("field", values), b -> b.field("other", otherValues))
        );
        Collections.shuffle(fields, random());
        CheckedConsumer<XContentBuilder, IOException> expected = b -> {
            b.field("field", values);
            b.field("other", otherValues);
        };
        assertOversizedArraysWithSiblingsFallBackToIgnoredSource(List.of("field", "other"), fields, expected, "field", "other");
    }

    /**
     * Maps {@code fieldNames} with the field type under test, randomly at the root or in an object, parses a document holding
     * {@code fields} in the given order, and asserts that no offsets were written for the {@code oversized} fields and that the
     * synthetic source is {@code expected}.
     */
    private void assertOversizedArraysWithSiblingsFallBackToIgnoredSource(
        List<String> fieldNames,
        List<CheckedConsumer<XContentBuilder, IOException>> fields,
        CheckedConsumer<XContentBuilder, IOException> expected,
        String... oversized
    ) throws Exception {
        boolean inObject = randomBoolean();
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        if (inObject) {
            mapping.startObject("obj").startObject("properties");
        }
        for (String fieldName : fieldNames) {
            mapping.startObject(fieldName);
            minimalMapping(mapping);
            mapping.endObject();
        }
        if (inObject) {
            mapping.endObject().endObject();
        }
        mapping.endObject().endObject().endObject();

        CheckedConsumer<XContentBuilder, IOException> document = inObject ? b -> {
            b.startObject("obj");
            for (var field : fields) {
                field.accept(b);
            }
            b.endObject();
        } : b -> {
            for (var field : fields) {
                field.accept(b);
            }
        };
        CheckedConsumer<XContentBuilder, IOException> expectedDocument = inObject ? b -> {
            b.startObject("obj");
            expected.accept(b);
            b.endObject();
        } : expected;
        try (var mapperService = createMapperService(mapping)) {
            var mapper = mapperService.documentMapper();

            var doc = mapper.parse(source(document));
            for (String fieldName : oversized) {
                assertThat(doc.rootDoc().getFields((inObject ? "obj." : "") + fieldName + ".offsets"), Matchers.empty());
            }
            assertThat(syntheticSource(mapper, document), Matchers.equalTo(toJson(expectedDocument)));
        }
    }

    /**
     * Values for an array with so many elements that its encoded offsets exceed {@link IndexWriter#MAX_TERM_LENGTH}, with
     * duplicates and nulls, like in {@link #testOversizedOffsetArrayFallsBackToIgnoredSource()}.
     */
    private List<Object> randomOversizedValues() {
        int numSlots = IndexWriter.MAX_TERM_LENGTH + between(1, 500);
        List<Object> values = new ArrayList<>(numSlots);
        List<Object> pool = new ArrayList<>();
        for (int i = 0; i < numSlots; i++) {
            if (randomInt(10) == 1) {
                values.add(null);
            } else if (randomInt(10) == 1 && pool.isEmpty() == false) {
                values.add(randomFrom(pool));
            } else {
                Object value = randomValue();
                pool.add(value);
                values.add(value);
            }
        }
        return values;
    }

    private static String toJson(CheckedConsumer<XContentBuilder, IOException> document) throws IOException {
        try (XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent())) {
            b.startObject();
            document.accept(b);
            b.endObject();
            return Strings.toString(b);
        }
    }

    /**
     * Like {@link #testOversizedOffsetArrayFallsBackToIgnoredSource()}, but the field is mapped dynamically by the very
     * document that holds the oversized array, so the field's mapper is not part of the mapping lookup while the document
     * is parsed. A dynamic template pins the field type under test, since plain dynamic mapping only produces a few types.
     */
    public void testOversizedOffsetArrayOnDynamicallyMappedFieldFallsBackToIgnoredSource() throws Exception {
        // No nulls here, unlike testOversizedOffsetArrayFallsBackToIgnoredSource: a null array element of a field that was
        // mapped dynamically by the current document never reaches the field mapper (see DocumentParser#parseNullValue), so
        // no null slot is recorded. That loss predates the oversized-offsets fallback and is independent of it.
        int numSlots = IndexWriter.MAX_TERM_LENGTH + between(1, 500);
        List<Object> values = new ArrayList<>(numSlots);
        List<Object> pool = new ArrayList<>();
        for (int i = 0; i < numSlots; i++) {
            if (randomInt(10) == 1 && pool.isEmpty() == false) {
                values.add(randomFrom(pool));
            } else {
                Object value = randomValue();
                pool.add(value);
                values.add(value);
            }
        }

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc");
        mapping.startArray("dynamic_templates");
        mapping.startObject().startObject("field_template").field("match", "field").startObject("mapping");
        minimalMapping(mapping);
        mapping.endObject().endObject().endObject();
        mapping.endArray();
        mapping.endObject().endObject();

        try (var mapperService = createMapperService(mapping); Directory directory = newDirectory()) {
            var mapper = mapperService.documentMapper();

            var doc = mapper.parse(source(b -> b.field("field", values)));
            assertThat(doc.rootDoc().getFields("field.offsets"), Matchers.empty());
            assertThat(doc.dynamicMappingsUpdate(), notNullValue());
            merge(mapperService, doc.dynamicMappingsUpdate().string());

            var indexWriter = indexWriterForSyntheticSource(directory);
            doc.updateSeqID(0, 0);
            doc.version().setLongValue(0);
            indexWriter.addDocuments(doc.docs());
            indexWriter.close();

            String expected;
            try (XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent())) {
                b.startObject().field("field", values).endObject();
                expected = Strings.toString(b);
            }
            try (DirectoryReader reader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                assertThat(syntheticSource(mapperService.documentMapper(), reader, 0), Matchers.equalTo(expected));
            }
        }
    }

    protected void minimalMapping(XContentBuilder b) throws IOException {
        String fieldTypeName = getFieldTypeName();
        assertThat(fieldTypeName, notNullValue());
        b.field("type", fieldTypeName);
    }

    protected abstract String getFieldTypeName();

    protected abstract Object randomValue();

    /**
     * A value that the field type under test rejects, and ignores with {@code ignore_malformed: true}, or {@code null} when the
     * field type has no {@code ignore_malformed} parameter. By default a string of letters, which none of the field types with
     * {@code ignore_malformed} parses.
     */
    protected Object randomMalformedValue() {
        return randomAlphaOfLength(10);
    }

    protected void verifyOffsets(String source) throws IOException {
        verifyOffsets(source, source);
    }

    protected void verifyOffsets(String source, String expectedSource) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties").startObject("field");
        minimalMapping(mapping);
        mapping.endObject().endObject().endObject().endObject();
        verifyOffsets(mapping, source, expectedSource);
    }

    private void verifyOffsets(XContentBuilder mapping, String source, String expectedSource) throws IOException {
        try (var mapperService = createMapperService(mapping)) {
            var mapper = mapperService.documentMapper();

            try (var directory = newDirectory()) {
                var iw = indexWriterForSyntheticSource(directory);
                var doc = mapper.parse(new SourceToParse("_id", new BytesArray(source), XContentType.JSON));
                doc.updateSeqID(0, 0);
                doc.version().setLongValue(0);
                iw.addDocuments(doc.docs());
                iw.close();
                try (var indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                    FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
                    var syntheticSourceLoader = fieldMapper.syntheticFieldLoader();
                    var leafReader = indexReader.leaves().getFirst().reader();
                    var docValueLoader = syntheticSourceLoader.docValuesLoader(leafReader, new int[] { 0 });
                    assertTrue(docValueLoader.advanceToDoc(0));
                    assertTrue(syntheticSourceLoader.hasValue());
                    XContentBuilder builder = jsonBuilder().startObject();
                    syntheticSourceLoader.write(builder);
                    builder.endObject();

                    var actual = Strings.toString(builder);
                    assertEquals(expectedSource, actual);
                }
            }
        }
    }

}
