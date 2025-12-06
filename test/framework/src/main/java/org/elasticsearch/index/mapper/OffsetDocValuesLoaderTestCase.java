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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashSet;

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

    protected void minimalMapping(XContentBuilder b) throws IOException {
        String fieldTypeName = getFieldTypeName();
        assertThat(fieldTypeName, notNullValue());
        b.field("type", fieldTypeName);
    }

    protected abstract String getFieldTypeName();

    protected abstract Object randomValue();

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
