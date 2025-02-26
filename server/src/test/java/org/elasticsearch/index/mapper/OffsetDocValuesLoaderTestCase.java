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

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
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
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "field": {
                            "type": "{{type}}",
                            "doc_values": false
                        }
                    }
                }
            }
            """.replace("{{type}}", getFieldTypeName());
        try (var mapperService = createMapperService(mapping)) {
            var fieldMapper = mapperService.mappingLookup().getMapper("field");
            assertThat(fieldMapper.getOffsetFieldName(), nullValue());
        }
    }

    public void testOffsetArrayStored() throws Exception {
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "field": {
                            "type": "{{type}}",
                            "store": true
                        }
                    }
                }
            }
            """.replace("{{type}}", getFieldTypeName());
        ;
        try (var mapperService = createMapperService(mapping)) {
            var fieldMapper = mapperService.mappingLookup().getMapper("field");
            assertThat(fieldMapper.getOffsetFieldName(), nullValue());
        }
    }

    public void testOffsetMultiFields() throws Exception {
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "field": {
                            "type": "{{type}}",
                            "fields": {
                                "sub": {
                                    "type": "text"
                                }
                            }
                        }
                    }
                }
            }
            """.replace("{{type}}", getFieldTypeName());
        try (var mapperService = createMapperService(mapping)) {
            var fieldMapper = mapperService.mappingLookup().getMapper("field");
            assertThat(fieldMapper.getOffsetFieldName(), nullValue());
        }
    }

    public void testOffsetArrayNoSyntheticSource() throws Exception {
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "field": {
                            "type": "{{type}}"
                        }
                    }
                }
            }
            """.replace("{{type}}", getFieldTypeName());
        try (var mapperService = createMapperService(Settings.EMPTY, mapping)) {
            var fieldMapper = mapperService.mappingLookup().getMapper("field");
            assertThat(fieldMapper.getOffsetFieldName(), nullValue());
        }
    }

    public void testOffsetArrayNoSourceArrayKeep() throws Exception {
        var settingsBuilder = Settings.builder().put("index.mapping.source.mode", "synthetic");
        String mapping;
        if (randomBoolean()) {
            mapping = """
                {
                    "_doc": {
                        "properties": {
                            "field": {
                                "type": "{{type}}",
                                "synthetic_source_keep": "{{synthetic_source_keep}}"
                            }
                        }
                    }
                }
                """.replace("{{synthetic_source_keep}}", randomBoolean() ? "none" : "all").replace("{{type}}", getFieldTypeName());
        } else {
            mapping = """
                {
                    "_doc": {
                        "properties": {
                            "field": {
                                "type": "{{type}}"
                            }
                        }
                    }
                }
                """.replace("{{type}}", getFieldTypeName());
            if (randomBoolean()) {
                settingsBuilder.put("index.mapping.synthetic_source_keep", "none");
            }
        }
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
        StringBuilder values = new StringBuilder();
        int numValues = randomIntBetween(0, 256);
        for (int i = 0; i < numValues; i++) {
            if (randomInt(10) == 1) {
                values.append("null");
            } else {
                String randomValue = randomValue();
                values.append('"').append(randomValue).append('"');
            }
            if (i != (numValues - 1)) {
                values.append(',');
            }
        }
        verifyOffsets("{\"field\":[" + values + "]}");
    }

    protected abstract String getFieldTypeName();

    protected abstract String randomValue();

    protected void verifyOffsets(String source) throws IOException {
        verifyOffsets(source, source);
    }

    protected void verifyOffsets(String source, String expectedSource) throws IOException {
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "field": {
                            "type": "{{type}}"
                        }
                    }
                }
            }
            """.replace("{{type}}", getFieldTypeName());
        verifyOffsets(mapping, source, expectedSource);
    }

    private void verifyOffsets(String mapping, String source, String expectedSource) throws IOException {
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
