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
import org.elasticsearch.index.mapper.SortedSetWithOffsetsDocValuesSyntheticFieldLoaderLayer.DocValuesWithOffsetsLoader;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.nullValue;

public class KeywordOffsetDocValuesLoaderTests extends MapperServiceTestCase {

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
                            "type": "keyword",
                            "doc_values": false
                        }
                    }
                }
            }
            """;
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
                            "type": "keyword",
                            "store": true
                        }
                    }
                }
            }
            """;
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
                            "type": "keyword",
                            "fields": {
                                "sub": {
                                    "type": "text"
                                }
                            }
                        }
                    }
                }
            }
            """;
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
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
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
                                "type": "keyword",
                                "synthetic_source_keep": "{{synthetic_source_keep}}"
                            }
                        }
                    }
                }
                """.replace("{{synthetic_source_keep}}", randomBoolean() ? "none" : "all");
        } else {
            mapping = """
                {
                    "_doc": {
                        "properties": {
                            "field": {
                                "type": "keyword"
                            }
                        }
                    }
                }
                """;
            if (randomBoolean()) {
                settingsBuilder.put("index.mapping.synthetic_source_keep", "none");
            }
        }
        try (var mapperService = createMapperService(settingsBuilder.build(), mapping)) {
            var fieldMapper = mapperService.mappingLookup().getMapper("field");
            assertThat(fieldMapper.getOffsetFieldName(), nullValue());
        }
    }

    public void testOffsetArray() throws Exception {
        verifyOffsets("{\"field\":[\"z\",\"x\",\"y\",\"c\",\"b\",\"a\"]}");
        verifyOffsets("{\"field\":[\"z\",null,\"y\",\"c\",null,\"a\"]}");
    }

    public void testOffsetNestedArray() throws Exception {
        verifyOffsets("{\"field\":[\"z\",[\"y\"],[\"c\"],null,\"a\"]}", "{\"field\":[\"z\",\"y\",\"c\",null,\"a\"]}");
        verifyOffsets(
            "{\"field\":[\"z\",[\"y\", [\"k\"]],[\"c\", [\"l\"]],null,\"a\"]}",
            "{\"field\":[\"z\",\"y\",\"k\",\"c\",\"l\",null,\"a\"]}"
        );
    }

    public void testOffsetEmptyArray() throws Exception {
        verifyOffsets("{\"field\":[]}");
    }

    public void testOffsetArrayWithNulls() throws Exception {
        verifyOffsets("{\"field\":[null,null,null]}");
    }

    public void testOffsetArrayRandom() throws Exception {
        StringBuilder values = new StringBuilder();
        int numValues = randomIntBetween(0, 256);
        for (int i = 0; i < numValues; i++) {
            if (randomInt(10) == 1) {
                values.append("null");
            } else {
                values.append('"').append(randomAlphanumericOfLength(2)).append('"');
            }
            if (i != (numValues - 1)) {
                values.append(',');
            }
        }
        verifyOffsets("{\"field\":[" + values + "]}");
    }

    private void verifyOffsets(String source) throws IOException {
        verifyOffsets(source, source);
    }

    private void verifyOffsets(String source, String expectedSource) throws IOException {
        String mapping = """
            {
                "_doc": {
                    "properties": {
                        "field": {
                            "type": "keyword"
                        }
                    }
                }
            }
            """;
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
                    var layer = new SortedSetWithOffsetsDocValuesSyntheticFieldLoaderLayer("field", "field.offsets");
                    var leafReader = indexReader.leaves().getFirst().reader();
                    var loader = (DocValuesWithOffsetsLoader) layer.docValuesLoader(leafReader, new int[] { 0 });
                    assertTrue(loader.advanceToDoc(0));
                    assertTrue(loader.count() > 0);
                    XContentBuilder builder = jsonBuilder().startObject();
                    builder.startArray("field");
                    loader.write(builder);
                    builder.endArray().endObject();

                    var actual = Strings.toString(builder);
                    assertEquals(expectedSource, actual);
                }
            }
        }
    }

}
