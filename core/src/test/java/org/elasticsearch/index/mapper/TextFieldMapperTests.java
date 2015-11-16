/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.core.TypeParsers;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class TextFieldMapperTests extends ESSingleNodeTestCase {

    public void testDefaults() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        assertEquals(mapping, docMapper.mapping().toString());

        ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", "bar baz")
            .endObject()
            .bytes()).type("type").id("1"));

        IndexableField[] fields = doc.rootDoc().getFields("foo");
        assertEquals(1, fields.length);
        final IndexableField field = fields[0];
        assertEquals("bar baz", field.stringValue());
        assertTrue(field.fieldType().tokenized());
        assertFalse(field.fieldType().omitNorms());
        assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, field.fieldType().indexOptions());
        assertFalse(field.fieldType().stored());
        assertEquals(DocValuesType.NONE, field.fieldType().docValuesType());
        assertFalse(field.fieldType().storeTermVectors());
    }

    public void testDocValues() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "text")
                            .field("doc_values", true)
                        .endObject()
                    .endObject()
                .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        try {
            parser.parse(mapping);
            fail();
        } catch (MapperParsingException e) {
            assertEquals("Field [foo] cannot have doc values", e.getMessage());
        }
    }

    public void testNullValue() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "text")
                            .field("null_value", "NULL")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        assertEquals(mapping, docMapper.mapping().toString());

        ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", (String) null)
            .endObject()
            .bytes()).type("type").id("1"));

        IndexableField[] fields = doc.rootDoc().getFields("foo");
        assertEquals(1, fields.length);
        final IndexableField field = fields[0];
        assertEquals("NULL", field.stringValue());
        assertTrue(field.fieldType().tokenized());
        assertFalse(field.fieldType().omitNorms());
        assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, field.fieldType().indexOptions());
        assertFalse(field.fieldType().stored());
        assertEquals(DocValuesType.NONE, field.fieldType().docValuesType());
        assertFalse(field.fieldType().storeTermVectors());
    }

    public void testIgnoreAbove() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "text")
                            .field("ignore_above", 10)
                        .endObject()
                    .endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        assertEquals(mapping, docMapper.mapping().toString());

        ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", "bar baz")
            .endObject()
            .bytes()).type("type").id("1"));
        assertNotNull(doc.rootDoc().get("foo"));

        doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("foo", "bar baz quux")
                .endObject()
                .bytes()).type("type").id("1"));
        assertNull(doc.rootDoc().get("foo"));
    }

    public void testTermVectors() throws IOException {
        final Map<String, Predicate<IndexableFieldType>> tvOptions = new HashMap<>();
        tvOptions.put("with_positions", ft -> ft.storeTermVectors() && ft.storeTermVectorPositions() && !ft.storeTermVectorOffsets());
        tvOptions.put("with_offsets", ft -> ft.storeTermVectors() && !ft.storeTermVectorPositions() && ft.storeTermVectorOffsets());
        tvOptions.put("with_positions_offsets", ft -> ft.storeTermVectors() && ft.storeTermVectorPositions() && ft.storeTermVectorOffsets());
        for (Map.Entry<String, Predicate<IndexableFieldType>> entry : tvOptions.entrySet()) {
            final String tvOption = entry.getKey();
            final Predicate<IndexableFieldType> fieldTypePredicate = entry.getValue();
            String mapping = XContentFactory.jsonBuilder().startObject()
                    .startObject("type")
                        .startObject("properties")
                            .startObject("foo")
                                .field("type", "text")
                                .field("term_vector", tvOption)
                            .endObject()
                        .endObject()
                    .endObject().endObject().string();

            DocumentMapper docMapper = createIndex("test-" + tvOption).mapperService().documentMapperParser().parse(mapping);
            assertEquals(mapping, docMapper.mapping().toString());

            ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("foo", "bar baz")
                .endObject()
                .bytes()).type("type").id("1"));

            IndexableField[] fields = doc.rootDoc().getFields("foo");
            assertEquals(1, fields.length);
            final IndexableField field = fields[0];
            assertEquals("bar baz", field.stringValue());
            assertTrue(field.fieldType().tokenized());
            assertFalse(field.fieldType().omitNorms());
            assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, field.fieldType().indexOptions());
            assertFalse(field.fieldType().stored());
            assertEquals(DocValuesType.NONE, field.fieldType().docValuesType());
            assertTrue(fieldTypePredicate.test(field.fieldType()));
        }
    }

    public void testIndexOptions() throws IOException {
        for (Map.Entry<String, IndexOptions> entry : Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<String, IndexOptions>(TypeParsers.INDEX_OPTIONS_DOCS, IndexOptions.DOCS),
                new AbstractMap.SimpleImmutableEntry<String, IndexOptions>(TypeParsers.INDEX_OPTIONS_FREQS, IndexOptions.DOCS_AND_FREQS),
                new AbstractMap.SimpleImmutableEntry<String, IndexOptions>(TypeParsers.INDEX_OPTIONS_POSITIONS, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS),
                new AbstractMap.SimpleImmutableEntry<String, IndexOptions>(TypeParsers.INDEX_OPTIONS_OFFSETS, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS))) {
            final String optionName = entry.getKey();
            final IndexOptions options = entry.getValue();
            String mapping = XContentFactory.jsonBuilder().startObject()
                    .startObject("type")
                        .startObject("properties")
                            .startObject("foo")
                                .field("type", "text")
                                .field("index_options", optionName)
                            .endObject()
                        .endObject()
                    .endObject().endObject().string();

            DocumentMapper docMapper = createIndex("test-" + optionName).mapperService().documentMapperParser().parse(mapping);
            if (options != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) { // default
                assertEquals(mapping, docMapper.mapping().toString());
            }

            ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("foo", "bar baz")
                .endObject()
                .bytes()).type("type").id("1"));

            IndexableField[] fields = doc.rootDoc().getFields("foo");
            assertEquals(1, fields.length);
            final IndexableField field = fields[0];
            assertEquals("bar baz", field.stringValue());
            assertEquals(options, field.fieldType().indexOptions());
        }
    }
}
