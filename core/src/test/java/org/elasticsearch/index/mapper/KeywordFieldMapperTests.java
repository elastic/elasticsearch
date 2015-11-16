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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.core.TypeParsers;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class KeywordFieldMapperTests extends ESSingleNodeTestCase {

    public void testDefaults() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "keyword")
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
        assertEquals(2, fields.length);

        final IndexableField indexField = fields[0];
        assertEquals("bar baz", indexField.stringValue());
        assertFalse(indexField.fieldType().tokenized());
        assertTrue(indexField.fieldType().omitNorms());
        assertEquals(IndexOptions.DOCS, indexField.fieldType().indexOptions());
        assertFalse(indexField.fieldType().stored());
        assertEquals(DocValuesType.NONE, indexField.fieldType().docValuesType());
        assertFalse(indexField.fieldType().storeTermVectors());

        final IndexableField dvField = fields[1];
        assertEquals(new BytesRef("bar baz"), dvField.binaryValue());
        assertEquals(IndexOptions.NONE, dvField.fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
    }

    public void testDisableIndex() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "keyword")
                            .field("index", false)
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
        assertEquals(0, fields.length);
    }

    public void testDisableIndexEnableDocValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "keyword")
                            .field("index", false)
                            .field("doc_values", true)
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

        final IndexableField dvField = fields[0];
        assertEquals(new BytesRef("bar baz"), dvField.binaryValue());
        assertEquals(IndexOptions.NONE, dvField.fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
    }

    public void testNullValue() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "keyword")
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
        assertEquals(2, fields.length);

        final IndexableField indexField = fields[0];
        assertEquals("NULL", indexField.stringValue());
        assertFalse(indexField.fieldType().tokenized());
        assertTrue(indexField.fieldType().omitNorms());
        assertEquals(IndexOptions.DOCS, indexField.fieldType().indexOptions());
        assertFalse(indexField.fieldType().stored());
        assertEquals(DocValuesType.NONE, indexField.fieldType().docValuesType());
        assertFalse(indexField.fieldType().storeTermVectors());

        final IndexableField dvField = fields[1];
        assertEquals(new BytesRef("NULL"), dvField.binaryValue());
        assertEquals(IndexOptions.NONE, dvField.fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
    }

    public void testIgnoreAbove() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "keyword")
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
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "keyword")
                            .field("term_vector", "with_positions_offsets")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        try {
            parser.parse(mapping);
            fail();
        } catch (MapperParsingException e) {
            assertEquals("Mapping definition for [foo] has unsupported parameters:  [term_vector : with_positions_offsets]", e.getMessage());
        }
    }

    public void testIndexOptions() throws IOException {
        for (Map.Entry<String, IndexOptions> entry : Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<String, IndexOptions>(TypeParsers.INDEX_OPTIONS_DOCS, IndexOptions.DOCS),
                new AbstractMap.SimpleImmutableEntry<String, IndexOptions>(TypeParsers.INDEX_OPTIONS_FREQS, IndexOptions.DOCS_AND_FREQS))) {
            final String optionName = entry.getKey();
            final IndexOptions options = entry.getValue();
            String mapping = XContentFactory.jsonBuilder().startObject()
                    .startObject("type")
                        .startObject("properties")
                            .startObject("foo")
                                .field("type", "keyword")
                                .field("index_options", optionName)
                            .endObject()
                        .endObject()
                    .endObject().endObject().string();

            DocumentMapper docMapper = createIndex("test-" + optionName).mapperService().documentMapperParser().parse(mapping);
            if (options != IndexOptions.DOCS) { // default
                assertEquals(mapping, docMapper.mapping().toString());
            }

            ParsedDocument doc = docMapper.parse(SourceToParse.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("foo", "bar baz")
                .endObject()
                .bytes()).type("type").id("1"));

            IndexableField[] fields = doc.rootDoc().getFields("foo");
            assertEquals(2, fields.length);
            final IndexableField field = fields[0];
            assertEquals("bar baz", field.stringValue());
            assertEquals(options, field.fieldType().indexOptions());
        }

        for (String options : Arrays.asList(TypeParsers.INDEX_OPTIONS_OFFSETS, TypeParsers.INDEX_OPTIONS_POSITIONS)) {
            String mapping = XContentFactory.jsonBuilder().startObject()
                    .startObject("type")
                        .startObject("properties")
                            .startObject("foo")
                                .field("type", "keyword")
                                .field("index_options", options)
                            .endObject()
                        .endObject()
                    .endObject().endObject().string();
            DocumentMapperParser parser = createIndex("test-" + options).mapperService().documentMapperParser();
            try {
                parser.parse(mapping);
                fail();
            } catch (MapperParsingException e) {
                assertEquals("Field [foo] of type [keyword] only supports indexing docs and freqs, but got [index_options=" + options + "]", e.getMessage());
            }
        }
    }
}
