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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.lucene.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class JsonFieldMapperTests extends ESSingleNodeTestCase {
    private IndexService indexService;
    private DocumentMapperParser parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "json")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("field")
                .field("key1", "value")
                .field("key2", true)
            .endObject()
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "type", "1", doc, XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        IndexableField field1 = fields[0];
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("value"), field1.binaryValue());
        assertTrue(field1.fieldType().omitNorms());

        IndexableField field2 = fields[1];
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("true"), field2.binaryValue());
        assertTrue(field2.fieldType().omitNorms());

        IndexableField[] fieldNamesFields = parsedDoc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertEquals(1, fieldNamesFields.length);

        IndexableField fieldNamesField = fieldNamesFields[0];
        assertEquals("field", fieldNamesField.stringValue());
    }

    public void testDisableIndex() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "json")
                        .field("index", false)
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("field")
                .field("key", "value")
            .endObject()
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "type", "1", doc, XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testEnableStore() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "json")
                        .field("index", false)
                        .field("store", true)
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("field")
                .field("key", "value")
            .endObject()
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "type", "1", doc, XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(1, fields.length);

        IndexableField field = fields[0];
        assertTrue(field.fieldType().stored());
    }

    public void testIndexOptions() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "json")
                        .field("index_options", "freqs")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        for (String indexOptions : Arrays.asList("positions", "offsets")) {
            String invalidMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "json")
                            .field("index_options", indexOptions)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> parser.parse("type", new CompressedXContent(invalidMapping)));
            assertEquals("The [json] field does not support positions, got [index_options]=" + indexOptions, e.getMessage());
        }
    }

    public void testEnableNorms() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
                .startObject("field")
                    .field("type", "json")
                    .field("norms", true)
                .endObject()
            .endObject()
        .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("field")
                .field("key", "value")
            .endObject()
        .endObject());
        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "type", "1", doc, XContentType.JSON));

        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertFalse(fields[0].fieldType().omitNorms());

        IndexableField[] fieldNamesFields = parsedDoc.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        assertEquals(0, fieldNamesFields.length);
    }

    public void testNullField() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "json")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .nullField("field")
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "type", "1", doc, XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testMalformedJson() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "json")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc1 = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .field("field", "not a JSON object")
        .endObject());

        expectThrows(MapperParsingException.class, () -> mapper.parse(
            SourceToParse.source("test", "type", "1", doc1, XContentType.JSON)));

        BytesReference doc2 = new BytesArray("{ \"field\": { \"key\": \"value\" ");
        expectThrows(MapperParsingException.class, () -> mapper.parse(
            SourceToParse.source("test", "type", "1", doc2, XContentType.JSON)));
    }

    public void testFieldMultiplicity() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "json")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startArray("field")
                .startObject()
                    .field("key1", "value")
                .endObject()
                .startObject()
                    .field("key2", true)
                    .field("key3", false)
                .endObject()
            .endArray()
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "type", "1", doc, XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(3, fields.length);

        IndexableField field1 = fields[0];
        assertEquals("field", field1.name());
        assertEquals(new BytesRef("value"), field1.binaryValue());

        IndexableField field2 = fields[1];
        assertEquals("field", field2.name());
        assertEquals(new BytesRef("true"), field2.binaryValue());

        IndexableField field3 = fields[2];
        assertEquals("field", field3.name());
        assertEquals(new BytesRef("false"), field3.binaryValue());
    }

    public void testIgnoreAbove() throws IOException {
         String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "json")
                        .field("ignore_above", 10)
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startArray("field")
                .startObject()
                    .field("key", "a longer than usual value")
                .endObject()
            .endArray()
        .endObject());

        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "type", "1", doc, XContentType.JSON));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }


    public void testNullValues() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "json")
                    .endObject()
                    .startObject("other_field")
                        .field("type", "json")
                        .field("null_value", "placeholder")
                    .endObject()
                .endObject()
            .endObject()
        .endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("field")
                .nullField("key")
            .endObject()
            .startObject("other_field")
                .nullField("key")
            .endObject()
        .endObject());
        ParsedDocument parsedDoc = mapper.parse(SourceToParse.source("test", "type", "1", doc, XContentType.JSON));

        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertEquals(0, fields.length);

        IndexableField[] otherFields = parsedDoc.rootDoc().getFields("other_field");
        assertEquals(1, otherFields.length);
        IndexableField field = otherFields[0];
        assertEquals(new BytesRef("placeholder"), field.binaryValue());
    }

     public void testSplitQueriesOnWhitespace() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "json")
                        .field("split_queries_on_whitespace", true)
                    .endObject()
                .endObject()
            .endObject().endObject());
        indexService.mapperService().merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        MappedFieldType fieldType = indexService.mapperService().fullName("field");
        assertThat(fieldType, instanceOf(JsonFieldMapper.JsonFieldType.class));

        JsonFieldMapper.JsonFieldType ft = (JsonFieldMapper.JsonFieldType) fieldType;
        assertThat(ft.searchAnalyzer(), equalTo(JsonFieldMapper.WHITESPACE_ANALYZER));
        assertTokenStreamContents(ft.searchAnalyzer().analyzer().tokenStream("", "Hello World"), new String[] {"Hello", "World"});
    }
}
