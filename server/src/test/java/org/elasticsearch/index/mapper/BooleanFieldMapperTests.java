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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.Matchers.containsString;

public class BooleanFieldMapperTests extends ESSingleNodeTestCase {

    private static final boolean FIELD1_IGNORE_MALFORMED = true;
    private static final boolean FIELD1_DONT_IGNORE_MALFORMED = !FIELD1_IGNORE_MALFORMED;
    private static final String FIELD1 = "field1";
    private static final String FIELD2 = "field2";

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

    public void testDefaults() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "boolean").endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", true)
                        .endObject()),
                XContentType.JSON));

        try (Directory dir = new RAMDirectory();
             IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())))) {
            w.addDocuments(doc.docs());
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                // boolean fields are indexed and have doc values by default
                assertEquals(new BytesRef("T"), leaf.terms("field").iterator().next());
                SortedNumericDocValues values = leaf.getSortedNumericDocValues("field");
                assertNotNull(values);
                assertTrue(values.advanceExact(0));
                assertEquals(1, values.docValueCount());
                assertEquals(1, values.nextValue());
            }
        }
    }

    public void testSerialization() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "boolean").endObject().endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));
        FieldMapper mapper = defaultMapper.mappers().getMapper("field");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"boolean\"}}", Strings.toString(builder));

        // now change some parameters
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "boolean")
                    .field("doc_values", "false")
                    .field("null_value", true)
                .endObject().endObject()
                .endObject().endObject());

        defaultMapper = parser.parse("type", new CompressedXContent(mapping));
        mapper = defaultMapper.mappers().getMapper("field");
        builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"boolean\",\"doc_values\":false,\"null_value\":true}}", Strings.toString(builder));
    }

    public void test_null() throws Exception{
        this.parseAndCheck(FIELD1_IGNORE_MALFORMED, null, null);
    }

    public void test_true() throws Exception{
        this.parseAndCheck(FIELD1_IGNORE_MALFORMED,true, BooleanFieldMapper.TRUE);
    }

    public void test_false() throws Exception{
        this.parseAndCheck(FIELD1_IGNORE_MALFORMED,false, BooleanFieldMapper.FALSE);
    }

    public void test_on() throws Exception{
        this.parseFails("on");
    }

    public void test_off() throws Exception{
        this.parseFails("off");
    }

    public void test_yes() throws Exception{
        this.parseFails("yes");
    }

    public void test_no() throws Exception{
        this.parseFails("no");
    }

    public void test_0() throws Exception{
        this.parseFails("0");
    }

    public void test_1() throws Exception{
        this.parseFails("1");
    }

    public void test_string_IgnoreMalformed() throws Exception{
        this.parseAndCheck(FIELD1_IGNORE_MALFORMED,
            sourceWithString(),
            null, BooleanFieldMapper.TRUE);
    }

    public void test_string() throws Exception{
        this.parseFails(sourceWithString());
    }

    private BytesReference sourceWithString() throws Exception{
        return BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD1, "**Never convertable to boolean**") // IGNORED
            .field(FIELD2, true)
            .endObject());
    }

    public void test_number_IgnoreMalformed() throws Exception{
        this.parseAndCheck(FIELD1_IGNORE_MALFORMED,
            this.sourceWithNumber(1),
            null, BooleanFieldMapper.TRUE);
    }

    public void test_number() throws Exception{
        this.parseFails(this.sourceWithNumber(1));
    }

    private BytesReference sourceWithNumber(final int value) throws Exception{
        return BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD1, value) // IGNORED
            .field(FIELD2, true)
            .endObject());
    }

    public void test_object_IgnoreMalformed() throws Exception{
        this.parseAndCheck(FIELD1_IGNORE_MALFORMED,
            this.sourceWithObject(),
            null, BooleanFieldMapper.TRUE);
    }

    public void test_object() throws Exception{
        this.parseFails(FIELD1_DONT_IGNORE_MALFORMED,
            this.sourceWithObject());
    }

    private BytesReference sourceWithObject() throws Exception{
        return BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD1) // IGNORED
            .field("field3", false)
            .endObject()
            .field(FIELD2, true)
            .endObject());
    }

    public void test_array_IgnoreMalformed() throws Exception{
        this.parseAndCheck(FIELD1_IGNORE_MALFORMED,
            sourceWithArray(),
            null, BooleanFieldMapper.TRUE);
    }

    public void test_array() throws Exception{
        this.parseFails(sourceWithArray());
    }

    private BytesReference sourceWithArray() throws Exception{
        return BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .startArray(FIELD1) // IGNORED
            .startObject()
            .endObject()
            .endArray()
            .field(FIELD2, true)
            .endObject());
    }

    private BytesReference source(final Object value1, final Object value2) throws IOException{
        return BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field(FIELD1, value1)
            .field(FIELD2, value2)
            .endObject());
    }

    private Document parse(final boolean field1IgnoreMalformed, final BytesReference source) throws IOException{
        final String mapping = this.mapping(field1IgnoreMalformed);
        final DocumentMapper mapper = this.parser.parse("type", new CompressedXContent(mapping));
        return mapper.parse(SourceToParse.source("test", "type", "1", source, XContentType.JSON)).rootDoc();
    }

    private void parseAndCheck(final boolean field1IgnoreMalformed,
                               final Object value,
                               final String expected) throws IOException
    {
        this.parseAndCheck(field1IgnoreMalformed, this.source(value, value), expected, expected);
    }

    private String mapping(final boolean field1IgnoreMalformed) throws IOException{
        return Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject(FIELD1)
            .field("type", "boolean")
            .field("ignore_malformed", field1IgnoreMalformed)
            .endObject()
            .startObject(FIELD2)
            .field("type", "boolean")
            .endObject()
            .endObject()
            .endObject()
            .endObject());
    }

    private void parseAndCheck(final boolean field1IgnoreMalformed,
                               final BytesReference source,
                               final String expected1,
                               final String expected2) throws IOException{
        final Document document = this.parse(field1IgnoreMalformed, source);
        assertEquals(FIELD2, expected2, document.get(FIELD2));
        assertEquals(FIELD1, expected1, document.get(FIELD1));
    }

    private void parseFails(final Object value) throws IOException{
        parseFails(FIELD1_DONT_IGNORE_MALFORMED, this.source(value, null));
    }

    private void parseFails(final boolean field1IgnoreMalformed,
                            final BytesReference source) {
        MapperParsingException ex = expectThrows(MapperParsingException.class,
            () -> parse(field1IgnoreMalformed, source));
        assertEquals("failed to parse [field1]", ex.getMessage());
    }

    public void testMultiFields() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "boolean")
                        .startObject("fields")
                            .startObject("as_string")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject().endObject()
                .endObject().endObject());
        DocumentMapper mapper = indexService.mapperService()
            .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());
        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .field("field", false)
                .endObject());
        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", source, XContentType.JSON));
        assertNotNull(doc.rootDoc().getField("field.as_string"));
    }

    public void testDocValues() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("bool1")
                    .field("type", "boolean")
                .endObject()
                .startObject("bool2")
                    .field("type", "boolean")
                    .field("index", false)
                .endObject()
                .startObject("bool3")
                    .field("type", "boolean")
                    .field("index", true)
                .endObject()
                .endObject()
                .endObject().endObject());

        DocumentMapper defaultMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument parsedDoc = defaultMapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("bool1", true)
                        .field("bool2", true)
                        .field("bool3", true)
                        .endObject()),
                XContentType.JSON));
        Document doc = parsedDoc.rootDoc();
        IndexableField[] fields = doc.getFields("bool1");
        assertEquals(2, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
        assertEquals(DocValuesType.SORTED_NUMERIC, fields[1].fieldType().docValuesType());
        fields = doc.getFields("bool2");
        assertEquals(1, fields.length);
        assertEquals(DocValuesType.SORTED_NUMERIC, fields[0].fieldType().docValuesType());
        fields = doc.getFields("bool3");
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
        assertEquals(DocValuesType.SORTED_NUMERIC, fields[1].fieldType().docValuesType());
    }

    public void testEmptyName() throws IOException {
        // after 5.x
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("").field("type", "boolean").endObject().endObject()
            .endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }
}
