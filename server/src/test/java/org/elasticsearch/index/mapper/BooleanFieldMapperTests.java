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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

public class BooleanFieldMapperTests extends ESSingleNodeTestCase {

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

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", true)
                        .endObject()),
                XContentType.JSON));

        try (Directory dir = new ByteBuffersDirectory();
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
        Mapper mapper = defaultMapper.mappers().getMapper("field");
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

    public void testParsesBooleansStrict() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "boolean")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));
        // omit "false"/"true" here as they should still be parsed correctly
        String randomValue = randomFrom("off", "no", "0", "on", "yes", "1");
        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .field("field", randomValue)
                .endObject());
        MapperParsingException ex = expectThrows(MapperParsingException.class,
                () -> defaultMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON)));
        assertEquals("failed to parse field [field] of type [boolean] in document with id '1'. " +
            "Preview of field's value: '" + randomValue + "'", ex.getMessage());
    }


    public void testParsesBooleansNestedStrict() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "boolean")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());
        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));
        // omit "false"/"true" here as they should still be parsed correctly
        String randomValue = "no";
        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("field")
                    .field("inner_field", randomValue)
                .endObject()
            .endObject());
        MapperParsingException ex = expectThrows(MapperParsingException.class,
                () -> defaultMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON)));
        assertEquals("failed to parse field [field] of type [boolean] in document with id '1'. " +
            "Preview of field's value: '{inner_field=" + randomValue + "}'", ex.getMessage());
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
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));
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

        ParsedDocument parsedDoc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
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

    public void testMeta() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "boolean")
                .field("meta", Collections.singletonMap("foo", "bar"))
                .endObject().endObject().endObject().endObject());

        DocumentMapper mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "boolean")
                .endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping2, mapper.mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "boolean")
                .field("meta", Collections.singletonMap("baz", "quux"))
                .endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testBoosts() throws Exception {
        String mapping = "{\"_doc\":{\"properties\":{\"field\":{\"type\":\"boolean\",\"boost\":2.0}}}}";
        DocumentMapper mapper = indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        MappedFieldType ft = indexService.mapperService().fieldType("field");
        assertEquals(new BoostQuery(new TermQuery(new Term("field", "T")), 2.0f), ft.termQuery("true", null));
    }

    public void testParseSourceValue() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());
        BooleanFieldMapper mapper = new BooleanFieldMapper.Builder("field").build(context);

        assertTrue(mapper.parseSourceValue(true));
        assertFalse(mapper.parseSourceValue("false"));
        assertFalse(mapper.parseSourceValue(""));
    }
}
