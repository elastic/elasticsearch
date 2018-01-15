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
import org.elasticsearch.common.Booleans;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class BooleanFieldMapperTests extends ESSingleNodeTestCase {
    private IndexService indexService;
    private DocumentMapperParser parser;
    private DocumentMapperParser preEs6Parser;

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();

        IndexService preEs6IndexService = createIndex("legacy",
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_0_0).build());
        preEs6Parser = preEs6IndexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDefaults() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "boolean").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(SourceToParse.source("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", true)
                .endObject()
                .bytes(),
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
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "boolean").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));
        FieldMapper mapper = defaultMapper.mappers().getMapper("field");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"boolean\"}}", builder.string());

        // now change some parameters
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "boolean")
                    .field("doc_values", "false")
                    .field("null_value", true)
                .endObject().endObject()
                .endObject().endObject().string();

        defaultMapper = parser.parse("type", new CompressedXContent(mapping));
        mapper = defaultMapper.mappers().getMapper("field");
        builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\"field\":{\"type\":\"boolean\",\"doc_values\":false,\"null_value\":true}}", builder.string());
    }

    public void testParsesPreEs6BooleansLenient() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field1")
                            .field("type", "boolean")
                        .endObject()
                        .startObject("field2")
                            .field("type", "boolean")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject().string();
        DocumentMapper defaultMapper = preEs6Parser.parse("type", new CompressedXContent(mapping));

        String falsy = randomFrom("false", "off", "no", "0");
        String truthy = randomFrom("true", "on", "yes", "1");

        ParsedDocument parsedDoc = defaultMapper.parse(SourceToParse.source("legacy", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
                .field("field1", falsy)
                .field("field2", truthy)
            .endObject()
            .bytes(),
            XContentType.JSON));
        Document doc = parsedDoc.rootDoc();
        assertEquals("F", doc.getField("field1").stringValue());
        assertEquals("T", doc.getField("field2").stringValue());

        List<String> expectedDeprecationWarnings = new ArrayList<>();
        if (Booleans.isBoolean(falsy) == false) {
            expectedDeprecationWarnings.add("Expected a boolean for property [field1] but got ["+ falsy + "]");
        }
        if (Booleans.isBoolean(truthy) == false) {
            expectedDeprecationWarnings.add("Expected a boolean for property [field2] but got [" + truthy + "]");
        }

        if (expectedDeprecationWarnings.isEmpty() == false) {
            assertWarnings(expectedDeprecationWarnings.toArray(new String[1]));
        }
    }

    public void testParsesEs6BooleansStrict() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "boolean")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject().string();
        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                    // omit "false"/"true" here as they should still be parsed correctly
                    .field("field", randomFrom("off", "no", "0", "on", "yes", "1"))
                .endObject().bytes();
        MapperParsingException ex = expectThrows(MapperParsingException.class, 
                () -> defaultMapper.parse(SourceToParse.source("test", "type", "1", source, XContentType.JSON)));
        assertEquals("failed to parse [field]", ex.getMessage());
    }

    public void testMultiFields() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "boolean")
                        .startObject("fields")
                            .startObject("as_string")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = indexService.mapperService()
            .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE, false);
        assertEquals(mapping, mapper.mappingSource().toString());
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                    .field("field", false)
                .endObject().bytes();
        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", source, XContentType.JSON));
        assertNotNull(doc.rootDoc().getField("field.as_string"));
    }

    public void testDocValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
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
                .endObject().endObject().string();

        DocumentMapper defaultMapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument parsedDoc = defaultMapper.parse(SourceToParse.source("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("bool1", true)
                .field("bool2", true)
                .field("bool3", true)
                .endObject()
                .bytes(),
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
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("").field("type", "boolean").endObject().endObject()
            .endObject().endObject().string();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }
}
