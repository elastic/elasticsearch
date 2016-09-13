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
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static org.hamcrest.Matchers.containsString;

public class NumberFieldMapperTests extends ESSingleNodeTestCase {

    private static final Set<String> TYPES = new HashSet<>(Arrays.asList("byte", "short", "integer", "long", "float", "double"));

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void before() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    public void testDefaults() throws Exception {
        for (String type : TYPES) {
            doTestDefaults(type);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void doTestDefaults(String type) throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", 123)
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNotIndexed() throws Exception {
        for (String type : TYPES) {
            doTestNotIndexed(type);
        }
    }

    public void doTestNotIndexed(String type) throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("index", false).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", 123)
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {
        for (String type : TYPES) {
            doTestNotIndexed(type);
        }
    }

    public void doTestNoDocValues(String type) throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("doc_values", false).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", 123)
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
    }

    public void testStore() throws Exception {
        for (String type : TYPES) {
            doTestStore(type);
        }
    }

    public void doTestStore(String type) throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("store", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", 123)
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(123, storedField.numericValue().doubleValue(), 0d);
    }

    public void testCoerce() throws Exception {
        for (String type : TYPES) {
            doTestCoerce(type);
        }
    }

    public void doTestCoerce(String type) throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "123")
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("coerce", false).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper2.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper2.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "123")
                .endObject()
                .bytes());
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    public void testIgnoreMalformed() throws Exception {
        for (String type : TYPES) {
            doTestIgnoreMalformed(type);
        }
    }

    public void doTestIgnoreMalformed(String type) throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "a")
                .endObject()
                .bytes());
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("For input string: \"a\""));

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("ignore_malformed", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper2.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "a")
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
    }

    public void testIncludeInAll() throws Exception {
        for (String type : TYPES) {
            doTestIncludeInAll(type);
        }
    }

    public void doTestIncludeInAll(String type) throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type)
                .field("include_in_all", true).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", 123)
                .endObject()
                .bytes());

        IndexableField[] fields = doc.rootDoc().getFields("_all");
        assertEquals(1, fields.length);
        assertEquals("123", fields[0].stringValue());

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", 123)
                .endObject()
                .bytes());

        fields = doc.rootDoc().getFields("_all");
        assertEquals(0, fields.length);
    }

    public void testRejectNorms() throws IOException {
        // not supported as of 5.0
        for (String type : Arrays.asList("byte", "short", "integer", "long", "float", "double")) {
            DocumentMapperParser parser = createIndex("index-" + type).mapperService().documentMapperParser();
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", type)
                        .field("norms", random().nextBoolean())
                    .endObject()
                .endObject().endObject().endObject().string();
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> parser.parse("type", new CompressedXContent(mapping)));
            assertThat(e.getMessage(), containsString("Mapping definition for [foo] has unsupported parameters:  [norms"));
        }
    }

    public void testNullValue() throws IOException {
        for (String type : TYPES) {
            doTestNullValue(type);
        }
    }

    private void doTestNullValue(String type) throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", type)
                        .endObject()
                    .endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .nullField("field")
                .endObject()
                .bytes());
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        Object missing;
        if (Arrays.asList("float", "double").contains(type)) {
            missing = 123d;
        } else {
            missing = 123L;
        }
        mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", type)
                            .field("null_value", missing)
                        .endObject()
                    .endObject()
                .endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .nullField("field")
                .endObject()
                .bytes());
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    public void testEmptyName() throws IOException {
        // after version 5
        for (String type : TYPES) {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("").field("type", type).endObject().endObject()
                .endObject().endObject().string();

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping))
            );
            assertThat(e.getMessage(), containsString("name cannot be empty string"));
        }

        // before 5.x
        Version oldVersion = VersionUtils.randomVersionBetween(getRandom(), Version.V_2_0_0, Version.V_2_3_5);
        Settings oldIndexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, oldVersion).build();
        indexService = createIndex("test_old", oldIndexSettings);
        parser = indexService.mapperService().documentMapperParser();
        for (String type : TYPES) {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("").field("type", type).endObject().endObject()
                .endObject().endObject().string();

            DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));
            assertEquals(mapping, defaultMapper.mappingSource().string());
        }
    }
}
