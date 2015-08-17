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

package org.elasticsearch.index.mapper.murmur3;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.Arrays;

public class Murmur3FieldMapperTests extends ESSingleNodeTestCase {

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void before() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
        parser.putTypeParser(Murmur3FieldMapper.CONTENT_TYPE, new Murmur3FieldMapper.TypeParser());
    }

    public void testDefaults() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "murmur3")
                .endObject().endObject().endObject().endObject().string();
        DocumentMapper mapper = parser.parse(mapping);
        ParsedDocument parsedDoc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject().field("field", "value").endObject().bytes());
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertNotNull(fields);
        assertEquals(Arrays.toString(fields), 1, fields.length);
        IndexableField field = fields[0];
        assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_NUMERIC, field.fieldType().docValuesType());
    }

    public void testDocValuesSettingNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "murmur3")
                .field("doc_values", false)
            .endObject().endObject().endObject().endObject().string();
        try {
            parser.parse(mapping);
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [doc_values] cannot be modified"));
        }

        // even setting to the default is not allowed, the setting is invalid
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "murmur3")
                .field("doc_values", true)
            .endObject().endObject().endObject().endObject().string();
        try {
            parser.parse(mapping);
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [doc_values] cannot be modified"));
        }
    }

    public void testIndexSettingNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "murmur3")
                .field("index", "not_analyzed")
            .endObject().endObject().endObject().endObject().string();
        try {
            parser.parse(mapping);
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [index] cannot be modified"));
        }

        // even setting to the default is not allowed, the setting is invalid
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "murmur3")
                .field("index", "no")
            .endObject().endObject().endObject().endObject().string();
        try {
            parser.parse(mapping);
            fail("expected a mapper parsing exception");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage().contains("Setting [index] cannot be modified"));
        }
    }

    public void testDocValuesSettingBackcompat() throws Exception {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        indexService = createIndex("test_bwc", settings);
        parser = indexService.mapperService().documentMapperParser();
        parser.putTypeParser(Murmur3FieldMapper.CONTENT_TYPE, new Murmur3FieldMapper.TypeParser());
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
                .field("type", "murmur3")
                .field("doc_values", false)
            .endObject().endObject().endObject().endObject().string();

        DocumentMapper docMapper = parser.parse(mapping);
        Murmur3FieldMapper mapper = (Murmur3FieldMapper)docMapper.mappers().getMapper("field");
        assertFalse(mapper.fieldType().hasDocValues());
    }

    public void testIndexSettingBackcompat() throws Exception {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        indexService = createIndex("test_bwc", settings);
        parser = indexService.mapperService().documentMapperParser();
        parser.putTypeParser(Murmur3FieldMapper.CONTENT_TYPE, new Murmur3FieldMapper.TypeParser());
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field")
            .field("type", "murmur3")
            .field("index", "not_analyzed")
            .endObject().endObject().endObject().endObject().string();

        DocumentMapper docMapper = parser.parse(mapping);
        Murmur3FieldMapper mapper = (Murmur3FieldMapper)docMapper.mappers().getMapper("field");
        assertEquals(IndexOptions.DOCS, mapper.fieldType().indexOptions());
    }

    // TODO: add more tests
}
