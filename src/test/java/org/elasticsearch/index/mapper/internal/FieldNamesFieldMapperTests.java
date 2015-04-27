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

package org.elasticsearch.index.mapper.internal;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

public class FieldNamesFieldMapperTests extends ElasticsearchSingleNodeTest {

    private static SortedSet<String> extract(String path) {
        SortedSet<String> set = new TreeSet<>();
        for (String fieldName : FieldNamesFieldMapper.extractFieldNames(path)) {
            set.add(fieldName);
        }
        return set;
    }

    private static <T> SortedSet<T> set(T... values) {
        return new TreeSet<>(Arrays.asList(values));
    }

    void assertFieldNames(SortedSet<String> expected, ParsedDocument doc) {
        String[] got = doc.rootDoc().getValues("_field_names");
        assertEquals(expected, set(got));
    }

    public void testExtractFieldNames() {
        assertEquals(set("abc"), extract("abc"));
        assertEquals(set("a", "a.b"), extract("a.b"));
        assertEquals(set("a", "a.b", "a.b.c"), extract("a.b.c"));
        // and now corner cases
        assertEquals(set("", ".a"), extract(".a"));
        assertEquals(set("a", "a."), extract("a."));
        assertEquals(set("", ".", ".."), extract(".."));
    }

    public void testInjectIntoDocDuringParsing() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = defaultMapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("a", "100")
                    .startObject("b")
                        .field("c", 42)
                    .endObject()
                .endObject()
                .bytes());
        
        assertFieldNames(set("a", "b", "b.c", "_uid", "_type", "_version", "_source", "_all"), doc);
    }
    
    public void testExplicitEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", true).endObject()
            .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldNamesFieldMapper fieldNamesMapper = docMapper.rootMapper(FieldNamesFieldMapper.class);
        assertTrue(fieldNamesMapper.enabled());

        ParsedDocument doc = docMapper.parse("type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes());

        assertFieldNames(set("field", "_uid", "_type", "_version", "_source", "_all"), doc);
    }

    public void testDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", false).endObject()
            .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        FieldNamesFieldMapper fieldNamesMapper = docMapper.rootMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.enabled());

        ParsedDocument doc = docMapper.parse("type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes());
        
        assertNull(doc.rootDoc().get("_field_names"));
    }
    
    public void testPre13Disabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        Settings indexSettings = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_2_4.id).build();
        DocumentMapper docMapper = createIndex("test", indexSettings).mapperService().documentMapperParser().parse(mapping);
        FieldNamesFieldMapper fieldNamesMapper = docMapper.rootMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.enabled());
    }
    
    public void testDisablingBackcompat() throws Exception {
        // before 1.5, disabling happened by setting index:no
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("index", "no").endObject()
            .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        DocumentMapper docMapper = createIndex("test", indexSettings).mapperService().documentMapperParser().parse(mapping);
        FieldNamesFieldMapper fieldNamesMapper = docMapper.rootMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.enabled());

        ParsedDocument doc = docMapper.parse("type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes());

        assertNull(doc.rootDoc().get("_field_names"));
    }

    public void testFieldTypeSettingsBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("store", "yes").endObject()
            .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        DocumentMapper docMapper = createIndex("test", indexSettings).mapperService().documentMapperParser().parse(mapping);
        FieldNamesFieldMapper fieldNamesMapper = docMapper.rootMapper(FieldNamesFieldMapper.class);
        assertTrue(fieldNamesMapper.fieldType().stored());
    }

    public void testMergingMappings() throws Exception {
        String enabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", true).endObject()
            .endObject().endObject().string();
        String disabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", false).endObject()
            .endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        
        DocumentMapper mapperEnabled = parser.parse(enabledMapping);
        DocumentMapper mapperDisabled = parser.parse(disabledMapping);
        mapperEnabled.merge(mapperDisabled.mapping(), false);
        assertFalse(mapperEnabled.rootMapper(FieldNamesFieldMapper.class).enabled());

        mapperEnabled = parser.parse(enabledMapping);
        mapperDisabled.merge(mapperEnabled.mapping(), false);
        assertTrue(mapperEnabled.rootMapper(FieldNamesFieldMapper.class).enabled());
    }
}
