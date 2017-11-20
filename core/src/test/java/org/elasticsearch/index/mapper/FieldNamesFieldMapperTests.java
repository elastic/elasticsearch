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

import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class FieldNamesFieldMapperTests extends ESSingleNodeTestCase {

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

    void assertFieldNames(Set<String> expected, ParsedDocument doc) {
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

    public void testFieldType() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").endObject()
            .endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().hasDocValues());
        assertEquals(IndexOptions.DOCS, fieldNamesMapper.fieldType().indexOptions());
        assertFalse(fieldNamesMapper.fieldType().tokenized());
        assertFalse(fieldNamesMapper.fieldType().stored());
        assertTrue(fieldNamesMapper.fieldType().omitNorms());
    }

    public void testInjectIntoDocDuringParsing() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(SourceToParse.source("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("a", "100")
                    .startObject("b")
                        .field("c", 42)
                    .endObject()
                .endObject()
                .bytes(),
                XContentType.JSON));

        assertFieldNames(Collections.emptySet(), doc);
    }

    public void testExplicitEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", true).endObject()
                .startObject("properties").startObject("field").field("type", "keyword").field("doc_values", false).endObject().endObject()
            .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertTrue(fieldNamesMapper.fieldType().isEnabled());

        ParsedDocument doc = docMapper.parse(SourceToParse.source("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes(),
            XContentType.JSON));

        assertFieldNames(set("field"), doc);
    }

    public void testDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", false).endObject()
            .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().isEnabled());

        ParsedDocument doc = docMapper.parse(SourceToParse.source("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes(),
            XContentType.JSON));

        assertNull(doc.rootDoc().get("_field_names"));
    }

    public void testMergingMappings() throws Exception {
        String enabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", true).endObject()
            .endObject().endObject().string();
        String disabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", false).endObject()
            .endObject().endObject().string();
        MapperService mapperService = createIndex("test").mapperService();

        DocumentMapper mapperEnabled = mapperService.merge("type", new CompressedXContent(enabledMapping), MapperService.MergeReason.MAPPING_UPDATE, false);
        DocumentMapper mapperDisabled = mapperService.merge("type", new CompressedXContent(disabledMapping), MapperService.MergeReason.MAPPING_UPDATE, false);
        assertFalse(mapperDisabled.metadataMapper(FieldNamesFieldMapper.class).fieldType().isEnabled());

        mapperEnabled = mapperService.merge("type", new CompressedXContent(enabledMapping), MapperService.MergeReason.MAPPING_UPDATE, false);
        assertTrue(mapperEnabled.metadataMapper(FieldNamesFieldMapper.class).fieldType().isEnabled());
    }
}
