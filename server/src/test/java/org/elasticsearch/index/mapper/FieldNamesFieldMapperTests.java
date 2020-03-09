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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;

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

    private static SortedSet<String> set(String... values) {
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
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").endObject()
            .endObject().endObject());

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().hasDocValues());
        assertEquals(IndexOptions.DOCS, fieldNamesMapper.fieldType().indexOptions());
        assertFalse(fieldNamesMapper.fieldType().tokenized());
        assertFalse(fieldNamesMapper.fieldType().stored());
        assertTrue(fieldNamesMapper.fieldType().omitNorms());
    }

    public void testInjectIntoDocDuringParsing() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("a", "100")
                            .startObject("b")
                                .field("c", 42)
                            .endObject()
                        .endObject()),
                XContentType.JSON));

        assertFieldNames(Collections.emptySet(), doc);
    }

    public void testUsingEnabledSettingThrows() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", true).endObject()
            .startObject("properties")
            .startObject("field").field("type", "keyword").field("doc_values", false).endObject()
            .endObject().endObject().endObject());
        MapperParsingException ex = expectThrows(MapperParsingException.class,
                () -> createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping)));

        assertEquals("The `enabled` setting for the `_field_names` field has been deprecated and removed but is still used in index [{}]. "
                + "Please remove it from your mappings and templates.", ex.getMessage());
    }

    /**
     * disabling the _field_names should still work for indices before 8.0
     */
    public void testUsingEnabledBefore8() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", false).endObject()
            .endObject().endObject());

        DocumentMapper docMapper = createIndex("test",
                Settings.builder()
                        .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(),
                                VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0))
                        .build()).mapperService()
                        .documentMapperParser()
            .parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().isEnabled());

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()),
            XContentType.JSON));

        assertNull(doc.rootDoc().get("_field_names"));
        assertWarnings(FieldNamesFieldMapper.TypeParser.ENABLED_DEPRECATION_MESSAGE.replace("{}", "test"));
    }

    /**
     * Merging the "_field_names" enabled setting is forbidden in 8.0, but we still want to tests the behavior on pre-8 indices
     */
    public void testMergingMappingsBefore8() throws Exception {
        String enabledMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", true).endObject()
            .endObject().endObject());
        String disabledMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", false).endObject()
            .endObject().endObject());
        MapperService mapperService = createIndex("test", Settings.builder()
                .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(),
                        VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0))
                .build()).mapperService();

        DocumentMapper mapperEnabled = mapperService.merge("type", new CompressedXContent(enabledMapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        DocumentMapper mapperDisabled = mapperService.merge("type", new CompressedXContent(disabledMapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        assertFalse(mapperDisabled.metadataMapper(FieldNamesFieldMapper.class).fieldType().isEnabled());

        mapperEnabled = mapperService.merge("type", new CompressedXContent(enabledMapping), MapperService.MergeReason.MAPPING_UPDATE);
        assertTrue(mapperEnabled.metadataMapper(FieldNamesFieldMapper.class).fieldType().isEnabled());
        assertWarnings(FieldNamesFieldMapper.TypeParser.ENABLED_DEPRECATION_MESSAGE.replace("{}", "test"));
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        /**
         * This is needed to force the index version with {@link IndexMetaData.SETTING_INDEX_VERSION_CREATED}.
         */
        return false;
    }
}
