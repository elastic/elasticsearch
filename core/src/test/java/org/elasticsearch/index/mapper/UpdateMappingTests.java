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

import org.elasticsearch.Version;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;


public class UpdateMappingTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testConflictFieldsMapping(String fieldName) throws Exception {
        //test store, ... all the parameters that are not to be changed just like in other fields
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject(fieldName)
                        .field("enabled", true)
                            .field("store", false)
                        .endObject()
                    .endObject()
            .endObject();
        XContentBuilder mappingUpdate = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject(fieldName)
                        .field("enabled", true)
                        .field("store", true)
                    .endObject()
                    .startObject("properties")
                        .startObject("text")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        testConflictWhileMergingAndMappingUnchanged(mapping, mappingUpdate);
    }

    protected void testConflictWhileMergingAndMappingUnchanged(XContentBuilder mapping, XContentBuilder mappingUpdate) throws IOException {
        IndexService indexService = createIndex("test", Settings.builder().build(), "type", mapping);
        CompressedXContent mappingBeforeUpdate = indexService.mapperService().documentMapper("type").mappingSource();
        // simulate like in MetaDataMappingService#putMapping
        try {
            indexService.mapperService().merge("type", new CompressedXContent(mappingUpdate.bytes()), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        // make sure simulate flag actually worked - no mappings applied
        CompressedXContent mappingAfterUpdate = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterUpdate, equalTo(mappingBeforeUpdate));
    }

    public void testConflictSameType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "long").endObject()
                .endObject().endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.builder().build(), "type", mapping).mapperService();

        XContentBuilder update = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "double").endObject()
                .endObject().endObject().endObject();

        try {
            mapperService.merge("type", new CompressedXContent(update.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        try {
            mapperService.merge("type", new CompressedXContent(update.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        assertThat(((FieldMapper) mapperService.documentMapper("type").mapping().root().getMapper("foo")).fieldType().typeName(),
                equalTo("long"));
    }

    public void testConflictNewType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("foo").field("type", "long").endObject()
                .endObject().endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.builder().build(), "type1", mapping).mapperService();

        XContentBuilder update = XContentFactory.jsonBuilder().startObject().startObject("type2")
                .startObject("properties").startObject("foo").field("type", "double").endObject()
                .endObject().endObject().endObject();

        try {
            mapperService.merge("type2", new CompressedXContent(update.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
            assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        try {
            mapperService.merge("type2", new CompressedXContent(update.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
            assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        assertThat(((FieldMapper) mapperService.documentMapper("type1").mapping().root().getMapper("foo")).fieldType().typeName(),
                equalTo("long"));
        assertNull(mapperService.documentMapper("type2"));
    }

    // same as the testConflictNewType except that the mapping update is on an existing type
    public void testConflictNewTypeUpdate() throws Exception {
        XContentBuilder mapping1 = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("foo").field("type", "long").endObject()
                .endObject().endObject().endObject();
        XContentBuilder mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type2").endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.builder().put("index.version.created",
            Version.V_5_6_0).build()).mapperService();

        mapperService.merge("type1", new CompressedXContent(mapping1.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mapperService.merge("type2", new CompressedXContent(mapping2.string()), MapperService.MergeReason.MAPPING_UPDATE, false);

        XContentBuilder update = XContentFactory.jsonBuilder().startObject().startObject("type2")
                .startObject("properties").startObject("foo").field("type", "double").endObject()
                .endObject().endObject().endObject();

        try {
            mapperService.merge("type2", new CompressedXContent(update.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
            assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        try {
            mapperService.merge("type2", new CompressedXContent(update.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
            assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        assertThat(((FieldMapper) mapperService.documentMapper("type1").mapping().root().getMapper("foo")).fieldType().typeName(),
                equalTo("long"));
        assertNotNull(mapperService.documentMapper("type2"));
        assertNull(mapperService.documentMapper("type2").mapping().root().getMapper("foo"));
    }

    public void testReuseMetaField() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("_id").field("type", "text").endObject()
                .endObject().endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.builder().build()).mapperService();

        try {
            mapperService.merge("type", new CompressedXContent(mapping.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Field [_id] is defined twice in [type]"));
        }

        try {
            mapperService.merge("type", new CompressedXContent(mapping.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Field [_id] is defined twice in [type]"));
        }
    }

    public void testRejectFieldDefinedTwice() throws IOException {
        String mapping1 = XContentFactory.jsonBuilder().startObject()
                .startObject("type1")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "object")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();
        String mapping2 = XContentFactory.jsonBuilder().startObject()
                .startObject("type2")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "long")
                        .endObject()
                    .endObject()
                .endObject().endObject().string();

        MapperService mapperService1 = createIndex("test1").mapperService();
        mapperService1.merge("type1", new CompressedXContent(mapping1), MergeReason.MAPPING_UPDATE, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapperService1.merge("type2", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE, false));
        assertThat(e.getMessage(), equalTo("[foo] is defined as a field in mapping [type2"
                + "] but this name is already used for an object in other types"));

        MapperService mapperService2 = createIndex("test2").mapperService();
        mapperService2.merge("type2", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE, false);
        e = expectThrows(IllegalArgumentException.class,
                () -> mapperService2.merge("type1", new CompressedXContent(mapping1), MergeReason.MAPPING_UPDATE, false));
        assertThat(e.getMessage(), equalTo("[foo] is defined as an object in mapping [type1"
                + "] but this name is already used for a field in other types"));
    }
}
