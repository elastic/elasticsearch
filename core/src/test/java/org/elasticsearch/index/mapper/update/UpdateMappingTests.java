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

package org.elasticsearch.index.mapper.update;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;


public class UpdateMappingTests extends ESSingleNodeTestCase {
    public void testAllEnabledAfterDisabled() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", false).endObject().endObject();
        XContentBuilder mappingUpdate = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", true).endObject().startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject();
        testConflictWhileMergingAndMappingUnchanged(mapping, mappingUpdate);
    }

    public void testAllDisabledAfterEnabled() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", true).endObject().endObject();
        XContentBuilder mappingUpdate = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", false).endObject().startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject();
        testConflictWhileMergingAndMappingUnchanged(mapping, mappingUpdate);
    }

    public void testAllDisabledAfterDefaultEnabled() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties").startObject("some_text").field("type", "string").endObject().endObject().endObject();
        XContentBuilder mappingUpdate = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", false).endObject().startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject();
        testConflictWhileMergingAndMappingUnchanged(mapping, mappingUpdate);
    }

    public void testAllEnabledAfterEnabled() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", true).endObject().endObject();
        XContentBuilder mappingUpdate = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", true).endObject().startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject();
        XContentBuilder expectedMapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("_all").field("enabled", true).endObject().startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject().endObject();
        testNoConflictWhileMergingAndMappingChanged(mapping, mappingUpdate, expectedMapping);
    }

    public void testAllDisabledAfterDisabled() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", false).endObject().endObject();
        XContentBuilder mappingUpdate = XContentFactory.jsonBuilder().startObject().startObject("_all").field("enabled", false).endObject().startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject();
        XContentBuilder expectedMapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("_all").field("enabled", false).endObject().startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject().endObject();
        testNoConflictWhileMergingAndMappingChanged(mapping, mappingUpdate, expectedMapping);
    }

    private void testNoConflictWhileMergingAndMappingChanged(XContentBuilder mapping, XContentBuilder mappingUpdate, XContentBuilder expectedMapping) throws IOException {
        IndexService indexService = createIndex("test", Settings.settingsBuilder().build(), "type", mapping);
        // simulate like in MetaDataMappingService#putMapping
        MergeResult mergeResult = indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingUpdate.bytes()), true).mapping(), false, false);
        // assure we have no conflicts
        assertThat(mergeResult.buildConflicts().length, equalTo(0));
        // make sure mappings applied
        CompressedXContent mappingAfterUpdate = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterUpdate.toString(), equalTo(expectedMapping.string()));
    }

    public void testConflictFieldsMapping(String fieldName) throws Exception {
        //test store, ... all the parameters that are not to be changed just like in other fields
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject(fieldName).field("enabled", true).field("store", "no").endObject()
                .endObject().endObject();
        XContentBuilder mappingUpdate = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject(fieldName).field("enabled", true).field("store", "yes").endObject()
                .startObject("properties").startObject("text").field("type", "string").endObject().endObject()
                .endObject().endObject();
        testConflictWhileMergingAndMappingUnchanged(mapping, mappingUpdate);
    }

    protected void testConflictWhileMergingAndMappingUnchanged(XContentBuilder mapping, XContentBuilder mappingUpdate) throws IOException {
        IndexService indexService = createIndex("test", Settings.settingsBuilder().build(), "type", mapping);
        CompressedXContent mappingBeforeUpdate = indexService.mapperService().documentMapper("type").mappingSource();
        // simulate like in MetaDataMappingService#putMapping
        MergeResult mergeResult = indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingUpdate.bytes()), true).mapping(), true, false);
        // assure we have conflicts
        assertThat(mergeResult.buildConflicts().length, equalTo(1));
        // make sure simulate flag actually worked - no mappings applied
        CompressedXContent mappingAfterUpdate = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterUpdate, equalTo(mappingBeforeUpdate));
    }

    public void testConflictSameType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "long").endObject()
                .endObject().endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.settingsBuilder().build(), "type", mapping).mapperService();

        XContentBuilder update = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "double").endObject()
                .endObject().endObject().endObject();

        try {
            mapperService.merge("type", new CompressedXContent(update.string()), false, false);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        try {
            mapperService.merge("type", new CompressedXContent(update.string()), false, false);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        assertTrue(mapperService.documentMapper("type").mapping().root().getMapper("foo") instanceof LongFieldMapper);
    }

    public void testConflictNewType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("foo").field("type", "long").endObject()
                .endObject().endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.settingsBuilder().build(), "type1", mapping).mapperService();

        XContentBuilder update = XContentFactory.jsonBuilder().startObject().startObject("type2")
                .startObject("properties").startObject("foo").field("type", "double").endObject()
                .endObject().endObject().endObject();

        try {
            mapperService.merge("type2", new CompressedXContent(update.string()), false, false);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
            assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        try {
            mapperService.merge("type2", new CompressedXContent(update.string()), false, false);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
            assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        assertTrue(mapperService.documentMapper("type1").mapping().root().getMapper("foo") instanceof LongFieldMapper);
        assertNull(mapperService.documentMapper("type2"));
    }

    // same as the testConflictNewType except that the mapping update is on an existing type
    public void testConflictNewTypeUpdate() throws Exception {
        XContentBuilder mapping1 = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("foo").field("type", "long").endObject()
                .endObject().endObject().endObject();
        XContentBuilder mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type2").endObject().endObject();
        MapperService mapperService = createIndex("test", Settings.settingsBuilder().build()).mapperService();

        mapperService.merge("type1", new CompressedXContent(mapping1.string()), false, false);
        mapperService.merge("type2", new CompressedXContent(mapping2.string()), false, false);

        XContentBuilder update = XContentFactory.jsonBuilder().startObject().startObject("type2")
                .startObject("properties").startObject("foo").field("type", "double").endObject()
                .endObject().endObject().endObject();

        try {
            mapperService.merge("type2", new CompressedXContent(update.string()), false, false);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
            assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        try {
            mapperService.merge("type2", new CompressedXContent(update.string()), false, false);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
            assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] cannot be changed from type [long] to [double]"));
        }

        assertTrue(mapperService.documentMapper("type1").mapping().root().getMapper("foo") instanceof LongFieldMapper);
        assertNotNull(mapperService.documentMapper("type2"));
        assertNull(mapperService.documentMapper("type2").mapping().root().getMapper("foo"));
    }

    public void testIndexFieldParsingBackcompat() throws IOException {
        IndexService indexService = createIndex("test", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build());
        XContentBuilder indexMapping = XContentFactory.jsonBuilder();
        boolean enabled = randomBoolean();
        indexMapping.startObject()
                .startObject("type")
                .startObject("_index")
                .field("enabled", enabled)
                .endObject()
                .endObject()
                .endObject();
        DocumentMapper documentMapper = indexService.mapperService().parse("type", new CompressedXContent(indexMapping.string()), true);
        assertThat(documentMapper.indexMapper().enabled(), equalTo(enabled));
        documentMapper = indexService.mapperService().parse("type", new CompressedXContent(documentMapper.mappingSource().string()), true);
        assertThat(documentMapper.indexMapper().enabled(), equalTo(enabled));
    }

    public void testTimestampParsing() throws IOException {
        IndexService indexService = createIndex("test", Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build());
        XContentBuilder indexMapping = XContentFactory.jsonBuilder();
        boolean enabled = randomBoolean();
        indexMapping.startObject()
                .startObject("type")
                .startObject("_timestamp")
                .field("enabled", enabled)
                .field("store", true)
                .startObject("fielddata")
                .field("format", "doc_values")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        DocumentMapper documentMapper = indexService.mapperService().parse("type", new CompressedXContent(indexMapping.string()), true);
        assertThat(documentMapper.timestampFieldMapper().enabled(), equalTo(enabled));
        assertTrue(documentMapper.timestampFieldMapper().fieldType().stored());
        assertTrue(documentMapper.timestampFieldMapper().fieldType().hasDocValues());
        documentMapper = indexService.mapperService().parse("type", new CompressedXContent(documentMapper.mappingSource().string()), true);
        assertThat(documentMapper.timestampFieldMapper().enabled(), equalTo(enabled));
        assertTrue(documentMapper.timestampFieldMapper().fieldType().hasDocValues());
        assertTrue(documentMapper.timestampFieldMapper().fieldType().stored());
    }

    public void testSizeTimestampIndexParsing() throws IOException {
        IndexService indexService = createIndex("test", Settings.settingsBuilder().build());
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/update/default_mapping_with_disabled_root_types.json");
        DocumentMapper documentMapper = indexService.mapperService().parse("type", new CompressedXContent(mapping), true);
        assertThat(documentMapper.mappingSource().string(), equalTo(mapping));
        documentMapper = indexService.mapperService().parse("type", new CompressedXContent(documentMapper.mappingSource().string()), true);
        assertThat(documentMapper.mappingSource().string(), equalTo(mapping));
    }

    public void testDefaultApplied() throws IOException {
        createIndex("test1", Settings.settingsBuilder().build());
        createIndex("test2", Settings.settingsBuilder().build());
        XContentBuilder defaultMapping = XContentFactory.jsonBuilder().startObject()
                .startObject(MapperService.DEFAULT_MAPPING).startObject("_timestamp").field("enabled", true).endObject().endObject()
                .endObject();
        client().admin().indices().preparePutMapping().setType(MapperService.DEFAULT_MAPPING).setSource(defaultMapping).get();
        XContentBuilder typeMapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type").startObject("_all").field("enabled", false).endObject().endObject()
                .endObject();
        client().admin().indices().preparePutMapping("test1").setType("type").setSource(typeMapping).get();
        client().admin().indices().preparePutMapping("test1", "test2").setType("type").setSource(typeMapping).get();

        GetMappingsResponse response = client().admin().indices().prepareGetMappings("test2").get();
        assertNotNull(response.getMappings().get("test2").get("type").getSourceAsMap().get("_all"));
        assertFalse((Boolean) ((LinkedHashMap) response.getMappings().get("test2").get("type").getSourceAsMap().get("_all")).get("enabled"));
        assertNotNull(response.getMappings().get("test2").get("type").getSourceAsMap().get("_timestamp"));
        assertTrue((Boolean)((LinkedHashMap)response.getMappings().get("test2").get("type").getSourceAsMap().get("_timestamp")).get("enabled"));
    }
}
