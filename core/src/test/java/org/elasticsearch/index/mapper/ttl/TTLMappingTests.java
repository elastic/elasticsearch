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

package org.elasticsearch.index.mapper.ttl;

import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TTLMappingTests extends ESSingleNodeTestCase {
    public void testSimpleDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1").ttl(Long.MAX_VALUE));

        assertThat(doc.rootDoc().getField("_ttl"), equalTo(null));
    }

    public void testEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl").field("enabled", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source(source).type("type").id("1").ttl(Long.MAX_VALUE));

        assertThat(doc.rootDoc().getField("_ttl").fieldType().stored(), equalTo(true));
        assertNotSame(IndexOptions.NONE, doc.rootDoc().getField("_ttl").fieldType().indexOptions());
        assertThat(doc.rootDoc().getField("_ttl").tokenStream(docMapper.mappers().indexAnalyzer(), null), notNullValue());
    }

    public void testDefaultValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        assertThat(docMapper.TTLFieldMapper().enabled(), equalTo(TTLFieldMapper.Defaults.ENABLED_STATE.enabled));
        assertThat(docMapper.TTLFieldMapper().fieldType().stored(), equalTo(TTLFieldMapper.Defaults.TTL_FIELD_TYPE.stored()));
        assertThat(docMapper.TTLFieldMapper().fieldType().indexOptions(), equalTo(TTLFieldMapper.Defaults.TTL_FIELD_TYPE.indexOptions()));
    }

    public void testSetValuesBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", "yes").field("store", "no")
                .endObject()
                .endObject().endObject().string();
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        DocumentMapper docMapper = createIndex("test", indexSettings).mapperService().documentMapperParser().parse(mapping);
        assertThat(docMapper.TTLFieldMapper().enabled(), equalTo(true));
        assertThat(docMapper.TTLFieldMapper().fieldType().stored(), equalTo(true)); // store was never serialized, so it was always lost

    }

    public void testThatEnablingTTLFieldOnMergeWorks() throws Exception {
        String mappingWithoutTtl = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        String mappingWithTtl = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", "yes")
                .endObject()
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper mapperWithoutTtl = parser.parse(mappingWithoutTtl);
        DocumentMapper mapperWithTtl = parser.parse(mappingWithTtl);

        mapperWithoutTtl.merge(mapperWithTtl.mapping(), false, false);

        assertThat(mapperWithoutTtl.TTLFieldMapper().enabled(), equalTo(true));
    }

    public void testThatChangingTTLKeepsMapperEnabled() throws Exception {
        String mappingWithTtl = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", "yes")
                .endObject()
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        String updatedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("default", "1w")
                .endObject()
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper initialMapper = parser.parse(mappingWithTtl);
        DocumentMapper updatedMapper = parser.parse(updatedMapping);

        initialMapper.merge(updatedMapper.mapping(), true, false);

        assertThat(initialMapper.TTLFieldMapper().enabled(), equalTo(true));
    }

    public void testThatDisablingTTLReportsConflict() throws Exception {
        String mappingWithTtl = getMappingWithTtlEnabled().string();
        String mappingWithTtlDisabled = getMappingWithTtlDisabled().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper initialMapper = parser.parse(mappingWithTtl);
        DocumentMapper updatedMapper = parser.parse(mappingWithTtlDisabled);

        try {
            initialMapper.merge(updatedMapper.mapping(), true, false);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        assertThat(initialMapper.TTLFieldMapper().enabled(), equalTo(true));
    }

    public void testThatDisablingTTLReportsConflictOnCluster() throws Exception {
        String mappingWithTtl = getMappingWithTtlEnabled().string();
        String mappingWithTtlDisabled = getMappingWithTtlDisabled().string();
        assertAcked(client().admin().indices().prepareCreate("testindex").addMapping("type", mappingWithTtl));
        GetMappingsResponse mappingsBeforeUpdateResponse = client().admin().indices().prepareGetMappings("testindex").addTypes("type").get();
        try {
            client().admin().indices().preparePutMapping("testindex").setSource(mappingWithTtlDisabled).setType("type").get();
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("_ttl cannot be disabled once it was enabled."));
        }
        GetMappingsResponse mappingsAfterUpdateResponse = client().admin().indices().prepareGetMappings("testindex").addTypes("type").get();
        assertThat(mappingsBeforeUpdateResponse.getMappings().get("testindex").get("type").source(), equalTo(mappingsAfterUpdateResponse.getMappings().get("testindex").get("type").source()));
    }

    public void testThatEnablingTTLAfterFirstDisablingWorks() throws Exception {
        String mappingWithTtl = getMappingWithTtlEnabled().string();
        String withTtlDisabled = getMappingWithTtlDisabled().string();
        assertAcked(client().admin().indices().prepareCreate("testindex").addMapping("type", withTtlDisabled));
        GetMappingsResponse mappingsAfterUpdateResponse = client().admin().indices().prepareGetMappings("testindex").addTypes("type").get();
        assertThat(mappingsAfterUpdateResponse.getMappings().get("testindex").get("type").sourceAsMap().get("_ttl").toString(), equalTo("{enabled=false}"));
        client().admin().indices().preparePutMapping("testindex").setSource(mappingWithTtl).setType("type").get();
        mappingsAfterUpdateResponse = client().admin().indices().prepareGetMappings("testindex").addTypes("type").get();
        assertThat(mappingsAfterUpdateResponse.getMappings().get("testindex").get("type").sourceAsMap().get("_ttl").toString(), equalTo("{enabled=true}"));
    }

    public void testNoConflictIfNothingSetAndDisabledLater() throws Exception {
        IndexService indexService = createIndex("testindex", Settings.settingsBuilder().build(), "type");
        XContentBuilder mappingWithTtlDisabled = getMappingWithTtlDisabled("7d");
        indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingWithTtlDisabled.string()), true).mapping(), randomBoolean(), false);
    }

    public void testNoConflictIfNothingSetAndEnabledLater() throws Exception {
        IndexService indexService = createIndex("testindex", Settings.settingsBuilder().build(), "type");
        XContentBuilder mappingWithTtlEnabled = getMappingWithTtlEnabled("7d");
        indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingWithTtlEnabled.string()), true).mapping(), randomBoolean(), false);
    }

    public void testMergeWithOnlyDefaultSet() throws Exception {
        XContentBuilder mappingWithTtlEnabled = getMappingWithTtlEnabled("7d");
        IndexService indexService = createIndex("testindex", Settings.settingsBuilder().build(), "type", mappingWithTtlEnabled);
        XContentBuilder mappingWithOnlyDefaultSet = getMappingWithOnlyTtlDefaultSet("6m");
        indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingWithOnlyDefaultSet.string()), true).mapping(), false, false);
        CompressedXContent mappingAfterMerge = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterMerge, equalTo(new CompressedXContent("{\"type\":{\"_ttl\":{\"enabled\":true,\"default\":360000},\"properties\":{\"field\":{\"type\":\"string\"}}}}")));
    }

    public void testMergeWithOnlyDefaultSetTtlDisabled() throws Exception {
        XContentBuilder mappingWithTtlEnabled = getMappingWithTtlDisabled("7d");
        IndexService indexService = createIndex("testindex", Settings.settingsBuilder().build(), "type", mappingWithTtlEnabled);
        CompressedXContent mappingAfterCreation = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterCreation, equalTo(new CompressedXContent("{\"type\":{\"_ttl\":{\"enabled\":false},\"properties\":{\"field\":{\"type\":\"string\"}}}}")));
        XContentBuilder mappingWithOnlyDefaultSet = getMappingWithOnlyTtlDefaultSet("6m");
        indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingWithOnlyDefaultSet.string()), true).mapping(), false, false);
        CompressedXContent mappingAfterMerge = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterMerge, equalTo(new CompressedXContent("{\"type\":{\"_ttl\":{\"enabled\":false},\"properties\":{\"field\":{\"type\":\"string\"}}}}")));
    }

    public void testThatSimulatedMergingLeavesStateUntouched() throws Exception {
        //check if default ttl changed when simulate set to true
        XContentBuilder mappingWithTtl = getMappingWithTtlEnabled("6d");
        IndexService indexService = createIndex("testindex", Settings.settingsBuilder().build(), "type", mappingWithTtl);
        CompressedXContent mappingBeforeMerge = indexService.mapperService().documentMapper("type").mappingSource();
        XContentBuilder mappingWithTtlDifferentDefault = getMappingWithTtlEnabled("7d");
        indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingWithTtlDifferentDefault.string()), true).mapping(), true, false);
        // make sure simulate flag actually worked - no mappings applied
        CompressedXContent mappingAfterMerge = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterMerge, equalTo(mappingBeforeMerge));

        client().admin().indices().prepareDelete("testindex").get();
        // check if enabled changed when simulate set to true
        XContentBuilder mappingWithoutTtl = getMappingWithTtlDisabled();
        indexService = createIndex("testindex", Settings.settingsBuilder().build(), "type", mappingWithoutTtl);
        mappingBeforeMerge = indexService.mapperService().documentMapper("type").mappingSource();
        XContentBuilder mappingWithTtlEnabled = getMappingWithTtlEnabled();
        indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingWithTtlEnabled.string()), true).mapping(), true, false);
        // make sure simulate flag actually worked - no mappings applied
        mappingAfterMerge = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterMerge, equalTo(mappingBeforeMerge));

        client().admin().indices().prepareDelete("testindex").get();
        // check if enabled changed when simulate set to true
        mappingWithoutTtl = getMappingWithTtlDisabled("6d");
        indexService = createIndex("testindex", Settings.settingsBuilder().build(), "type", mappingWithoutTtl);
        mappingBeforeMerge = indexService.mapperService().documentMapper("type").mappingSource();
        mappingWithTtlEnabled = getMappingWithTtlEnabled("7d");
        indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingWithTtlEnabled.string()), true).mapping(), true, false);
        // make sure simulate flag actually worked - no mappings applied
        mappingAfterMerge = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterMerge, equalTo(mappingBeforeMerge));

        client().admin().indices().prepareDelete("testindex").get();
        // check if switching simulate flag off works
        mappingWithoutTtl = getMappingWithTtlDisabled("6d");
        indexService = createIndex("testindex", Settings.settingsBuilder().build(), "type", mappingWithoutTtl);
        mappingWithTtlEnabled = getMappingWithTtlEnabled("7d");
        indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingWithTtlEnabled.string()), true).mapping(), false, false);
        // make sure simulate flag actually worked - mappings applied
        mappingAfterMerge = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterMerge, equalTo(new CompressedXContent("{\"type\":{\"_ttl\":{\"enabled\":true,\"default\":604800000},\"properties\":{\"field\":{\"type\":\"string\"}}}}")));

        client().admin().indices().prepareDelete("testindex").get();
        // check if switching simulate flag off works if nothing was applied in the beginning
        indexService = createIndex("testindex", Settings.settingsBuilder().build(), "type");
        mappingWithTtlEnabled = getMappingWithTtlEnabled("7d");
        indexService.mapperService().documentMapper("type").merge(indexService.mapperService().parse("type", new CompressedXContent(mappingWithTtlEnabled.string()), true).mapping(), false, false);
        // make sure simulate flag actually worked - mappings applied
        mappingAfterMerge = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterMerge, equalTo(new CompressedXContent("{\"type\":{\"_ttl\":{\"enabled\":true,\"default\":604800000},\"properties\":{\"field\":{\"type\":\"string\"}}}}")));

    }

    public void testIncludeInObjectBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_ttl").field("enabled", true).endObject()
            .endObject().endObject().string();
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        DocumentMapper docMapper = createIndex("test", settings).mapperService().documentMapperParser().parse(mapping);

        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("_ttl", "2d").endObject();
        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);
        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        request.process(MetaData.builder().build(), mappingMetaData, true, "test");

        // _ttl in a document never worked, so backcompat is ignoring the field
        assertNull(request.ttl());
        assertNull(docMapper.parse("test", "type", "1", doc.bytes()).rootDoc().get("_ttl"));
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_ttl").field("enabled", true).endObject()
            .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        try {
            docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject().field("_ttl", "2d").endObject().bytes());
            fail("Expected failure to parse metadata field");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Field [_ttl] is a metadata field and cannot be added inside a document"));
        }
    }

    private org.elasticsearch.common.xcontent.XContentBuilder getMappingWithTtlEnabled() throws IOException {
        return getMappingWithTtlEnabled(null);
    }

    private org.elasticsearch.common.xcontent.XContentBuilder getMappingWithTtlDisabled() throws IOException {
        return getMappingWithTtlDisabled(null);
    }

    private org.elasticsearch.common.xcontent.XContentBuilder getMappingWithTtlEnabled(String defaultValue) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", true);
        if (defaultValue != null) {
            mapping.field("default", defaultValue);
        }
        return mapping.endObject()
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject();
    }

    private org.elasticsearch.common.xcontent.XContentBuilder getMappingWithTtlDisabled(String defaultValue) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", false);
        if (defaultValue != null) {
            mapping.field("default", defaultValue);
        }
        return mapping.endObject()
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject();
    }

    private org.elasticsearch.common.xcontent.XContentBuilder getMappingWithOnlyTtlDefaultSet(String defaultValue) throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl").field("default", defaultValue).endObject()
                .startObject("properties").field("field").startObject().field("type", "string").endObject().endObject()
                .endObject().endObject();
    }
}
