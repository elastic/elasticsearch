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
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
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
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source("test", "type", "1", source).ttl(Long.MAX_VALUE));

        assertThat(doc.rootDoc().getField("_ttl"), equalTo(null));
    }

    public void testEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl").field("enabled", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source("test", "type", "1", source).ttl(Long.MAX_VALUE));

        assertThat(doc.rootDoc().getField("_ttl").fieldType().stored(), equalTo(true));
        assertNotSame(IndexOptions.NONE, doc.rootDoc().getField("_ttl").fieldType().indexOptions());
        assertThat(doc.rootDoc().getField("_ttl").tokenStream(docMapper.mappers().indexAnalyzer(), null), notNullValue());
    }

    public void testDefaultValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        assertThat(docMapper.TTLFieldMapper().enabled(), equalTo(TTLFieldMapper.Defaults.ENABLED_STATE.enabled));
        assertThat(docMapper.TTLFieldMapper().fieldType().stored(), equalTo(TTLFieldMapper.Defaults.TTL_FIELD_TYPE.stored()));
        assertThat(docMapper.TTLFieldMapper().fieldType().indexOptions(), equalTo(TTLFieldMapper.Defaults.TTL_FIELD_TYPE.indexOptions()));
    }

    public void testThatEnablingTTLFieldOnMergeWorks() throws Exception {
        String mappingWithoutTtl = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").field("field").startObject().field("type", "text").endObject().endObject()
                .endObject().endObject().string();

        String mappingWithTtl = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", "yes")
                .endObject()
                .startObject("properties").field("field").startObject().field("type", "text").endObject().endObject()
                .endObject().endObject().string();

        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapperWithoutTtl = mapperService.merge("type", new CompressedXContent(mappingWithoutTtl), MapperService.MergeReason.MAPPING_UPDATE, false);
        DocumentMapper mapperWithTtl = mapperService.merge("type", new CompressedXContent(mappingWithTtl), MapperService.MergeReason.MAPPING_UPDATE, false);

        assertThat(mapperWithoutTtl.TTLFieldMapper().enabled(), equalTo(false));
        assertThat(mapperWithTtl.TTLFieldMapper().enabled(), equalTo(true));
    }

    public void testThatChangingTTLKeepsMapperEnabled() throws Exception {
        String mappingWithTtl = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("enabled", "yes")
                .endObject()
                .startObject("properties").field("field").startObject().field("type", "text").endObject().endObject()
                .endObject().endObject().string();

        String updatedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl")
                .field("default", "1w")
                .endObject()
                .startObject("properties").field("field").startObject().field("type", "text").endObject().endObject()
                .endObject().endObject().string();

        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper initialMapper = mapperService.merge("type", new CompressedXContent(mappingWithTtl), MapperService.MergeReason.MAPPING_UPDATE, false);
        DocumentMapper updatedMapper = mapperService.merge("type", new CompressedXContent(updatedMapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        assertThat(initialMapper.TTLFieldMapper().enabled(), equalTo(true));
        assertThat(updatedMapper.TTLFieldMapper().enabled(), equalTo(true));
    }

    public void testThatDisablingTTLReportsConflict() throws Exception {
        String mappingWithTtl = getMappingWithTtlEnabled().string();
        String mappingWithTtlDisabled = getMappingWithTtlDisabled().string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper initialMapper = mapperService.merge("type", new CompressedXContent(mappingWithTtl), MapperService.MergeReason.MAPPING_UPDATE, false);

        try {
            mapperService.merge("type", new CompressedXContent(mappingWithTtlDisabled), MapperService.MergeReason.MAPPING_UPDATE, false);
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
        IndexService indexService = createIndex("testindex", Settings.builder().build(), "type");
        XContentBuilder mappingWithTtlDisabled = getMappingWithTtlDisabled("7d");
        indexService.mapperService().merge("type", new CompressedXContent(mappingWithTtlDisabled.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
    }

    public void testNoConflictIfNothingSetAndEnabledLater() throws Exception {
        IndexService indexService = createIndex("testindex", Settings.builder().build(), "type");
        XContentBuilder mappingWithTtlEnabled = getMappingWithTtlEnabled("7d");
        indexService.mapperService().merge("type", new CompressedXContent(mappingWithTtlEnabled.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
    }

    public void testMergeWithOnlyDefaultSet() throws Exception {
        XContentBuilder mappingWithTtlEnabled = getMappingWithTtlEnabled("7d");
        IndexService indexService = createIndex("testindex", Settings.builder().build(), "type", mappingWithTtlEnabled);
        XContentBuilder mappingWithOnlyDefaultSet = getMappingWithOnlyTtlDefaultSet("6m");
        indexService.mapperService().merge("type", new CompressedXContent(mappingWithOnlyDefaultSet.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        CompressedXContent mappingAfterMerge = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterMerge, equalTo(new CompressedXContent("{\"type\":{\"_ttl\":{\"enabled\":true,\"default\":360000},\"properties\":{\"field\":{\"type\":\"text\"}}}}")));
    }

    public void testMergeWithOnlyDefaultSetTtlDisabled() throws Exception {
        XContentBuilder mappingWithTtlEnabled = getMappingWithTtlDisabled("7d");
        IndexService indexService = createIndex("testindex", Settings.builder().build(), "type", mappingWithTtlEnabled);
        CompressedXContent mappingAfterCreation = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterCreation, equalTo(new CompressedXContent("{\"type\":{\"_ttl\":{\"enabled\":false},\"properties\":{\"field\":{\"type\":\"text\"}}}}")));
        XContentBuilder mappingWithOnlyDefaultSet = getMappingWithOnlyTtlDefaultSet("6m");
        indexService.mapperService().merge("type", new CompressedXContent(mappingWithOnlyDefaultSet.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        CompressedXContent mappingAfterMerge = indexService.mapperService().documentMapper("type").mappingSource();
        assertThat(mappingAfterMerge, equalTo(new CompressedXContent("{\"type\":{\"_ttl\":{\"enabled\":false},\"properties\":{\"field\":{\"type\":\"text\"}}}}")));
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_ttl").field("enabled", true).endObject()
            .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

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
                .startObject("properties").field("field").startObject().field("type", "text").endObject().endObject()
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
                .startObject("properties").field("field").startObject().field("type", "text").endObject().endObject()
                .endObject().endObject();
    }

    private org.elasticsearch.common.xcontent.XContentBuilder getMappingWithOnlyTtlDefaultSet(String defaultValue) throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_ttl").field("default", defaultValue).endObject()
                .startObject("properties").field("field").startObject().field("type", "text").endObject().endObject()
                .endObject().endObject();
    }
}
