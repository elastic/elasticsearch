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

package org.elasticsearch.index.mapper.size;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.uninverting.UninvertingReader;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugin.mapper.MapperSizePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import org.apache.lucene.index.IndexableField;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.instanceOf;

public class SizeMappingTests extends ESSingleNodeTestCase {
    private static Version[] TEST_VERSIONS =
        new Version[] {Version.V_5_0_0_alpha1, Version.V_5_0_0_alpha2, Version.V_5_0_0_alpha3, Version.V_5_0_0_alpha4};

    @BeforeClass
    static void setupTests() {
        checkFieldCache = false;
    }

    @AfterClass
    static void cleanupTests() {
        checkFieldCache = true;
    }

    @After
    void ensureFielddata() throws Exception {
        String[] entries = UninvertingReader.getUninvertedStats();
        for (String entry : entries) {
            assertThat(entry, containsString("_size"));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperSizePlugin.class, InternalSettingsPlugin.class);
    }

    public void testSizeEnabled() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type", "_size", "enabled=true");
        DocumentMapper docMapper = service.mapperService().documentMapper("type");

        BytesReference source = XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source("test", "type", "1", source));

        boolean stored = false;
        boolean points = false;
        for (IndexableField field : doc.rootDoc().getFields("_size")) {
            stored |= field.fieldType().stored();
            points |= field.fieldType().pointDimensionCount() > 0;
        }
        assertTrue(stored);
        assertTrue(points);
    }

    public void testSizeDisabled() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type", "_size", "enabled=false");
        DocumentMapper docMapper = service.mapperService().documentMapper("type");

        BytesReference source = XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source("test", "type", "1", source));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testSizeNotSet() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type");
        DocumentMapper docMapper = service.mapperService().documentMapper("type");

        BytesReference source = XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source("test", "type", "1", source));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testThatDisablingWorksWhenMerging() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type", "_size", "enabled=true");
        DocumentMapper docMapper = service.mapperService().documentMapper("type");
        assertThat(docMapper.metadataMapper(SizeFieldMapper.class).enabled(), is(true));

        String disabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_size").field("enabled", false).endObject()
            .endObject().endObject().string();
        docMapper = service.mapperService().merge("type", new CompressedXContent(disabledMapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        assertThat(docMapper.metadataMapper(SizeFieldMapper.class).enabled(), is(false));
    }

    public void testBWCMapper() throws Exception {
        checkFieldCache = false;
        {
            // IntPoint && docvalues=true, fielddata=false for V_5_0_0_alpha4
            IndexService service = createIndex("foo", Settings.EMPTY, "bar", "_size", "enabled=true");
            DocumentMapper docMapper = service.mapperService().documentMapper("bar");
            assertThat(docMapper.metadataMapper(LegacySizeFieldMapper.class), nullValue());
            assertThat(docMapper.metadataMapper(SizeFieldMapper.class), not(nullValue()));
            SizeFieldMapper mapper = docMapper.metadataMapper(SizeFieldMapper.class);
            assertThat(mapper.enabled(), is(true));
            assertThat(mapper.fieldType(), instanceOf(SizeFieldMapper.SizeFieldType.class));
            SizeFieldMapper.SizeFieldType ft = (SizeFieldMapper.SizeFieldType) mapper.fieldType();
            assertThat(ft.hasDocValues(), is(true));
            assertThat(ft.fielddata(), is(false));
        }

        {
            // IntPoint with docvalues=false, fielddata=true if version > V_5_0_0_alpha2 && version < V_5_0_0_alpha4
            IndexService service = createIndex("foo2",
                Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_0_0_alpha3.id).build(),
                "bar", "_size", "enabled=true");
            DocumentMapper docMapper = service.mapperService().documentMapper("bar");
            assertThat(docMapper.metadataMapper(LegacySizeFieldMapper.class), nullValue());
            assertThat(docMapper.metadataMapper(SizeFieldMapper.class), not(nullValue()));
            SizeFieldMapper mapper = docMapper.metadataMapper(SizeFieldMapper.class);
            assertThat(mapper.enabled(), is(true));
            assertThat(mapper.fieldType(), instanceOf(SizeFieldMapper.SizeFieldType.class));
            SizeFieldMapper.SizeFieldType ft = (SizeFieldMapper.SizeFieldType) mapper.fieldType();
            assertThat(ft.hasDocValues(), is(false));
            assertThat(ft.fielddata(), is(true));
        }

        {
            // LegacyIntField with docvalues=false, fielddata=true if version < V_5_0_0_alpha2
            IndexService service = createIndex("foo3",
                Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_0_0_alpha1.id).build(),
                "bar", "_size", "enabled=true");
            DocumentMapper docMapper = service.mapperService().documentMapper("bar");
            assertThat(docMapper.metadataMapper(LegacySizeFieldMapper.class), not(nullValue()));
            assertThat(docMapper.metadataMapper(SizeFieldMapper.class), nullValue());
            LegacySizeFieldMapper mapper = docMapper.metadataMapper(LegacySizeFieldMapper.class);
            assertThat(mapper.enabled(), is(true));
            assertThat(mapper.fieldType(), instanceOf(LegacySizeFieldMapper.LegacySizeFieldType.class));
            LegacySizeFieldMapper.LegacySizeFieldType ft = (LegacySizeFieldMapper.LegacySizeFieldType) mapper.fieldType();
            assertThat(ft.hasDocValues(), is(false));
            assertThat(ft.fielddata(), is(true));
        }
    }

    // issue 5053
    public void testThatUpdatingMappingShouldNotRemoveSizeMappingConfiguration() throws Exception {
        String index = "foo";
        String type = "mytype";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_size").field("enabled", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).addMapping(type, builder));

        // check mapping again
        assertSizeMappingEnabled(index, type, true);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject().startObject("properties").startObject("otherField").field("type", "text").endObject().endObject().endObject();
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setType(type).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure size field is still in mapping
        assertSizeMappingEnabled(index, type, true);
    }

    public void testThatSizeCanBeSwitchedOnAndOff() throws Exception {
        String index = "foo";
        String type = "mytype";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_size").field("enabled", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).addMapping(type, builder));

        // check mapping again
        assertSizeMappingEnabled(index, type, true);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject().startObject("_size").field("enabled", false).endObject().endObject();
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setType(type).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure size field is still in mapping
        assertSizeMappingEnabled(index, type, false);
    }

    private void assertSizeMappingEnabled(String index, String type, boolean enabled) throws IOException {
        String errMsg = String.format(Locale.ROOT, "Expected size field mapping to be " + (enabled ? "enabled" : "disabled") + " for %s/%s", index, type);
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).addTypes(type).get();
        Map<String, Object> mappingSource = getMappingsResponse.getMappings().get(index).get(type).getSourceAsMap();
        assertThat(errMsg, mappingSource, hasKey("_size"));
        String sizeAsString = mappingSource.get("_size").toString();
        assertThat(sizeAsString, is(notNullValue()));
        assertThat(errMsg, sizeAsString, is("{enabled=" + (enabled) + "}"));
    }

    public void testBasic() throws Exception {
        Version indexVersion = randomFrom(TEST_VERSIONS);
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED,
            indexVersion.id).build();
        client().admin().indices().prepareCreate("test")
            .setSettings(indexSettings)
            .addMapping("type", "_size", "enabled=true")
            .execute().actionGet();
        String content = "a";
        for (int i = 0; i < 10; i++) {
            final String source = "{\"f\":\"" + content + "\"}";
            client().prepareIndex("test", "type", Integer.toString(i)).setSource(source).execute().actionGet();
            content += "a";
        }
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        for (int i = 0; i < 10; i++) {
            GetResponse getResponse = client().prepareGet("test", "type", Integer.toString(i))
                .setFields("_size").setFetchSource(true).get();
            assertNotNull(getResponse.getField("_size"));
            assertEquals(getResponse.getSourceAsString().length(), getResponse.getField("_size").getValue());
        }

        SearchResponse response =
            client().prepareSearch("test")
                .fields("_size")
                .addSort(SortBuilders.fieldSort("_size").order(SortOrder.DESC))
                .setSize(10)
                .execute().actionGet();
        assertThat(response.getHits().totalHits(), equalTo(10L));
        int size = 19;
        for (int i = 10; i < 0; i ++) {
            assertThat(response.getHits().getHits()[i].getId(), equalTo(Integer.toString(i)));
            assertThat(response.getHits().getHits()[i].field("_size").getValue(), equalTo(size));
            size --;
        }
        client().admin().indices().prepareClose("test").execute().actionGet();
    }
}
