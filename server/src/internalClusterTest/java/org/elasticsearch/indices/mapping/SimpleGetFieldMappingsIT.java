/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.mapping;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_METADATA_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class SimpleGetFieldMappingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testGetMappingsWhereThereAreNone() {
        createIndex("index");
        GetFieldMappingsResponse response = indicesAdmin().prepareGetFieldMappings().get();
        assertThat(response.mappings().size(), equalTo(1));
        assertThat(response.mappings().get("index").size(), equalTo(0));

        assertThat(response.fieldMappings("index", "field"), Matchers.nullValue());
    }

    private XContentBuilder getMappingForType() throws IOException {
        return jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .endObject()
            .startObject("alias")
            .field("type", "alias")
            .field("path", "field1")
            .endObject()
            .startObject("obj")
            .startObject("properties")
            .startObject("subfield")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

    public void testGetFieldMappings() throws Exception {
        assertAcked(
            prepareCreate("indexa").setMapping(getMappingForType()),
            indicesAdmin().prepareCreate("indexb").setMapping(getMappingForType())
        );

        // Get mappings by full name
        GetFieldMappingsResponse response = indicesAdmin().prepareGetFieldMappings("indexa").setFields("field1", "obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "field1").fullName(), equalTo("field1"));
        assertThat(response.fieldMappings("indexa", "field1").sourceAsMap(), hasKey("field1"));
        assertThat(response.fieldMappings("indexa", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "obj.subfield").sourceAsMap(), hasKey("subfield"));

        // Get mappings by name
        response = indicesAdmin().prepareGetFieldMappings("indexa").setFields("field1", "obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "field1").fullName(), equalTo("field1"));
        assertThat(response.fieldMappings("indexa", "field1").sourceAsMap(), hasKey("field1"));
        assertThat(response.fieldMappings("indexa", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "obj.subfield").sourceAsMap(), hasKey("subfield"));

        // get mappings by name across multiple indices
        response = indicesAdmin().prepareGetFieldMappings().setFields("obj.subfield").get();
        assertThat(response.fieldMappings("indexa", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexa", "obj.subfield").sourceAsMap(), hasKey("subfield"));
        assertThat(response.fieldMappings("indexb", "obj.subfield").fullName(), equalTo("obj.subfield"));
        assertThat(response.fieldMappings("indexb", "obj.subfield").sourceAsMap(), hasKey("subfield"));

    }

    @SuppressWarnings("unchecked")
    public void testSimpleGetFieldMappingsWithDefaults() throws Exception {
        assertAcked(prepareCreate("test").setMapping(getMappingForType()));
        indicesAdmin().preparePutMapping("test").setSource("num", "type=long").get();
        indicesAdmin().preparePutMapping("test").setSource("field2", "type=text,index=false").get();

        GetFieldMappingsResponse response = indicesAdmin().prepareGetFieldMappings()
            .setFields("num", "field1", "field2", "obj.subfield")
            .includeDefaults(true)
            .get();

        assertThat((Map<String, Object>) response.fieldMappings("test", "num").sourceAsMap().get("num"), hasEntry("index", Boolean.TRUE));
        assertThat((Map<String, Object>) response.fieldMappings("test", "num").sourceAsMap().get("num"), hasEntry("type", "long"));
        assertThat(
            (Map<String, Object>) response.fieldMappings("test", "field1").sourceAsMap().get("field1"),
            hasEntry("index", Boolean.TRUE)
        );
        assertThat((Map<String, Object>) response.fieldMappings("test", "field1").sourceAsMap().get("field1"), hasEntry("type", "text"));
        assertThat((Map<String, Object>) response.fieldMappings("test", "field2").sourceAsMap().get("field2"), hasEntry("type", "text"));
        assertThat(
            (Map<String, Object>) response.fieldMappings("test", "obj.subfield").sourceAsMap().get("subfield"),
            hasEntry("type", "keyword")
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetFieldMappingsWithFieldAlias() throws Exception {
        assertAcked(prepareCreate("test").setMapping(getMappingForType()));

        GetFieldMappingsResponse response = indicesAdmin().prepareGetFieldMappings().setFields("alias", "field1").get();

        FieldMappingMetadata aliasMapping = response.fieldMappings("test", "alias");
        assertThat(aliasMapping.fullName(), equalTo("alias"));
        assertThat(aliasMapping.sourceAsMap(), hasKey("alias"));
        assertThat((Map<String, Object>) aliasMapping.sourceAsMap().get("alias"), hasEntry("type", "alias"));

        FieldMappingMetadata field1Mapping = response.fieldMappings("test", "field1");
        assertThat(field1Mapping.fullName(), equalTo("field1"));
        assertThat(field1Mapping.sourceAsMap(), hasKey("field1"));
    }

    // fix #6552
    public void testSimpleGetFieldMappingsWithPretty() throws Exception {
        assertAcked(prepareCreate("index").setMapping(getMappingForType()));
        Map<String, String> params = new HashMap<>();
        params.put("pretty", "true");
        GetFieldMappingsResponse response = indicesAdmin().prepareGetFieldMappings("index").setFields("field1", "obj.subfield").get();
        XContentBuilder responseBuilder = XContentFactory.jsonBuilder().prettyPrint();
        response.toXContent(responseBuilder, new ToXContent.MapParams(params));
        String responseStrings = Strings.toString(responseBuilder);

        XContentBuilder prettyJsonBuilder = XContentFactory.jsonBuilder().prettyPrint();
        try (var parser = createParser(JsonXContent.jsonXContent, responseStrings)) {
            prettyJsonBuilder.copyCurrentStructure(parser);
        }
        assertThat(responseStrings, equalTo(Strings.toString(prettyJsonBuilder)));

        params.put("pretty", "false");

        response = indicesAdmin().prepareGetFieldMappings("index").setFields("field1", "obj.subfield").get();
        responseBuilder = XContentFactory.jsonBuilder().prettyPrint().lfAtEnd();
        response.toXContent(responseBuilder, new ToXContent.MapParams(params));
        responseStrings = Strings.toString(responseBuilder);

        prettyJsonBuilder = XContentFactory.jsonBuilder().prettyPrint();
        try (var parser = createParser(JsonXContent.jsonXContent, responseStrings)) {
            prettyJsonBuilder.copyCurrentStructure(parser);
        }
        assertThat(responseStrings, not(equalTo(Strings.toString(prettyJsonBuilder))));

    }

    public void testGetFieldMappingsWithBlocks() throws Exception {
        assertAcked(prepareCreate("test").setMapping(getMappingForType()));

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("test", block);
                GetFieldMappingsResponse response = indicesAdmin().prepareGetFieldMappings("test")
                    .setFields("field1", "obj.subfield")
                    .get();
                assertThat(response.fieldMappings("test", "field1").fullName(), equalTo("field1"));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        try {
            enableIndexBlock("test", SETTING_BLOCKS_METADATA);
            assertBlocked(indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT), INDEX_METADATA_BLOCK);
        } finally {
            disableIndexBlock("test", SETTING_BLOCKS_METADATA);
        }
    }
}
