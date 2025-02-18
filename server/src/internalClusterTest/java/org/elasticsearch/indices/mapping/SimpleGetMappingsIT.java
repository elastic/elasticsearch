/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.mapping;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_METADATA_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SimpleGetMappingsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testGetMappingsWhereThereAreNone() {
        createIndex("index");
        GetMappingsResponse response = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT).get();
        assertThat(response.mappings().containsKey("index"), equalTo(true));
        assertEquals(MappingMetadata.EMPTY_MAPPINGS, response.mappings().get("index"));
    }

    private XContentBuilder getMappingForType() throws IOException {
        return jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

    public void testSimpleGetMappings() throws Exception {
        indicesAdmin().prepareCreate("indexa").setMapping(getMappingForType()).get();
        indicesAdmin().prepareCreate("indexb").setMapping(getMappingForType()).get();

        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        // Get all mappings
        GetMappingsResponse response = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT).get();
        assertThat(response.mappings().size(), equalTo(2));
        assertThat(response.mappings().get("indexa"), notNullValue());
        assertThat(response.mappings().get("indexb"), notNullValue());

        // Get all mappings, via wildcard support
        response = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, "*").get();
        assertThat(response.mappings().size(), equalTo(2));
        assertThat(response.mappings().get("indexa"), notNullValue());
        assertThat(response.mappings().get("indexb"), notNullValue());

        // Get mappings in indexa
        response = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, "indexa").get();
        assertThat(response.mappings().size(), equalTo(1));
        assertThat(response.mappings().get("indexa"), notNullValue());
    }

    public void testGetMappingsWithBlocks() throws IOException {
        indicesAdmin().prepareCreate("test").setMapping(getMappingForType()).get();
        ensureGreen();

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("test", block);
                GetMappingsResponse response = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT).get();
                assertThat(response.mappings().size(), equalTo(1));
                assertNotNull(response.mappings().get("test"));
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
