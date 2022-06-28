/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.exists.types;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class TypesExistsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testSimple() throws Exception {
        Client client = client();
        CreateIndexResponse response1 = client.admin()
            .indices()
            .prepareCreate("test1")
            .addMapping("type1", jsonBuilder().startObject().startObject("type1").endObject().endObject())
            .execute()
            .actionGet();
        CreateIndexResponse response2 = client.admin()
            .indices()
            .prepareCreate("test2")
            .addMapping("type2", jsonBuilder().startObject().startObject("type2").endObject().endObject())
            .execute()
            .actionGet();
        client.admin().indices().prepareAliases().addAlias("test1", "alias1").execute().actionGet();
        assertAcked(response1);
        assertAcked(response2);

        TypesExistsResponse response = client.admin().indices().prepareTypesExists("test1").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("test1").setTypes("type2").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));
        try {
            client.admin().indices().prepareTypesExists("notExist").setTypes("type1").execute().actionGet();
            fail("Exception should have been thrown");
        } catch (IndexNotFoundException e) {}
        try {
            client.admin().indices().prepareTypesExists("notExist").setTypes("type0").execute().actionGet();
            fail("Exception should have been thrown");
        } catch (IndexNotFoundException e) {}
        response = client.admin().indices().prepareTypesExists("alias1").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        response = client.admin().indices().prepareTypesExists("*").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));
        response = client.admin().indices().prepareTypesExists("test1", "test2").setTypes("type1").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));
        response = client.admin().indices().prepareTypesExists("test1", "test2").setTypes("type2").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));
    }

    public void testTypesExistsWithBlocks() throws IOException {
        assertAcked(prepareCreate("ro").addMapping("type1", jsonBuilder().startObject().startObject("type1").endObject().endObject()));
        ensureGreen("ro");

        // Request is not blocked
        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("ro", block);
                assertThat(
                    client().admin().indices().prepareTypesExists("ro").setTypes("type1").execute().actionGet().isExists(),
                    equalTo(true)
                );
            } finally {
                disableIndexBlock("ro", block);
            }
        }

        // Request is blocked
        try {
            enableIndexBlock("ro", IndexMetadata.SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareTypesExists("ro").setTypes("type1"));
        } finally {
            disableIndexBlock("ro", IndexMetadata.SETTING_BLOCKS_METADATA);
        }
    }
}
