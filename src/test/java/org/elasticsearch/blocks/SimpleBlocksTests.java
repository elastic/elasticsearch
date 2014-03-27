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

package org.elasticsearch.blocks;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashMap;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.notNullValue;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)
public class SimpleBlocksTests extends ElasticsearchIntegrationTest {
    
    @Test
    public void verifyIndexAndClusterReadOnly() throws Exception {
        // cluster.read_only = null: write and metadata not blocked
        canCreateIndex("test1");
        canIndexDocument("test1");
        setIndexReadOnly("test1", "false");
        canIndexExists("test1");

        // cluster.read_only = true: block write and metadata
        setClusterReadOnly("true");
        canNotCreateIndex("test2");
        // even if index has index.read_only = false
        canNotIndexDocument("test1");
        canNotIndexExists("test1");

        // cluster.read_only = false: removes the block
        setClusterReadOnly("false");
        canCreateIndex("test2");
        canIndexDocument("test2");
        canIndexDocument("test1");
        canIndexExists("test1");


        // newly created an index has no blocks
        canCreateIndex("ro");
        canIndexDocument("ro");
        canIndexExists("ro");

        // adds index write and metadata block
        setIndexReadOnly( "ro", "true");
        canNotIndexDocument("ro");
        canNotIndexExists("ro");

        // other indices not blocked
        canCreateIndex("rw");
        canIndexDocument("rw");
        canIndexExists("rw");

        // blocks can be removed
        setIndexReadOnly("ro", "false");
        canIndexDocument("ro");
        canIndexExists("ro");
    }

    @Test
    public void testIndexReadWriteMetaDataBlocks() {
        canCreateIndex("test1");
        canIndexDocument("test1");
        client().admin().indices().prepareUpdateSettings("test1")
                .setSettings(settingsBuilder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true))
                .execute().actionGet();
        canNotIndexDocument("test1");
        client().admin().indices().prepareUpdateSettings("test1")
                .setSettings(settingsBuilder().put(IndexMetaData.SETTING_BLOCKS_WRITE, false))
                .execute().actionGet();
        canIndexDocument("test1");
    }

    private void canCreateIndex(String index) {
        try {
            CreateIndexResponse r = client().admin().indices().prepareCreate(index).execute().actionGet();
            assertThat(r, notNullValue());
        } catch (ClusterBlockException e) {
            fail();
        }
    }

    private void canNotCreateIndex(String index) {
        try {
            client().admin().indices().prepareCreate(index).execute().actionGet();
            fail();
        } catch (ClusterBlockException e) {
            // all is well
        }
    }

    private void canIndexDocument(String index) {
        try {
            IndexRequestBuilder builder = client().prepareIndex(index, "zzz");
            builder.setSource("foo", "bar");
            IndexResponse r = builder.execute().actionGet();
            assertThat(r, notNullValue());
        } catch (ClusterBlockException e) {
            fail();
        }
    }

    private void canNotIndexDocument(String index) {
        try {
            IndexRequestBuilder builder = client().prepareIndex(index, "zzz");
            builder.setSource("foo", "bar");
            builder.execute().actionGet();
            fail();
        } catch (ClusterBlockException e) {
            // all is well
        }
    }

    private void canIndexExists(String index) {
        try {
            IndicesExistsResponse r = client().admin().indices().prepareExists(index).execute().actionGet();
            assertThat(r, notNullValue());
        } catch (ClusterBlockException e) {
            fail();
        }
    }

    private void canNotIndexExists(String index) {
        try {
            IndicesExistsResponse r = client().admin().indices().prepareExists(index).execute().actionGet();
            fail();
        } catch (ClusterBlockException e) {
            // all is well
        }
    }

    private void setClusterReadOnly(String value) {
        Settings settings = settingsBuilder().put(MetaData.SETTING_READ_ONLY, value).build();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();
    }

    private void setIndexReadOnly(String index, Object value) {
        HashMap<String, Object> newSettings = new HashMap<>();
        newSettings.put(IndexMetaData.SETTING_READ_ONLY, value);

        UpdateSettingsRequestBuilder settingsRequest = client().admin().indices().prepareUpdateSettings(index);
        settingsRequest.setSettings(newSettings);
        UpdateSettingsResponse settingsResponse = settingsRequest.execute().actionGet();
        assertThat(settingsResponse, notNullValue());
    }
}
