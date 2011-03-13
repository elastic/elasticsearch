/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.versioning;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class SimpleVersioningTests extends AbstractNodesTests {

    private Client client;
    private Client client2;

    @BeforeClass public void createNodes() throws Exception {
        // make sure we use bloom filters here!
        Settings settings = ImmutableSettings.settingsBuilder().put("index.engine.robin.async_load_bloom", false).build();
        startNode("server1", settings);
        startNode("server2", settings);
        client = client("server1");
        client2 = client("server2");
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @Test public void testExternalVersioning() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        IndexResponse indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(12).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.version(), equalTo(12l));

        indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(14).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.version(), equalTo(14l));

        try {
            client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        client.admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client.prepareGet("test", "type", "1").execute().actionGet().version(), equalTo(14l));
        }
    }

    @Test public void testSimpleVersioning() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        IndexResponse indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertThat(indexResponse.version(), equalTo(1l));

        indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(1).execute().actionGet();
        assertThat(indexResponse.version(), equalTo(2l));

        try {
            client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client2.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client.prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client2.prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client.prepareDelete("test", "type", "1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client2.prepareDelete("test", "type", "1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        client.admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client.prepareGet("test", "type", "1").execute().actionGet().version(), equalTo(2l));
        }

        // search with versioning
        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).setVersion(true).execute().actionGet();
            assertThat(searchResponse.hits().getAt(0).version(), equalTo(2l));
        }

        // search without versioning
        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
            assertThat(searchResponse.hits().getAt(0).version(), equalTo(-1l));
        }
    }

    @Test public void testSimpleVersioningWithFlush() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        IndexResponse indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertThat(indexResponse.version(), equalTo(1l));

        client.admin().indices().prepareFlush().execute().actionGet();

        indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(1).execute().actionGet();
        assertThat(indexResponse.version(), equalTo(2l));

        client.admin().indices().prepareFlush().execute().actionGet();

        try {
            client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client2.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client.prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client2.prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client.prepareDelete("test", "type", "1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client2.prepareDelete("test", "type", "1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        client.admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client.prepareGet("test", "type", "1").execute().actionGet().version(), equalTo(2l));
        }

        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).setVersion(true).execute().actionGet();
            assertThat(searchResponse.hits().getAt(0).version(), equalTo(2l));
        }
    }

    @Test public void testVersioningWithBulk() {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        BulkResponse bulkResponse = client.prepareBulk().add(client.prepareIndex("test", "type", "1").setSource("field1", "value1_1")).execute().actionGet();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.items().length, equalTo(1));
        IndexResponse indexResponse = bulkResponse.items()[0].response();
        assertThat(indexResponse.version(), equalTo(1l));
    }
}
