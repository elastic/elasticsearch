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
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.*;

import java.util.HashMap;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class SimpleVersioningTests extends AbstractNodesTests {

    private Client client;
    private Client client2;

    @BeforeClass
    public void createNodes() throws Exception {
        // make sure we use bloom filters here!
        Settings settings = ImmutableSettings.settingsBuilder().put("index.engine.robin.async_load_bloom", false).build();
        startNode("server1", settings);
        startNode("server2", settings);
        client = client("server1");
        client2 = client("server2");
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }


    @BeforeMethod
    public void setUp() {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

    }


    @Test
    public void testExternalVersioningInitialDelete() throws Exception {

        // Note - external version doesn't throw version conflicts on deletes of non existent records. This is different from internal versioning
        DeleteResponse deleteResponse = client2.prepareDelete("test", "type", "1").setVersion(17).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(true));

        try {
            // this should conflict with the delete command transaction which told us that the object was deleted at version 17.
            client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute().actionGet();
            throw new AssertionError("Expected to get a version conflict but didn't get one");
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        IndexResponse indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(18).
                setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(18L));
        assertThat(indexResponse.getPreviousVersion(), equalTo(17L));


    }

    @Test
    public void testExternalVersioning() throws Exception {

        IndexResponse indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(12).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(12l));
        assertThat(indexResponse.getPreviousVersion(), equalTo(Engine.VERSION_NOT_FOUND));

        indexResponse = client2.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(14).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(14l));
        assertThat(indexResponse.getPreviousVersion(), equalTo(12l));

        assertThrows(client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute(),
                     VersionConflictEngineException.class);


        client.admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client.prepareGet("test", "type", "1").execute().actionGet().getVersion(), equalTo(14l));
        }

        // deleting with a lower version fails.
        assertThrows(
            client2.prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL).execute(),
            VersionConflictEngineException.class);



        // Delete with a higher version deletes all versions up to the given one.
        DeleteResponse deleteResponse = client2.prepareDelete("test", "type", "1").setVersion(17).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(17l));
        assertThat(deleteResponse.getPreviousVersion(), equalTo(14l));

        // Deleting with a lower version keeps on failing after a delete.
        assertThrows(
            client2.prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL).execute(),
            VersionConflictEngineException.class);


        // But delete with a higher version is OK.
        deleteResponse = client2.prepareDelete("test", "type", "1").setVersion(18).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(18l));


        // TODO: This behavior breaks rest api returning http status 201, good news is that it this is only the case until deletes GC kicks in.
        indexResponse = client2.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(19).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(19l));
        assertThat(indexResponse.getPreviousVersion(), equalTo(18l));


        deleteResponse = client2.prepareDelete("test", "type", "1").setVersion(20).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(20l));
        assertThat(deleteResponse.getPreviousVersion(), equalTo(19l));

        // Make sure that the next delete will be GC. Note we do it on the index settings so it will be cleaned up
        HashMap<String,Object> newSettings = new HashMap<String, Object>();
        newSettings.put("index.gc_deletes",-1);
        client.admin().indices().prepareUpdateSettings("test").setSettings(newSettings).execute().actionGet();

        Thread.sleep(300); // gc works based on estimated sampled time. Give it a chance...

        // And now we have previous version return -1
        indexResponse = client2.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(20).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(20l));
        assertThat(indexResponse.getPreviousVersion(), equalTo(Engine.VERSION_NOT_FOUND));


    }

    @Test
    public void testInternalVersioningInitialDelete() throws Exception {
        assertThrows(client2.prepareDelete("test", "type", "1").setVersion(17).execute(),
                VersionConflictEngineException.class);

        IndexResponse indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_1")
                .setCreate(true).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));
        assertThat(indexResponse.getPreviousVersion(), equalTo(Engine.VERSION_NOT_FOUND));
    }


    @Test
    public void testInternalVersioning() throws Exception {



        IndexResponse indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));
        assertThat(indexResponse.getPreviousVersion(), equalTo(Engine.VERSION_NOT_FOUND));

        indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(1).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(2l));
        assertThat(indexResponse.getPreviousVersion(), equalTo(1l));

        assertThrows(
            client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
            VersionConflictEngineException.class);

        assertThrows(
            client2.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
            VersionConflictEngineException.class);

        assertThrows(
            client.prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);
        assertThrows(
            client2.prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(
                client.prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(2).execute(),
                DocumentAlreadyExistsException.class);
        assertThrows(
                client2.prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(2).execute(),
                DocumentAlreadyExistsException.class);


        assertThrows(client.prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);
        assertThrows(client2.prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);

        client.admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client.prepareGet("test", "type", "1").execute().actionGet().getVersion(), equalTo(2l));
        }

        // search with versioning
        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).setVersion(true).execute().actionGet();
            assertThat(searchResponse.getHits().getAt(0).version(), equalTo(2l));
        }

        // search without versioning
        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
            assertThat(searchResponse.getHits().getAt(0).version(), equalTo(Engine.VERSION_NOT_FOUND));
        }

        DeleteResponse deleteResponse = client2.prepareDelete("test", "type", "1").setVersion(2).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(3l));
        assertThat(deleteResponse.getPreviousVersion(), equalTo(2l));

        assertThrows(client2.prepareDelete("test", "type", "1").setVersion(2).execute(), VersionConflictEngineException.class);


        // This is intricate - the object was deleted but a delete transaction was with the right version. We add another one
        // and thus the transcation is increased.
        deleteResponse = client2.prepareDelete("test", "type", "1").setVersion(3).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(4l));
        assertThat(deleteResponse.getPreviousVersion(), equalTo(3l));
    }

    @Test
    public void testSimpleVersioningWithFlush() throws Exception {

        IndexResponse indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));
        assertThat(indexResponse.getPreviousVersion(), equalTo(Engine.VERSION_NOT_FOUND));

        client.admin().indices().prepareFlush().execute().actionGet();

        indexResponse = client.prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(1).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(2l));
        assertThat(indexResponse.getPreviousVersion(), equalTo(1l));

        client.admin().indices().prepareFlush().execute().actionGet();

        assertThrows(client.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(client2.prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);
        assertThrows(client.prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(client2.prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(client.prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);
        assertThrows(client2.prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);

        client.admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client.prepareGet("test", "type", "1").execute().actionGet().getVersion(), equalTo(2l));
        }

        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).setVersion(true).execute().actionGet();
            assertThat(searchResponse.getHits().getAt(0).version(), equalTo(2l));
        }
    }

    @Test
    public void testVersioningWithBulk() {

        BulkResponse bulkResponse = client.prepareBulk().add(client.prepareIndex("test", "type", "1").setSource("field1", "value1_1")).execute().actionGet();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(1));
        IndexResponse indexResponse = bulkResponse.getItems()[0].getResponse();
        assertThat(indexResponse.getVersion(), equalTo(1l));
    }
}
