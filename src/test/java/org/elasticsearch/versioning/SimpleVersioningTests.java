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
package org.elasticsearch.versioning;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashMap;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleVersioningTests extends ElasticsearchIntegrationTest {

    @Test
    public void testExternalVersioningInitialDelete() throws Exception {
        createIndex("test");
        ensureGreen();

        // Note - external version doesn't throw version conflicts on deletes of non existent records. This is different from internal versioning

        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(17).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(false));

        // this should conflict with the delete command transaction which told us that the object was deleted at version 17.
        assertThrows(
                client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute(),
                VersionConflictEngineException.class
        );

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(18).
                setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(18L));
    }

    @Test
    public void testForce() throws Exception {
        createIndex("test");

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(12).setVersionType(VersionType.FORCE).get();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(12).setVersionType(VersionType.FORCE).get();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(14).setVersionType(VersionType.FORCE).get();
        assertThat(indexResponse.getVersion(), equalTo(14l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.FORCE).get();
        assertThat(indexResponse.getVersion(), equalTo(13l));

        client().admin().indices().prepareRefresh().execute().actionGet();
        if (randomBoolean()) {
            refresh();
        }
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").get().getVersion(), equalTo(13l));
        }

        // deleting with a lower version works.
        long v= randomIntBetween(12,14);
        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(v).setVersionType(VersionType.FORCE).get();
        assertThat(deleteResponse.isFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(v));
    }

    @Test
    public void testExternalGTE() throws Exception {
        createIndex("test");

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(12).setVersionType(VersionType.EXTERNAL_GTE).get();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(12).setVersionType(VersionType.EXTERNAL_GTE).get();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(14).setVersionType(VersionType.EXTERNAL_GTE).get();
        assertThat(indexResponse.getVersion(), equalTo(14l));

        assertThrows(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL_GTE),
                VersionConflictEngineException.class);

        client().admin().indices().prepareRefresh().execute().actionGet();
        if (randomBoolean()) {
            refresh();
        }
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").get().getVersion(), equalTo(14l));
        }

        // deleting with a lower version fails.
        assertThrows(
                client().prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL_GTE),
                VersionConflictEngineException.class);

        // Delete with a higher or equal version deletes all versions up to the given one.
        long v= randomIntBetween(14,17);
        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(v).setVersionType(VersionType.EXTERNAL_GTE).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(v));

        // Deleting with a lower version keeps on failing after a delete.
        assertThrows(
                client().prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL_GTE).execute(),
                VersionConflictEngineException.class);


        // But delete with a higher version is OK.
        deleteResponse = client().prepareDelete("test", "type", "1").setVersion(18).setVersionType(VersionType.EXTERNAL_GTE).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(18l));
    }

    @Test
    public void testExternalVersioning() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(12).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(14).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(14l));

        assertThrows(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute(),
                     VersionConflictEngineException.class);

        if (randomBoolean()) {
            refresh();
        }
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").execute().actionGet().getVersion(), equalTo(14l));
        }

        // deleting with a lower version fails.
        assertThrows(
            client().prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL).execute(),
            VersionConflictEngineException.class);

        // Delete with a higher version deletes all versions up to the given one.
        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(17).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(17l));

        // Deleting with a lower version keeps on failing after a delete.
        assertThrows(
            client().prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL).execute(),
            VersionConflictEngineException.class);


        // But delete with a higher version is OK.
        deleteResponse = client().prepareDelete("test", "type", "1").setVersion(18).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(18l));


        // TODO: This behavior breaks rest api returning http status 201, good news is that it this is only the case until deletes GC kicks in.
        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(19).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(19l));


        deleteResponse = client().prepareDelete("test", "type", "1").setVersion(20).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(20l));

        // Make sure that the next delete will be GC. Note we do it on the index settings so it will be cleaned up
        HashMap<String,Object> newSettings = new HashMap<>();
        newSettings.put("index.gc_deletes",-1);
        client().admin().indices().prepareUpdateSettings("test").setSettings(newSettings).execute().actionGet();

        Thread.sleep(300); // gc works based on estimated sampled time. Give it a chance...

        // And now we have previous version return -1
        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(20).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(20l));
    }

    @Test
    public void testInternalVersioningInitialDelete() throws Exception {
        createIndex("test");
        ensureGreen();

        assertThrows(client().prepareDelete("test", "type", "1").setVersion(17).execute(),
                VersionConflictEngineException.class);

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1")
                .setCreate(true).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));
    }


    @Test
    public void testInternalVersioning() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(1).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(2l));

        assertThrows(
                client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(
            client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
            VersionConflictEngineException.class);

        assertThrows(
            client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);
        assertThrows(
            client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(
                client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(2).execute(),
                DocumentAlreadyExistsException.class);
        assertThrows(
                client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(2).execute(),
                DocumentAlreadyExistsException.class);


        assertThrows(client().prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);
        assertThrows(client().prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);

        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").execute().actionGet().getVersion(), equalTo(2l));
        }

        // search with versioning
        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setVersion(true).execute().actionGet();
            assertThat(searchResponse.getHits().getAt(0).version(), equalTo(2l));
        }

        // search without versioning
        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
            assertThat(searchResponse.getHits().getAt(0).version(), equalTo(Versions.NOT_FOUND));
        }

        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(2).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(3l));

        assertThrows(client().prepareDelete("test", "type", "1").setVersion(2).execute(), VersionConflictEngineException.class);


        // This is intricate - the object was deleted but a delete transaction was with the right version. We add another one
        // and thus the transaction is increased.
        deleteResponse = client().prepareDelete("test", "type", "1").setVersion(3).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(4l));
    }

    @Test
    public void testSimpleVersioningWithFlush() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));

        client().admin().indices().prepareFlush().execute().actionGet();

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(1).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(2l));

        client().admin().indices().prepareFlush().execute().actionGet();

        assertThrows(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);
        assertThrows(client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(client().prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);
        assertThrows(client().prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);

        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").execute().actionGet().getVersion(), equalTo(2l));
        }

        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setVersion(true).execute().actionGet();
            assertThat(searchResponse.getHits().getAt(0).version(), equalTo(2l));
        }
    }

    @Test
    public void testVersioningWithBulk() {
        createIndex("test");
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk().add(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1")).execute().actionGet();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(1));
        IndexResponse indexResponse = bulkResponse.getItems()[0].getResponse();
        assertThat(indexResponse.getVersion(), equalTo(1l));
    }
}
