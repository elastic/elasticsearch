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

package org.elasticsearch.versioning;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class SimpleVersioningTests extends ElasticsearchIntegrationTest {


    @Test
    public void testExternalVersioningInitialDelete() throws Exception {

        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(17).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(true));

        try {
            client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(18).setVersionType(VersionType.EXTERNAL).execute().actionGet();
    }

    @Test
    public void testExternalVersioning() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(12).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(14).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(14l));

        try {
            client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").execute().actionGet().getVersion(), equalTo(14l));
        }

        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(17).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(17l));

        try {
            client().prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        deleteResponse = client().prepareDelete("test", "type", "1").setVersion(18).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(18l));
    }

    @Test
    public void testSimpleVersioning() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(1).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(2l));

        try {
            client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client().prepareDelete("test", "type", "1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client().prepareDelete("test", "type", "1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

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
            assertThat(searchResponse.getHits().getAt(0).version(), equalTo(-1l));
        }

        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(2).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(3l));

        try {
            client().prepareDelete("test", "type", "1").setVersion(2).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        deleteResponse = client().prepareDelete("test", "type", "1").setVersion(3).execute().actionGet();
        assertThat(deleteResponse.isNotFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(4l));
    }

    @Test
    public void testSimpleVersioningWithFlush() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));

        client().admin().indices().prepareFlush().execute().actionGet();

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(1).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(2l));

        client().admin().indices().prepareFlush().execute().actionGet();

        try {
            client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client().prepareDelete("test", "type", "1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

        try {
            client().prepareDelete("test", "type", "1").setVersion(1).execute().actionGet();
        } catch (ElasticSearchException e) {
            assertThat(e.unwrapCause(), instanceOf(VersionConflictEngineException.class));
        }

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
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException e) {
            // its ok
        }
        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        BulkResponse bulkResponse = client().prepareBulk().add(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1")).execute().actionGet();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(1));
        IndexResponse indexResponse = bulkResponse.getItems()[0].getResponse();
        assertThat(indexResponse.getVersion(), equalTo(1l));
    }
}
