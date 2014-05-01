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

package org.elasticsearch.cluster;

import com.google.common.base.Predicate;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.HashMap;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
@ClusterScope(scope= Scope.TEST, numDataNodes =0)
public class NoMasterNodeTests extends ElasticsearchIntegrationTest {

    @Test
    @TestLogging("action:TRACE,cluster.service:TRACE")
    public void testNoMasterActions() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("action.auto_create_index", false)
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put(DiscoverySettings.NO_MASTER_BLOCK, "all")
                .build();

        TimeValue timeout = TimeValue.timeValueMillis(200);

        cluster().startNode(settings);
        // start a second node, create an index, and then shut it down so we have no master block
        cluster().startNode(settings);
        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        cluster().stopRandomDataNode();
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
            }
        }), equalTo(true));


        try {
            client().prepareGet("test", "type1", "1").execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            client().prepareMultiGet().add("test", "type1", "1").execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            PercolateSourceBuilder percolateSource = new PercolateSourceBuilder();
            percolateSource.percolateDocument().setDoc(new HashMap());
            client().preparePercolate()
                    .setIndices("test").setDocumentType("type1")
                    .setSource(percolateSource).execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        long now = System.currentTimeMillis();
        try {
            client().prepareUpdate("test", "type1", "1").setScript("test script").setTimeout(timeout).execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            client().admin().indices().prepareAnalyze("test", "this is a test").execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            client().prepareCount("test").execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        now = System.currentTimeMillis();
        try {
            client().prepareIndex("test", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout).execute().actionGet();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        cluster().startNode(settings);
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
    }

    @Test
    public void testNoMasterActions_writeMasterBlock() throws Exception {
        Settings settings = settingsBuilder()
                .put("discovery.type", "zen")
                .put("action.auto_create_index", false)
                .put("discovery.zen.minimum_master_nodes", 2)
                .put("discovery.zen.ping_timeout", "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put(DiscoverySettings.NO_MASTER_BLOCK, "write")
                .build();

        cluster().startNode(settings);
        // start a second node, create an index, and then shut it down so we have no master block
        cluster().startNode(settings);
        prepareCreate("test1").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).get();
        prepareCreate("test2").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).get();
        client().admin().cluster().prepareHealth("_all").setWaitForGreenStatus().get();
        client().prepareIndex("test1", "type1", "1").setSource("field", "value1").get();
        client().prepareIndex("test2", "type1", "1").setSource("field", "value1").get();
        refresh();

        cluster().stopRandomDataNode();
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).get().getState();
                return state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
            }
        }), equalTo(true));


        GetResponse getResponse = client().prepareGet("test1", "type1", "1").get();
        assertExists(getResponse);

        CountResponse countResponse = client().prepareCount("test1").get();
        assertHitCount(countResponse, 1l);

        SearchResponse searchResponse = client().prepareSearch("test1").get();
        assertHitCount(searchResponse, 1l);

        countResponse = client().prepareCount("test2").get();
        assertThat(countResponse.getTotalShards(), equalTo(2));
        assertThat(countResponse.getSuccessfulShards(), equalTo(1));

        TimeValue timeout = TimeValue.timeValueMillis(200);
        long now = System.currentTimeMillis();
        try {
            client().prepareUpdate("test1", "type1", "1").setDoc("field", "value2").setTimeout(timeout).get();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        now = System.currentTimeMillis();
        try {
            client().prepareIndex("test1", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout).get();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        cluster().startNode(settings);
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").get();
    }
}
