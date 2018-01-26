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

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoMinMasterNodes = false)
public class NoMasterNodeIT extends ESIntegTestCase {

    public void testNoMasterActions() throws Exception {
        Settings settings = Settings.builder()
                .put("action.auto_create_index", true)
                .put("discovery.zen.minimum_master_nodes", 2)
                .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put(DiscoverySettings.NO_MASTER_BLOCK_SETTING.getKey(), "all")
                .build();

        final TimeValue timeout = TimeValue.timeValueMillis(10);

        internalCluster().startNode(settings);
        // start a second node, create an index, and then shut it down so we have no master block
        internalCluster().startNode(settings);
        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        internalCluster().stopRandomDataNode();
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
            assertTrue(state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID));
        });

        assertThrows(client().prepareGet("test", "type1", "1"),
                ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(client().prepareGet("no_index", "type1", "1"),
                ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(client().prepareMultiGet().add("test", "type1", "1"),
                ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(client().prepareMultiGet().add("no_index", "type1", "1"),
                ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );


        assertThrows(client().admin().indices().prepareAnalyze("test", "this is a test"),
                ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(client().admin().indices().prepareAnalyze("no_index", "this is a test"),
                ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(client().prepareSearch("test").setSize(0),
                ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(client().prepareSearch("no_index").setSize(0),
                ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        checkUpdateAction(false, timeout,
                client().prepareUpdate("test", "type1", "1")
                        .setScript(new Script(
                            ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "test script", Collections.emptyMap())).setTimeout(timeout));

        checkUpdateAction(true, timeout,
                client().prepareUpdate("no_index", "type1", "1")
                        .setScript(new Script(
                            ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "test script", Collections.emptyMap())).setTimeout(timeout));


        checkWriteAction(
            client().prepareIndex("test", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout));

        checkWriteAction(
            client().prepareIndex("no_index", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout));

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject().endObject()));
        bulkRequestBuilder.add(client().prepareIndex("test", "type1", "2").setSource(XContentFactory.jsonBuilder().startObject().endObject()));
        bulkRequestBuilder.setTimeout(timeout);
        checkWriteAction(bulkRequestBuilder);

        bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("no_index", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject().endObject()));
        bulkRequestBuilder.add(client().prepareIndex("no_index", "type1", "2").setSource(XContentFactory.jsonBuilder().startObject().endObject()));
        bulkRequestBuilder.setTimeout(timeout);
        checkWriteAction(bulkRequestBuilder);

        internalCluster().startNode(settings);
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
    }

    void checkUpdateAction(boolean autoCreateIndex, TimeValue timeout, ActionRequestBuilder<?, ?, ?> builder) {
        // we clean the metadata when loosing a master, therefore all operations on indices will auto create it, if allowed
        try {
            builder.get();
            fail("expected ClusterBlockException or MasterNotDiscoveredException");
        } catch (ClusterBlockException | MasterNotDiscoveredException e) {
            if (e instanceof MasterNotDiscoveredException) {
                assertTrue(autoCreateIndex);
            } else {
                assertFalse(autoCreateIndex);
            }
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
    }

    void checkWriteAction(ActionRequestBuilder<?, ?, ?> builder) {
        try {
            builder.get();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
    }

    public void testNoMasterActionsWriteMasterBlock() throws Exception {
        Settings settings = Settings.builder()
                .put("action.auto_create_index", false)
                .put("discovery.zen.minimum_master_nodes", 2)
                .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), "200ms")
                .put("discovery.initial_state_timeout", "500ms")
                .put(DiscoverySettings.NO_MASTER_BLOCK_SETTING.getKey(), "write")
                .build();

        internalCluster().startNode(settings);
        // start a second node, create an index, and then shut it down so we have no master block
        internalCluster().startNode(settings);
        prepareCreate("test1").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)).get();
        prepareCreate("test2").setSettings(
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();
        client().admin().cluster().prepareHealth("_all").setWaitForGreenStatus().get();
        client().prepareIndex("test1", "type1", "1").setSource("field", "value1").get();
        client().prepareIndex("test2", "type1", "1").setSource("field", "value1").get();
        refresh();

        ensureSearchable("test1", "test2");

        ClusterStateResponse clusterState = client().admin().cluster().prepareState().get();
        logger.info("Cluster state:\n{}", clusterState.getState());

        internalCluster().stopRandomDataNode();
        assertTrue(awaitBusy(() -> {
                    ClusterState state = client().admin().cluster().prepareState().setLocal(true).get().getState();
                    return state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
                }
        ));

        GetResponse getResponse = client().prepareGet("test1", "type1", "1").get();
        assertExists(getResponse);

        SearchResponse countResponse = client().prepareSearch("test1").setSize(0).get();
        assertHitCount(countResponse, 1L);

        SearchResponse searchResponse = client().prepareSearch("test1").get();
        assertHitCount(searchResponse, 1L);

        countResponse = client().prepareSearch("test2").setSize(0).get();
        assertThat(countResponse.getTotalShards(), equalTo(2));
        assertThat(countResponse.getSuccessfulShards(), equalTo(1));

        TimeValue timeout = TimeValue.timeValueMillis(200);
        long now = System.currentTimeMillis();
        try {
            client().prepareUpdate("test1", "type1", "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field", "value2").setTimeout(timeout).get();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        try {
            client().prepareIndex("test1", "type1", "1").setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout).get();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        internalCluster().startNode(settings);
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").get();
    }
}
