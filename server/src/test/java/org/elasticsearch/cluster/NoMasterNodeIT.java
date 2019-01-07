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
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;

import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoMinMasterNodes = false)
public class NoMasterNodeIT extends ESIntegTestCase {

    @Override
    protected int numberOfReplicas() {
        return 2;
    }

    public void testNoMasterActions() throws Exception {
        Settings settings = Settings.builder()
            .put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), true)
            .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), Integer.MAX_VALUE)
            .put(DiscoverySettings.NO_MASTER_BLOCK_SETTING.getKey(), "all")
            .put(ClusterBootstrapService.INITIAL_MASTER_NODE_COUNT_SETTING.getKey(), 3)
            .build();

        final TimeValue timeout = TimeValue.timeValueMillis(10);

        internalCluster().startNodes(3, settings);

        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        internalCluster().stopRandomDataNode();

        internalCluster().restartRandomDataNode(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {

                final Client remainingClient = client(Arrays.stream(
                    internalCluster().getNodeNames()).filter(n -> n.equals(nodeName) == false).findAny().get());

                assertBusy(() -> {
                    ClusterState state = remainingClient.admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                    assertTrue(state.blocks().hasGlobalBlockWithId(DiscoverySettings.NO_MASTER_BLOCK_ID));
                });

                assertThrows(remainingClient.prepareGet("test", "type1", "1"),
                    ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
                );

                assertThrows(remainingClient.prepareGet("no_index", "type1", "1"),
                    ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
                );

                assertThrows(remainingClient.prepareMultiGet().add("test", "type1", "1"),
                    ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
                );

                assertThrows(remainingClient.prepareMultiGet().add("no_index", "type1", "1"),
                    ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
                );

                assertThrows(remainingClient.admin().indices().prepareAnalyze("test", "this is a test"),
                    ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
                );

                assertThrows(remainingClient.admin().indices().prepareAnalyze("no_index", "this is a test"),
                    ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
                );

                assertThrows(remainingClient.prepareSearch("test").setSize(0),
                    ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
                );

                assertThrows(remainingClient.prepareSearch("no_index").setSize(0),
                    ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
                );

                checkUpdateAction(false, timeout,
                    remainingClient.prepareUpdate("test", "type1", "1")
                        .setScript(new Script(
                            ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "test script",
                            Collections.emptyMap())).setTimeout(timeout));

                checkUpdateAction(true, timeout,
                    remainingClient.prepareUpdate("no_index", "type1", "1")
                        .setScript(new Script(
                            ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "test script",
                            Collections.emptyMap())).setTimeout(timeout));


                checkWriteAction(remainingClient.prepareIndex("test", "type1", "1")
                    .setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout));

                checkWriteAction(remainingClient.prepareIndex("no_index", "type1", "1")
                    .setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout));

                BulkRequestBuilder bulkRequestBuilder = remainingClient.prepareBulk();
                bulkRequestBuilder.add(remainingClient.prepareIndex("test", "type1", "1")
                    .setSource(XContentFactory.jsonBuilder().startObject().endObject()));
                bulkRequestBuilder.add(remainingClient.prepareIndex("test", "type1", "2")
                    .setSource(XContentFactory.jsonBuilder().startObject().endObject()));
                bulkRequestBuilder.setTimeout(timeout);
                checkWriteAction(bulkRequestBuilder);

                bulkRequestBuilder = remainingClient.prepareBulk();
                bulkRequestBuilder.add(remainingClient.prepareIndex("no_index", "type1", "1")
                    .setSource(XContentFactory.jsonBuilder().startObject().endObject()));
                bulkRequestBuilder.add(remainingClient.prepareIndex("no_index", "type1", "2")
                    .setSource(XContentFactory.jsonBuilder().startObject().endObject()));
                bulkRequestBuilder.setTimeout(timeout);
                checkWriteAction(bulkRequestBuilder);

                return Settings.EMPTY;
            }
        });

        internalCluster().startNode(settings);

        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("3").execute().actionGet();
    }

    void checkUpdateAction(boolean autoCreateIndex, TimeValue timeout, ActionRequestBuilder<?, ?> builder) {
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

    void checkWriteAction(ActionRequestBuilder<?, ?> builder) {
        try {
            builder.get();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
    }

    public void testNoMasterActionsWriteMasterBlock() throws Exception {
        Settings settings = Settings.builder()
            .put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), false)
            .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), Integer.MAX_VALUE)
            .put(DiscoverySettings.NO_MASTER_BLOCK_SETTING.getKey(), "write")
            .put(ClusterBootstrapService.INITIAL_MASTER_NODE_COUNT_SETTING.getKey(), 3)
            .build();

        internalCluster().startNodes(3, settings);

        prepareCreate("test1").setSettings(
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)).get();
        prepareCreate("test2").setSettings(
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();
        client().admin().cluster().prepareHealth("_all").setWaitForGreenStatus().get();
        client().prepareIndex("test1", "type1", "1").setSource("field", "value1").get();
        client().prepareIndex("test2", "type1", "1").setSource("field", "value1").get();
        refresh();

        ensureSearchable("test1", "test2");

        ClusterStateResponse clusterState = client().admin().cluster().prepareState().get();
        logger.info("Cluster state:\n{}", clusterState.getState());

        internalCluster().stopRandomDataNode();
        internalCluster().restartRandomDataNode(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {

                final Client remainingClient = client(Arrays.stream(
                    internalCluster().getNodeNames()).filter(n -> n.equals(nodeName) == false).findAny().get());

                assertTrue(awaitBusy(() -> {
                        ClusterState state = remainingClient.admin().cluster().prepareState().setLocal(true).get().getState();
                        return state.blocks().hasGlobalBlockWithId(DiscoverySettings.NO_MASTER_BLOCK_ID);
                    }
                ));

                GetResponse getResponse = remainingClient.prepareGet("test1", "type1", "1").get();
                assertExists(getResponse);

                SearchResponse countResponse = remainingClient.prepareSearch("test1").setAllowPartialSearchResults(true).setSize(0).get();
                assertHitCount(countResponse, 1L);

                logger.info("--> here 3");
                SearchResponse searchResponse = remainingClient.prepareSearch("test1").setAllowPartialSearchResults(true).get();
                assertHitCount(searchResponse, 1L);

                countResponse = remainingClient.prepareSearch("test2").setAllowPartialSearchResults(true).setSize(0).get();
                assertThat(countResponse.getTotalShards(), equalTo(3));
                assertThat(countResponse.getSuccessfulShards(), equalTo(1));

                TimeValue timeout = TimeValue.timeValueMillis(200);
                long now = System.currentTimeMillis();
                try {
                    remainingClient.prepareUpdate("test1", "type1", "1")
                        .setDoc(Requests.INDEX_CONTENT_TYPE, "field", "value2").setTimeout(timeout).get();
                    fail("Expected ClusterBlockException");
                } catch (ClusterBlockException e) {
                    assertThat(System.currentTimeMillis() - now, greaterThan(timeout.millis() - 50));
                    assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
                } catch (Exception e) {
                    logger.info("unexpected", e);
                    throw e;
                }

                try {
                    remainingClient.prepareIndex("test1", "type1", "1")
                        .setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout).get();
                    fail("Expected ClusterBlockException");
                } catch (ClusterBlockException e) {
                    assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
                }

                logger.info("finished assertions, restarting node [{}]", nodeName);

                return Settings.EMPTY;
            }
        });

        internalCluster().startNode(settings);
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("3").get();
    }
}
