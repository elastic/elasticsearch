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
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.IsolateAllNodes;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class NoMasterNodeIT extends ESIntegTestCase {

    @Override
    protected int numberOfReplicas() {
        return 2;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testNoMasterActions() throws Exception {
        Settings settings = Settings.builder()
            .put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), true)
            .put(NoMasterBlockService.NO_MASTER_BLOCK_SETTING.getKey(), "all")
            .build();

        final TimeValue timeout = TimeValue.timeValueMillis(10);

        final List<String> nodes = internalCluster().startNodes(3, settings);

        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        final NetworkDisruption disruptionScheme
            = new NetworkDisruption(new IsolateAllNodes(new HashSet<>(nodes)), new NetworkDisconnect());
        internalCluster().setDisruptionScheme(disruptionScheme);
        disruptionScheme.startDisrupting();

        final Client clientToMasterlessNode = client();

        assertBusy(() -> {
            ClusterState state = clientToMasterlessNode.admin().cluster().prepareState().setLocal(true)
                .execute().actionGet().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
        });

        assertThrows(clientToMasterlessNode.prepareGet("test", "1"),
            ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(clientToMasterlessNode.prepareGet("no_index", "1"),
            ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(clientToMasterlessNode.prepareMultiGet().add("test", "1"),
            ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(clientToMasterlessNode.prepareMultiGet().add("no_index", "1"),
            ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(clientToMasterlessNode.admin().indices().prepareAnalyze("test", "this is a test"),
            ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(clientToMasterlessNode.admin().indices().prepareAnalyze("no_index", "this is a test"),
            ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(clientToMasterlessNode.prepareSearch("test").setSize(0),
            ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        assertThrows(clientToMasterlessNode.prepareSearch("no_index").setSize(0),
            ClusterBlockException.class, RestStatus.SERVICE_UNAVAILABLE
        );

        checkUpdateAction(false, timeout,
            clientToMasterlessNode.prepareUpdate("test", "1")
                .setScript(new Script(
                    ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "test script",
                    Collections.emptyMap())).setTimeout(timeout));

        checkUpdateAction(true, timeout,
            clientToMasterlessNode.prepareUpdate("no_index", "1")
                .setScript(new Script(
                    ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "test script",
                    Collections.emptyMap())).setTimeout(timeout));


        checkWriteAction(clientToMasterlessNode.prepareIndex("test").setId("1")
            .setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout));

        checkWriteAction(clientToMasterlessNode.prepareIndex("no_index").setId("1")
            .setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout));

        BulkRequestBuilder bulkRequestBuilder = clientToMasterlessNode.prepareBulk();
        bulkRequestBuilder.add(clientToMasterlessNode.prepareIndex("test").setId("1")
            .setSource(XContentFactory.jsonBuilder().startObject().endObject()));
        bulkRequestBuilder.add(clientToMasterlessNode.prepareIndex("test").setId("2")
            .setSource(XContentFactory.jsonBuilder().startObject().endObject()));
        bulkRequestBuilder.setTimeout(timeout);
        checkWriteAction(bulkRequestBuilder);

        bulkRequestBuilder = clientToMasterlessNode.prepareBulk();
        bulkRequestBuilder.add(clientToMasterlessNode.prepareIndex("no_index").setId("1")
            .setSource(XContentFactory.jsonBuilder().startObject().endObject()));
        bulkRequestBuilder.add(clientToMasterlessNode.prepareIndex("no_index").setId("2")
            .setSource(XContentFactory.jsonBuilder().startObject().endObject()));
        bulkRequestBuilder.setTimeout(timeout);
        checkWriteAction(bulkRequestBuilder);

        internalCluster().clearDisruptionScheme(true);
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
            .put(NoMasterBlockService.NO_MASTER_BLOCK_SETTING.getKey(), "write")
            .build();

        final List<String> nodes = internalCluster().startNodes(3, settings);

        prepareCreate("test1").setSettings(
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)).get();
        prepareCreate("test2").setSettings(
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();
        client().admin().cluster().prepareHealth("_all").setWaitForGreenStatus().get();
        client().prepareIndex("test1").setId("1").setSource("field", "value1").get();
        client().prepareIndex("test2").setId("1").setSource("field", "value1").get();
        refresh();

        ensureSearchable("test1", "test2");

        ClusterStateResponse clusterState = client().admin().cluster().prepareState().get();
        logger.info("Cluster state:\n{}", clusterState.getState());

        final NetworkDisruption disruptionScheme
            = new NetworkDisruption(new IsolateAllNodes(new HashSet<>(nodes)), new NetworkDisconnect());
        internalCluster().setDisruptionScheme(disruptionScheme);
        disruptionScheme.startDisrupting();

        final Client clientToMasterlessNode = client();

        assertBusy(() -> {
            ClusterState state = clientToMasterlessNode.admin().cluster().prepareState().setLocal(true).get().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
        });

        GetResponse getResponse = clientToMasterlessNode.prepareGet("test1", "1").get();
        assertExists(getResponse);

        SearchResponse countResponse = clientToMasterlessNode.prepareSearch("test1").setAllowPartialSearchResults(true).setSize(0).get();
        assertHitCount(countResponse, 1L);

        logger.info("--> here 3");
        SearchResponse searchResponse = clientToMasterlessNode.prepareSearch("test1").setAllowPartialSearchResults(true).get();
        assertHitCount(searchResponse, 1L);

        countResponse = clientToMasterlessNode.prepareSearch("test2").setAllowPartialSearchResults(true).setSize(0).get();
        assertThat(countResponse.getTotalShards(), equalTo(3));
        assertThat(countResponse.getSuccessfulShards(), equalTo(1));

        TimeValue timeout = TimeValue.timeValueMillis(200);
        long now = System.currentTimeMillis();
        try {
            clientToMasterlessNode.prepareUpdate("test1", "1")
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
            clientToMasterlessNode.prepareIndex("test1").setId("1")
                .setSource(XContentFactory.jsonBuilder().startObject().endObject()).setTimeout(timeout).get();
            fail("Expected ClusterBlockException");
        } catch (ClusterBlockException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }

        internalCluster().clearDisruptionScheme(true);
    }
}
