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

package org.elasticsearch.action.bulk;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.TransportBulkActionTookTests.Resolver;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;

public class TransportBulkActionTests extends ESTestCase {

    /** Services needed by bulk action */
    private TransportService transportService;
    private ClusterService clusterService;
    private ThreadPool threadPool;

    private TestTransportBulkAction bulkAction;
    private TransportShardBulkAction shardBulkAction;

    class TestTransportBulkAction extends TransportBulkAction {

        boolean indexCreated = false; // set when the "real" index is created

        TestTransportBulkAction(TransportShardBulkAction shardBulkAction) {
            super(Settings.EMPTY, TransportBulkActionTests.this.threadPool, transportService, clusterService, null, shardBulkAction,
                    null, new ActionFilters(Collections.emptySet()), new Resolver(Settings.EMPTY),
                    new AutoCreateIndex(Settings.EMPTY, clusterService.getClusterSettings(), new Resolver(Settings.EMPTY)));
        }

        @Override
        protected boolean needToCheck() {
            return true;
        }

        @Override
        void createIndex(String index, TimeValue timeout, ActionListener<CreateIndexResponse> listener) {
            indexCreated = true;
            listener.onResponse(null);
        }
    }

    class TestTransportShardBulkAction extends TransportShardBulkAction {

        TestTransportShardBulkAction(TransportService transportService) {
            super(Settings.EMPTY, transportService, null, null, TransportBulkActionTests.this.threadPool,
                null, null, null, new ActionFilters(Collections.emptySet()), null);
        }

        @Override
        protected void doExecute(Task task, BulkShardRequest request, ActionListener<BulkShardResponse> listener) {
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("TransportBulkActionTookTests");
        clusterService = createClusterService(threadPool);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = new TransportService(clusterService.getSettings(), capturingTransport, threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        bulkAction = new TestTransportBulkAction(new TestTransportShardBulkAction(transportService));
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }

    public void testDeleteNonExistingDocDoesNotCreateIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest().add(new DeleteRequest("index", "type", "id"));

        bulkAction.execute(null, bulkRequest, ActionListener.wrap(response -> {
            assertFalse(bulkAction.indexCreated);
            BulkItemResponse[] bulkResponses = ((BulkResponse) response).getItems();
            assertEquals(bulkResponses.length, 1);
            assertTrue(bulkResponses[0].isFailed());
            assertTrue(bulkResponses[0].getFailure().getCause() instanceof IndexNotFoundException);
            assertEquals("index", bulkResponses[0].getFailure().getIndex());
        }, exception -> {
            throw new AssertionError(exception);
        }));
    }

    public void testDeleteNonExistingDocExternalVersionCreatesIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest()
                .add(new DeleteRequest("index", "type", "id").versionType(VersionType.EXTERNAL).version(0));

        bulkAction.execute(null, bulkRequest, ActionListener.wrap(response -> {
            assertTrue(bulkAction.indexCreated);
        }, exception -> {
            throw new AssertionError(exception);
        }));
    }

    public void testDeleteNonExistingDocExternalGteVersionCreatesIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest()
                .add(new DeleteRequest("index2", "type", "id").versionType(VersionType.EXTERNAL_GTE).version(0));

        bulkAction.execute(null, bulkRequest, ActionListener.wrap(response -> {
            assertTrue(bulkAction.indexCreated);
        }, exception -> {
            throw new AssertionError(exception);
        }));
    }

    public void testRoutingResolvesToWriteIndexAliasMetaData() throws Exception {
        ClusterState oldState = clusterService.state();
        clusterService.submitStateUpdateTask("_update", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetaData indexMetaData = IndexMetaData.builder("index1")
                            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                            .numberOfShards(1).numberOfReplicas(0)
                            .putAlias(AliasMetaData.builder("alias1").routing("1").writeIndex(true)).build();
                IndexMetaData indexMetaData2 = IndexMetaData.builder("index2")
                    .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(1).numberOfReplicas(0)
                    .putAlias(AliasMetaData.builder("alias1").writeIndex(false)).build();
                IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetaData.getIndex())
                    .addShard(TestShardRouting.newShardRouting(new ShardId(indexMetaData.getIndex(), 0),
                        currentState.nodes().getMasterNodeId(), true, ShardRoutingState.STARTED)).build();
                return ClusterState.builder(currentState)
                    .metaData(MetaData.builder(currentState.metaData())
                        .put(IndexMetaData.builder(indexMetaData))
                        .put(IndexMetaData.builder(indexMetaData2)))
                    .routingTable(RoutingTable.builder(currentState.routingTable()).add(indexRoutingTable).build())
                    .build();
            }

            @Override
            public void onFailure(String source, Exception e) {
            }
        });

        // assert that the cluster state update task has completed before continuing with requests
        assertBusy(() -> assertNotNull(clusterService.state().metaData().getAliasAndIndexLookup().get("alias1")));

        IndexRequest indexRequestWithRouting = new IndexRequest("alias1", "type", "id");
        indexRequestWithRouting.source(Collections.singletonMap("foo", "bar"));
        DeleteRequest deleteRequestWithRouting = new DeleteRequest("alias1", "type", "id");
        UpdateRequest updateRequestWithRouting = new UpdateRequest("alias1", "type", "id");
        updateRequestWithRouting.doc(Collections.singletonMap("foo", "baz"));
        BulkRequest bulkRequest = new BulkRequest()
            .add(indexRequestWithRouting)
            .add(deleteRequestWithRouting)
            .add(updateRequestWithRouting);

        bulkAction.execute(null, bulkRequest, ActionListener.wrap(response -> {
            assertThat(indexRequestWithRouting.routing(), equalTo("1"));
            assertThat(deleteRequestWithRouting.routing(), equalTo("1"));
            assertThat(updateRequestWithRouting.routing(), equalTo("1"));
        }, exception -> {
            throw new AssertionError(exception);
        }));
    }
}
