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

package org.elasticsearch.action.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.UUIDs.randomBase64UUID;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiGetActionTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static TransportService transportService;
    private static ClusterService clusterService;
    private static TransportMultiGetAction transportAction;
    private static TransportShardMultiGetAction shardAction;

    @BeforeClass
    public static void beforeClass() throws Exception {
        threadPool = new TestThreadPool(TransportMultiGetActionTests.class.getSimpleName());

        transportService = new TransportService(Settings.EMPTY, mock(Transport.class), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(Settings.builder().put("node.name", "node1").build(),
                boundAddress.publishAddress(), randomBase64UUID()), null, emptySet()) {
            @Override
            public TaskManager getTaskManager() {
                return taskManager;
            }
        };

        final Index index1 = new Index("index1", randomBase64UUID());
        final Index index2 = new Index("index2", randomBase64UUID());
        final ClusterState clusterState = ClusterState.builder(new ClusterName(TransportMultiGetActionTests.class.getSimpleName()))
            .metaData(new MetaData.Builder()
                .put(new IndexMetaData.Builder(index1.getName())
                    .settings(Settings.builder().put("index.version.created", Version.CURRENT)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(IndexMetaData.SETTING_INDEX_UUID, index1.getUUID()))
                    .putMapping(
                        XContentHelper.convertToJson(BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject()
                                .startObject("_doc")
                                    .startObject("_routing")
                                        .field("required", false)
                                    .endObject()
                                .endObject()
                            .endObject()), true, XContentType.JSON)))
                .put(new IndexMetaData.Builder(index2.getName())
                    .settings(Settings.builder().put("index.version.created", Version.CURRENT)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1)
                        .put(IndexMetaData.SETTING_INDEX_UUID, index1.getUUID()))
                    .putMapping(
                        XContentHelper.convertToJson(BytesReference.bytes(XContentFactory.jsonBuilder()
                            .startObject()
                                .startObject("_doc")
                                    .startObject("_routing")
                                        .field("required", true)
                                    .endObject()
                                .endObject()
                            .endObject()), true, XContentType.JSON)))).build();

        final ShardIterator index1ShardIterator = mock(ShardIterator.class);
        when(index1ShardIterator.shardId()).thenReturn(new ShardId(index1, randomInt()));

        final ShardIterator index2ShardIterator = mock(ShardIterator.class);
        when(index2ShardIterator.shardId()).thenReturn(new ShardId(index2, randomInt()));

        final OperationRouting operationRouting = mock(OperationRouting.class);
        when(operationRouting.getShards(eq(clusterState), eq(index1.getName()), anyString(), anyString(), anyString()))
            .thenReturn(index1ShardIterator);
        when(operationRouting.shardId(eq(clusterState), eq(index1.getName()), anyString(), anyString()))
            .thenReturn(new ShardId(index1, randomInt()));
        when(operationRouting.getShards(eq(clusterState), eq(index2.getName()), anyString(), anyString(), anyString()))
            .thenReturn(index2ShardIterator);
        when(operationRouting.shardId(eq(clusterState), eq(index2.getName()), anyString(), anyString()))
            .thenReturn(new ShardId(index2, randomInt()));

        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(transportService.getLocalNode());
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(operationRouting);

        shardAction = new TransportShardMultiGetAction(clusterService, transportService, mock(IndicesService.class), threadPool,
            new ActionFilters(emptySet()), new Resolver()) {
            @Override
            protected void doExecute(Task task, MultiGetShardRequest request, ActionListener<MultiGetShardResponse> listener) {
            }
        };
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        transportService = null;
        clusterService = null;
        transportAction = null;
        shardAction = null;
    }

    public void testTransportMultiGetAction() {
        final Task task = createTask();
        final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        final MultiGetRequestBuilder request = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        request.add(new MultiGetRequest.Item("index1", "1"));
        request.add(new MultiGetRequest.Item("index1", "2"));

        final AtomicBoolean shardActionInvoked = new AtomicBoolean(false);
        transportAction = new TransportMultiGetAction(transportService, clusterService, client,
            new ActionFilters(emptySet()), new Resolver()) {
            @Override
            protected void executeShardAction(final ActionListener<MultiGetResponse> listener,
                                              final AtomicArray<MultiGetItemResponse> responses,
                                              final Map<ShardId, MultiGetShardRequest> shardRequests) {
                shardActionInvoked.set(true);
                assertEquals(2, responses.length());
                assertNull(responses.get(0));
                assertNull(responses.get(1));
            }
        };

        ActionTestUtils.execute(transportAction, task, request.request(), new ActionListenerAdapter());
        assertTrue(shardActionInvoked.get());
    }

    public void testTransportMultiGetAction_withMissingRouting() {
        final Task task = createTask();
        final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        final MultiGetRequestBuilder request = new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE);
        request.add(new MultiGetRequest.Item("index2", "1").routing("1"));
        request.add(new MultiGetRequest.Item("index2", "2"));

        final AtomicBoolean shardActionInvoked = new AtomicBoolean(false);
        transportAction = new TransportMultiGetAction(transportService, clusterService, client,
            new ActionFilters(emptySet()), new Resolver()) {
            @Override
            protected void executeShardAction(final ActionListener<MultiGetResponse> listener,
                                              final AtomicArray<MultiGetItemResponse> responses,
                                              final Map<ShardId, MultiGetShardRequest> shardRequests) {
                shardActionInvoked.set(true);
                assertEquals(2, responses.length());
                assertNull(responses.get(0));
                assertThat(responses.get(1).getFailure().getFailure(), instanceOf(RoutingMissingException.class));
                assertThat(responses.get(1).getFailure().getFailure().getMessage(),
                    equalTo("routing is required for [index2]/[_doc]/[2]"));
            }
        };

        ActionTestUtils.execute(transportAction, task, request.request(), new ActionListenerAdapter());
        assertTrue(shardActionInvoked.get());

    }

    private static Task createTask() {
        return new Task(randomLong(), "transport", MultiGetAction.NAME, "description",
            new TaskId(randomLong() + ":" + randomLong()), emptyMap());
    }

    static class Resolver extends IndexNameExpressionResolver {

        @Override
        public Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
            return new Index("index1", randomBase64UUID());
        }
    }

    static class ActionListenerAdapter implements ActionListener<MultiGetResponse> {

        @Override
        public void onResponse(MultiGetResponse response) {
        }

        @Override
        public void onFailure(Exception e) {
        }
    }
}
