/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.get.TransportMultiGetActionTests;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportMultiTermVectorsActionTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static TransportService transportService;
    private static ClusterService clusterService;
    private static TransportMultiTermVectorsAction transportAction;
    private static TransportShardMultiTermsVectorAction shardAction;

    @BeforeClass
    public static void beforeClass() throws Exception {
        threadPool = new TestThreadPool(TransportMultiGetActionTests.class.getSimpleName());

        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(
                Settings.builder().put("node.name", "node1").build(),
                boundAddress.publishAddress(),
                randomBase64UUID()
            ),
            null,
            emptySet()
        );

        final Index index1 = new Index("index1", randomBase64UUID());
        final Index index2 = new Index("index2", randomBase64UUID());
        final ClusterState clusterState = ClusterState.builder(new ClusterName(TransportMultiGetActionTests.class.getSimpleName()))
            .metadata(
                new Metadata.Builder().put(
                    new IndexMetadata.Builder(index1.getName()).settings(
                        indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, index1.getUUID())
                    )
                        .putMapping(
                            XContentHelper.convertToJson(
                                BytesReference.bytes(
                                    XContentFactory.jsonBuilder()
                                        .startObject()
                                        .startObject("_doc")
                                        .startObject("_routing")
                                        .field("required", false)
                                        .endObject()
                                        .endObject()
                                        .endObject()
                                ),
                                true,
                                XContentType.JSON
                            )
                        )
                )
                    .put(
                        new IndexMetadata.Builder(index2.getName()).settings(
                            indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, index1.getUUID())
                        )
                            .putMapping(
                                XContentHelper.convertToJson(
                                    BytesReference.bytes(
                                        XContentFactory.jsonBuilder()
                                            .startObject()
                                            .startObject("_doc")
                                            .startObject("_routing")
                                            .field("required", true)
                                            .endObject()
                                            .endObject()
                                            .endObject()
                                    ),
                                    true,
                                    XContentType.JSON
                                )
                            )
                    )
            )
            .build();

        final ShardIterator index1ShardIterator = mock(ShardIterator.class);
        when(index1ShardIterator.shardId()).thenReturn(new ShardId(index1, randomInt()));

        final ShardIterator index2ShardIterator = mock(ShardIterator.class);
        when(index2ShardIterator.shardId()).thenReturn(new ShardId(index2, randomInt()));

        final OperationRouting operationRouting = mock(OperationRouting.class);
        when(
            operationRouting.getShards(eq(clusterState), eq(index1.getName()), anyString(), nullable(String.class), nullable(String.class))
        ).thenReturn(index1ShardIterator);
        when(operationRouting.shardId(eq(clusterState), eq(index1.getName()), nullable(String.class), nullable(String.class))).thenReturn(
            new ShardId(index1, randomInt())
        );
        when(
            operationRouting.getShards(eq(clusterState), eq(index2.getName()), anyString(), nullable(String.class), nullable(String.class))
        ).thenReturn(index2ShardIterator);
        when(operationRouting.shardId(eq(clusterState), eq(index2.getName()), nullable(String.class), nullable(String.class))).thenReturn(
            new ShardId(index2, randomInt())
        );

        clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(transportService.getLocalNode());
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(operationRouting);

        shardAction = new TransportShardMultiTermsVectorAction(
            clusterService,
            transportService,
            mock(IndicesService.class),
            threadPool,
            new ActionFilters(emptySet()),
            new Resolver()
        ) {
            @Override
            protected void doExecute(
                Task task,
                MultiTermVectorsShardRequest request,
                ActionListener<MultiTermVectorsShardResponse> listener
            ) {}
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
        final MultiTermVectorsRequestBuilder request = new MultiTermVectorsRequestBuilder(client, MultiTermVectorsAction.INSTANCE);
        request.add(new TermVectorsRequest("index1", "1"));
        request.add(new TermVectorsRequest("index2", "2"));

        final AtomicBoolean shardActionInvoked = new AtomicBoolean(false);
        transportAction = new TransportMultiTermVectorsAction(
            transportService,
            clusterService,
            client,
            new ActionFilters(emptySet()),
            new Resolver()
        ) {
            @Override
            protected void executeShardAction(
                final ActionListener<MultiTermVectorsResponse> listener,
                final AtomicArray<MultiTermVectorsItemResponse> responses,
                final Map<ShardId, MultiTermVectorsShardRequest> shardRequests
            ) {
                shardActionInvoked.set(true);
                assertEquals(2, responses.length());
                assertNull(responses.get(0));
                assertNull(responses.get(1));
            }
        };

        ActionTestUtils.execute(transportAction, task, request.request(), ActionListener.noop());
        assertTrue(shardActionInvoked.get());
    }

    public void testTransportMultiGetAction_withMissingRouting() {
        final Task task = createTask();
        final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        final MultiTermVectorsRequestBuilder request = new MultiTermVectorsRequestBuilder(client, MultiTermVectorsAction.INSTANCE);
        request.add(new TermVectorsRequest("index2", "1").routing("1"));
        request.add(new TermVectorsRequest("index2", "2"));

        final AtomicBoolean shardActionInvoked = new AtomicBoolean(false);
        transportAction = new TransportMultiTermVectorsAction(
            transportService,
            clusterService,
            client,
            new ActionFilters(emptySet()),
            new Resolver()
        ) {
            @Override
            protected void executeShardAction(
                final ActionListener<MultiTermVectorsResponse> listener,
                final AtomicArray<MultiTermVectorsItemResponse> responses,
                final Map<ShardId, MultiTermVectorsShardRequest> shardRequests
            ) {
                shardActionInvoked.set(true);
                assertEquals(2, responses.length());
                assertNull(responses.get(0));
                assertThat(responses.get(1).getFailure().getCause(), instanceOf(RoutingMissingException.class));
                assertThat(responses.get(1).getFailure().getCause().getMessage(), equalTo("routing is required for [index1]/[type2]/[2]"));
            }
        };

        ActionTestUtils.execute(transportAction, task, request.request(), ActionListener.noop());
        assertTrue(shardActionInvoked.get());
    }

    private static Task createTask() {
        return new Task(
            randomLong(),
            "transport",
            MultiTermVectorsAction.NAME,
            "description",
            new TaskId(randomLong() + ":" + randomLong()),
            emptyMap()
        );
    }

    static class Resolver extends IndexNameExpressionResolver {

        Resolver() {
            super(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE);
        }

        @Override
        public Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
            return new Index("index1", randomBase64UUID());
        }
    }

}
