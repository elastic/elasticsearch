/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Request;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Response;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction;
import org.elasticsearch.xpack.transform.action.TransportGetCheckpointAction;
import org.elasticsearch.xpack.transform.action.TransportGetCheckpointNodeAction;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformGetCheckpointTests extends ESSingleNodeTestCase {

    private TransportService transportService;
    private ClusterService clusterService;
    private IndicesService indicesService;
    private ThreadPool threadPool;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private MockTransport mockTransport;
    private Task transformTask;
    private final String indexNamePattern = "test_index-";
    private String[] testIndices;
    private int numberOfNodes;
    private int numberOfIndices;
    private int numberOfShards;

    private TestTransportGetCheckpointAction getCheckpointAction;
    private TestTransportGetCheckpointNodeAction getCheckpointNodeAction;
    private ClusterState clusterStateWithIndex;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        numberOfNodes = randomIntBetween(1, 10);
        numberOfIndices = randomIntBetween(1, 10);
        // create at least as many shards as nodes, so every node has at least 1 shard
        numberOfShards = randomIntBetween(numberOfNodes, numberOfNodes * 3);
        threadPool = new TestThreadPool("GetCheckpointActionTests");
        indexNameExpressionResolver = new MockResolver();
        clusterService = getInstanceFromNode(ClusterService.class);
        indicesService = getInstanceFromNode(IndicesService.class);
        mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                if (action.equals(GetCheckpointNodeAction.NAME)) {
                    getCheckpointNodeAction.execute(
                        null,
                        (GetCheckpointNodeAction.Request) request,
                        ActionListener.wrap(r -> { this.handleResponse(requestId, r); }, e -> {
                            this.handleError(requestId, new TransportException(e.getMessage(), e));

                        })
                    );
                }
            }
        };

        transportService = mockTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        List<String> testIndicesList = new ArrayList<>();
        for (int i = 0; i < numberOfIndices; ++i) {
            testIndicesList.add(indexNamePattern + i);
        }
        testIndices = testIndicesList.toArray(new String[0]);
        clusterStateWithIndex = ClusterStateCreationUtils.state(numberOfNodes, testIndices, numberOfShards);

        transformTask = new Task(
            1L,
            "persistent",
            "action",
            TransformField.PERSISTENT_TASK_DESCRIPTION_PREFIX + "the_id",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
        getCheckpointAction = new TestTransportGetCheckpointAction();
        getCheckpointNodeAction = new TestTransportGetCheckpointNodeAction();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        super.tearDown();
    }

    public void testEmptyCheckpoint() throws InterruptedException {
        GetCheckpointAction.Request request = new GetCheckpointAction.Request(Strings.EMPTY_ARRAY, IndicesOptions.LENIENT_EXPAND_OPEN);
        assertCheckpointAction(request, response -> {
            assertNotNull(response.getCheckpoints());
            Map<String, long[]> checkpoints = response.getCheckpoints();
            assertTrue(checkpoints.isEmpty());

        });
    }

    public void testSingleIndexRequest() throws InterruptedException {
        GetCheckpointAction.Request request = new GetCheckpointAction.Request(
            new String[] { indexNamePattern + "0" },
            IndicesOptions.LENIENT_EXPAND_OPEN
        );

        assertCheckpointAction(request, response -> {
            assertNotNull(response.getCheckpoints());
            Map<String, long[]> checkpoints = response.getCheckpoints();
            assertEquals(1, checkpoints.size());
            assertTrue(checkpoints.containsKey(indexNamePattern + "0"));
            for (int i = 0; i < numberOfShards; ++i) {
                assertEquals(42 + i, checkpoints.get(indexNamePattern + "0")[i]);
            }
            assertEquals(numberOfNodes, getCheckpointNodeAction.getCalls());

        });
    }

    public void testMultiIndexRequest() throws InterruptedException {
        GetCheckpointAction.Request request = new GetCheckpointAction.Request(testIndices, IndicesOptions.LENIENT_EXPAND_OPEN);
        assertCheckpointAction(request, response -> {
            assertNotNull(response.getCheckpoints());
            Map<String, long[]> checkpoints = response.getCheckpoints();
            assertEquals(testIndices.length, checkpoints.size());
            for (int i = 0; i < this.numberOfIndices; ++i) {
                assertTrue(checkpoints.containsKey(indexNamePattern + i));
                for (int j = 0; j < numberOfShards; ++j) {
                    assertEquals(42 + i + j, checkpoints.get(indexNamePattern + i)[j]);
                }
            }
            assertEquals(numberOfNodes, getCheckpointNodeAction.getCalls());
        });
    }

    class TestTransportGetCheckpointAction extends TransportGetCheckpointAction {

        TestTransportGetCheckpointAction() {
            super(transportService, new ActionFilters(emptySet()), indicesService, clusterService, indexNameExpressionResolver);
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            resolveIndicesAndGetCheckpoint(task, request, listener, clusterStateWithIndex);
        }

    }

    class TestTransportGetCheckpointNodeAction extends TransportGetCheckpointNodeAction {

        private final IndicesService mockIndicesService;
        private int calls;

        TestTransportGetCheckpointNodeAction() {
            super(transportService, new ActionFilters(emptySet()), indicesService);
            calls = 0;
            mockIndicesService = mock(IndicesService.class);
            for (int i = 0; i < numberOfIndices; ++i) {
                IndexService mockIndexService = mock(IndexService.class);
                IndexMetadata indexMeta = clusterStateWithIndex.metadata().index(indexNamePattern + i);

                IndexSettings mockIndexSettings = new IndexSettings(indexMeta, clusterService.getSettings());
                when(mockIndexService.getIndexSettings()).thenReturn(mockIndexSettings);
                for (int j = 0; j < numberOfShards; ++j) {
                    IndexShard mockIndexShard = mock(IndexShard.class);
                    when(mockIndexService.getShard(j)).thenReturn(mockIndexShard);
                    SeqNoStats seqNoStats = new SeqNoStats(42 + i + j, 42 + i + j, 42 + i + j);
                    when(mockIndexShard.seqNoStats()).thenReturn(seqNoStats);
                }

                when(mockIndicesService.indexServiceSafe(indexMeta.getIndex())).thenReturn(mockIndexService);
            }
        }

        @Override
        protected void doExecute(
            Task task,
            GetCheckpointNodeAction.Request request,
            ActionListener<GetCheckpointNodeAction.Response> listener
        ) {
            ++calls;
            getGlobalCheckpoints(mockIndicesService, request.getShards(), listener);
        }

        public int getCalls() {
            return calls;
        }
    }

    static class MockResolver extends IndexNameExpressionResolver {
        MockResolver() {
            super(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE);
        }

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }

        @Override
        public String[] concreteIndexNames(
            ClusterState state,
            IndicesOptions options,
            boolean includeDataStreams,
            String... indexExpressions
        ) {
            return indexExpressions;
        }

        @Override
        public Index[] concreteIndices(ClusterState state, IndicesRequest request) {
            Index[] out = new Index[request.indices().length];
            for (int x = 0; x < out.length; x++) {
                out[x] = new Index(request.indices()[x], "_na_");
            }
            return out;
        }
    }

    private void assertCheckpointAction(GetCheckpointAction.Request request, Consumer<GetCheckpointAction.Response> furtherTests)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<GetCheckpointAction.Response> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            furtherTests.accept(r);
        }, e -> { fail("got unexpected exception: " + e); }), latch);

        ActionTestUtils.execute(getCheckpointAction, transformTask, request, listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }
}
