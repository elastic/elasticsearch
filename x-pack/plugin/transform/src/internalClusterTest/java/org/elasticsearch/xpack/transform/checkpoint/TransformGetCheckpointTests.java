/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformGetCheckpointTests extends ESSingleNodeTestCase {

    private static final int NUMBER_OF_SHARDS = 5;
    private TransportService transportService;
    private ClusterService clusterService;
    private IndicesService indicesService;
    private ThreadPool threadPool;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private MockTransport mockTransport;
    private final String indexName = "test_index";

    private TestTransportGetCheckpointAction getCheckpointAction;
    private TestTransportGetCheckpointNodeAction getCheckpointNodeAction;
    private ClusterState clusterStateWithIndex;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("GetCheckpointActionTests");
        indexNameExpressionResolver = new MockResolver();
        clusterService = getInstanceFromNode(ClusterService.class);
        indicesService = getInstanceFromNode(IndicesService.class);
        mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                if (action.equals(GetCheckpointNodeAction.NAME)) {

                    getCheckpointNodeAction.execute(null, (GetCheckpointNodeAction.Request) request, ActionListener.wrap(r -> {
                        this.handleResponse(requestId, r);

                    }, e -> {
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
        getCheckpointAction = new TestTransportGetCheckpointAction();
        getCheckpointNodeAction = new TestTransportGetCheckpointNodeAction();
        clusterStateWithIndex = ClusterStateCreationUtils.state(indexName, 3, NUMBER_OF_SHARDS);
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        super.tearDown();
    }

    public void testEmptyCheckpoint() {

    }

    public void testSingleNodeRequests() {
        GetCheckpointAction.Request request = new GetCheckpointAction.Request(
            new String[] { indexName },
            IndicesOptions.LENIENT_EXPAND_OPEN
        );
        Task transformTask = new Task(
            1L,
            "persistent",
            "action",
            TransformField.PERSISTENT_TASK_DESCRIPTION_PREFIX + "the_id",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );

        ActionTestUtils.execute(getCheckpointAction, transformTask, request, ActionListener.wrap(response -> {
            assertNotNull(response.getCheckpoints());
            Map<String, long[]> checkpoints = response.getCheckpoints();
            assertEquals(1, checkpoints.size());
            assertTrue(checkpoints.containsKey(indexName));
            for (int i = 0; i < NUMBER_OF_SHARDS; ++i) {
                assertEquals(42 + i, checkpoints.get(indexName)[i]);
            }
        }, e -> { fail("got unexpected exception: " + e.getMessage()); }));

    }

    class TestTransportGetCheckpointAction extends TransportGetCheckpointAction {

        public TestTransportGetCheckpointAction() {
            super(transportService, new ActionFilters(emptySet()), indicesService, clusterService, indexNameExpressionResolver);
        }

        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            resolveIndicesAndGetCheckpoint(task, request, listener, clusterStateWithIndex);
        }

    }

    class TestTransportGetCheckpointNodeAction extends TransportGetCheckpointNodeAction {

        public TestTransportGetCheckpointNodeAction() {
            super(transportService, new ActionFilters(emptySet()), indicesService);
        }

        protected void doExecute(
            Task task,
            GetCheckpointNodeAction.Request request,
            ActionListener<GetCheckpointNodeAction.Response> listener
        ) {
            IndicesService mockIndicesService = mock(IndicesService.class);
            IndexService mockIndexService = mock(IndexService.class);
            when(mockIndicesService.indexServiceSafe(any())).thenReturn(mockIndexService);
            IndexSettings mockIndexSettings = new IndexSettings(
                clusterStateWithIndex.metadata().index(indexName),
                clusterService.getSettings()
            );
            when(mockIndexService.getIndexSettings()).thenReturn(mockIndexSettings);
            for (int i = 0; i < NUMBER_OF_SHARDS; ++i) {
                IndexShard mockIndexShard = mock(IndexShard.class);
                when(mockIndexService.getShard(i)).thenReturn(mockIndexShard);
                SeqNoStats seqNoStats = new SeqNoStats(42 + i, 42 + i, 42 + i);
                when(mockIndexShard.seqNoStats()).thenReturn(seqNoStats);
            }

            getGlobalCheckpoints(mockIndicesService, request.getShards(), listener);
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

}
