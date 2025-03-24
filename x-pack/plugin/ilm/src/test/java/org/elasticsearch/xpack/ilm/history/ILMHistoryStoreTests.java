/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.history;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.ilm.history.ILMHistoryStore.ILM_HISTORY_DATA_STREAM;
import static org.elasticsearch.xpack.ilm.history.ILMHistoryTemplateRegistry.ILM_TEMPLATE_NAME;
import static org.elasticsearch.xpack.ilm.history.ILMHistoryTemplateRegistry.INDEX_TEMPLATE_VERSION;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ILMHistoryStoreTests extends ESTestCase {

    private ThreadPool threadPool;
    private VerifyingClient client;
    private ClusterService clusterService;
    private ILMHistoryStore historyStore;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING))
        );
        clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);
        ILMHistoryTemplateRegistry registry = new ILMHistoryTemplateRegistry(
            clusterService.getSettings(),
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY
        );
        ClusterState state = clusterService.state();
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(state)
                .metadata(Metadata.builder(state.metadata()).indexTemplates(registry.getComposableTemplateConfigs()))
                .build()
        );
        historyStore = new ILMHistoryStore(client, clusterService, threadPool, ActionListener.noop(), TimeValue.timeValueMillis(500));
    }

    @After
    public void setdown() {
        historyStore.close();
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testNoActionIfDisabled() throws Exception {
        ClusterState state = clusterService.state();
        Metadata.Builder metadata = Metadata.builder(state.metadata())
            .persistentSettings(Settings.builder().put(LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.getKey(), false).build());
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(state).metadata(metadata));

        String policyId = randomAlphaOfLength(5);
        final long timestamp = randomNonNegativeLong();
        ILMHistoryItem record = ILMHistoryItem.success("index", policyId, timestamp, null, null);

        CountDownLatch latch = new CountDownLatch(1);
        client.setVerifier((a, r, l) -> {
            fail("the history store is disabled, no action should have been taken");
            latch.countDown();
            return null;
        });
        historyStore.putAsync(record);
        assertFalse(latch.await(2, TimeUnit.SECONDS));
    }

    public void testPut() throws Exception {
        String policyId = randomAlphaOfLength(5);
        final long timestamp = randomNonNegativeLong();
        {
            ILMHistoryItem record = ILMHistoryItem.success(
                "index",
                policyId,
                timestamp,
                10L,
                LifecycleExecutionState.builder().setPhase("phase").build()
            );

            AtomicInteger calledTimes = new AtomicInteger(0);
            client.setVerifier((action, request, listener) -> {
                calledTimes.incrementAndGet();
                assertSame(TransportBulkAction.TYPE, action);
                assertThat(request, instanceOf(BulkRequest.class));
                BulkRequest bulkRequest = (BulkRequest) request;
                bulkRequest.requests().forEach(dwr -> assertEquals(ILM_HISTORY_DATA_STREAM, dwr.index()));
                assertNotNull(listener);

                // The content of this BulkResponse doesn't matter, so just make it have the same number of responses
                int responses = bulkRequest.numberOfActions();
                return new BulkResponse(
                    IntStream.range(0, responses)
                        .mapToObj(
                            i -> BulkItemResponse.success(
                                i,
                                DocWriteRequest.OpType.INDEX,
                                new IndexResponse(new ShardId("index", "uuid", 0), randomAlphaOfLength(10), 1, 1, 1, true)
                            )
                        )
                        .toArray(BulkItemResponse[]::new),
                    1000L
                );
            });

            historyStore.putAsync(record);
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }

        {
            final String cause = randomAlphaOfLength(9);
            Exception failureException = new RuntimeException(cause);
            ILMHistoryItem record = ILMHistoryItem.failure(
                "index",
                policyId,
                timestamp,
                10L,
                LifecycleExecutionState.builder().setPhase("phase").build(),
                failureException
            );

            AtomicInteger calledTimes = new AtomicInteger(0);
            client.setVerifier((action, request, listener) -> {
                if (action == TransportCreateIndexAction.TYPE && request instanceof CreateIndexRequest) {
                    return new CreateIndexResponse(true, true, ((CreateIndexRequest) request).index());
                }
                calledTimes.incrementAndGet();
                assertSame(TransportBulkAction.TYPE, action);
                assertThat(request, instanceOf(BulkRequest.class));
                BulkRequest bulkRequest = (BulkRequest) request;
                bulkRequest.requests().forEach(dwr -> {
                    assertEquals(ILM_HISTORY_DATA_STREAM, dwr.index());
                    assertThat(dwr, instanceOf(IndexRequest.class));
                    IndexRequest ir = (IndexRequest) dwr;
                    String indexedDocument = ir.source().utf8ToString();
                    assertThat(indexedDocument, Matchers.containsString("runtime_exception"));
                    assertThat(indexedDocument, Matchers.containsString(cause));
                });
                assertNotNull(listener);

                // The content of this BulkResponse doesn't matter, so just make it have the same number of responses with failures
                int responses = bulkRequest.numberOfActions();
                return new BulkResponse(
                    IntStream.range(0, responses)
                        .mapToObj(
                            i -> BulkItemResponse.failure(
                                i,
                                DocWriteRequest.OpType.INDEX,
                                new BulkItemResponse.Failure("index", i + "", failureException)
                            )
                        )
                        .toArray(BulkItemResponse[]::new),
                    1000L
                );
            });

            historyStore.putAsync(record);
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }
    }

    /*
     * This tests that things behave correctly if we throw a lot of data at the ILMHistoryStore quickly -- multiple flushes occur, all
     * counts add up, and no deadlock occurs.
     */
    // @TestLogging(
    // value = "org.elasticsearch.action.bulk:trace",
    // reason = "Logging information about locks useful for tracking down deadlock"
    // )
    public void testMultipleFlushes() throws Exception {
        String policyId = randomAlphaOfLength(5);
        final long timestamp = randomNonNegativeLong();
        AtomicLong actions = new AtomicLong(0);
        long numberOfDocs = 400_000;
        CountDownLatch latch = new CountDownLatch((int) numberOfDocs);
        client.setVerifier((action, request, listener) -> {
            assertSame(TransportBulkAction.TYPE, action);
            assertThat(request, instanceOf(BulkRequest.class));
            BulkRequest bulkRequest = (BulkRequest) request;
            List<DocWriteRequest<?>> realRequests = bulkRequest.requests();
            realRequests.forEach(dwr -> assertEquals(ILM_HISTORY_DATA_STREAM, dwr.index()));
            assertNotNull(listener);

            // The content of this BulkResponse doesn't matter, so just make it have the same number of responses
            int responses = bulkRequest.numberOfActions();
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(
                "index",
                "1",
                new Exception("message"),
                RestStatus.TOO_MANY_REQUESTS
            );
            DocWriteResponse response = new IndexResponse(new ShardId("index", "indexUUID", 1), "1", 1L, 1L, 1L, true);
            BulkResponse bulkItemResponse = new BulkResponse(
                IntStream.range(0, responses)
                    .mapToObj(
                        i -> randomBoolean()
                            ? BulkItemResponse.success(i, DocWriteRequest.OpType.INDEX, response)
                            : BulkItemResponse.failure(i, DocWriteRequest.OpType.INDEX, failure)
                    )
                    .toArray(BulkItemResponse[]::new),
                1000L
            );
            return bulkItemResponse;
        });
        try (ILMHistoryStore localHistoryStore = new ILMHistoryStore(client, clusterService, threadPool, new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse response) {
                int itemsInResponse = response.getItems().length;
                actions.addAndGet(itemsInResponse);
                for (int i = 0; i < itemsInResponse; i++) {
                    latch.countDown();
                }
                logger.info("cumulative responses: {}", actions.get());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e);
                fail(e.getMessage());
            }
        }, TimeValue.timeValueMillis(randomIntBetween(50, 1000)))) {
            for (int i = 0; i < numberOfDocs; i++) {
                ILMHistoryItem record1 = ILMHistoryItem.success(
                    "index",
                    policyId,
                    timestamp,
                    10L,
                    LifecycleExecutionState.builder().setPhase("phase").build()
                );
                localHistoryStore.putAsync(record1);
            }
            latch.await(5, TimeUnit.SECONDS);
            assertThat(actions.get(), equalTo(numberOfDocs));
        }
    }

    public void testTemplateNameIsVersioned() {
        assertThat(ILM_TEMPLATE_NAME, endsWith("-" + INDEX_TEMPLATE_VERSION));
    }

    /**
     * A client that delegates to a verifying function for action/request/listener
     */
    public static class VerifyingClient extends NoOpClient {

        private TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier = (a, r, l) -> {
            fail("verifier not set");
            return null;
        };

        VerifyingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            try {
                listener.onResponse((Response) verifier.apply(action, request, listener));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        VerifyingClient setVerifier(TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier) {
            this.verifier = verifier;
            return this;
        }
    }

}
