/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.history;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.ilm.history.ILMHistoryStore.ILM_HISTORY_ALIAS;
import static org.elasticsearch.xpack.ilm.history.ILMHistoryStore.ILM_HISTORY_INDEX_PREFIX;
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
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        historyStore = new ILMHistoryStore(Settings.EMPTY, client, clusterService, threadPool);
    }

    @After
    public void setdown() {
        historyStore.close();
        clusterService.close();
        client.close();
        threadPool.shutdownNow();
    }

    public void testNoActionIfDisabled() throws Exception {
        Settings settings = Settings.builder().put(LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.getKey(), false).build();
        try (ILMHistoryStore disabledHistoryStore = new ILMHistoryStore(settings, client, null, threadPool)) {
            String policyId = randomAlphaOfLength(5);
            final long timestamp = randomNonNegativeLong();
            ILMHistoryItem record = ILMHistoryItem.success("index", policyId, timestamp, null, null);

            CountDownLatch latch = new CountDownLatch(1);
            client.setVerifier((a, r, l) -> {
                fail("the history store is disabled, no action should have been taken");
                latch.countDown();
                return null;
            });
            disabledHistoryStore.putAsync(record);
            latch.await(10, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    public void testPut() throws Exception {
        String policyId = randomAlphaOfLength(5);
        final long timestamp = randomNonNegativeLong();
        {
            ILMHistoryItem record = ILMHistoryItem.success("index", policyId, timestamp, 10L,
                LifecycleExecutionState.builder()
                    .setPhase("phase")
                    .build());

            AtomicInteger calledTimes = new AtomicInteger(0);
            client.setVerifier((action, request, listener) -> {
                if (action instanceof CreateIndexAction && request instanceof CreateIndexRequest) {
                    return new CreateIndexResponse(true, true, ((CreateIndexRequest) request).index());
                }
                calledTimes.incrementAndGet();
                assertThat(action, instanceOf(BulkAction.class));
                assertThat(request, instanceOf(BulkRequest.class));
                BulkRequest bulkRequest = (BulkRequest) request;
                bulkRequest.requests().forEach(dwr -> assertEquals(ILM_HISTORY_ALIAS, dwr.index()));
                assertNotNull(listener);

                // The content of this BulkResponse doesn't matter, so just make it have the same number of responses
                int responses = bulkRequest.numberOfActions();
                return new BulkResponse(IntStream.range(0, responses)
                    .mapToObj(i -> new BulkItemResponse(i, DocWriteRequest.OpType.INDEX,
                        new IndexResponse(new ShardId("index", "uuid", 0), randomAlphaOfLength(10), 1, 1, 1, true)))
                    .toArray(BulkItemResponse[]::new),
                    1000L);
            });

            historyStore.putAsync(record);
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }

        {
            final String cause = randomAlphaOfLength(9);
            Exception failureException = new RuntimeException(cause);
            ILMHistoryItem record = ILMHistoryItem.failure("index", policyId, timestamp, 10L,
                LifecycleExecutionState.builder()
                    .setPhase("phase")
                    .build(), failureException);

            AtomicInteger calledTimes = new AtomicInteger(0);
            client.setVerifier((action, request, listener) -> {
                if (action instanceof CreateIndexAction && request instanceof CreateIndexRequest) {
                    return new CreateIndexResponse(true, true, ((CreateIndexRequest) request).index());
                }
                calledTimes.incrementAndGet();
                assertThat(action, instanceOf(BulkAction.class));
                assertThat(request, instanceOf(BulkRequest.class));
                BulkRequest bulkRequest = (BulkRequest) request;
                bulkRequest.requests().forEach(dwr -> {
                    assertEquals(ILM_HISTORY_ALIAS, dwr.index());
                    assertThat(dwr, instanceOf(IndexRequest.class));
                    IndexRequest ir = (IndexRequest) dwr;
                    String indexedDocument = ir.source().utf8ToString();
                    assertThat(indexedDocument, Matchers.containsString("runtime_exception"));
                    assertThat(indexedDocument, Matchers.containsString(cause));
                });
                assertNotNull(listener);

                // The content of this BulkResponse doesn't matter, so just make it have the same number of responses with failures
                int responses = bulkRequest.numberOfActions();
                return new BulkResponse(IntStream.range(0, responses)
                    .mapToObj(i -> new BulkItemResponse(i, DocWriteRequest.OpType.INDEX,
                        new BulkItemResponse.Failure("index", i + "", failureException)))
                    .toArray(BulkItemResponse[]::new),
                    1000L);
            });

            historyStore.putAsync(record);
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }
    }

    public void testHistoryIndexNeedsCreation() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder())
            .build();

        client.setVerifier((a, r, l) -> {
            assertThat(a, instanceOf(CreateIndexAction.class));
            assertThat(r, instanceOf(CreateIndexRequest.class));
            CreateIndexRequest request = (CreateIndexRequest) r;
            assertThat(request.aliases(), Matchers.hasSize(1));
            request.aliases().forEach(alias -> {
                assertThat(alias.name(), equalTo(ILM_HISTORY_ALIAS));
                assertTrue(alias.writeIndex());
            });
            return new CreateIndexResponse(true, true, request.index());
        });

        CountDownLatch latch = new CountDownLatch(1);
        ILMHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            Assert::assertTrue,
            ex -> {
                logger.error(ex);
                fail("should have called onResponse, not onFailure");
            }), latch));

        ElasticsearchAssertions.awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testHistoryIndexProperlyExistsAlready() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder(ILM_HISTORY_INDEX_PREFIX + "000001")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))
                    .putAlias(AliasMetadata.builder(ILM_HISTORY_ALIAS)
                        .writeIndex(true)
                        .build())))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        ILMHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            Assert::assertFalse,
            ex -> {
                logger.error(ex);
                fail("should have called onResponse, not onFailure");
            }), latch));

        ElasticsearchAssertions.awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testHistoryIndexHasNoWriteIndex() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder(ILM_HISTORY_INDEX_PREFIX + "000001")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))
                    .putAlias(AliasMetadata.builder(ILM_HISTORY_ALIAS)
                        .build()))
                .put(IndexMetadata.builder(randomAlphaOfLength(5))
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))
                    .putAlias(AliasMetadata.builder(ILM_HISTORY_ALIAS)
                        .build())))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        ILMHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            indexCreated -> fail("should have called onFailure, not onResponse"),
            ex -> {
                assertThat(ex, instanceOf(IllegalStateException.class));
                assertThat(ex.getMessage(), Matchers.containsString("ILM history alias [" + ILM_HISTORY_ALIAS +
                    "does not have a write index"));
            }), latch));

        ElasticsearchAssertions.awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testHistoryIndexNotAlias() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder(ILM_HISTORY_ALIAS)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        ILMHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            indexCreated -> fail("should have called onFailure, not onResponse"),
            ex -> {
                assertThat(ex, instanceOf(IllegalStateException.class));
                assertThat(ex.getMessage(), Matchers.containsString("ILM history alias [" + ILM_HISTORY_ALIAS +
                    "] already exists as concrete index"));
            }), latch));

        ElasticsearchAssertions.awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testHistoryIndexCreatedConcurrently() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder())
            .build();

        client.setVerifier((a, r, l) -> {
            assertThat(a, instanceOf(CreateIndexAction.class));
            assertThat(r, instanceOf(CreateIndexRequest.class));
            CreateIndexRequest request = (CreateIndexRequest) r;
            assertThat(request.aliases(), Matchers.hasSize(1));
            request.aliases().forEach(alias -> {
                assertThat(alias.name(), equalTo(ILM_HISTORY_ALIAS));
                assertTrue(alias.writeIndex());
            });
            throw new ResourceAlreadyExistsException("that index already exists");
        });

        CountDownLatch latch = new CountDownLatch(1);
        ILMHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            Assert::assertFalse,
            ex -> {
                logger.error(ex);
                fail("should have called onResponse, not onFailure");
            }), latch));

        ElasticsearchAssertions.awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testHistoryAliasDoesntExistButIndexDoes() throws InterruptedException {
        final String initialIndex = ILM_HISTORY_INDEX_PREFIX + "000001";
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder(initialIndex)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        ILMHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            response -> {
                logger.error(response);
                fail("should have called onFailure, not onResponse");
            },
            ex -> {
                assertThat(ex, instanceOf(IllegalStateException.class));
                assertThat(ex.getMessage(), Matchers.containsString("ILM history index [" + initialIndex +
                    "] already exists but does not have alias [" + ILM_HISTORY_ALIAS + "]"));
            }), latch));

        ElasticsearchAssertions.awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void assertContainsMap(String indexedDocument, Map<String, Object> map) {
        map.forEach((k, v) -> {
            assertThat(indexedDocument, Matchers.containsString(k));
            if (v instanceof Map) {
                assertContainsMap(indexedDocument, (Map<String, Object>) v);
            }
            if (v instanceof Iterable) {
                ((Iterable) v).forEach(elem -> {
                    assertThat(indexedDocument, Matchers.containsString(elem.toString()));
                });
            } else {
                assertThat(indexedDocument, Matchers.containsString(v.toString()));
            }
        });
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
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
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
