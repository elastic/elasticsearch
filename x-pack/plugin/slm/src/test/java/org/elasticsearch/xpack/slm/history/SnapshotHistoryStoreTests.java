/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.history;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.core.ilm.GenerateSnapshotNameStep.generateSnapshotName;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.slm.history.SnapshotHistoryStore.SLM_HISTORY_DATA_STREAM;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsEqual.equalTo;

public class SnapshotHistoryStoreTests extends ESTestCase {

    private ThreadPool threadPool;
    private SnapshotLifecycleTemplateRegistryTests.VerifyingClient client;
    private SnapshotHistoryStore historyStore;
    private ClusterService clusterService;

    @Before
    public void setup() throws IOException {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new SnapshotLifecycleTemplateRegistryTests.VerifyingClient(threadPool);
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(SLM_HISTORY_INDEX_ENABLED_SETTING))
        );
        clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);
        ClusterState state = clusterService.state();
        Metadata.Builder metadataBuilder = Metadata.builder(state.getMetadata())
            .indexTemplates(
                Map.of(
                    SnapshotLifecycleTemplateRegistry.SLM_TEMPLATE_CONFIG.getTemplateName(),
                    SnapshotLifecycleTemplateRegistry.SLM_TEMPLATE_CONFIG.load(ComposableIndexTemplate::parse)
                )
            );
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(state).metadata(metadataBuilder).build());
        historyStore = new SnapshotHistoryStore(client, clusterService, threadPool);
    }

    @After
    public void stopClusterServiceAndThreadPool() throws Exception {
        historyStore.close();
        clusterService.stop();
        threadPool.shutdownNow();
    }

    public void testNoActionIfDisabled() throws Exception {
        ClusterState state = clusterService.state();
        Metadata.Builder metadata = Metadata.builder(state.metadata())
            .persistentSettings(Settings.builder().put(SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false).build());
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(state).metadata(metadata));

        String policyId = randomAlphaOfLength(5);
        SnapshotLifecyclePolicy policy = randomSnapshotLifecyclePolicy(policyId);
        final long timestamp = randomNonNegativeLong();
        String snapshotId = generateSnapshotName(policy.getName());
        SnapshotHistoryItem record = SnapshotHistoryItem.creationSuccessRecord(timestamp, policy, snapshotId);

        client.setVerifier((a, r, l) -> {
            fail("the history store is disabled, no action should have been taken");
            return null;
        });
        historyStore.putAsync(record);
    }

    @SuppressWarnings("unchecked")
    public void testPut() throws Exception {
        String policyId = randomAlphaOfLength(5);
        SnapshotLifecyclePolicy policy = randomSnapshotLifecyclePolicy(policyId);
        final long timestamp = randomNonNegativeLong();
        String snapshotId = generateSnapshotName(policy.getName());
        {
            SnapshotHistoryItem record = SnapshotHistoryItem.creationSuccessRecord(timestamp, policy, snapshotId);

            AtomicInteger calledTimes = new AtomicInteger(0);
            client.setVerifier((action, request, listener) -> {
                calledTimes.incrementAndGet();
                assertThat(action, sameInstance(TransportBulkAction.TYPE));
                assertThat(request, instanceOf(BulkRequest.class));
                BulkRequest bulkRequest = (BulkRequest) request;
                assertEquals(1, bulkRequest.numberOfActions());
                IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);
                assertEquals(SLM_HISTORY_DATA_STREAM, indexRequest.index());
                final String indexedDocument = indexRequest.source().utf8ToString();
                assertThat(indexedDocument, containsString(policy.getId()));
                assertThat(indexedDocument, containsString(policy.getRepository()));
                assertThat(indexedDocument, containsString(snapshotId));
                if (policy.getConfig() != null) {
                    assertContainsMap(indexedDocument, policy.getConfig());
                }
                assertNotNull(listener);
                // Return a successful bulk response
                return new BulkResponse(new BulkItemResponse[0], 0L);
            });

            historyStore.putAsync(record);
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }

        {
            final String cause = randomAlphaOfLength(9);
            Exception failureException = new RuntimeException(cause);
            SnapshotHistoryItem record = SnapshotHistoryItem.creationFailureRecord(timestamp, policy, snapshotId, failureException);

            AtomicInteger calledTimes = new AtomicInteger(0);
            client.setVerifier((action, request, listener) -> {
                if (action == TransportCreateIndexAction.TYPE && request instanceof CreateIndexRequest) {
                    return new CreateIndexResponse(true, true, ((CreateIndexRequest) request).index());
                }
                calledTimes.incrementAndGet();
                assertThat(action, sameInstance(TransportBulkAction.TYPE));
                assertThat(request, instanceOf(BulkRequest.class));
                BulkRequest bulkRequest = (BulkRequest) request;
                assertEquals(1, bulkRequest.numberOfActions());
                IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(0);
                assertEquals(SLM_HISTORY_DATA_STREAM, indexRequest.index());
                final String indexedDocument = indexRequest.source().utf8ToString();
                assertThat(indexedDocument, containsString(policy.getId()));
                assertThat(indexedDocument, containsString(policy.getRepository()));
                assertThat(indexedDocument, containsString(snapshotId));
                if (policy.getConfig() != null) {
                    assertContainsMap(indexedDocument, policy.getConfig());
                }
                assertThat(indexedDocument, containsString("runtime_exception"));
                assertThat(indexedDocument, containsString(cause));
                assertNotNull(listener);
                // Return a successful bulk response
                return new BulkResponse(new BulkItemResponse[0], 0L);
            });

            historyStore.putAsync(record);
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }
    }

    public void testRetryOnEsRejectedExecutionException() throws Exception {
        String policyId = randomAlphaOfLength(5);
        SnapshotLifecyclePolicy policy = randomSnapshotLifecyclePolicy(policyId);
        final long timestamp = randomNonNegativeLong();
        String snapshotId = generateSnapshotName(policy.getName());
        SnapshotHistoryItem record = SnapshotHistoryItem.creationSuccessRecord(timestamp, policy, snapshotId);

        AtomicInteger attemptCount = new AtomicInteger(0);
        CountDownLatch successLatch = new CountDownLatch(1);

        // Use test-only constructor with short flushInterval for faster test
        SnapshotHistoryStore retryHistoryStore = new SnapshotHistoryStore(
            client,
            clusterService,
            threadPool,
            ActionListener.wrap(response -> successLatch.countDown(), e -> {
                // BulkProcessor2 will handle retries internally
                if (attemptCount.get() < 3) {
                    // First few attempts should fail with rejection
                    if (e.getCause() instanceof EsRejectedExecutionException == false) {
                        fail("Expected EsRejectedExecutionException on initial attempts");
                    }
                }
            }),
            TimeValue.timeValueMillis(100) // Short flush interval for faster test
        );

        try {
            client.setVerifier((action, request, listener) -> {
                int currentAttempt = attemptCount.incrementAndGet();
                assertThat(action, sameInstance(TransportBulkAction.TYPE));
                assertThat(request, instanceOf(BulkRequest.class));

                if (currentAttempt <= 2) {
                    // First 2 attempts fail with EsRejectedExecutionException
                    // VerifyingClient will call listener.onFailure with this exception
                    throw new EsRejectedExecutionException("rejected");
                } else {
                    // Third attempt succeeds - return response for VerifyingClient to call listener.onResponse
                    return new BulkResponse(new BulkItemResponse[0], 0L);
                }
            });

            retryHistoryStore.putAsync(record);

            // Wait for success after retries
            assertTrue("BulkProcessor2 should retry and eventually succeed", successLatch.await(10, TimeUnit.SECONDS));
            assertEquals("Should have retried 3 times total", 3, attemptCount.get());
        } finally {
            retryHistoryStore.close();
        }
    }

    @SuppressWarnings("unchecked")
    private void assertContainsMap(String indexedDocument, Map<String, Object> map) {
        map.forEach((k, v) -> {
            assertThat(indexedDocument, containsString(k));
            if (v instanceof Map) {
                assertContainsMap(indexedDocument, (Map<String, Object>) v);
            }
            if (v instanceof Iterable) {
                ((Iterable) v).forEach(elem -> { assertThat(indexedDocument, containsString(elem.toString())); });
            } else {
                assertThat(indexedDocument, containsString(v.toString()));
            }
        });
    }

    public static SnapshotLifecyclePolicy randomSnapshotLifecyclePolicy(String id) {
        Map<String, Object> config = null;
        if (randomBoolean()) {
            config = new HashMap<>();
            for (int i = 0; i < randomIntBetween(2, 5); i++) {
                config.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
            }
        }

        return new SnapshotLifecyclePolicy(
            id,
            randomAlphaOfLength(4),
            SnapshotLifecyclePolicyMetadataTests.randomSchedule(),
            randomAlphaOfLength(4),
            config,
            null
        );
    }
}
