/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm.history;

import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.core.ilm.GenerateSnapshotNameStep.generateSnapshotName;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore.SLM_HISTORY_DATA_STREAM;
import static org.elasticsearch.xpack.core.slm.history.SnapshotLifecycleTemplateRegistry.TEMPLATE_SLM_HISTORY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
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
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ComposableIndexTemplate template =
            ComposableIndexTemplate.parse(JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, TEMPLATE_SLM_HISTORY.loadBytes()));
        ClusterState state = clusterService.state();
        Metadata.Builder metadataBuilder =
            Metadata.builder(state.getMetadata()).indexTemplates(Map.of(TEMPLATE_SLM_HISTORY.getTemplateName(), template));
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(state).metadata(metadataBuilder).build());
        historyStore = new SnapshotHistoryStore(Settings.EMPTY, client, clusterService);
        clusterService.stop();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.stop();
        threadPool.shutdownNow();
    }

    public void testNoActionIfDisabled() {
        Settings settings = Settings.builder().put(SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false).build();
        SnapshotHistoryStore disabledHistoryStore = new SnapshotHistoryStore(settings, client, null);
        String policyId = randomAlphaOfLength(5);
        SnapshotLifecyclePolicy policy = randomSnapshotLifecyclePolicy(policyId);
        final long timestamp = randomNonNegativeLong();
        String snapshotId = generateSnapshotName(policy.getName());
        SnapshotHistoryItem record = SnapshotHistoryItem.creationSuccessRecord(timestamp, policy, snapshotId);

        client.setVerifier((a, r, l) -> {
            fail("the history store is disabled, no action should have been taken");
            return null;
        });
        disabledHistoryStore.putAsync(record);
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
                assertThat(action, instanceOf(IndexAction.class));
                assertThat(request, instanceOf(IndexRequest.class));
                IndexRequest indexRequest = (IndexRequest) request;
                assertEquals(SLM_HISTORY_DATA_STREAM, indexRequest.index());
                final String indexedDocument = indexRequest.source().utf8ToString();
                assertThat(indexedDocument, containsString(policy.getId()));
                assertThat(indexedDocument, containsString(policy.getRepository()));
                assertThat(indexedDocument, containsString(snapshotId));
                if (policy.getConfig() != null) {
                    assertContainsMap(indexedDocument, policy.getConfig());
                }
                assertNotNull(listener);
                // The content of this IndexResponse doesn't matter, so just make it 100% random
                return new IndexResponse(
                    new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), randomInt(100)),
                    randomAlphaOfLength(5),
                    randomLongBetween(1, 1000),
                    randomLongBetween(1, 1000),
                    randomLongBetween(1, 1000),
                    randomBoolean());
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
                if (action instanceof CreateIndexAction && request instanceof CreateIndexRequest) {
                    return new CreateIndexResponse(true, true, ((CreateIndexRequest) request).index());
                }
                calledTimes.incrementAndGet();
                assertThat(action, instanceOf(IndexAction.class));
                assertThat(request, instanceOf(IndexRequest.class));
                IndexRequest indexRequest = (IndexRequest) request;
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
                // The content of this IndexResponse doesn't matter, so just make it 100% random
                return new IndexResponse(
                    new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), randomInt(100)),
                    randomAlphaOfLength(5),
                    randomLongBetween(1, 1000),
                    randomLongBetween(1, 1000),
                    randomLongBetween(1, 1000),
                    randomBoolean());
            });

            historyStore.putAsync(record);
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
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
                ((Iterable) v).forEach(elem -> {
                    assertThat(indexedDocument, containsString(elem.toString()));
                });
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
        return new SnapshotLifecyclePolicy(id,
            randomAlphaOfLength(4),
            randomSchedule(),
            randomAlphaOfLength(4),
            config,
            null);
    }

    private static String randomSchedule() {
        return randomIntBetween(0, 59) + " " +
            randomIntBetween(0, 59) + " " +
            randomIntBetween(0, 12) + " * * ?";
    }
}
