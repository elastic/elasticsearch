/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm.history;

import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore.getHistoryIndexNameForTime;
import static org.elasticsearch.xpack.core.slm.history.SnapshotLifecycleTemplateRegistry.INDEX_TEMPLATE_VERSION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;

public class SnapshotHistoryStoreTests extends ESTestCase {

    private ThreadPool threadPool;
    private SnapshotLifecycleTemplateRegistryTests.VerifyingClient client;
    private SnapshotHistoryStore historyStore;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new SnapshotLifecycleTemplateRegistryTests.VerifyingClient(threadPool);
        historyStore = new SnapshotHistoryStore(Settings.EMPTY, client, ZoneOffset.UTC);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testNoActionIfDisabled() {
        Settings settings = Settings.builder().put(SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false).build();
        SnapshotHistoryStore disabledHistoryStore = new SnapshotHistoryStore(settings, client, ZoneOffset.UTC);
        String policyId = randomAlphaOfLength(5);
        SnapshotLifecyclePolicy policy = randomSnapshotLifecyclePolicy(policyId);
        final long timestamp = randomNonNegativeLong();
        SnapshotLifecyclePolicy.ResolverContext context = new SnapshotLifecyclePolicy.ResolverContext(timestamp);
        String snapshotId = policy.generateSnapshotName(context);
        SnapshotHistoryItem record = SnapshotHistoryItem.successRecord(timestamp, policy, snapshotId);

        client.setVerifier((a,r,l) -> {
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
        SnapshotLifecyclePolicy.ResolverContext context = new SnapshotLifecyclePolicy.ResolverContext(timestamp);
        String snapshotId = policy.generateSnapshotName(context);
        {
            SnapshotHistoryItem record = SnapshotHistoryItem.successRecord(timestamp, policy, snapshotId);

            AtomicInteger calledTimes = new AtomicInteger(0);
            client.setVerifier((action, request, listener) -> {
                calledTimes.incrementAndGet();
                assertThat(action, instanceOf(IndexAction.class));
                assertThat(request, instanceOf(IndexRequest.class));
                IndexRequest indexRequest = (IndexRequest) request;
                assertEquals(getHistoryIndexNameForTime(Instant.ofEpochMilli(timestamp).atZone(ZoneOffset.UTC)), indexRequest.index());
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
                    randomAlphaOfLength(5),
                    randomLongBetween(1,1000),
                    randomLongBetween(1,1000),
                    randomLongBetween(1,1000),
                    randomBoolean());
            });

            historyStore.putAsync(record);
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }

        {
            final String cause = randomAlphaOfLength(9);
            Exception failureException = new RuntimeException(cause);
            SnapshotHistoryItem record = SnapshotHistoryItem.failureRecord(timestamp, policy, snapshotId, failureException);

            AtomicInteger calledTimes = new AtomicInteger(0);
            client.setVerifier((action, request, listener) -> {
                calledTimes.incrementAndGet();
                assertThat(action, instanceOf(IndexAction.class));
                assertThat(request, instanceOf(IndexRequest.class));
                IndexRequest indexRequest = (IndexRequest) request;
                assertEquals(getHistoryIndexNameForTime(Instant.ofEpochMilli(timestamp).atZone(ZoneOffset.UTC)), indexRequest.index());
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
                    randomAlphaOfLength(5),
                    randomLongBetween(1,1000),
                    randomLongBetween(1,1000),
                    randomLongBetween(1,1000),
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
            } if (v instanceof Iterable) {
                ((Iterable) v).forEach(elem -> {
                    assertThat(indexedDocument, containsString(elem.toString()));
                });
            } else {
                assertThat(indexedDocument, containsString(v.toString()));
            }
        });
    }


    public void testIndexNameGeneration() {
        String indexTemplateVersion = INDEX_TEMPLATE_VERSION;
        assertThat(getHistoryIndexNameForTime(Instant.ofEpochMilli((long) 0).atZone(ZoneOffset.UTC)),
            equalTo(".slm-history-"+ indexTemplateVersion +"-1970.01"));
        assertThat(getHistoryIndexNameForTime(Instant.ofEpochMilli(100000000000L).atZone(ZoneOffset.UTC)),
            equalTo(".slm-history-" + indexTemplateVersion + "-1973.03"));
        assertThat(getHistoryIndexNameForTime(Instant.ofEpochMilli(1416582852000L).atZone(ZoneOffset.UTC)),
            equalTo(".slm-history-" + indexTemplateVersion + "-2014.11"));
        assertThat(getHistoryIndexNameForTime(Instant.ofEpochMilli(2833165811000L).atZone(ZoneOffset.UTC)),
            equalTo(".slm-history-" + indexTemplateVersion + "-2059.10"));
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
            config);
    }

    private static String randomSchedule() {
        return randomIntBetween(0, 59) + " " +
            randomIntBetween(0, 59) + " " +
            randomIntBetween(0, 12) + " * * ?";
    }
}
