/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle.history;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicy;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.snapshotlifecycle.history.SnapshotHistoryStore.getHistoryIndexNameForTime;
import static org.elasticsearch.xpack.core.snapshotlifecycle.history.SnapshotLifecycleTemplateRegistry.INDEX_TEMPLATE_VERSION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SnapshotHistoryStoreTests extends ESTestCase {

    private ClusterService clusterService;
    private Client client;
    private SnapshotHistoryStore historyStore;
    private SnapshotLifecycleTemplateRegistry registry;

    @Before
    public void setup() {
        Settings settings = Settings.builder().put("node.name", randomAlphaOfLength(10)).build();
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(settings));
        clusterService = mock(ClusterService.class);
        registry = mock(SnapshotLifecycleTemplateRegistry.class);

        historyStore = new SnapshotHistoryStore(registry, client, ZoneOffset.UTC, clusterService);
    }

    @SuppressWarnings("unchecked")
    public void testPut() throws IOException {
        String policyId = randomAlphaOfLength(5);
        SnapshotLifecyclePolicy policy = randomSnapshotLifecyclePolicy(policyId);
        final long timestamp = randomNonNegativeLong();
        SnapshotLifecyclePolicy.ResolverContext context = new SnapshotLifecyclePolicy.ResolverContext(timestamp);
        String snapshotId = policy.generateSnapshotName(context);
        {
            SnapshotHistoryItem record = SnapshotHistoryItem.successRecord(timestamp, policy, snapshotId);

            historyStore.putAsync(record);
            ArgumentCaptor<IndexRequest> indexRequest = ArgumentCaptor.forClass(IndexRequest.class);
            verify(client, times(1)).index(indexRequest.capture(), notNull(ActionListener.class));

            assertEquals(getHistoryIndexNameForTime(Instant.ofEpochMilli(timestamp).atZone(ZoneOffset.UTC)),
                indexRequest.getValue().index());
            final String indexedDocument = indexRequest.getValue().source().utf8ToString();
            assertThat(indexedDocument, containsString(policy.getId()));
            assertThat(indexedDocument, containsString(policy.getRepository()));
            assertThat(indexedDocument, containsString(snapshotId));
            if (policy.getConfig() != null) {
                assertContainsMap(indexedDocument, policy.getConfig());
            }
        }

        {
            final String cause = randomAlphaOfLength(9);
            Exception failureException = new RuntimeException(cause);
            SnapshotHistoryItem record = SnapshotHistoryItem.failureRecord(timestamp, policy, snapshotId, failureException);
            historyStore.putAsync(record);
            ArgumentCaptor<IndexRequest> indexRequest = ArgumentCaptor.forClass(IndexRequest.class);
            verify(client, times(2)).index(indexRequest.capture(), notNull(ActionListener.class));

            assertEquals(getHistoryIndexNameForTime(Instant.ofEpochMilli(timestamp).atZone(ZoneOffset.UTC)),
                indexRequest.getValue().index());
            final String indexedDocument = indexRequest.getValue().source().utf8ToString();
            assertThat(indexedDocument, containsString(policy.getId()));
            assertThat(indexedDocument, containsString(policy.getRepository()));
            assertThat(indexedDocument, containsString(snapshotId));
            if (policy.getConfig() != null) {
                assertContainsMap(indexedDocument, policy.getConfig());
            }
            assertThat(indexedDocument, containsString("runtime_exception"));
            assertThat(indexedDocument, containsString(cause));
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
        Map<String, Object> config = new HashMap<>();
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            config.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
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
