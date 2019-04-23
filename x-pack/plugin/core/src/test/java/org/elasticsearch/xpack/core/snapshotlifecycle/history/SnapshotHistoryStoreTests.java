/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle.history;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.elasticsearch.xpack.core.snapshotlifecycle.history.SnapshotCreationHistoryItemTests.randomSnapshotConfiguration;
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
    public void testPut() {
        String policyId = randomAlphaOfLength(5);
        String repository = randomAlphaOfLength(6);
        String snapshotId = randomAlphaOfLength(7);
        CreateSnapshotRequest request = new CreateSnapshotRequest(repository, snapshotId);
        Map<String, Object> config = randomBoolean() ? null : randomSnapshotConfiguration();
        final long timestamp = randomNonNegativeLong();
        SnapshotCreationHistoryItem record = SnapshotCreationHistoryItem.successRecord(timestamp, policyId, request, config);

        historyStore.putAsync(record);
        ArgumentCaptor<IndexRequest> indexRequest = ArgumentCaptor.forClass(IndexRequest.class);
        verify(client, times(1)).index(indexRequest.capture(), notNull(ActionListener.class));

        assertEquals(getHistoryIndexNameForTime(Instant.ofEpochMilli(timestamp).atZone(ZoneOffset.UTC)),
            indexRequest.getValue().index());
        final String indexedDocument = indexRequest.getValue().source().utf8ToString();
        assertThat(indexedDocument, containsString(policyId));
        assertThat(indexedDocument, containsString(repository));
        assertThat(indexedDocument, containsString(snapshotId));
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
}
