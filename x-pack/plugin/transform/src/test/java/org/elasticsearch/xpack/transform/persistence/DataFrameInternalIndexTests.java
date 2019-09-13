/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DataFrameInternalIndexTests extends ESTestCase {

    public static ClusterState STATE_WITH_LATEST_VERSIONED_INDEX_TEMPLATE;

    static {
        ImmutableOpenMap.Builder<String, IndexTemplateMetaData> mapBuilder = ImmutableOpenMap.builder();
        try {
            mapBuilder.put(DataFrameInternalIndex.LATEST_INDEX_VERSIONED_NAME, DataFrameInternalIndex.getIndexTemplateMetaData());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        MetaData.Builder metaBuilder = MetaData.builder();
        metaBuilder.templates(mapBuilder.build());
        ClusterState.Builder csBuilder = ClusterState.builder(ClusterName.DEFAULT);
        csBuilder.metaData(metaBuilder.build());
        STATE_WITH_LATEST_VERSIONED_INDEX_TEMPLATE = csBuilder.build();
    }

    public void testHaveLatestVersionedIndexTemplate() {

        assertTrue(DataFrameInternalIndex.haveLatestVersionedIndexTemplate(STATE_WITH_LATEST_VERSIONED_INDEX_TEMPLATE));
        assertFalse(DataFrameInternalIndex.haveLatestVersionedIndexTemplate(ClusterState.EMPTY_STATE));
    }

    public void testInstallLatestVersionedIndexTemplateIfRequired_GivenNotRequired() {

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(DataFrameInternalIndexTests.STATE_WITH_LATEST_VERSIONED_INDEX_TEMPLATE);

        Client client = mock(Client.class);

        AtomicBoolean gotResponse = new AtomicBoolean(false);
        ActionListener<Void> testListener = ActionListener.wrap(aVoid -> gotResponse.set(true), e -> fail(e.getMessage()));

        DataFrameInternalIndex.installLatestVersionedIndexTemplateIfRequired(clusterService, client, testListener);

        assertTrue(gotResponse.get());
        verifyNoMoreInteractions(client);
    }

    public void testInstallLatestVersionedIndexTemplateIfRequired_GivenRequired() {

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        IndicesAdminClient indicesClient = mock(IndicesAdminClient.class);
        doAnswer(
            invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(new AcknowledgedResponse(true));
            return null;
        }).when(indicesClient).putTemplate(any(), any());

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        AtomicBoolean gotResponse = new AtomicBoolean(false);
        ActionListener<Void> testListener = ActionListener.wrap(aVoid -> gotResponse.set(true), e -> fail(e.getMessage()));

        DataFrameInternalIndex.installLatestVersionedIndexTemplateIfRequired(clusterService, client, testListener);

        assertTrue(gotResponse.get());
        verify(client, times(1)).threadPool();
        verify(client, times(1)).admin();
        verifyNoMoreInteractions(client);
        verify(adminClient, times(1)).indices();
        verifyNoMoreInteractions(adminClient);
        verify(indicesClient, times(1)).putTemplate(any(), any());
        verifyNoMoreInteractions(indicesClient);
    }
}
