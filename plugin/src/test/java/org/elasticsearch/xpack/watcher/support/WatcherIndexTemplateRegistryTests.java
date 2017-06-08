/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.InternalClient;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class WatcherIndexTemplateRegistryTests extends ESTestCase {

    private WatcherIndexTemplateRegistry registry;
    private Client client;

    @Before
    public void createRegistryAndClient() {
        Set<Setting<?>> registeredSettings = new HashSet<>();
        registeredSettings.add(WatcherIndexTemplateRegistry.HISTORY_TEMPLATE_SETTING);
        registeredSettings.add(WatcherIndexTemplateRegistry.TRIGGERED_TEMPLATE_SETTING);
        registeredSettings.add(WatcherIndexTemplateRegistry.WATCHES_TEMPLATE_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, registeredSettings);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.newDirectExecutorService());

        client = mock(Client.class);
        InternalClient internalClient = new InternalClient(Settings.EMPTY, threadPool, client);
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);
        doAnswer(new Answer<Void>() {
            @SuppressWarnings("unchecked")
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                ActionListener<PutIndexTemplateResponse> listener =
                        (ActionListener<PutIndexTemplateResponse>) invocationOnMock.getArguments()[2];
                listener.onResponse(new TestPutIndexTemplateResponse(true));
                return null;
            }
        }).when(client).execute(same(PutIndexTemplateAction.INSTANCE), any(), any());

        ClusterService clusterService = mock(ClusterService.class);
        registry = new WatcherIndexTemplateRegistry(Settings.EMPTY, clusterSettings, clusterService, threadPool, internalClient);
    }

    private ClusterChangedEvent createClusterChangedEvent(List<String> existingTemplateNames) {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeMaster()).thenReturn(true);
        ClusterState cs = mock(ClusterState.class);
        ClusterBlocks clusterBlocks = mock(ClusterBlocks.class);
        when(clusterBlocks.hasGlobalBlock(eq(GatewayService.STATE_NOT_RECOVERED_BLOCK))).thenReturn(false);
        when(cs.blocks()).thenReturn(clusterBlocks);
        when(event.state()).thenReturn(cs);

        MetaData metaData = mock(MetaData.class);
        ImmutableOpenMap.Builder<String, IndexTemplateMetaData> indexTemplates = ImmutableOpenMap.builder();
        for (String name : existingTemplateNames) {
            indexTemplates.put(name, mock(IndexTemplateMetaData.class));
        }

        when(metaData.getTemplates()).thenReturn(indexTemplates.build());
        when(cs.metaData()).thenReturn(metaData);

        return event;
    }

    public void testThatNonExistingTemplatesAreAddedImmediately() {
        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList());
        registry.clusterChanged(event);
        ArgumentCaptor<PutIndexTemplateRequest> argumentCaptor = ArgumentCaptor.forClass(PutIndexTemplateRequest.class);
        verify(client, times(3)).execute(anyObject(), argumentCaptor.capture(), anyObject());

        // now delete one template from the cluster state and lets retry
        ClusterChangedEvent newEvent = createClusterChangedEvent(Arrays.asList(WatcherIndexTemplateRegistry.HISTORY_TEMPLATE_NAME,
                WatcherIndexTemplateRegistry.TRIGGERED_TEMPLATE_NAME));
        registry.clusterChanged(newEvent);
        verify(client, times(4)).execute(anyObject(), argumentCaptor.capture(), anyObject());
    }

    private static class TestPutIndexTemplateResponse extends PutIndexTemplateResponse {
        TestPutIndexTemplateResponse(boolean acknowledged) {
            super(acknowledged);
        }

        TestPutIndexTemplateResponse() {
            super();
        }
    }
}
