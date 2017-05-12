/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
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

    public void testThatDeprecatedTemplatesAreRemovedOnce() throws Exception {
        List<String> templateNames = new ArrayList<>();

        // old index templates to be deleted
        boolean containsWatchesTemplate = randomBoolean();
        if (containsWatchesTemplate) {
            templateNames.add("watches");
        }
        boolean containsTriggeredWatchesTemplates = randomBoolean();
        if (containsTriggeredWatchesTemplates) {
            templateNames.add("triggered_watches");
        }
        boolean containsVersionedWatchHistoryTemplate = randomBoolean();
        if (containsVersionedWatchHistoryTemplate) {
            templateNames.add("watcher_history_" + randomIntBetween(0, 100));
        }

        List<String> templatesInClusterState = new ArrayList<>();
        templatesInClusterState.addAll(Arrays.asList(WatcherIndexTemplateRegistry.HISTORY_TEMPLATE_NAME,
                WatcherIndexTemplateRegistry.TRIGGERED_TEMPLATE_NAME, WatcherIndexTemplateRegistry.WATCHES_TEMPLATE_NAME));
        templatesInClusterState.addAll(templateNames);
        ClusterChangedEvent event = createClusterChangedEvent(templatesInClusterState);
        registry.clusterChanged(event);

        ArgumentCaptor<DeleteIndexTemplateRequest> requestArgumentCaptor = ArgumentCaptor.forClass(DeleteIndexTemplateRequest.class);
        verify(client, times(templateNames.size())).execute(anyObject(), requestArgumentCaptor.capture(), anyObject());
        assertThat(requestArgumentCaptor.getAllValues(), hasSize(templateNames.size()));
        List<String> deletedTemplateNames = requestArgumentCaptor.getAllValues().stream().map(DeleteIndexTemplateRequest::name)
                .collect(Collectors.toList());
        if (containsWatchesTemplate) {
            assertThat(deletedTemplateNames, hasItem("watches"));
        }
        if (containsTriggeredWatchesTemplates) {
            assertThat(deletedTemplateNames, hasItem("triggered_watches"));
        }
        if (containsVersionedWatchHistoryTemplate) {
            assertThat(deletedTemplateNames, hasItem(startsWith("watcher_history_")));
        }

        // a second event with removed templates should not trigger any further requests, so the invocation count stays the same
        ClusterChangedEvent newEvent = createClusterChangedEvent(Arrays.asList(WatcherIndexTemplateRegistry.HISTORY_TEMPLATE_NAME,
                WatcherIndexTemplateRegistry.TRIGGERED_TEMPLATE_NAME, WatcherIndexTemplateRegistry.WATCHES_TEMPLATE_NAME));
        registry.clusterChanged(newEvent);
        verify(client, times(templateNames.size())).execute(anyObject(), anyObject(), anyObject());
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
}
