/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyZeroInteractions;

public class WatcherIndexTemplateRegistryTests extends ESTestCase {

    private WatcherIndexTemplateRegistry registry;
    private Client client;

    @Before
    public void createRegistryAndClient() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.newDirectExecutorService());

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);
        doAnswer(invocationOnMock -> {
            ActionListener<AcknowledgedResponse> listener =
                    (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(new TestPutIndexTemplateResponse(true));
            return null;
        }).when(indicesAdminClient).putTemplate(any(PutIndexTemplateRequest.class), any(ActionListener.class));

        ClusterService clusterService = mock(ClusterService.class);
        registry = new WatcherIndexTemplateRegistry(clusterService, threadPool, client);
    }

    public void testThatNonExistingTemplatesAreAddedImmediately() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), nodes);
        registry.clusterChanged(event);
        ArgumentCaptor<PutIndexTemplateRequest> argumentCaptor = ArgumentCaptor.forClass(PutIndexTemplateRequest.class);
        verify(client.admin().indices(), times(3)).putTemplate(argumentCaptor.capture(), anyObject());

        // now delete one template from the cluster state and lets retry
        ClusterChangedEvent newEvent = createClusterChangedEvent(Arrays.asList(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME,
                WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME), nodes);
        registry.clusterChanged(newEvent);
        verify(client.admin().indices(), times(4)).putTemplate(argumentCaptor.capture(), anyObject());
    }

    public void testThatTemplatesExist() {
        assertThat(WatcherIndexTemplateRegistry.validate(createClusterState(".watch-history")), is(false));
        assertThat(WatcherIndexTemplateRegistry.validate(createClusterState(".watch-history", ".triggered_watches", ".watches")),
                is(false));
        assertThat(WatcherIndexTemplateRegistry.validate(createClusterState(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME,
                ".triggered_watches", ".watches")), is(true));
        assertThat(WatcherIndexTemplateRegistry.validate(createClusterState(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME,
                ".triggered_watches", ".watches", "whatever", "else")), is(true));
    }

    // if a node is newer than the master node, the template needs to be applied as well
    // otherwise a rolling upgrade would not work as expected, when the node has a .watches shard on it
    public void testThatTemplatesAreAppliedOnNewerNodes() {
        DiscoveryNode localNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode masterNode = new DiscoveryNode("master", ESTestCase.buildNewFakeTransportAddress(), Version.V_6_0_0);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("master").add(localNode).add(masterNode).build();

        ClusterChangedEvent event = createClusterChangedEvent(Arrays.asList(WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME,
                WatcherIndexTemplateRegistryField.WATCHES_TEMPLATE_NAME, ".watch-history-6"), nodes);
        registry.clusterChanged(event);

        ArgumentCaptor<PutIndexTemplateRequest> argumentCaptor = ArgumentCaptor.forClass(PutIndexTemplateRequest.class);
        verify(client.admin().indices(), times(1)).putTemplate(argumentCaptor.capture(), anyObject());
        assertThat(argumentCaptor.getValue().name(), is(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME));
    }

    public void testThatTemplatesAreNotAppliedOnSameVersionNodes() {
        DiscoveryNode localNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode masterNode = new DiscoveryNode("master", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("master").add(localNode).add(masterNode).build();

        ClusterChangedEvent event = createClusterChangedEvent(Arrays.asList(WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME,
                WatcherIndexTemplateRegistryField.WATCHES_TEMPLATE_NAME, ".watch-history-6"), nodes);
        registry.clusterChanged(event);

        verifyZeroInteractions(client);
    }

    public void testThatMissingMasterNodeDoesNothing() {
        DiscoveryNode localNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").add(localNode).build();

        ClusterChangedEvent event = createClusterChangedEvent(Arrays.asList(WatcherIndexTemplateRegistryField.TRIGGERED_TEMPLATE_NAME,
                WatcherIndexTemplateRegistryField.WATCHES_TEMPLATE_NAME, ".watch-history-6"), nodes);
        registry.clusterChanged(event);

        verifyZeroInteractions(client);
    }

    private ClusterChangedEvent createClusterChangedEvent(List<String> existingTemplateNames, DiscoveryNodes nodes) {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeMaster()).thenReturn(nodes.isLocalNodeElectedMaster());
        ClusterState cs = mock(ClusterState.class);
        ClusterBlocks clusterBlocks = mock(ClusterBlocks.class);
        when(clusterBlocks.hasGlobalBlock(eq(GatewayService.STATE_NOT_RECOVERED_BLOCK))).thenReturn(false);
        when(cs.blocks()).thenReturn(clusterBlocks);
        when(event.state()).thenReturn(cs);

        when(cs.getNodes()).thenReturn(nodes);

        MetaData metaData = mock(MetaData.class);
        ImmutableOpenMap.Builder<String, IndexTemplateMetaData> indexTemplates = ImmutableOpenMap.builder();
        for (String name : existingTemplateNames) {
            indexTemplates.put(name, mock(IndexTemplateMetaData.class));
        }

        when(metaData.getTemplates()).thenReturn(indexTemplates.build());
        when(cs.metaData()).thenReturn(metaData);

        return event;
    }

    private ClusterState createClusterState(String ... existingTemplates) {
        MetaData.Builder metaDataBuilder = MetaData.builder();
        for (String templateName : existingTemplates) {
            metaDataBuilder.put(IndexTemplateMetaData.builder(templateName)
                    .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false))));
        }

        return ClusterState.builder(new ClusterName("foo")).metaData(metaDataBuilder.build()).build();
    }

    private static class TestPutIndexTemplateResponse extends AcknowledgedResponse {
        TestPutIndexTemplateResponse(boolean acknowledged) {
            super(acknowledged);
        }

        TestPutIndexTemplateResponse() {
            super();
        }
    }
}
