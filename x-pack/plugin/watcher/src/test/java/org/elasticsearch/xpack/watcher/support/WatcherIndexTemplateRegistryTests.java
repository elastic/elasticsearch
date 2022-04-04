/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;
import org.elasticsearch.xpack.watcher.Watcher;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField.INDEX_TEMPLATE_VERSION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WatcherIndexTemplateRegistryTests extends ESTestCase {

    private WatcherIndexTemplateRegistry registry;
    private NamedXContentRegistry xContentRegistry;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;

    @SuppressWarnings("unchecked")
    @Before
    public void createRegistryAndClient() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);
        doAnswer(invocationOnMock -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[1];
            listener.onResponse(new TestPutIndexTemplateResponse(true));
            return null;
        }).when(indicesAdminClient).putTemplate(any(PutIndexTemplateRequest.class), any(ActionListener.class));

        clusterService = mock(ClusterService.class);
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(
            Arrays.asList(
                new NamedXContentRegistry.Entry(
                    LifecycleType.class,
                    new ParseField(TimeseriesLifecycleType.TYPE),
                    (p) -> TimeseriesLifecycleType.INSTANCE
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse)
            )
        );
        xContentRegistry = new NamedXContentRegistry(entries);
        registry = new WatcherIndexTemplateRegistry(Settings.EMPTY, clusterService, threadPool, client, xContentRegistry);
    }

    public void testThatNonExistingTemplatesAreAddedImmediately() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
        ArgumentCaptor<PutComposableIndexTemplateAction.Request> argumentCaptor = ArgumentCaptor.forClass(
            PutComposableIndexTemplateAction.Request.class
        );
        verify(client, times(1)).execute(same(PutComposableIndexTemplateAction.INSTANCE), argumentCaptor.capture(), any());

        // now delete one template from the cluster state and lets retry
        Map<String, Integer> existingTemplates = new HashMap<>();
        existingTemplates.put(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME, INDEX_TEMPLATE_VERSION);
        ClusterChangedEvent newEvent = createClusterChangedEvent(existingTemplates, nodes);
        registry.clusterChanged(newEvent);
        argumentCaptor = ArgumentCaptor.forClass(PutComposableIndexTemplateAction.Request.class);
        verify(client, times(1)).execute(same(PutComposableIndexTemplateAction.INSTANCE), argumentCaptor.capture(), any());
        PutComposableIndexTemplateAction.Request req = argumentCaptor.getAllValues()
            .stream()
            .filter(r -> r.name().equals(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected the watch history template to be put"));
        assertThat(req.indexTemplate().template().settings().get("index.lifecycle.name"), equalTo("watch-history-ilm-policy-16"));
    }

    public void testThatNonExistingTemplatesAreAddedEvenWithILMUsageDisabled() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        registry = new WatcherIndexTemplateRegistry(
            Settings.builder().put(Watcher.USE_ILM_INDEX_MANAGEMENT.getKey(), false).build(),
            clusterService,
            threadPool,
            client,
            xContentRegistry
        );
        ClusterChangedEvent event = createClusterChangedEvent(Settings.EMPTY, Collections.emptyMap(), Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
        ArgumentCaptor<PutComposableIndexTemplateAction.Request> argumentCaptor = ArgumentCaptor.forClass(
            PutComposableIndexTemplateAction.Request.class
        );
        verify(client, times(1)).execute(same(PutComposableIndexTemplateAction.INSTANCE), argumentCaptor.capture(), any());

        // now delete one template from the cluster state and lets retry
        Map<String, Integer> existingTemplates = new HashMap<>();
        existingTemplates.put(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME, INDEX_TEMPLATE_VERSION);
        ClusterChangedEvent newEvent = createClusterChangedEvent(existingTemplates, nodes);
        registry.clusterChanged(newEvent);
        ArgumentCaptor<PutIndexTemplateRequest> captor = ArgumentCaptor.forClass(PutIndexTemplateRequest.class);
        verify(client, times(1)).execute(same(PutComposableIndexTemplateAction.INSTANCE), argumentCaptor.capture(), any());
        captor.getAllValues().forEach(req -> assertNull(req.settings().get("index.lifecycle.name")));
        verify(client, times(0)).execute(eq(PutLifecycleAction.INSTANCE), any(), any());
    }

    public void testThatNonExistingPoliciesAreAddedImmediately() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
        verify(client, times(1)).execute(eq(PutLifecycleAction.INSTANCE), any(), any());
    }

    public void testPolicyAlreadyExists() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        List<LifecyclePolicy> policies = registry.getPolicyConfigs();
        assertThat(policies, hasSize(1));
        LifecyclePolicy policy = policies.get(0);
        policyMap.put(policy.getName(), policy);
        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), policyMap, nodes);
        registry.clusterChanged(event);
        verify(client, times(0)).execute(eq(PutLifecycleAction.INSTANCE), any(), any());
    }

    public void testNoPolicyButILMDisabled() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        registry = new WatcherIndexTemplateRegistry(
            Settings.builder().put(Watcher.USE_ILM_INDEX_MANAGEMENT.getKey(), false).build(),
            clusterService,
            threadPool,
            client,
            xContentRegistry
        );
        ClusterChangedEvent event = createClusterChangedEvent(Settings.EMPTY, Collections.emptyMap(), Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
        verify(client, times(0)).execute(eq(PutLifecycleAction.INSTANCE), any(), any());
    }

    public void testPolicyAlreadyExistsButDiffers() throws IOException {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        String policyStr = "{\"phases\":{\"delete\":{\"min_age\":\"1m\",\"actions\":{\"delete\":{}}}}}";
        List<LifecyclePolicy> policies = registry.getPolicyConfigs();
        assertThat(policies, hasSize(1));
        LifecyclePolicy policy = policies.get(0);
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry), policyStr)
        ) {
            LifecyclePolicy different = LifecyclePolicy.parse(parser, policy.getName());
            policyMap.put(policy.getName(), different);
            ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), policyMap, nodes);
            registry.clusterChanged(event);
            verify(client, times(0)).execute(eq(PutLifecycleAction.INSTANCE), any(), any());
        }
    }

    public void testThatTemplatesExist() {
        {
            Map<String, Integer> existingTemplates = new HashMap<>();
            existingTemplates.put(".watch-history", INDEX_TEMPLATE_VERSION);
            assertThat(WatcherIndexTemplateRegistry.validate(createClusterState(existingTemplates)), is(false));
        }

        {
            Map<String, Integer> existingTemplates = new HashMap<>();
            existingTemplates.put(".watch-history", INDEX_TEMPLATE_VERSION);
            existingTemplates.put(".triggered_watches", INDEX_TEMPLATE_VERSION);
            existingTemplates.put(".watches", INDEX_TEMPLATE_VERSION);
            assertThat(WatcherIndexTemplateRegistry.validate(createClusterState(existingTemplates)), is(false));
        }

        {
            Map<String, Integer> existingTemplates = new HashMap<>();
            existingTemplates.put(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME, INDEX_TEMPLATE_VERSION);
            existingTemplates.put(".triggered_watches", INDEX_TEMPLATE_VERSION);
            existingTemplates.put(".watches", INDEX_TEMPLATE_VERSION);
            assertThat(WatcherIndexTemplateRegistry.validate(createClusterState(existingTemplates)), is(true));
        }

        {
            Map<String, Integer> existingTemplates = new HashMap<>();
            existingTemplates.put(".watch-history-11", 11);
            existingTemplates.put(".triggered_watches", 11);
            existingTemplates.put(".watches", 11);
            assertThat(WatcherIndexTemplateRegistry.validate(createClusterState(existingTemplates)), is(false));
        }

        {
            Map<String, Integer> existingTemplates = new HashMap<>();
            existingTemplates.put(".watch-history-15", 15);
            existingTemplates.put(".triggered_watches", 15);
            existingTemplates.put(".watches", 15);
            assertThat(WatcherIndexTemplateRegistry.validate(createClusterState(existingTemplates)), is(true));
        }

        {
            Map<String, Integer> existingTemplates = new HashMap<>();
            existingTemplates.put(WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME, INDEX_TEMPLATE_VERSION);
            existingTemplates.put(".triggered_watches", INDEX_TEMPLATE_VERSION);
            existingTemplates.put(".watches", INDEX_TEMPLATE_VERSION);
            existingTemplates.put("whatever", null);
            existingTemplates.put("else", null);

            assertThat(WatcherIndexTemplateRegistry.validate(createClusterState(existingTemplates)), is(true));
        }
    }

    public void testThatTemplatesAreNotAppliedOnSameVersionNodes() {
        DiscoveryNode localNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode masterNode = new DiscoveryNode("master", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("master").add(localNode).add(masterNode).build();

        Map<String, Integer> existingTemplates = new HashMap<>();
        existingTemplates.put(".watch-history-6", null);
        ClusterChangedEvent event = createClusterChangedEvent(existingTemplates, nodes);
        registry.clusterChanged(event);

        verifyNoMoreInteractions(client);
    }

    public void testThatMissingMasterNodeDoesNothing() {
        DiscoveryNode localNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").add(localNode).build();

        Map<String, Integer> existingTemplates = new HashMap<>();
        existingTemplates.put(".watch-history-6", null);
        ClusterChangedEvent event = createClusterChangedEvent(existingTemplates, nodes);
        registry.clusterChanged(event);

        verifyNoMoreInteractions(client);
    }

    private ClusterChangedEvent createClusterChangedEvent(Map<String, Integer> existingTemplateNames, DiscoveryNodes nodes) {
        return createClusterChangedEvent(existingTemplateNames, Collections.emptyMap(), nodes);
    }

    private ClusterState createClusterState(
        Settings nodeSettings,
        Map<String, Integer> existingTemplates,
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes
    ) {
        ImmutableOpenMap.Builder<String, IndexTemplateMetadata> indexTemplates = ImmutableOpenMap.builder();
        for (Map.Entry<String, Integer> template : existingTemplates.entrySet()) {
            final IndexTemplateMetadata mockTemplate = mock(IndexTemplateMetadata.class);
            when(mockTemplate.version()).thenReturn(template.getValue());
            when(mockTemplate.getVersion()).thenReturn(template.getValue());
            indexTemplates.put(template.getKey(), mockTemplate);
        }

        Map<String, LifecyclePolicyMetadata> existingILMMeta = existingPolicies.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new LifecyclePolicyMetadata(e.getValue(), Collections.emptyMap(), 1, 1)));
        IndexLifecycleMetadata ilmMeta = new IndexLifecycleMetadata(existingILMMeta, OperationMode.RUNNING);

        return ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .templates(indexTemplates.build())
                    .transientSettings(nodeSettings)
                    .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
                    .build()
            )
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .build();
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Map<String, Integer> existingTemplateNames,
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes
    ) {
        return createClusterChangedEvent(Settings.EMPTY, existingTemplateNames, existingPolicies, nodes);
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Settings nodeSettings,
        Map<String, Integer> existingTemplates,
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes
    ) {
        ClusterState cs = createClusterState(nodeSettings, existingTemplates, existingPolicies, nodes);
        ClusterChangedEvent realEvent = new ClusterChangedEvent(
            "created-from-test",
            cs,
            ClusterState.builder(new ClusterName("test")).build()
        );
        ClusterChangedEvent event = spy(realEvent);
        when(event.localNodeMaster()).thenReturn(nodes.isLocalNodeElectedMaster());

        return event;
    }

    private ClusterState createClusterState(Map<String, Integer> existingTemplates) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        HashMap<String, ComposableIndexTemplate> templates = new HashMap<>();
        for (Map.Entry<String, Integer> template : existingTemplates.entrySet()) {
            ComposableIndexTemplate indexTemplate = mock(ComposableIndexTemplate.class);
            when(indexTemplate.version()).thenReturn(template.getValue() == null ? null : (long) template.getValue());
            when(indexTemplate.indexPatterns()).thenReturn(Arrays.asList(generateRandomStringArray(10, 100, false, false)));
            templates.put(template.getKey(), indexTemplate);
        }
        metadataBuilder.indexTemplates(templates);

        return ClusterState.builder(new ClusterName("foo")).metadata(metadataBuilder.build()).build();
    }

    private static class TestPutIndexTemplateResponse extends AcknowledgedResponse {
        TestPutIndexTemplateResponse(boolean acknowledged) {
            super(acknowledged);
        }
    }
}
