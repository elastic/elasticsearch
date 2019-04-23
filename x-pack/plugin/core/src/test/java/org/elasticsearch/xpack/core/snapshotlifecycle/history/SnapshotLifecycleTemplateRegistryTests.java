/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle.history;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;
import org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;
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

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.elasticsearch.xpack.core.snapshotlifecycle.history.SnapshotLifecycleTemplateRegistry.SLM_POLICY_NAME;
import static org.elasticsearch.xpack.core.snapshotlifecycle.history.SnapshotLifecycleTemplateRegistry.SLM_TEMPLATE_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyZeroInteractions;

public class SnapshotLifecycleTemplateRegistryTests extends ESTestCase {
    private SnapshotLifecycleTemplateRegistry registry;
    private NamedXContentRegistry xContentRegistry;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;

    @Before
    public void createRegistryAndClient() {
        threadPool = mock(ThreadPool.class);
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

        clusterService = mock(ClusterService.class);
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(Arrays.asList(
            new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TimeseriesLifecycleType.TYPE),
                (p) -> TimeseriesLifecycleType.INSTANCE),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse)));
        xContentRegistry = new NamedXContentRegistry(entries);
        registry = new SnapshotLifecycleTemplateRegistry(Settings.EMPTY, clusterService, threadPool, client, xContentRegistry);
    }

    public void testThatNonExistingTemplatesAreAddedImmediately() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), nodes);
        registry.clusterChanged(event);
        ArgumentCaptor<PutIndexTemplateRequest> argumentCaptor = ArgumentCaptor.forClass(PutIndexTemplateRequest.class);
        verify(client.admin().indices(), times(registry.getTemplateConfigs().size())).putTemplate(argumentCaptor.capture(), anyObject());

        // now delete one template from the cluster state and lets retry
        ClusterChangedEvent newEvent = createClusterChangedEvent(Collections.emptyList(), nodes);
        registry.clusterChanged(newEvent);
        ArgumentCaptor<PutIndexTemplateRequest> captor = ArgumentCaptor.forClass(PutIndexTemplateRequest.class);
        verify(client.admin().indices(), times(registry.getTemplateConfigs().size() + 1)).putTemplate(captor.capture(), anyObject());
        PutIndexTemplateRequest req = captor.getAllValues().stream()
            .filter(r -> r.name().equals(SLM_TEMPLATE_NAME))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected the slm history template to be put"));
        assertThat(req.settings().get("index.lifecycle.name"), equalTo(SLM_POLICY_NAME));
    }

    public void testThatNonExistingPoliciesAreAddedImmediately() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), nodes);
        registry.clusterChanged(event);
        verify(client, times(registry.getPolicyConfigs().size())).execute(eq(PutLifecycleAction.INSTANCE), anyObject(), anyObject());
    }

    public void testPolicyAlreadyExists() {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        List<LifecyclePolicy> policies = registry.getPolicyConfigs().stream()
            .map(policyConfig -> policyConfig.load(xContentRegistry))
            .collect(Collectors.toList());
        assertThat(policies, hasSize(1));
        LifecyclePolicy policy = policies.get(0);
        policyMap.put(policy.getName(), policy);
        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), policyMap, nodes);
        registry.clusterChanged(event);
        verify(client, times(0)).execute(eq(PutLifecycleAction.INSTANCE), anyObject(), anyObject());
    }

    public void testThatTemplatesExist() throws IOException {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        String policyStr = "{\"phases\":{\"delete\":{\"min_age\":\"1m\",\"actions\":{\"delete\":{}}}}}";
        List<LifecyclePolicy> policies = registry.getPolicyConfigs().stream()
            .map(policyConfig -> policyConfig.load(xContentRegistry))
            .collect(Collectors.toList());
        assertThat(policies, hasSize(1));
        LifecyclePolicy policy = policies.get(0);
        try (XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry, LoggingDeprecationHandler.THROW_UNSUPPORTED_OPERATION, policyStr)) {
            LifecyclePolicy different = LifecyclePolicy.parse(parser, policy.getName());
            policyMap.put(policy.getName(), different);
            ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), policyMap, nodes);
            registry.clusterChanged(event);
            verify(client, times(0)).execute(eq(PutLifecycleAction.INSTANCE), anyObject(), anyObject());
        }
    }

    public void testThatMissingMasterNodeDoesNothing() {
        DiscoveryNode localNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").add(localNode).build();

        ClusterChangedEvent event = createClusterChangedEvent(Arrays.asList(SLM_TEMPLATE_NAME), nodes);
        registry.clusterChanged(event);

        verifyZeroInteractions(client);
    }

    public void testPolicyAlreadyExistsButDiffers() throws IOException {
        DiscoveryNode node = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        String policyStr = "{\"phases\":{\"delete\":{\"min_age\":\"1m\",\"actions\":{\"delete\":{}}}}}";
        List<LifecyclePolicy> policies = registry.getPolicyConfigs().stream()
            .map(policyConfig -> policyConfig.load(xContentRegistry))
            .collect(Collectors.toList());
        assertThat(policies, hasSize(1));
        LifecyclePolicy policy = policies.get(0);
        try (XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry, LoggingDeprecationHandler.THROW_UNSUPPORTED_OPERATION, policyStr)) {
            LifecyclePolicy different = LifecyclePolicy.parse(parser, policy.getName());
            policyMap.put(policy.getName(), different);
            ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), policyMap, nodes);
            registry.clusterChanged(event);
            verify(client, times(0)).execute(eq(PutLifecycleAction.INSTANCE), anyObject(), anyObject());
        }
    }

    public void testValidate() {
        assertFalse(registry.validate(createClusterState(Settings.EMPTY, Collections.emptyList(), Collections.emptyMap(), null)));
        assertFalse(registry.validate(createClusterState(Settings.EMPTY, List.of(SLM_TEMPLATE_NAME), Collections.emptyMap(), null)));

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        policyMap.put(SLM_POLICY_NAME, new LifecyclePolicy(SLM_POLICY_NAME, new HashMap<>()));
        assertFalse(registry.validate(createClusterState(Settings.EMPTY, Collections.emptyList(), policyMap, null)));

        assertTrue(registry.validate(createClusterState(Settings.EMPTY, List.of(SLM_TEMPLATE_NAME), policyMap, null)));
    }

    // -------------

    private ClusterChangedEvent createClusterChangedEvent(List<String> existingTemplateNames, DiscoveryNodes nodes) {
        return createClusterChangedEvent(existingTemplateNames, Collections.emptyMap(), nodes);
    }

    private ClusterChangedEvent createClusterChangedEvent(List<String> existingTemplateNames,
                                                          Map<String, LifecyclePolicy> existingPolicies,
                                                          DiscoveryNodes nodes) {
        ClusterState cs = createClusterState(Settings.EMPTY, existingTemplateNames, existingPolicies, nodes);
        ClusterChangedEvent realEvent = new ClusterChangedEvent("created-from-test", cs,
            ClusterState.builder(new ClusterName("test")).build());
        ClusterChangedEvent event = spy(realEvent);
        when(event.localNodeMaster()).thenReturn(nodes.isLocalNodeElectedMaster());

        return event;
    }

    private ClusterState createClusterState(Settings nodeSettings,
                                            List<String> existingTemplateNames,
                                            Map<String, LifecyclePolicy> existingPolicies,
                                            DiscoveryNodes nodes) {
        ImmutableOpenMap.Builder<String, IndexTemplateMetaData> indexTemplates = ImmutableOpenMap.builder();
        for (String name : existingTemplateNames) {
            indexTemplates.put(name, mock(IndexTemplateMetaData.class));
        }

        Map<String, LifecyclePolicyMetadata> existingILMMeta = existingPolicies.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new LifecyclePolicyMetadata(e.getValue(), Collections.emptyMap(), 1, 1)));
        IndexLifecycleMetadata ilmMeta = new IndexLifecycleMetadata(existingILMMeta, OperationMode.RUNNING);

        return ClusterState.builder(new ClusterName("test"))
            .metaData(MetaData.builder()
                .templates(indexTemplates.build())
                .transientSettings(nodeSettings)
                .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
                .build())
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .build();
    }

    private static class TestPutIndexTemplateResponse extends AcknowledgedResponse {
        TestPutIndexTemplateResponse(boolean acknowledged) {
            super(acknowledged);
        }
    }
}
