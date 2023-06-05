/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
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
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ProfilingIndexTemplateRegistryTests extends ESTestCase {
    private ProfilingIndexTemplateRegistry registry;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        registry = new ProfilingIndexTemplateRegistry(Settings.EMPTY, clusterService, threadPool, client, NamedXContentRegistry.EMPTY);
        registry.setTemplatesEnabled(true);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testThatMissingMasterNodeDoesNothing() {
        DiscoveryNode localNode = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").add(localNode).build();

        client.setVerifier((a, r, l) -> {
            fail("if the master is missing nothing should happen");
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
    }

    public void testThatNonExistingTemplatesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        // component templates are already existing, just add composable templates
        Map<String, Integer> componentTemplates = new HashMap<>();
        for (String templateName : registry.getComponentTemplateConfigs().keySet()) {
            componentTemplates.put(templateName, ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION);
        }
        ClusterChangedEvent event = createClusterChangedEvent(componentTemplates, Collections.emptyMap(), nodes);

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyComposableTemplateInstalled(calledTimes, action, request, listener));
        registry.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(registry.getComposableTemplateConfigs().size())));

        calledTimes.set(0);

        // attempting to register the event multiple times as a race condition can yield this test flaky, namely:
        // when calling registry.clusterChanged(newEvent) the templateCreationsInProgress state that the IndexTemplateRegistry maintains
        // might've not yet been updated to reflect that the first template registration was complete, so a second template registration
        // will not be issued anymore, leaving calledTimes to 0
        assertBusy(() -> {
            // now delete one template from the cluster state and lets retry
            ClusterChangedEvent newEvent = createClusterChangedEvent(Collections.emptyMap(), Collections.emptyMap(), nodes);
            registry.clusterChanged(newEvent);
            assertThat(calledTimes.get(), greaterThan(1));
        });
    }

    public void testThatNonExistingPoliciesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutLifecycleAction) {
                calledTimes.incrementAndGet();
                assertThat(action, instanceOf(PutLifecycleAction.class));
                assertThat(request, instanceOf(PutLifecycleAction.Request.class));
                final PutLifecycleAction.Request putRequest = (PutLifecycleAction.Request) request;
                assertThat(putRequest.getPolicy().getName(), equalTo("profiling"));
                assertNotNull(listener);
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComponentTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComposableIndexTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent newEvent = createClusterChangedEvent(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            nodes
        );
        registry.clusterChanged(newEvent);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(registry.getPolicyConfigs().size())));
    }

    public void testPolicyAlreadyExists() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        List<LifecyclePolicy> policies = registry.getPolicyConfigs();
        assertThat(policies, hasSize(1));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutComponentTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComposableIndexTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutIndexTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutLifecycleAction) {
                fail("if the policy already exists it should not be re-put");
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), Collections.emptyMap(), policyMap, nodes);
        registry.clusterChanged(event);
    }

    public void testPolicyAlreadyExistsButDiffers() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        String policyStr = String.format(
            Locale.ROOT,
            "{\"_meta\":{\"version\":%d},\"phases\":{\"delete\":{\"min_age\":\"1m\",\"actions\":{\"delete\":{}}}}}",
            ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION
        );
        List<LifecyclePolicy> policies = registry.getPolicyConfigs();
        assertThat(policies, hasSize(1));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutComponentTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComposableIndexTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutIndexTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutLifecycleAction) {
                fail("if the policy already exists it should not be re-put");
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(
                        new NamedXContentRegistry(
                            List.of(
                                new NamedXContentRegistry.Entry(
                                    LifecycleAction.class,
                                    new ParseField(DeleteAction.NAME),
                                    DeleteAction::parse
                                )
                            )
                        )
                    ),
                    policyStr
                )
        ) {
            LifecyclePolicy different = LifecyclePolicy.parse(parser, policies.get(0).getName());
            policyMap.put(policies.get(0).getName(), different);
            ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), Collections.emptyMap(), policyMap, nodes);
            registry.clusterChanged(event);
        }
    }

    public void testPolicyUpgraded() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        // set version to 0 to force an upgrade (proper versions start at 1)
        String priorPolicyStr = "{\"_meta\":{\"version\":0},\"phases\":{\"delete\":{\"min_age\":\"1m\",\"actions\":{\"delete\":{}}}}}";
        List<LifecyclePolicy> policies = registry.getPolicyConfigs();
        assertThat(policies, hasSize(1));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutComponentTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComposableIndexTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutIndexTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutLifecycleAction) {
                calledTimes.incrementAndGet();
                assertThat(action, instanceOf(PutLifecycleAction.class));
                assertThat(request, instanceOf(PutLifecycleAction.Request.class));
                final PutLifecycleAction.Request putRequest = (PutLifecycleAction.Request) request;
                assertThat(putRequest.getPolicy().getName(), equalTo("profiling"));
                assertNotNull(listener);
                return AcknowledgedResponse.TRUE;

            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(
                        new NamedXContentRegistry(
                            List.of(
                                new NamedXContentRegistry.Entry(
                                    LifecycleAction.class,
                                    new ParseField(DeleteAction.NAME),
                                    DeleteAction::parse
                                )
                            )
                        )
                    ),
                    priorPolicyStr
                )
        ) {
            LifecyclePolicy priorPolicy = LifecyclePolicy.parse(parser, policies.get(0).getName());
            policyMap.put(policies.get(0).getName(), priorPolicy);
            ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), Collections.emptyMap(), policyMap, nodes);
            registry.clusterChanged(event);
            // we've changed one policy that should be upgraded
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }
    }

    private ActionResponse verifyComposableTemplateInstalled(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action instanceof PutComponentTemplateAction) {
            // Ignore this, it's verified in another test
            return AcknowledgedResponse.TRUE;
        } else if (action instanceof PutLifecycleAction) {
            // Ignore this, it's verified in another test
            return AcknowledgedResponse.TRUE;
        } else if (action instanceof PutComposableIndexTemplateAction) {
            calledTimes.incrementAndGet();
            assertThat(action, instanceOf(PutComposableIndexTemplateAction.class));
            assertThat(request, instanceOf(PutComposableIndexTemplateAction.Request.class));
            final PutComposableIndexTemplateAction.Request putRequest = ((PutComposableIndexTemplateAction.Request) request);
            assertThat(putRequest.indexTemplate().version(), equalTo((long) ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION));
            assertNotNull(listener);
            return AcknowledgedResponse.TRUE;
        } else if (action instanceof PutIndexTemplateAction) {
            // Ignore this, it's verified in another test
            return AcknowledgedResponse.TRUE;
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Map<String, Integer> existingComponentTemplates,
        Map<String, Integer> existingComposableTemplates,
        DiscoveryNodes nodes
    ) {
        return createClusterChangedEvent(existingComponentTemplates, existingComposableTemplates, Collections.emptyMap(), nodes);
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Map<String, Integer> existingComponentTemplates,
        Map<String, Integer> existingComposableTemplates,
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes
    ) {
        ClusterState cs = createClusterState(
            Settings.EMPTY,
            existingComponentTemplates,
            existingComposableTemplates,
            existingPolicies,
            nodes
        );
        ClusterChangedEvent realEvent = new ClusterChangedEvent(
            "created-from-test",
            cs,
            ClusterState.builder(new ClusterName("test")).build()
        );
        ClusterChangedEvent event = spy(realEvent);
        when(event.localNodeMaster()).thenReturn(nodes.isLocalNodeElectedMaster());

        return event;
    }

    private ClusterState createClusterState(
        Settings nodeSettings,
        Map<String, Integer> existingComponentTemplates,
        Map<String, Integer> existingComposableTemplates,
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes
    ) {
        Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (Map.Entry<String, Integer> template : existingComponentTemplates.entrySet()) {
            ComponentTemplate mockTemplate = mock(ComponentTemplate.class);
            when(mockTemplate.version()).thenReturn(template.getValue() == null ? null : (long) template.getValue());
            componentTemplates.put(template.getKey(), mockTemplate);
        }
        Map<String, ComposableIndexTemplate> composableTemplates = new HashMap<>();
        for (Map.Entry<String, Integer> template : existingComposableTemplates.entrySet()) {
            ComposableIndexTemplate mockTemplate = mock(ComposableIndexTemplate.class);
            when(mockTemplate.version()).thenReturn(template.getValue() == null ? null : (long) template.getValue());
            composableTemplates.put(template.getKey(), mockTemplate);
        }

        Map<String, LifecyclePolicyMetadata> existingILMMeta = existingPolicies.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new LifecyclePolicyMetadata(e.getValue(), Collections.emptyMap(), 1, 1)));
        IndexLifecycleMetadata ilmMeta = new IndexLifecycleMetadata(existingILMMeta, OperationMode.RUNNING);

        return ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .componentTemplates(componentTemplates)
                    .indexTemplates(composableTemplates)
                    .transientSettings(nodeSettings)
                    .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
                    .build()
            )
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .build();
    }
}
