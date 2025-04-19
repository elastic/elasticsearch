/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
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
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.junit.annotations.TestLogging;
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
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class StackTemplateRegistryTests extends ESTestCase {
    private StackTemplateRegistry registry;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        registry = new StackTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY,
            TestProjectResolvers.mustExecuteFirst()
        );
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testDisabledDoesNotAddIndexTemplates() {
        Settings settings = Settings.builder().put(StackTemplateRegistry.STACK_TEMPLATES_ENABLED.getKey(), false).build();
        StackTemplateRegistry disabledRegistry = new StackTemplateRegistry(
            settings,
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY,
            TestProjectResolvers.mustExecuteFirst()
        );
        assertThat(disabledRegistry.getComposableTemplateConfigs(), anEmptyMap());
    }

    public void testDisabledStillAddsComponentTemplatesAndIlmPolicies() {
        Settings settings = Settings.builder().put(StackTemplateRegistry.STACK_TEMPLATES_ENABLED.getKey(), false).build();
        StackTemplateRegistry disabledRegistry = new StackTemplateRegistry(
            settings,
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY,
            TestProjectResolvers.mustExecuteFirst()
        );
        assertThat(disabledRegistry.getComponentTemplateConfigs(), not(anEmptyMap()));
        assertThat(
            disabledRegistry.getComponentTemplateConfigs()
                .keySet()
                .stream()
                // We have a naming convention that internal component templates contain `@`. See also put-component-template.asciidoc.
                .filter(t -> t.contains("@") == false)
                .collect(Collectors.toSet()),
            empty()
        );
        assertThat(disabledRegistry.getLifecyclePolicies(), not(empty()));
        assertThat(
            // We have a naming convention that internal ILM policies contain `@`. See also put-lifecycle.asciidoc.
            disabledRegistry.getLifecyclePolicies().stream().filter(p -> p.getName().contains("@") == false).collect(Collectors.toSet()),
            empty()
        );
    }

    public void testThatNonExistingTemplatesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), nodes);

        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> verifyComponentTemplateInstalled(calledTimes, action, request, listener));
        registry.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(registry.getComponentTemplateConfigs().size())));

        calledTimes.set(0);

        // attempting to register the event multiple times as a race condition can yield this test flaky, namely:
        // when calling registry.clusterChanged(newEvent) the templateCreationsInProgress state that the IndexTemplateRegistry maintains
        // might've not yet been updated to reflect that the first template registration was complete, so a second template registration
        // will not be issued anymore, leaving calledTimes to 0
        assertBusy(() -> {
            // now delete one template from the cluster state and lets retry
            ClusterChangedEvent newEvent = createClusterChangedEvent(Collections.emptyMap(), nodes);
            registry.clusterChanged(newEvent);
            assertThat(calledTimes.get(), greaterThan(1));
        });
    }

    public void testThatNonExistingPoliciesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> {
            if (action == ILMActions.PUT) {
                calledTimes.incrementAndGet();
                assertThat(request, instanceOf(PutLifecycleRequest.class));
                final PutLifecycleRequest putRequest = (PutLifecycleRequest) request;
                assertThat(
                    putRequest.getPolicy().getName(),
                    anyOf(
                        equalTo(StackTemplateRegistry.LOGS_ILM_POLICY_NAME),
                        equalTo(StackTemplateRegistry.METRICS_ILM_POLICY_NAME),
                        equalTo(StackTemplateRegistry.SYNTHETICS_ILM_POLICY_NAME),
                        equalTo(StackTemplateRegistry.TRACES_ILM_POLICY_NAME),
                        equalTo(StackTemplateRegistry.ILM_7_DAYS_POLICY_NAME),
                        equalTo(StackTemplateRegistry.ILM_30_DAYS_POLICY_NAME),
                        equalTo(StackTemplateRegistry.ILM_90_DAYS_POLICY_NAME),
                        equalTo(StackTemplateRegistry.ILM_180_DAYS_POLICY_NAME),
                        equalTo(StackTemplateRegistry.ILM_365_DAYS_POLICY_NAME)
                    )
                );
                assertNotNull(listener);
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComponentTemplateAction) {
                // Ignore this, it's verified in another test
                return new StackTemplateRegistryTests.TestPutIndexTemplateResponse(true);
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(9)));
    }

    public void testPolicyAlreadyExists() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        List<LifecyclePolicy> policies = registry.getLifecyclePolicies();
        assertThat(policies, hasSize(9));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutComponentTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                fail("if the policy already exists it should not be re-put");
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), policyMap, nodes, true);
        registry.clusterChanged(event);
    }

    public void testThatIndependentPipelinesAreAdded() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> {
            if (action == PutPipelineTransportAction.TYPE) {
                calledTimes.incrementAndGet();
                return AcknowledgedResponse.TRUE;
            }
            if (action instanceof PutComponentTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        ClusterChangedEvent event = createInitialClusterChangedEvent(nodes);
        registry.clusterChanged(event);
        assertBusy(
            () -> assertThat(
                calledTimes.get(),
                equalTo(
                    Long.valueOf(
                        registry.getIngestPipelines()
                            .stream()
                            .filter(ingestPipelineConfig -> ingestPipelineConfig.getPipelineDependencies().isEmpty())
                            .count()
                    ).intValue()
                )
            )
        );
    }

    public void testPolicyAlreadyExistsButDiffers() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        String policyStr = "{\"phases\":{\"delete\":{\"min_age\":\"1m\",\"actions\":{\"delete\":{}}}}}";
        List<LifecyclePolicy> policies = registry.getLifecyclePolicies();
        assertThat(policies, hasSize(9));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutComponentTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
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
            ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), policyMap, nodes, true);
            registry.clusterChanged(event);
        }
    }

    public void testThatVersionedOldTemplatesAreUpgraded() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.singletonMap(
                StackTemplateRegistry.LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME,
                StackTemplateRegistry.REGISTRY_VERSION - 1
            ),
            nodes
        );
        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> verifyComponentTemplateInstalled(calledTimes, action, request, listener));
        registry.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(registry.getComponentTemplateConfigs().size())));
    }

    public void testThatUnversionedOldTemplatesAreUpgraded() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.singletonMap(StackTemplateRegistry.LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME, null),
            nodes
        );
        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> verifyComponentTemplateInstalled(calledTimes, action, request, listener));
        registry.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(registry.getComponentTemplateConfigs().size())));
    }

    public void testMissingNonRequiredTemplates() throws Exception {
        StackRegistryWithNonRequiredTemplates registryWithNonRequiredTemplate = new StackRegistryWithNonRequiredTemplates(
            Settings.EMPTY,
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY,
            TestProjectResolvers.mustExecuteFirst()
        );

        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.singletonMap(StackTemplateRegistry.LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME, null),
            nodes
        );

        final AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutComponentTemplateAction) {
                // Ignore such
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // Ignore such
                return AcknowledgedResponse.TRUE;
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                calledTimes.incrementAndGet();
                assertThat(request, instanceOf(TransportPutComposableIndexTemplateAction.Request.class));
                TransportPutComposableIndexTemplateAction.Request putComposableTemplateRequest =
                    (TransportPutComposableIndexTemplateAction.Request) request;
                assertThat(putComposableTemplateRequest.name(), equalTo("syslog"));
                ComposableIndexTemplate composableIndexTemplate = putComposableTemplateRequest.indexTemplate();
                assertThat(composableIndexTemplate.composedOf(), hasSize(2));
                assertThat(composableIndexTemplate.composedOf().get(0), equalTo("logs@settings"));
                assertThat(composableIndexTemplate.composedOf().get(1), equalTo("syslog@custom"));
                assertThat(composableIndexTemplate.getIgnoreMissingComponentTemplates(), hasSize(1));
                assertThat(composableIndexTemplate.getIgnoreMissingComponentTemplates().get(0), equalTo("syslog@custom"));
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request:" + request.toString());
                return null;
            }
        });

        registryWithNonRequiredTemplate.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
    }

    @TestLogging(value = "org.elasticsearch.xpack.core.template:DEBUG", reason = "test")
    public void testSameOrHigherVersionTemplateNotUpgraded() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, Integer> versions = new HashMap<>();
        versions.put(StackTemplateRegistry.DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.ECS_DYNAMIC_MAPPINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.METRICS_TSDB_SETTINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.AGENTLESS_SETTINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.AGENTLESS_MAPPINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.KIBANA_REPORTING_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.TRACES_MAPPINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        versions.put(StackTemplateRegistry.TRACES_SETTINGS_COMPONENT_TEMPLATE_NAME, StackTemplateRegistry.REGISTRY_VERSION);
        ClusterChangedEvent sameVersionEvent = createClusterChangedEvent(versions, nodes);
        client.setVerifier((action, request, listener) -> {
            if (request instanceof PutComponentTemplateAction.Request put) {
                fail("template should not have been re-installed: " + put.name());
                return null;
            } else if (action == ILMActions.PUT) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request:" + request.toString());
                return null;
            }
        });
        registry.clusterChanged(sameVersionEvent);

        versions.clear();
        versions.put(
            StackTemplateRegistry.DATA_STREAMS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.ECS_DYNAMIC_MAPPINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.LOGS_SETTINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.LOGS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.METRICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.METRICS_TSDB_SETTINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.METRICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.SYNTHETICS_SETTINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.SYNTHETICS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.AGENTLESS_SETTINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.AGENTLESS_MAPPINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.KIBANA_REPORTING_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.TRACES_MAPPINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        versions.put(
            StackTemplateRegistry.TRACES_SETTINGS_COMPONENT_TEMPLATE_NAME,
            StackTemplateRegistry.REGISTRY_VERSION + randomIntBetween(1, 1000)
        );
        ClusterChangedEvent higherVersionEvent = createClusterChangedEvent(versions, nodes);
        registry.clusterChanged(higherVersionEvent);
    }

    public void testThatMissingMasterNodeDoesNothing() {
        DiscoveryNode localNode = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").add(localNode).build();

        client.setVerifier((a, r, l) -> {
            fail("if the master is missing nothing should happen");
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.singletonMap(StackTemplateRegistry.LOGS_INDEX_TEMPLATE_NAME, null),
            nodes
        );
        registry.clusterChanged(event);
    }

    public void testThatTemplatesAreNotDeprecated() {
        for (ComposableIndexTemplate it : registry.getComposableTemplateConfigs().values()) {
            assertFalse(it.isDeprecated());
        }
        for (LifecyclePolicy ilm : registry.getLifecyclePolicies()) {
            assertFalse(ilm.isDeprecated());
        }
        for (ComponentTemplate ct : registry.getComponentTemplateConfigs().values()) {
            assertFalse(ct.deprecated());
        }
        registry.getIngestPipelines()
            .stream()
            .map(ipc -> new PipelineConfiguration(ipc.getId(), ipc.loadConfig(), XContentType.JSON))
            .map(PipelineConfiguration::getConfig)
            .forEach(p -> assertFalse((Boolean) p.get("deprecated")));
    }

    // -------------

    /**
     * A client that delegates to a verifying function for action/request/listener
     */
    public static class VerifyingClient extends NoOpClient {

        private TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier = (a, r, l) -> {
            fail("verifier not set");
            return null;
        };

        VerifyingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            try {
                listener.onResponse((Response) verifier.apply(action, request, listener));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        public VerifyingClient setVerifier(TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier) {
            this.verifier = verifier;
            return this;
        }
    }

    private ActionResponse verifyComponentTemplateInstalled(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action instanceof PutComponentTemplateAction) {
            calledTimes.incrementAndGet();
            assertThat(request, instanceOf(PutComponentTemplateAction.Request.class));
            final PutComponentTemplateAction.Request putRequest = (PutComponentTemplateAction.Request) request;
            assertThat(putRequest.componentTemplate().version(), equalTo((long) StackTemplateRegistry.REGISTRY_VERSION));
            assertNotNull(listener);
            return new TestPutIndexTemplateResponse(true);
        } else if (action == ILMActions.PUT) {
            // Ignore this, it's verified in another test
            return AcknowledgedResponse.TRUE;
        } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
            // Ignore this, it's verified in another test
            return AcknowledgedResponse.TRUE;
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ClusterChangedEvent createClusterChangedEvent(Map<String, Integer> existingTemplates, DiscoveryNodes nodes) {
        return createClusterChangedEvent(existingTemplates, Collections.emptyMap(), nodes, true);
    }

    private ClusterChangedEvent createInitialClusterChangedEvent(DiscoveryNodes nodes) {
        return createClusterChangedEvent(Collections.emptyMap(), Collections.emptyMap(), nodes, false);
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Map<String, Integer> existingTemplates,
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes,
        boolean addRegistryPipelines
    ) {
        ClusterState cs = createClusterState(Settings.EMPTY, existingTemplates, existingPolicies, nodes, addRegistryPipelines);
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
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes,
        boolean addRegistryPipelines
    ) {
        Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (Map.Entry<String, Integer> template : existingComponentTemplates.entrySet()) {
            ComponentTemplate mockTemplate = mock(ComponentTemplate.class);
            when(mockTemplate.version()).thenReturn(template.getValue() == null ? null : (long) template.getValue());
            componentTemplates.put(template.getKey(), mockTemplate);
        }

        Map<String, LifecyclePolicyMetadata> existingILMMeta = existingPolicies.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new LifecyclePolicyMetadata(e.getValue(), Collections.emptyMap(), 1, 1)));
        IndexLifecycleMetadata ilmMeta = new IndexLifecycleMetadata(existingILMMeta, OperationMode.RUNNING);

        // adding the registry pipelines, as they may be dependencies for index templates
        Map<String, PipelineConfiguration> ingestPipelines = new HashMap<>();
        if (addRegistryPipelines) {
            for (IngestPipelineConfig ingestPipelineConfig : registry.getIngestPipelines()) {
                // we cannot mock PipelineConfiguration as it is a final class
                ingestPipelines.put(
                    ingestPipelineConfig.getId(),
                    new PipelineConfiguration(ingestPipelineConfig.getId(), ingestPipelineConfig.loadConfig(), XContentType.JSON)
                );
            }
        }
        IngestMetadata ingestMetadata = new IngestMetadata(ingestPipelines);

        return ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .componentTemplates(componentTemplates)
                    .transientSettings(nodeSettings)
                    .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
                    .putCustom(IngestMetadata.TYPE, ingestMetadata)
                    .build()
            )
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
