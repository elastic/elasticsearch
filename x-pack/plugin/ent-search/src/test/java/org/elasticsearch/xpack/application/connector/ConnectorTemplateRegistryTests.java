/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.ingest.PutPipelineAction;
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
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry.ACCESS_CONTROL_INDEX_NAME_PATTERN;
import static org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry.CONNECTOR_INDEX_NAME_PATTERN;
import static org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ConnectorTemplateRegistryTests extends ESTestCase {
    private ConnectorTemplateRegistry registry;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        registry = new ConnectorTemplateRegistry(clusterService, threadPool, client, NamedXContentRegistry.EMPTY);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testThatNonExistingComposableTemplatesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        Map<String, Integer> existingComponentTemplates = Map.of(
            ConnectorTemplateRegistry.CONNECTOR_TEMPLATE_NAME + "-mappings",
            ConnectorTemplateRegistry.REGISTRY_VERSION,
            ConnectorTemplateRegistry.CONNECTOR_TEMPLATE_NAME + "-settings",
            ConnectorTemplateRegistry.REGISTRY_VERSION,
            ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + "-mappings",
            ConnectorTemplateRegistry.REGISTRY_VERSION,
            ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + "-settings",
            ConnectorTemplateRegistry.REGISTRY_VERSION,
            ConnectorTemplateRegistry.ACCESS_CONTROL_TEMPLATE_NAME,
            ConnectorTemplateRegistry.REGISTRY_VERSION
        );

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), existingComponentTemplates, nodes);

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
            // now delete one template from the cluster state and let's retry
            ClusterChangedEvent newEvent = createClusterChangedEvent(Collections.emptyMap(), existingComponentTemplates, nodes);
            registry.clusterChanged(newEvent);
            assertThat(calledTimes.get(), greaterThan(2));
        });
    }

    public void testThatNonExistingComponentTemplatesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(
                ConnectorTemplateRegistry.ENT_SEARCH_GENERIC_PIPELINE_NAME,
                ConnectorTemplateRegistry.REGISTRY_VERSION
            ),
            Collections.emptyMap(),
            nodes
        );

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
            // now delete all templates from the cluster state and let's retry
            ClusterChangedEvent newEvent = createClusterChangedEvent(Collections.emptyMap(), Collections.emptyMap(), nodes);
            registry.clusterChanged(newEvent);
            assertThat(calledTimes.get(), greaterThan(4));
        });
    }

    public void testThatVersionedOldComponentTemplatesAreUpgraded() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.emptyMap(),
            Collections.singletonMap(
                ConnectorTemplateRegistry.CONNECTOR_TEMPLATE_NAME + "-settings",
                ConnectorTemplateRegistry.REGISTRY_VERSION - 1
            ),
            Collections.singletonMap(
                ConnectorTemplateRegistry.ENT_SEARCH_GENERIC_PIPELINE_NAME,
                ConnectorTemplateRegistry.REGISTRY_VERSION
            ),
            Collections.emptyMap(),
            nodes
        );
        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> verifyComponentTemplateInstalled(calledTimes, action, request, listener));
        registry.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(registry.getComponentTemplateConfigs().size())));
    }

    public void testThatUnversionedOldComponentTemplatesAreUpgraded() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.emptyMap(),
            Collections.singletonMap(ConnectorTemplateRegistry.CONNECTOR_TEMPLATE_NAME + "-mappings", null),
            Collections.singletonMap(
                ConnectorTemplateRegistry.ENT_SEARCH_GENERIC_PIPELINE_NAME,
                ConnectorTemplateRegistry.REGISTRY_VERSION
            ),
            Collections.emptyMap(),
            nodes
        );
        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> verifyComponentTemplateInstalled(calledTimes, action, request, listener));
        registry.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(registry.getComponentTemplateConfigs().size())));
    }

    public void testSameOrHigherVersionComponentTemplateNotUpgraded() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, Integer> versions = new HashMap<>();
        versions.put(ConnectorTemplateRegistry.CONNECTOR_TEMPLATE_NAME + "-mappings", ConnectorTemplateRegistry.REGISTRY_VERSION);
        versions.put(ConnectorTemplateRegistry.CONNECTOR_TEMPLATE_NAME + "-settings", ConnectorTemplateRegistry.REGISTRY_VERSION);
        versions.put(ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + "-mappings", ConnectorTemplateRegistry.REGISTRY_VERSION);
        versions.put(ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + "-settings", ConnectorTemplateRegistry.REGISTRY_VERSION);
        versions.put(ConnectorTemplateRegistry.ACCESS_CONTROL_TEMPLATE_NAME, ConnectorTemplateRegistry.REGISTRY_VERSION);
        ClusterChangedEvent sameVersionEvent = createClusterChangedEvent(Collections.emptyMap(), versions, nodes);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutPipelineAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            }
            if (action instanceof PutComponentTemplateAction) {
                fail("template should not have been re-installed");
                return null;
            } else if (action instanceof PutLifecycleAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComposableIndexTemplateAction) {
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
            ConnectorTemplateRegistry.CONNECTOR_TEMPLATE_NAME + "-mappings",
            ConnectorTemplateRegistry.REGISTRY_VERSION + randomIntBetween(0, 1000)
        );
        versions.put(
            ConnectorTemplateRegistry.CONNECTOR_TEMPLATE_NAME + "-settings",
            ConnectorTemplateRegistry.REGISTRY_VERSION + randomIntBetween(0, 1000)
        );
        versions.put(
            ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + "-mappings",
            ConnectorTemplateRegistry.REGISTRY_VERSION + randomIntBetween(0, 1000)
        );
        versions.put(
            ConnectorTemplateRegistry.CONNECTOR_SYNC_JOBS_TEMPLATE_NAME + "-settings",
            ConnectorTemplateRegistry.REGISTRY_VERSION + randomIntBetween(0, 1000)
        );
        versions.put(
            ConnectorTemplateRegistry.ACCESS_CONTROL_TEMPLATE_NAME,
            ConnectorTemplateRegistry.REGISTRY_VERSION + randomIntBetween(0, 1000)
        );
        ClusterChangedEvent higherVersionEvent = createClusterChangedEvent(Collections.emptyMap(), versions, nodes);
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
            Collections.singletonMap(ConnectorTemplateRegistry.CONNECTOR_TEMPLATE_NAME, null),
            Collections.emptyMap(),
            nodes
        );
        registry.clusterChanged(event);
    }

    public void testThatNonExistingPipelinesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        AtomicInteger calledTimes = new AtomicInteger(0);
        client.setVerifier((action, request, listener) -> {
            if (action instanceof PutPipelineAction) {
                calledTimes.incrementAndGet();
                return AcknowledgedResponse.TRUE;
            }
            if (action instanceof PutComponentTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutLifecycleAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComposableIndexTemplateAction) {
                // Ignore this, it's verified in another test
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(registry.getIngestPipelines().size())));
    }

    public void testThatNothingIsInstalledWhenAllNodesAreNotUpdated() {
        DiscoveryNode updatedNode = DiscoveryNodeUtils.create("updatedNode");
        DiscoveryNode outdatedNode = DiscoveryNodeUtils.create("outdatedNode", ESTestCase.buildNewFakeTransportAddress(), Version.V_8_9_0);
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .localNodeId("updatedNode")
            .masterNodeId("updatedNode")
            .add(updatedNode)
            .add(outdatedNode)
            .build();

        client.setVerifier((a, r, l) -> {
            fail("if some cluster mode are not updated to at least v.8.10.0 nothing should happen");
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
    }

    // -------------

    /**
     * A client that delegates to a verifying function for action/request/listener
     */
    private static class VerifyingClient extends NoOpClient {

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

        public void setVerifier(TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier) {
            this.verifier = verifier;
        }
    }

    private ActionResponse verifyComposableTemplateInstalled(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action instanceof PutPipelineAction) {
            // Ignore this, it's verified in another test
            return AcknowledgedResponse.TRUE;
        }
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
            final PutComposableIndexTemplateAction.Request putRequest = (PutComposableIndexTemplateAction.Request) request;
            assertThat(putRequest.indexTemplate().version(), equalTo((long) ConnectorTemplateRegistry.REGISTRY_VERSION));
            final List<String> indexPatterns = putRequest.indexTemplate().indexPatterns();
            assertThat(indexPatterns, hasSize(1));
            assertThat(
                indexPatterns,
                contains(oneOf(ACCESS_CONTROL_INDEX_NAME_PATTERN, CONNECTOR_INDEX_NAME_PATTERN, CONNECTOR_SYNC_JOBS_INDEX_NAME_PATTERN))
            );
            assertNotNull(listener);
            return new TestPutIndexTemplateResponse(true);
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ActionResponse verifyComponentTemplateInstalled(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action instanceof PutPipelineAction) {
            return AcknowledgedResponse.TRUE;
        }
        if (action instanceof PutComponentTemplateAction) {
            calledTimes.incrementAndGet();
            assertThat(action, instanceOf(PutComponentTemplateAction.class));
            assertThat(request, instanceOf(PutComponentTemplateAction.Request.class));
            final PutComponentTemplateAction.Request putRequest = (PutComponentTemplateAction.Request) request;
            assertThat(putRequest.componentTemplate().version(), equalTo((long) ConnectorTemplateRegistry.REGISTRY_VERSION));
            assertNotNull(listener);
            return new TestPutIndexTemplateResponse(true);
        } else if (action instanceof PutLifecycleAction) {
            // Ignore this, it's verified in another test
            return AcknowledgedResponse.TRUE;
        } else if (action instanceof PutComposableIndexTemplateAction) {
            // Ignore this, it's verified in another test
            return AcknowledgedResponse.TRUE;
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Map<String, Integer> existingComposableTemplates,
        Map<String, Integer> existingComponentTemplates,
        DiscoveryNodes nodes
    ) {
        return createClusterChangedEvent(
            existingComposableTemplates,
            existingComponentTemplates,
            Collections.emptyMap(),
            Collections.emptyMap(),
            nodes
        );
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Map<String, Integer> existingComposableTemplates,
        Map<String, Integer> existingComponentTemplates,
        Map<String, Integer> existingIngestPipelines,
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes
    ) {
        ClusterState cs = createClusterState(
            existingComposableTemplates,
            existingComponentTemplates,
            existingIngestPipelines,
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
        Map<String, Integer> existingComposableTemplates,
        Map<String, Integer> existingComponentTemplates,
        Map<String, Integer> existingIngestPipelines,
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes
    ) {
        Map<String, ComposableIndexTemplate> composableTemplates = new HashMap<>();
        for (Map.Entry<String, Integer> template : existingComposableTemplates.entrySet()) {
            ComposableIndexTemplate mockTemplate = mock(ComposableIndexTemplate.class);
            when(mockTemplate.version()).thenReturn(template.getValue() == null ? null : (long) template.getValue());
            composableTemplates.put(template.getKey(), mockTemplate);
        }

        Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (Map.Entry<String, Integer> template : existingComponentTemplates.entrySet()) {
            ComponentTemplate mockTemplate = mock(ComponentTemplate.class);
            when(mockTemplate.version()).thenReturn(template.getValue() == null ? null : (long) template.getValue());
            componentTemplates.put(template.getKey(), mockTemplate);
        }

        Map<String, PipelineConfiguration> ingestPipelines = new HashMap<>();
        for (Map.Entry<String, Integer> pipelineEntry : existingIngestPipelines.entrySet()) {
            // we cannot mock PipelineConfiguration as it is a final class
            ingestPipelines.put(
                pipelineEntry.getKey(),
                new PipelineConfiguration(
                    pipelineEntry.getKey(),
                    new BytesArray(Strings.format("{\"version\": %d}", pipelineEntry.getValue())),
                    XContentType.JSON
                )
            );
        }
        IngestMetadata ingestMetadata = new IngestMetadata(ingestPipelines);

        Map<String, LifecyclePolicyMetadata> existingILMMeta = existingPolicies.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new LifecyclePolicyMetadata(e.getValue(), Collections.emptyMap(), 1, 1)));
        IndexLifecycleMetadata ilmMeta = new IndexLifecycleMetadata(existingILMMeta, OperationMode.RUNNING);

        return ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .indexTemplates(composableTemplates)
                    .componentTemplates(componentTemplates)
                    .transientSettings(Settings.EMPTY)
                    .putCustom(IngestMetadata.TYPE, ingestMetadata)
                    .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
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
