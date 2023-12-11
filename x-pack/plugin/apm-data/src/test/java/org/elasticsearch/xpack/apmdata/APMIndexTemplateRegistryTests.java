/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apmdata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
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
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;
import org.elasticsearch.xpack.stack.StackTemplateRegistry;
import org.elasticsearch.xpack.stack.StackTemplateRegistryAccessor;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.XPackSettings.APM_DATA_ENABLED;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class APMIndexTemplateRegistryTests extends ESTestCase {
    private APMIndexTemplateRegistry apmIndexTemplateRegistry;
    private StackTemplateRegistryAccessor stackTemplateRegistryAccessor;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        FeatureService featureService = new FeatureService(List.of());
        stackTemplateRegistryAccessor = new StackTemplateRegistryAccessor(
            new StackTemplateRegistry(Settings.EMPTY, clusterService, threadPool, client, NamedXContentRegistry.EMPTY, featureService)
        );
        apmIndexTemplateRegistry = new APMIndexTemplateRegistry(
            Settings.builder().put(APM_DATA_ENABLED.getKey(), true).build(),
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY
        );
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

        ClusterChangedEvent event = createClusterChangedEvent(Map.of(), Map.of(), nodes);
        apmIndexTemplateRegistry.clusterChanged(event);
    }

    public void testThatIndependentTemplatesAreAddedImmediatelyIfMissing() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        AtomicInteger actualInstalledIndexTemplates = new AtomicInteger(0);
        AtomicInteger actualInstalledComponentTemplates = new AtomicInteger(0);
        AtomicInteger actualInstalledIngestPipelines = new AtomicInteger(0);

        client.setVerifier(
            (action, request, listener) -> verifyActions(
                actualInstalledIndexTemplates,
                actualInstalledComponentTemplates,
                actualInstalledIngestPipelines,
                action,
                request,
                listener
            )
        );
        apmIndexTemplateRegistry.clusterChanged(createClusterChangedEvent(Map.of(), Map.of(), nodes));

        assertBusy(() -> assertThat(actualInstalledIngestPipelines.get(), equalTo(getIndependentPipelineConfigs().size())));
        assertBusy(() -> assertThat(actualInstalledComponentTemplates.get(), equalTo(getIndependentComponentTemplateConfigs().size())));

        // index templates should not be installed as they are dependent in component templates and ingest pipelines
        assertThat(actualInstalledIndexTemplates.get(), equalTo(0));
    }

    public void testIngestPipelines() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        final List<IngestPipelineConfig> pipelineConfigs = apmIndexTemplateRegistry.getIngestPipelines();
        assertThat(pipelineConfigs, is(not(empty())));

        pipelineConfigs.forEach(ingestPipelineConfig -> {
            AtomicInteger putPipelineRequestsLocal = new AtomicInteger(0);
            client.setVerifier((a, r, l) -> {
                if (r instanceof PutPipelineRequest && ingestPipelineConfig.getId().equals(((PutPipelineRequest) r).getId())) {
                    putPipelineRequestsLocal.incrementAndGet();
                }
                return AcknowledgedResponse.TRUE;
            });

            apmIndexTemplateRegistry.clusterChanged(
                createClusterChangedEvent(Map.of(), Map.of(), ingestPipelineConfig.getPipelineDependencies(), nodes)
            );
            try {
                assertBusy(() -> assertThat(putPipelineRequestsLocal.get(), greaterThanOrEqualTo(1)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testComponentTemplates() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        AtomicInteger actualInstalledIndexTemplates = new AtomicInteger(0);
        AtomicInteger actualInstalledComponentTemplates = new AtomicInteger(0);
        AtomicInteger actualInstalledIngestPipelines = new AtomicInteger(0);

        client.setVerifier(
            (action, request, listener) -> verifyActions(
                actualInstalledIndexTemplates,
                actualInstalledComponentTemplates,
                actualInstalledIngestPipelines,
                action,
                request,
                listener
            )
        );
        apmIndexTemplateRegistry.clusterChanged(
            createClusterChangedEvent(
                Map.of(),
                Map.of(),
                apmIndexTemplateRegistry.getIngestPipelines().stream().map(IngestPipelineConfig::getId).collect(Collectors.toList()),
                nodes
            )
        );

        assertBusy(
            () -> assertThat(
                actualInstalledComponentTemplates.get(),
                equalTo(apmIndexTemplateRegistry.getComponentTemplateConfigs().size())
            )
        );

        // ingest pipelines should not have been installed as we used a cluster state that includes them already
        assertThat(actualInstalledIngestPipelines.get(), equalTo(0));
        // index templates should not be installed as they are dependent in component templates and ingest pipelines
        assertThat(actualInstalledIndexTemplates.get(), equalTo(0));
    }

    public void testIndexTemplates() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        AtomicInteger actualInstalledIndexTemplates = new AtomicInteger(0);
        AtomicInteger actualInstalledComponentTemplates = new AtomicInteger(0);
        AtomicInteger actualInstalledIngestPipelines = new AtomicInteger(0);

        client.setVerifier(
            (action, request, listener) -> verifyActions(
                actualInstalledIndexTemplates,
                actualInstalledComponentTemplates,
                actualInstalledIngestPipelines,
                action,
                request,
                listener
            )
        );

        // we need to add all component templates of the APM registry as well as the stack registry to ensure that all dependencies of
        // the APM-registry index templates exist
        Map<String, Integer> componentTemplates = new HashMap<>();
        apmIndexTemplateRegistry.getComponentTemplateConfigs()
            .forEach((key, value) -> componentTemplates.put(key, apmIndexTemplateRegistry.getVersion()));
        stackTemplateRegistryAccessor.getComponentTemplateConfigs()
            .forEach((key, value) -> componentTemplates.put(key, apmIndexTemplateRegistry.getVersion()));
        apmIndexTemplateRegistry.clusterChanged(
            createClusterChangedEvent(
                componentTemplates,
                Map.of(),
                apmIndexTemplateRegistry.getIngestPipelines().stream().map(IngestPipelineConfig::getId).collect(Collectors.toList()),
                nodes
            )
        );

        assertBusy(
            () -> assertThat(actualInstalledIndexTemplates.get(), equalTo(apmIndexTemplateRegistry.getComposableTemplateConfigs().size()))
        );

        // ingest pipelines and component templates should not have been installed as we used a cluster state that includes them already
        assertThat(actualInstalledComponentTemplates.get(), equalTo(0));
        assertThat(actualInstalledIngestPipelines.get(), equalTo(0));
    }

    private Map<String, ComponentTemplate> getIndependentComponentTemplateConfigs() {
        return apmIndexTemplateRegistry.getComponentTemplateConfigs().entrySet().stream().filter(template -> {
            Settings settings = template.getValue().template().settings();
            return settings == null || (settings.get("index.default_pipeline") == null && settings.get("index.final_pipeline") == null);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private List<IngestPipelineConfig> getIndependentPipelineConfigs() {
        return apmIndexTemplateRegistry.getIngestPipelines()
            .stream()
            .filter(pipelineConfig -> pipelineConfig.getPipelineDependencies().isEmpty())
            .collect(Collectors.toList());
    }

    private ActionResponse verifyActions(
        AtomicInteger indexTemplatesCounter,
        AtomicInteger componentTemplatesCounter,
        AtomicInteger ingestPipelinesCounter,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action instanceof PutComponentTemplateAction) {
            componentTemplatesCounter.incrementAndGet();
            return AcknowledgedResponse.TRUE;
        } else if (action instanceof PutComposableIndexTemplateAction) {
            indexTemplatesCounter.incrementAndGet();
            assertThat(request, instanceOf(PutComposableIndexTemplateAction.Request.class));
            final PutComposableIndexTemplateAction.Request putRequest = ((PutComposableIndexTemplateAction.Request) request);
            assertThat(putRequest.indexTemplate().version(), equalTo((long) apmIndexTemplateRegistry.getVersion()));
            assertNotNull(listener);
            return AcknowledgedResponse.TRUE;
        } else if (action instanceof PutPipelineAction) {
            ingestPipelinesCounter.incrementAndGet();
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
        return createClusterChangedEvent(existingComponentTemplates, existingComposableTemplates, Collections.emptyList(), nodes);
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Map<String, Integer> existingComponentTemplates,
        Map<String, Integer> existingComposableTemplates,
        List<String> ingestPipelines,
        DiscoveryNodes nodes
    ) {
        return createClusterChangedEvent(existingComponentTemplates, existingComposableTemplates, ingestPipelines, Map.of(), nodes);
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Map<String, Integer> existingComponentTemplates,
        Map<String, Integer> existingComposableTemplates,
        List<String> ingestPipelines,
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes
    ) {
        ClusterState cs = createClusterState(
            Settings.EMPTY,
            existingComponentTemplates,
            existingComposableTemplates,
            ingestPipelines,
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
        List<String> ingestPipelines,
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
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new LifecyclePolicyMetadata(e.getValue(), Map.of(), 1, 1)));
        IndexLifecycleMetadata ilmMeta = new IndexLifecycleMetadata(existingILMMeta, OperationMode.RUNNING);

        Map<String, PipelineConfiguration> ingestPipelineConfigurations = new HashMap<>();
        if (ingestPipelines.isEmpty() == false) {
            for (IngestPipelineConfig ingestPipelineConfig : apmIndexTemplateRegistry.getIngestPipelines()) {
                if (ingestPipelines.contains(ingestPipelineConfig.getId())) {
                    // we cannot mock PipelineConfiguration as it is a final class
                    ingestPipelineConfigurations.put(
                        ingestPipelineConfig.getId(),
                        new PipelineConfiguration(ingestPipelineConfig.getId(), ingestPipelineConfig.loadConfig(), XContentType.YAML)
                    );
                }
            }
        }
        IngestMetadata ingestMetadata = new IngestMetadata(ingestPipelineConfigurations);

        return ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .componentTemplates(componentTemplates)
                    .indexTemplates(composableTemplates)
                    .transientSettings(nodeSettings)
                    .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
                    .putCustom(IngestMetadata.TYPE, ingestMetadata)
                    .build()
            )
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .build();
    }
}
