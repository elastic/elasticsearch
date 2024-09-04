/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apmdata;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamFeatures;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class APMIndexTemplateRegistryTests extends ESTestCase {
    private APMIndexTemplateRegistry apmIndexTemplateRegistry;
    private StackTemplateRegistryAccessor stackTemplateRegistryAccessor;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        final ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Set.of(APMPlugin.APM_DATA_REGISTRY_ENABLED).stream())
                .collect(Collectors.toSet())
        );

        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings);
        FeatureService featureService = new FeatureService(List.of(new DataStreamFeatures()));
        stackTemplateRegistryAccessor = new StackTemplateRegistryAccessor(
            new StackTemplateRegistry(Settings.EMPTY, clusterService, threadPool, client, NamedXContentRegistry.EMPTY, featureService)
        );

        apmIndexTemplateRegistry = new APMIndexTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY,
            featureService
        );
        apmIndexTemplateRegistry.setEnabled(true);
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

    public void testThatDisablingRegistryDoesNothing() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        apmIndexTemplateRegistry.setEnabled(false);
        assertThat(apmIndexTemplateRegistry.getComponentTemplateConfigs().entrySet(), hasSize(0));
        assertThat(apmIndexTemplateRegistry.getComposableTemplateConfigs().entrySet(), hasSize(0));
        assertThat(apmIndexTemplateRegistry.getIngestPipelines(), hasSize(0));

        client.setVerifier((a, r, l) -> {
            fail("if the registry is disabled nothing should happen");
            return null;
        });
        ClusterChangedEvent event = createClusterChangedEvent(Map.of(), Map.of(), nodes);
        apmIndexTemplateRegistry.clusterChanged(event);

        apmIndexTemplateRegistry.setEnabled(true);
        assertThat(apmIndexTemplateRegistry.getComponentTemplateConfigs().entrySet(), not(hasSize(0)));
        assertThat(apmIndexTemplateRegistry.getComposableTemplateConfigs().entrySet(), not(hasSize(0)));
        assertThat(apmIndexTemplateRegistry.getIngestPipelines(), not(hasSize(0)));
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

    public void testIngestPipelines() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        final List<IngestPipelineConfig> pipelineConfigs = apmIndexTemplateRegistry.getIngestPipelines();
        assertThat(pipelineConfigs, is(not(empty())));

        final Set<String> expectedPipelines = apmIndexTemplateRegistry.getIngestPipelines()
            .stream()
            .map(IngestPipelineConfig::getId)
            .collect(Collectors.toSet());
        final Set<String> installedPipelines = ConcurrentHashMap.newKeySet(pipelineConfigs.size());
        client.setVerifier((a, r, l) -> {
            if (r instanceof PutPipelineRequest putPipelineRequest) {
                if (expectedPipelines.contains(putPipelineRequest.getId())) {
                    installedPipelines.add(putPipelineRequest.getId());
                }
            }
            return AcknowledgedResponse.TRUE;
        });

        assertBusy(() -> {
            apmIndexTemplateRegistry.clusterChanged(createClusterChangedEvent(Map.of(), Map.of(), List.copyOf(installedPipelines), nodes));
            assertThat(installedPipelines, equalTo(expectedPipelines));
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

    public void testIndexTemplateConventions() throws Exception {
        for (Map.Entry<String, ComposableIndexTemplate> entry : apmIndexTemplateRegistry.getComposableTemplateConfigs().entrySet()) {
            final String name = entry.getKey();
            final int atIndex = name.lastIndexOf('@');
            assertThat(atIndex, not(equalTo(-1)));
            assertThat(name.substring(atIndex + 1), equalTo("template"));

            final String dataStreamType = name.substring(0, name.indexOf('-'));
            assertThat(dataStreamType, isIn(List.of("logs", "metrics", "traces")));

            final ComposableIndexTemplate template = entry.getValue();
            assertThat(template.indexPatterns().size(), equalTo(1));

            final String namePrefix = name.substring(0, atIndex);
            switch (namePrefix) {
                case "logs-apm.app", "metrics-apm.app":
                    // These two data streams have a service-specific dataset.
                    assertThat(template.indexPatterns().get(0), equalTo(namePrefix + ".*-*"));
                    break;
                default:
                    assertThat(template.indexPatterns().get(0), equalTo(namePrefix + "-*"));
                    break;
            }

            // Each index template should be composed of the following optional component templates:
            // <data_stream.type>@custom
            // <data_stream.type>-<data_stream.dataset>@custom
            final List<String> optionalComponentTemplates = template.composedOf()
                .stream()
                .filter(t -> template.getIgnoreMissingComponentTemplates().contains(t))
                .toList();
            assertThat(optionalComponentTemplates, containsInAnyOrder(namePrefix + "@custom", dataStreamType + "@custom"));

            // There should be no required custom component templates.
            final List<String> requiredCustomComponentTemplates = template.getRequiredComponentTemplates()
                .stream()
                .filter(t -> t.endsWith("@custom"))
                .toList();
            assertThat(requiredCustomComponentTemplates, empty());

            final Settings settings = template.template().settings();
            if (namePrefix.equals("traces-apm.sampled")) {
                // traces-apm.sampled does not have any ingest pipelines.
                assertThat(settings, equalTo(null));
            } else {
                final boolean isIntervalDataStream = dataStreamType.equals("metrics") && namePrefix.matches(".*\\.[0-9]+m");
                final String defaultPipeline = settings.get("index.default_pipeline");
                if (isIntervalDataStream) {
                    // e.g. metrics-apm.service_transaction.10m should call
                    // metrics-apm.service_transaction@default-pipeline
                    final String withoutInterval = namePrefix.substring(0, namePrefix.lastIndexOf('.'));
                    assertThat(defaultPipeline, equalTo(withoutInterval + "@default-pipeline"));
                } else {
                    // All other data streams should call a default pipeline
                    // specific to the data stream.
                    assertThat(defaultPipeline, equalTo(namePrefix + "@default-pipeline"));
                    break;
                }

                final String finalPipeline = settings.get("index.final_pipeline");
                switch (dataStreamType) {
                    case "metrics", "traces":
                        assertThat(finalPipeline, equalTo(dataStreamType + "-apm@pipeline"));
                        break;
                    default:
                        assertThat(finalPipeline, equalTo("apm@pipeline"));
                        break;
                }
            }
        }
    }

    public void testThatNothingIsInstalledWhenAllNodesAreNotUpdated() {
        DiscoveryNode updatedNode = DiscoveryNodeUtils.create("updatedNode");
        DiscoveryNode outdatedNode = DiscoveryNodeUtils.create("outdatedNode", ESTestCase.buildNewFakeTransportAddress(), Version.V_8_10_0);
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .localNodeId("updatedNode")
            .masterNodeId("updatedNode")
            .add(updatedNode)
            .add(outdatedNode)
            .build();

        client.setVerifier((a, r, l) -> {
            fail("if some cluster mode are not updated to at least v.8.11.0 nothing should happen");
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(Map.of(), Map.of(), nodes);
        apmIndexTemplateRegistry.clusterChanged(event);
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
        } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
            indexTemplatesCounter.incrementAndGet();
            assertThat(request, instanceOf(TransportPutComposableIndexTemplateAction.Request.class));
            final TransportPutComposableIndexTemplateAction.Request putRequest =
                ((TransportPutComposableIndexTemplateAction.Request) request);
            assertThat(putRequest.indexTemplate().version(), equalTo((long) apmIndexTemplateRegistry.getVersion()));
            assertNotNull(listener);
            return AcknowledgedResponse.TRUE;
        } else if (action == PutPipelineTransportAction.TYPE) {
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
        List<String> existingIngestPipelines,
        Map<String, LifecyclePolicy> existingPolicies,
        DiscoveryNodes nodes
    ) {
        ClusterState cs = createClusterState(
            Settings.EMPTY,
            existingComponentTemplates,
            existingComposableTemplates,
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
