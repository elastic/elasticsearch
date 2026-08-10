/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperFeatures;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class IndexTemplateRegistryTests extends ESTestCase {

    // Matches the marker settings baked into the fixtures defined in TestRegistryWithNodeFeatureFilters.
    private static final NodeFeature FEATURE_ONE = new NodeFeature("feature_one");
    private static final NodeFeature FEATURE_TWO = new NodeFeature("feature_two");
    private static final Predicate<Template> MATCHES_COMPONENT_TWO = t -> "1".equals(t.settings().get("index.number_of_replicas"));
    private static final Predicate<Template> MATCHES_INDEX_TWO = t -> "2".equals(t.settings().get("index.number_of_shards"));

    private final ProjectId projectId = randomProjectIdOrDefault();

    private TestRegistryWithCustomPlugin registry;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        registry = new TestRegistryWithCustomPlugin(Settings.EMPTY, clusterService, threadPool, client, NamedXContentRegistry.EMPTY);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testThatIndependentPipelinesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == PutPipelineTransportAction.TYPE) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutPipelineAction(calledTimes, action, request, listener, "custom-plugin-final_pipeline");
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // the composable template is not expected to be added, as it's dependency is not available in the cluster state
                // custom-plugin-settings.json is not expected to be added as it contains a dependency on the default_pipeline
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyMap(), nodes);
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatDependentPipelinesAreAddedIfDependenciesExist() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == PutPipelineTransportAction.TYPE) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutPipelineAction(calledTimes, action, request, listener, "custom-plugin-default_pipeline");
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // the composable template is not expected to be added, as it's dependency is not available in the cluster state
                // custom-plugin-settings.json is not expected to be added as it contains a dependency on the default_pipeline
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Map.of("custom-plugin-final_pipeline", 3),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatTemplateIsAddedIfAllDependenciesExist() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action instanceof PutComponentTemplateAction) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutComponentTemplate(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // the composable template is not expected to be added, as it's dependency is not available in the cluster state
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatTemplateIsNotAddedIfNotAllDependenciesExist() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == PutPipelineTransportAction.TYPE) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutPipelineAction(calledTimes, action, request, listener, "custom-plugin-default_pipeline");
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // the template is not expected to be added, as the final pipeline is missing
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Map.of("custom-plugin-final_pipeline", 3),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatComposableTemplateIsAddedIfDependenciesExist() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutComposableIndexTemplateAction(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else if (action == PutPipelineTransportAction.TYPE) {
                // ignore pipelines in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // other components should be added as they already exist with the right version already
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.singletonMap("custom-plugin-settings", 3), nodes);
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatComposableTemplateIsAddedIfDependenciesHaveRightVersion() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutComposableIndexTemplateAction(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComponentTemplateAction) {
                // ignore the component template upgrade
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else if (action == PutPipelineTransportAction.TYPE) {
                // ignore pipelines in this case
                return AcknowledgedResponse.TRUE;
            } else {
                // other components should be added as they already exist with the right version already
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        // unless the registry requires rollovers after index template updates, the dependencies only need to be available, without regard
        // to their version
        ClusterChangedEvent event = createClusterChangedEvent(Collections.singletonMap("custom-plugin-settings", 2), nodes);
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);

        // when a registry requires rollovers after index template updates, the upgrade should occur only if the dependencies are have
        // the required version
        registry.setApplyRollover(true);
        calledTimesMap.values().forEach(calledTimes -> calledTimes.set(0));
        registry.clusterChanged(event);
        Thread.sleep(100L);
        assertCalledTimes(calledTimesMap, event, 0);
        event = createClusterChangedEvent(Collections.singletonMap("custom-plugin-settings", 3), nodes);
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 1);
    }

    public void testThatTemplatesAreUpgradedWhenNeeded() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
            if (action == PutPipelineTransportAction.TYPE) {
                assertPutPipelineAction(
                    calledTimes,
                    action,
                    request,
                    listener,
                    "custom-plugin-default_pipeline",
                    "custom-plugin-final_pipeline"
                );
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else if (action instanceof PutComponentTemplateAction) {
                assertPutComponentTemplate(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                assertPutComposableIndexTemplateAction(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Map.of("custom-plugin-settings", 2, "custom-plugin-template", 2),
            Collections.emptyMap(),
            Map.of("custom-plugin-default_pipeline", 2, "custom-plugin-final_pipeline", 2),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 4);
    }

    public void testAutomaticRollover() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterState state = createClusterState(
            Map.of("custom-plugin-settings", 3, "custom-plugin-template", 3),
            Collections.emptyMap(),
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );
        Map<String, ComposableIndexTemplate> composableTemplateConfigs = registry.getComposableTemplateConfigs();
        final var metadataBuilder = Metadata.builder(state.metadata());
        for (Map.Entry<String, ComposableIndexTemplate> entry : composableTemplateConfigs.entrySet()) {
            ComposableIndexTemplate template = entry.getValue();
            for (var project : state.metadata().projects().values()) {
                metadataBuilder.put(
                    metadataBuilder.getProject(project.id())
                        .put(
                            entry.getKey(),
                            ComposableIndexTemplate.builder()
                                .indexPatterns(template.indexPatterns())
                                .template(template.template())
                                .componentTemplates(template.composedOf())
                                .priority(template.priority())
                                .version(2L)
                                .metadata(template.metadata())
                                .dataStreamTemplate(template.getDataStreamTemplate())
                                .build()
                        )
                );
            }
        }
        for (var project : state.metadata().projects().values()) {
            metadataBuilder.put(
                metadataBuilder.getProject(project.id())
                    .put(DataStreamTestHelper.newInstance("logs-my_app-1", Collections.singletonList(new Index(".ds-ds1-000001", "ds1i"))))
                    .put(DataStreamTestHelper.newInstance("logs-my_app-2", Collections.singletonList(new Index(".ds-ds2-000001", "ds2i"))))
                    .put(
                        DataStreamTestHelper.newInstance("traces-my_app-1", Collections.singletonList(new Index(".ds-ds3-000001", "ds3i")))
                    )
            );
        }
        state = ClusterState.builder(state).metadata(metadataBuilder).build();
        ClusterChangedEvent event = createClusterChangedEvent(nodes, state);

        Map<ProjectId, AtomicInteger> rolloverCounterMap = new ConcurrentHashMap<>();
        Map<ProjectId, AtomicInteger> putIndexTemplateCounterMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action instanceof RolloverAction) {
                final var rolloverCounter = rolloverCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                rolloverCounter.incrementAndGet();
                RolloverRequest rolloverRequest = ((RolloverRequest) request);
                assertThat(rolloverRequest.getRolloverTarget(), startsWith("logs-my_app-"));
                assertThat(rolloverRequest.isLazy(), equalTo(true));
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                final var putIndexTemplateCounter = putIndexTemplateCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                putIndexTemplateCounter.incrementAndGet();
            }
            return AcknowledgedResponse.TRUE;
        });

        registry.clusterChanged(event);
        assertCalledTimes(putIndexTemplateCounterMap, event, 1);
        // no rollover on upgrade because the test registry doesn't support automatic rollover by default
        Thread.sleep(100L);
        assertCalledTimes(rolloverCounterMap, event, 0);

        // test successful rollovers
        registry.setApplyRollover(true);
        putIndexTemplateCounterMap.values().forEach(calledTimes -> calledTimes.set(0));
        registry.clusterChanged(event);
        assertCalledTimes(putIndexTemplateCounterMap, event, 1);
        assertCalledTimes(rolloverCounterMap, event, 2);
        var rolloverResponsesRef = registry.getRolloverResponses();
        var projectIds = state.metadata().projects().keySet();
        assertBusy(() -> {
            assertThat(rolloverResponsesRef.keySet(), equalTo(projectIds));
            for (var rolloverResponses : rolloverResponsesRef.values()) {
                assertNotNull(rolloverResponses.get());
                assertThat(rolloverResponses.get(), hasSize(2));
            }
        });

        // test again, to verify that the per-index-template creation lock gets released for reuse
        putIndexTemplateCounterMap.values().forEach(calledTimes -> calledTimes.set(0));
        rolloverCounterMap.values().forEach(calledTimes -> calledTimes.set(0));
        rolloverResponsesRef.values().forEach(v -> v.set(Set.of()));
        registry.clusterChanged(event);
        assertCalledTimes(putIndexTemplateCounterMap, event, 1);
        assertCalledTimes(rolloverCounterMap, event, 2);
        assertBusy(() -> rolloverResponsesRef.values().forEach(v -> assertThat(v.get(), hasSize(2))));

        // test rollover failures
        putIndexTemplateCounterMap.values().forEach(calledTimes -> calledTimes.set(0));
        rolloverCounterMap.values().forEach(calledTimes -> calledTimes.set(0));
        client.setVerifier((projectId, action, request, listener) -> {
            if (action instanceof RolloverAction) {
                final var rolloverCounter = rolloverCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                rolloverCounter.incrementAndGet();
                RolloverRequest rolloverRequest = ((RolloverRequest) request);
                assertThat(rolloverRequest.getRolloverTarget(), startsWith("logs-my_app-"));
                throw new RuntimeException("Failed to rollover " + rolloverRequest.getRolloverTarget());
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                final var putIndexTemplateCounter = putIndexTemplateCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                putIndexTemplateCounter.incrementAndGet();
            }
            return AcknowledgedResponse.TRUE;
        });
        registry.clusterChanged(event);
        assertCalledTimes(putIndexTemplateCounterMap, event, 1);
        assertCalledTimes(rolloverCounterMap, event, 2);
        var rolloverFailureRefMap = registry.getRolloverFailure();
        assertBusy(() -> {
            assertThat(rolloverFailureRefMap.keySet(), equalTo(projectIds));
            rolloverFailureRefMap.values().forEach(rolloverFailureRef -> {
                assertNotNull(rolloverFailureRef.get());
                Exception rolloverFailure = rolloverFailureRef.get();
                assertThat(rolloverFailure.getMessage(), startsWith("Failed to rollover logs-my_app-"));
                Throwable[] suppressed = rolloverFailure.getSuppressed();
                assertThat(suppressed.length, equalTo(1));
                assertThat(suppressed[0].getMessage(), startsWith("Failed to rollover logs-my_app-"));
            });
        });
    }

    public void testRolloverForFreshInstalledIndexTemplate() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        ClusterState state = createClusterState(
            Map.of("custom-plugin-settings", 3, "custom-plugin-template", 3),
            Collections.emptyMap(),
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );
        final var metadataBuilder = Metadata.builder(state.metadata());
        for (var project : state.metadata().projects().values()) {
            metadataBuilder.put(
                metadataBuilder.getProject(project.id())
                    .put(DataStreamTestHelper.newInstance("logs-my_app-1", Collections.singletonList(new Index(".ds-ds1-000001", "ds1i"))))
                    .put(DataStreamTestHelper.newInstance("logs-my_app-2", Collections.singletonList(new Index(".ds-ds2-000001", "ds2i"))))
                    .put(
                        DataStreamTestHelper.newInstance("traces-my_app-1", Collections.singletonList(new Index(".ds-ds3-000001", "ds3i")))
                    )
            );
        }
        state = ClusterState.builder(state).metadata(metadataBuilder).build();
        ClusterChangedEvent event = createClusterChangedEvent(nodes, state);

        Map<ProjectId, AtomicInteger> rolloverCounterMap = new ConcurrentHashMap<>();
        Map<ProjectId, AtomicInteger> putIndexTemplateCounterMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action instanceof RolloverAction) {
                final var rolloverCounter = rolloverCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                rolloverCounter.incrementAndGet();
                RolloverRequest rolloverRequest = ((RolloverRequest) request);
                assertThat(rolloverRequest.getRolloverTarget(), startsWith("logs-my_app-"));
            } else if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                final var putIndexTemplateCounter = putIndexTemplateCounterMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                putIndexTemplateCounter.incrementAndGet();
            }
            return AcknowledgedResponse.TRUE;
        });

        registry.setApplyRollover(true);
        registry.clusterChanged(event);
        assertCalledTimes(putIndexTemplateCounterMap, event, 1);
        // rollover should be triggered even for the first installation, since the template
        // may now take precedence over a data stream's existing index template
        assertCalledTimes(rolloverCounterMap, event, 2);
    }

    public void testThatTemplatesAreNotUpgradedWhenNotNeeded() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // ignore this
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                // ignore lifecycle policies in this case
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Map.of("custom-plugin-settings", 3),
            Collections.emptyMap(),
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, 0);
    }

    public void testThatNonExistingPoliciesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // ignore this
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutLifecycleAction(calledTimes, action, request, listener);
                return AcknowledgedResponse.TRUE;
            } else {
                fail("client called with unexpected request: " + request.toString());
                return null;
            }
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Map.of("custom-plugin-settings", 3),
            Map.of(),
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );
        registry.clusterChanged(event);
        assertCalledTimes(calledTimesMap, event, registry.getLifecyclePolicies().size());
    }

    public void testPolicyAlreadyExists() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        List<LifecyclePolicy> policies = registry.getLifecyclePolicies();
        assertThat(policies, hasSize(1));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // ignore this
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                fail("if the policy already exists it should not be re-put");
            } else {
                fail("client called with unexpected request: " + request.toString());
            }
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(
            Map.of("custom-plugin-settings", 3),
            policyMap,
            Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
            nodes
        );

        registry.clusterChanged(event);
    }

    public void testPolicyAlreadyExistsButDiffers() throws IOException {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        String policyStr = "{\"phases\":{\"delete\":{\"min_age\":\"1m\",\"actions\":{\"delete\":{}}}}}";
        List<LifecyclePolicy> policies = registry.getLifecyclePolicies();
        assertThat(policies, hasSize(1));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // ignore this
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
            ClusterChangedEvent event = createClusterChangedEvent(
                Map.of("custom-plugin-settings", 3),
                policyMap,
                Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
                nodes
            );
            registry.clusterChanged(event);
        }
    }

    public void testPolicyUpgraded() throws Exception {
        registry.setPolicyUpgradeRequired(true);
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        Map<String, LifecyclePolicy> policyMap = new HashMap<>();
        String priorPolicyStr = "{\"phases\":{\"delete\":{\"min_age\":\"1m\",\"actions\":{\"delete\":{}}}}}";
        List<LifecyclePolicy> policies = registry.getLifecyclePolicies();
        assertThat(policies, hasSize(1));
        policies.forEach(p -> policyMap.put(p.getName(), p));

        Map<ProjectId, AtomicInteger> calledTimesMap = new ConcurrentHashMap<>();
        client.setVerifier((projectId, action, request, listener) -> {
            if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                // ignore this
                return AcknowledgedResponse.TRUE;
            } else if (action == ILMActions.PUT) {
                final var calledTimes = calledTimesMap.computeIfAbsent(projectId, k -> new AtomicInteger(0));
                assertPutLifecycleAction(calledTimes, action, request, listener);
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
            ClusterChangedEvent event = createClusterChangedEvent(
                Map.of("custom-plugin-settings", 3),
                policyMap,
                Map.of("custom-plugin-default_pipeline", 3, "custom-plugin-final_pipeline", 3),
                nodes
            );
            registry.clusterChanged(event);
            // we've changed one policy that should be upgraded
            assertCalledTimes(calledTimesMap, event, 1);
        }
    }

    private static void assertPutComponentTemplate(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        assertThat(action, instanceOf(PutComponentTemplateAction.class));
        assertThat(request, instanceOf(PutComponentTemplateAction.Request.class));
        final PutComponentTemplateAction.Request putRequest = (PutComponentTemplateAction.Request) request;
        assertThat(putRequest.name(), equalTo("custom-plugin-settings"));
        ComponentTemplate componentTemplate = putRequest.componentTemplate();
        assertThat(componentTemplate.template().settings().get("index.default_pipeline"), equalTo("custom-plugin-default_pipeline"));
        assertThat(componentTemplate.metadata().get("description"), equalTo("settings for my application logs"));
        assertNotNull(listener);
        calledTimes.incrementAndGet();
    }

    private static void assertPutComposableIndexTemplateAction(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        assertThat(request, instanceOf(TransportPutComposableIndexTemplateAction.Request.class));
        TransportPutComposableIndexTemplateAction.Request putComposableTemplateRequest =
            (TransportPutComposableIndexTemplateAction.Request) request;
        assertThat(putComposableTemplateRequest.name(), equalTo("custom-plugin-template"));
        ComposableIndexTemplate composableIndexTemplate = putComposableTemplateRequest.indexTemplate();
        assertThat(composableIndexTemplate.composedOf(), hasSize(2));
        assertThat(composableIndexTemplate.composedOf().get(0), equalTo("custom-plugin-settings"));
        assertThat(composableIndexTemplate.composedOf().get(1), equalTo("syslog@custom"));
        assertThat(composableIndexTemplate.getIgnoreMissingComponentTemplates(), hasSize(1));
        assertThat(composableIndexTemplate.getIgnoreMissingComponentTemplates().get(0), equalTo("syslog@custom"));
        assertNotNull(listener);
        calledTimes.incrementAndGet();
    }

    private static void assertPutPipelineAction(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener,
        String... pipelineIds
    ) {
        assertSame(PutPipelineTransportAction.TYPE, action);
        assertThat(request, instanceOf(PutPipelineRequest.class));
        final PutPipelineRequest putRequest = (PutPipelineRequest) request;
        assertThat(putRequest.getId(), oneOf(pipelineIds));
        PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(
            putRequest.getId(),
            putRequest.getSource(),
            putRequest.getXContentType()
        );
        List<?> processors = (List<?>) pipelineConfiguration.getConfig().get("processors");
        assertThat(processors, hasSize(1));
        Map<?, ?> setProcessor = (Map<?, ?>) ((Map<?, ?>) processors.get(0)).get("set");
        assertNotNull(setProcessor.get("field"));
        assertNotNull(setProcessor.get("copy_from"));
        assertNotNull(listener);
        calledTimes.incrementAndGet();
    }

    private static void assertPutLifecycleAction(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        assertSame(ILMActions.PUT, action);
        assertThat(request, instanceOf(PutLifecycleRequest.class));
        final PutLifecycleRequest putRequest = (PutLifecycleRequest) request;
        assertThat(putRequest.getPolicy().getName(), equalTo("custom-plugin-policy"));
        assertNotNull(listener);
        calledTimes.incrementAndGet();
    }

    private static void assertCalledTimes(Map<ProjectId, AtomicInteger> calledTimesMap, ClusterChangedEvent event, int expectedTimes)
        throws Exception {
        assertBusy(() -> {
            if (expectedTimes > 0) {
                assertThat(calledTimesMap.keySet(), equalTo(event.state().metadata().projects().keySet()));
            }
            for (var calledTimes : calledTimesMap.values()) {
                assertThat(calledTimes.get(), equalTo(expectedTimes));
            }
        });
    }

    private ClusterChangedEvent createClusterChangedEvent(Map<String, Integer> existingTemplates, DiscoveryNodes nodes) {
        return createClusterChangedEvent(existingTemplates, Collections.emptyMap(), Collections.emptyMap(), nodes);
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Map<String, Integer> existingTemplates,
        Map<String, LifecyclePolicy> existingPolicies,
        Map<String, Integer> existingIngestPipelines,
        DiscoveryNodes nodes
    ) {
        ClusterState clusterState = createClusterState(existingTemplates, existingPolicies, existingIngestPipelines, nodes);
        return createClusterChangedEvent(nodes, clusterState);
    }

    private ClusterChangedEvent createClusterChangedEvent(DiscoveryNodes nodes, ClusterState state) {
        ClusterChangedEvent realEvent = new ClusterChangedEvent(
            "created-from-test",
            state,
            ClusterState.builder(new ClusterName("test")).build()
        );
        ClusterChangedEvent event = spy(realEvent);
        when(event.localNodeMaster()).thenReturn(nodes.isLocalNodeElectedMaster());

        return event;
    }

    private ClusterState createClusterState(
        Map<String, Integer> existingComponentTemplates,
        Map<String, LifecyclePolicy> existingPolicies,
        Map<String, Integer> existingIngestPipelines,
        DiscoveryNodes nodes
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

        return ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .transientSettings(Settings.EMPTY)
                    .put(
                        ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID)
                            .componentTemplates(componentTemplates)
                            .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
                            .putCustom(IngestMetadata.TYPE, ingestMetadata)
                            .build()
                    )
                    .put(
                        ProjectMetadata.builder(projectId)
                            .componentTemplates(componentTemplates)
                            .putCustom(IndexLifecycleMetadata.TYPE, ilmMeta)
                            .putCustom(IngestMetadata.TYPE, ingestMetadata)
                            .build()
                    )
                    .build()
            )
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .build();
    }

    public void testRejectsTemplateWithFleetManagedPriorityAndManagedType() throws Exception {
        // Create a registry that returns a template with the reserved Fleet priority and a managed data stream type
        List<String> conflictingPatterns = List.of(
            "logs-*",
            "logs-foo*",
            "metrics-*",
            "metrics-foo-*",
            "traces-*",
            "traces-foo-*",
            "synthetics-*",
            "synthetics-foo-*",
            "profiling-*",
            "profiling-foo-*"
        );
        for (String indexPattern : conflictingPatterns) {
            long fleetPriority = randomFrom(150L, 200L);
            IndexTemplateRegistry registryWithReservedPriority = new IndexTemplateRegistry(
                Settings.EMPTY,
                clusterService,
                threadPool,
                client,
                NamedXContentRegistry.EMPTY,
                new FeatureService(List.of())
            ) {
                @Override
                protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
                    return Map.of(
                        "test-template",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of("this-is-fine", indexPattern))
                            .priority(fleetPriority)
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .build()
                    );
                }

                @Override
                protected String getOrigin() {
                    return "test";
                }
            };

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                "Expected index pattern [" + indexPattern + "] to be rejected",
                registryWithReservedPriority::initialize
            );
            assertThat(e.getMessage(), startsWith("Composable index template [test-template] with index patterns "));
            assertThat(e.getMessage(), containsString("and priority [" + fleetPriority + "]"));
            assertThat(e.getMessage(), containsString("would conflict with the managed index pattern ["));
        }
    }

    public void testAllowsFleetManagedPriorityForNonReservedPattern() {
        long fleetPriority = randomFrom(150L, 200L);
        // A composable template with a Fleet-managed priority should be allowed
        // as long as its index patterns don't overlap with Fleet-managed type patterns
        IndexTemplateRegistry registryWithNonReservedPattern = new IndexTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY,
            new FeatureService(List.of())
        ) {
            @Override
            protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
                return Map.of(
                    "test-template",
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("my-custom-*"))
                        .priority(fleetPriority)
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build()
                );
            }

            @Override
            protected String getOrigin() {
                return "test";
            }
        };

        // Should not throw because the pattern does not overlap with Fleet-managed type patterns
        registryWithNonReservedPattern.initialize();
    }

    public void testRejectsStreamsManagedPatternRegardlessOfPriority() {
        for (String indexPattern : List.of("logs", "logs.foo", "logs.ecs", "logs.otel")) {
            long priority = randomLongBetween(0, 1000);
            IndexTemplateRegistry registryWithStreamsManagedPattern = new IndexTemplateRegistry(
                Settings.EMPTY,
                clusterService,
                threadPool,
                client,
                NamedXContentRegistry.EMPTY,
                new FeatureService(List.of())
            ) {
                @Override
                protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
                    return Map.of(
                        "test-template",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(indexPattern))
                            .priority(priority)
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .build()
                    );
                }

                @Override
                protected String getOrigin() {
                    return "test";
                }
            };

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                "Expected streams-managed index pattern [" + indexPattern + "] to be rejected at non-Fleet priority",
                registryWithStreamsManagedPattern::initialize
            );
            assertThat(e.getMessage(), startsWith("Composable index template [test-template] with index patterns "));
            assertThat(e.getMessage(), containsString("and priority [" + priority + "]"));
            assertThat(e.getMessage(), containsString("would conflict with the managed index pattern ["));
        }
    }

    public void testAllowsNonFleetPriorityWithManagedTypePattern() {
        // A composable template with a non-Fleet priority should be allowed
        // even if its index patterns overlap with Fleet-managed type patterns
        IndexTemplateRegistry registryWithDifferentPriority = new IndexTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY,
            new FeatureService(List.of())
        ) {
            @Override
            protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
                return Map.of(
                    "test-template",
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("logs-test-*"))
                        .priority(100L)
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build()
                );
            }

            @Override
            protected String getOrigin() {
                return "test";
            }
        };

        // Should not throw because priority 100 is not a Fleet-reserved priority
        registryWithDifferentPriority.initialize();
    }

    // ------------- functionality unit test --------

    public void testFindRolloverTargetDataStreams() {
        ComposableIndexTemplate it1 = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("ds1*", "ds2*", "ds3*"))
            .priority(100L)
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();

        ComposableIndexTemplate it2 = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("ds2*"))
            .priority(200L)
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();

        ComposableIndexTemplate it5 = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("ds5*"))
            .priority(200L)
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();

        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(DataStreamTestHelper.newInstance("ds1", Collections.singletonList(new Index(".ds-ds1-000001", "ds1i"))))
            .put(DataStreamTestHelper.newInstance("ds2", Collections.singletonList(new Index(".ds-ds2-000001", "ds2i"))))
            .put(DataStreamTestHelper.newInstance("ds3", Collections.singletonList(new Index(".ds-ds3-000001", "ds3i"))))
            .put(DataStreamTestHelper.newInstance("ds4", Collections.singletonList(new Index(".ds-ds4-000001", "ds4i"))))
            .put("it1", it1)
            .put("it2", it2)
            .put("it5", it5)
            .build();

        assertThat(IndexTemplateRegistry.findRolloverTargetDataStreams(project, "it1", it1), containsInAnyOrder("ds1", "ds3"));
        assertThat(IndexTemplateRegistry.findRolloverTargetDataStreams(project, "it2", it2), contains("ds2"));
        assertThat(IndexTemplateRegistry.findRolloverTargetDataStreams(project, "it5", it5), empty());
    }

    public void testNoFiltersReturnsAllTemplatesUnfiltered() {
        TestRegistryWithNodeFeatureFilters registry = createRegistryWithFilters(Map.of());
        ClusterState state = stateWithNodeFeatures();
        Map<String, ComponentTemplate> componentTemplates = registry.getComponentTemplatesReadyToInstall(state);
        Map<String, ComposableIndexTemplate> composableTemplates = registry.getComposableTemplatesReadyToInstall(state);
        assertThat(componentTemplates.keySet(), containsInAnyOrder("test-one@component-template", "test-two@component-template"));
        assertThat(composableTemplates.keySet(), containsInAnyOrder("test-index-one@template", "test-index-two@template"));
        assertThat(registry.allFeaturesSupported(), equalTo(true));
    }

    public void testFeaturePresentOnAllNodesKeepsAllTemplates() {
        TestRegistryWithNodeFeatureFilters registry = createRegistryWithFilters(Map.of(FEATURE_ONE, MATCHES_COMPONENT_TWO));
        ClusterState state = stateWithNodeFeatures(FEATURE_ONE);
        assertThat(
            registry.getComponentTemplatesReadyToInstall(state).keySet(),
            containsInAnyOrder("test-one@component-template", "test-two@component-template")
        );
        assertThat(
            registry.getComposableTemplatesReadyToInstall(state).keySet(),
            containsInAnyOrder("test-index-one@template", "test-index-two@template")
        );
        assertThat(registry.allFeaturesSupported(), equalTo(true));
    }

    public void testTemporalitySettingRequiresClusterFeature() {
        Predicate<Template> filter = IndexTemplateRegistry.NODE_FEATURE_FILTERS.get(MapperFeatures.TSDB_METRIC_TEMPORALITY_SUPPORT);
        assertThat(filter, notNullValue());
        assertThat(
            filter.test(
                new Template(
                    Settings.builder().put(IndexSettings.TIME_SERIES_TEMPORALITY_FIELD.getKey(), "temporality").build(),
                    null,
                    null
                )
            ),
            equalTo(true)
        );
        assertThat(filter.test(new Template(Settings.EMPTY, null, null)), equalTo(false));
        assertThat(filter.test(null), equalTo(false));
    }

    public void testFeatureAbsentFiltersMatchingComponentTemplate() {
        // FEATURE_ONE is not reported by any node, so templates matching its filter must be excluded.
        TestRegistryWithNodeFeatureFilters registry = createRegistryWithFilters(Map.of(FEATURE_ONE, MATCHES_COMPONENT_TWO));
        ClusterState state = stateWithNodeFeatures();
        assertThat(registry.getComponentTemplatesReadyToInstall(state).keySet(), containsInAnyOrder("test-one@component-template"));
        // Composable templates are unaffected, since the filter only matches component template "test-two@component-template".
        assertThat(
            registry.getComposableTemplatesReadyToInstall(state).keySet(),
            containsInAnyOrder("test-index-one@template", "test-index-two@template")
        );
        assertThat(registry.allFeaturesSupported(), equalTo(false));
    }

    public void testFeatureAbsentFiltersMatchingComposableTemplate() {
        TestRegistryWithNodeFeatureFilters registry = createRegistryWithFilters(Map.of(FEATURE_ONE, MATCHES_INDEX_TWO));
        ClusterState state = stateWithNodeFeatures();
        assertThat(
            registry.getComponentTemplatesReadyToInstall(state).keySet(),
            containsInAnyOrder("test-one@component-template", "test-two@component-template")
        );
        assertThat(registry.getComposableTemplatesReadyToInstall(state).keySet(), containsInAnyOrder("test-index-one@template"));
        assertThat(registry.allFeaturesSupported(), equalTo(false));
    }

    public void testOnlyUnsupportedFeaturesFilterTemplates() {
        // FEATURE_ONE is present cluster-wide so its filter is inert; FEATURE_TWO is absent so its filter applies.
        TestRegistryWithNodeFeatureFilters registry = createRegistryWithFilters(
            Map.of(FEATURE_ONE, MATCHES_COMPONENT_TWO, FEATURE_TWO, MATCHES_INDEX_TWO)
        );
        ClusterState stateWithFeatureOne = stateWithNodeFeatures(FEATURE_ONE);
        assertThat(
            registry.getComponentTemplatesReadyToInstall(stateWithFeatureOne).keySet(),
            containsInAnyOrder("test-one@component-template", "test-two@component-template")
        );
        assertThat(
            registry.getComposableTemplatesReadyToInstall(stateWithFeatureOne).keySet(),
            containsInAnyOrder("test-index-one@template")
        );
        assertThat(registry.allFeaturesSupported(), equalTo(false));
        // Second filter is also supported
        ClusterState stateWithBothFeatures = stateWithNodeFeatures(FEATURE_ONE, FEATURE_TWO);
        assertThat(
            registry.getComponentTemplatesReadyToInstall(stateWithBothFeatures).keySet(),
            containsInAnyOrder("test-one@component-template", "test-two@component-template")
        );
        assertThat(
            registry.getComposableTemplatesReadyToInstall(stateWithBothFeatures).keySet(),
            containsInAnyOrder("test-index-one@template", "test-index-two@template")
        );
        assertThat(registry.allFeaturesSupported(), equalTo(true));
    }

    public void testEmptyFilterMapReturnsSameMapInstance() {
        TestRegistryWithNodeFeatureFilters registry = createRegistryWithFilters(Map.of());
        ClusterState state = stateWithNodeFeatures();
        // After the first call allFeaturesSupported is set to true; subsequent calls short-circuit
        // and return the same backing map instance rather than building a new filtered copy.
        Map<String, ComponentTemplate> first = registry.getComponentTemplatesReadyToInstall(state);
        assertThat(registry.getComponentTemplatesReadyToInstall(state), sameInstance(first));
        assertThat(registry.allFeaturesSupported(), equalTo(true));
    }

    /**
     * Verifies the rolling-upgrade lifecycle of a feature-gated template:
     * <ol>
     *   <li>While the cluster is mixed (not all nodes report FEATURE_ONE), the composable template
     *       that requires FEATURE_ONE is suppressed and only the unblocked template is installed.</li>
     *   <li>Once all nodes report FEATURE_ONE the registry unblocks the gated template, installs it
     *       on the next cluster-changed event, and marks {@code allFeaturesSupported} as {@code true}.</li>
     * </ol>
     */
    public void testRollingUpgradeInstallsGatedTemplateAfterAllNodesUpgraded() throws Exception {
        // Two-node cluster: local node is the master; otherNode is a second data node.
        // Phase 1 simulates a rolling upgrade in progress: the local node has upgraded and reports
        // FEATURE_ONE, but otherNode hasn't yet. The intersection across nodes is empty, so the
        // feature is not considered cluster-wide and the gated template must stay suppressed.
        DiscoveryNode localNode = clusterService.localNode();
        DiscoveryNode otherNode = DiscoveryNodeUtils.create("other");
        DiscoveryNodes twoNodes = DiscoveryNodes.builder()
            .localNodeId(localNode.getId())
            .masterNodeId(localNode.getId())
            .add(localNode)
            .add(otherNode)
            .build();

        Set<String> installedIndexTemplates = ConcurrentHashMap.newKeySet();
        NoOpClient trackingClient = new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Req extends ActionRequest, Resp extends ActionResponse> void doExecute(
                ActionType<Resp> action,
                Req request,
                ActionListener<Resp> listener
            ) {
                if (action == TransportPutComposableIndexTemplateAction.TYPE) {
                    installedIndexTemplates.add(((TransportPutComposableIndexTemplateAction.Request) request).name());
                }
                listener.onResponse((Resp) AcknowledgedResponse.TRUE);
            }
        };

        // FEATURE_ONE gates test-index-two@template (matched by MATCHES_INDEX_TWO).
        TestRegistryWithNodeFeatureFilters registry = new TestRegistryWithNodeFeatureFilters(
            Settings.EMPTY,
            clusterService,
            threadPool,
            trackingClient,
            NamedXContentRegistry.EMPTY,
            new FeatureService(List.of()),
            Map.of(FEATURE_ONE, MATCHES_INDEX_TWO)
        );

        // Phase 1: mixed cluster — local node reports FEATURE_ONE but otherNode does not.
        // The feature intersection across all nodes is therefore empty, so test-index-two@template
        // must be suppressed. The cluster state already has both component templates installed
        // (prerequisite for composable templates) but no composable templates yet.
        Map<String, Set<String>> mixedNodeFeatures = Map.of(localNode.getId(), Set.of(FEATURE_ONE.id()), otherNode.getId(), Set.of());
        ClusterState phaseOneState = buildClusterStateForFeatureTest(
            Map.of("test-one@component-template", 1L, "test-two@component-template", 1L),
            Map.of(),
            twoNodes,
            mixedNodeFeatures
        );
        registry.clusterChanged(createClusterChangedEvent(twoNodes, phaseOneState));

        assertBusy(() -> assertThat(installedIndexTemplates, containsInAnyOrder("test-index-one@template")));
        assertThat(registry.allFeaturesSupported(), equalTo(false));

        // Phase 2: otherNode has now upgraded — all nodes report FEATURE_ONE.
        // The gated template must be installed on the next cluster-changed event.
        // Cluster state reflects what was installed in phase 1: component templates and
        // test-index-one@template are present, but test-index-two@template is still absent.
        Map<String, Set<String>> allNodesUpgradedFeatures = Map.of(
            localNode.getId(),
            Set.of(FEATURE_ONE.id()),
            otherNode.getId(),
            Set.of(FEATURE_ONE.id())
        );
        ClusterState phaseTwoState = buildClusterStateForFeatureTest(
            Map.of("test-one@component-template", 1L, "test-two@component-template", 1L),
            Map.of("test-index-one@template", 1L),
            twoNodes,
            allNodesUpgradedFeatures
        );
        registry.clusterChanged(createClusterChangedEvent(twoNodes, phaseTwoState));

        assertBusy(() -> assertThat(installedIndexTemplates, containsInAnyOrder("test-index-one@template", "test-index-two@template")));
        assertThat(registry.allFeaturesSupported(), equalTo(true));
    }

    private ClusterState stateWithNodeFeatures(NodeFeature... features) {
        DiscoveryNode localNode = clusterService.localNode();
        Set<String> featureIds = Arrays.stream(features).map(NodeFeature::id).collect(Collectors.toUnmodifiableSet());
        return ClusterState.builder(clusterService.state()).nodeFeatures(Map.of(localNode.getId(), featureIds)).build();
    }

    private TestRegistryWithNodeFeatureFilters createRegistryWithFilters(Map<NodeFeature, Predicate<Template>> nodeFeatureFilters) {
        return new TestRegistryWithNodeFeatureFilters(
            Settings.EMPTY,
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY,
            new FeatureService(List.of()),
            nodeFeatureFilters
        );
    }

    private ClusterState buildClusterStateForFeatureTest(
        Map<String, Long> componentTemplateVersions,
        Map<String, Long> indexTemplateVersions,
        DiscoveryNodes nodes,
        Map<String, Set<String>> nodeFeaturesByNode
    ) {
        Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (Map.Entry<String, Long> entry : componentTemplateVersions.entrySet()) {
            ComponentTemplate mockTemplate = mock(ComponentTemplate.class);
            when(mockTemplate.version()).thenReturn(entry.getValue());
            componentTemplates.put(entry.getKey(), mockTemplate);
        }

        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID)
            .componentTemplates(componentTemplates);
        for (Map.Entry<String, Long> entry : indexTemplateVersions.entrySet()) {
            ComposableIndexTemplate mockTemplate = mock(ComposableIndexTemplate.class);
            when(mockTemplate.version()).thenReturn(entry.getValue());
            projectBuilder.put(entry.getKey(), mockTemplate);
        }

        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().transientSettings(Settings.EMPTY).put(projectBuilder.build()).build())
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes);
        if (nodeFeaturesByNode.isEmpty() == false) {
            stateBuilder.nodeFeatures(nodeFeaturesByNode);
        }
        return stateBuilder.build();
    }

    /**
     * Minimal {@link IndexTemplateRegistry} that provides two component templates and two composable
     * templates with distinct settings, used to exercise node-feature-based template filtering.
     * <p>
     * The settings baked into the templates match the predicates {@link #MATCHES_COMPONENT_TWO} and
     * {@link #MATCHES_INDEX_TWO}: {@code test-two@component-template} has
     * {@code index.number_of_replicas=1} and {@code test-index-two@template} has
     * {@code index.number_of_shards=2}.
     */
    static class TestRegistryWithNodeFeatureFilters extends IndexTemplateRegistry {

        private final Map<String, ComponentTemplate> componentTemplates;
        private final Map<String, ComposableIndexTemplate> composableTemplates;

        TestRegistryWithNodeFeatureFilters(
            Settings nodeSettings,
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            NamedXContentRegistry xContentRegistry,
            FeatureService featureService,
            Map<NodeFeature, Predicate<Template>> nodeFeatureFilters
        ) {
            super(nodeSettings, clusterService, threadPool, client, xContentRegistry, featureService, nodeFeatureFilters);
            componentTemplates = Map.of(
                "test-one@component-template",
                new ComponentTemplate(new Template(Settings.builder().put("index.number_of_replicas", 0).build(), null, null), 1L, null),
                "test-two@component-template",
                new ComponentTemplate(new Template(Settings.builder().put("index.number_of_replicas", 1).build(), null, null), 1L, null)
            );
            composableTemplates = Map.of(
                "test-index-one@template",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("test-index-one-*"))
                    .template(new Template(Settings.builder().put("index.number_of_shards", 1).build(), null, null))
                    .componentTemplates(List.of("test-one@component-template"))
                    .version(1L)
                    .build(),
                "test-index-two@template",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("test-index-two-*"))
                    .template(new Template(Settings.builder().put("index.number_of_shards", 2).build(), null, null))
                    .componentTemplates(List.of("test-two@component-template"))
                    .version(1L)
                    .build()
            );
        }

        @Override
        protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
            return componentTemplates;
        }

        @Override
        protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
            return composableTemplates;
        }

        @Override
        protected String getOrigin() {
            return "test";
        }
    }

    /**
     * A client that delegates to a verifying function for action/request/listener
     */
    public static class VerifyingClient extends NoOpClient {

        private Verifier verifier = (p, a, r, l) -> {
            fail("verifier not set");
            return null;
        };

        VerifyingClient(ThreadPool threadPool) {
            super(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext()));
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            try {
                final ProjectId projectId = ProjectId.fromId(
                    threadPool().getThreadContext().getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER)
                );
                listener.onResponse((Response) verifier.verify(projectId, action, request, listener));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        public VerifyingClient setVerifier(Verifier verifier) {
            this.verifier = verifier;
            return this;
        }
    }

    private interface Verifier {
        ActionResponse verify(ProjectId projectId, ActionType<?> action, ActionRequest request, ActionListener<?> listener);
    }
}
