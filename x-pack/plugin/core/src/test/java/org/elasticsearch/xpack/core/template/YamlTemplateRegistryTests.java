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
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class YamlTemplateRegistryTests extends ESTestCase {

    // Matches the marker settings baked into the fixtures under src/test/resources/{component,index}-templates.
    private static final Predicate<Template> MATCHES_COMPONENT_TWO = t -> "1".equals(t.settings().get("index.number_of_replicas"));
    private static final Predicate<Template> MATCHES_INDEX_TWO = t -> "2".equals(t.settings().get("index.number_of_shards"));

    private static final NodeFeature FEATURE_ONE = new NodeFeature("test_feature_one");
    private static final NodeFeature FEATURE_TWO = new NodeFeature("test_feature_two");

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private DiscoveryNode discoveryNode;
    private Client client;

    @Before
    public void setUpServices() {
        threadPool = new TestThreadPool(getClass().getName());
        discoveryNode = DiscoveryNodeUtils.create("node", "node");
        clusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode);
        client = new NoOpClient(threadPool);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        threadPool.shutdownNow();
    }

    private TestYamlTemplateRegistry createRegistry(Map<NodeFeature, Predicate<Template>> nodeFeatureFilters) {
        TestYamlTemplateRegistry registry = new TestYamlTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            client,
            NamedXContentRegistry.EMPTY,
            new FeatureService(List.of()),
            nodeFeatureFilters
        );
        registry.setEnabled(true);
        return registry;
    }

    private void setClusterNodeFeatures(NodeFeature... features) {
        Set<String> featureIds = Set.of();
        if (features.length > 0) {
            featureIds = Set.copyOf(Arrays.stream(features).map(NodeFeature::id).toList());
        }
        ClusterState newState = ClusterState.builder(clusterService.state())
            .nodeFeatures(Map.of(discoveryNode.getId(), featureIds))
            .build();
        ClusterServiceUtils.setState(clusterService, newState);
    }

    public void testNoFiltersReturnsAllTemplatesUnfiltered() {
        TestYamlTemplateRegistry registry = createRegistry(Map.of());
        Map<String, ?> componentTemplates = registry.getComponentTemplateConfigs();
        Map<String, ?> composableTemplates = registry.getComposableTemplateConfigs();
        assertThat(componentTemplates.keySet(), containsInAnyOrder("test-one@component-template", "test-two@component-template"));
        assertThat(composableTemplates.keySet(), containsInAnyOrder("test-index-one@template", "test-index-two@template"));
        assertThat(registry.allFeaturesSupported(), equalTo(true));
    }

    public void testFeaturePresentOnAllNodesKeepsAllTemplates() {
        setClusterNodeFeatures(FEATURE_ONE);
        TestYamlTemplateRegistry registry = createRegistry(Map.of(FEATURE_ONE, MATCHES_COMPONENT_TWO));
        assertThat(
            registry.getComponentTemplateConfigs().keySet(),
            containsInAnyOrder("test-one@component-template", "test-two@component-template")
        );
        assertThat(
            registry.getComposableTemplateConfigs().keySet(),
            containsInAnyOrder("test-index-one@template", "test-index-two@template")
        );
        assertThat(registry.allFeaturesSupported(), equalTo(true));
    }

    public void testFeatureAbsentFiltersMatchingComponentTemplate() {
        // FEATURE_ONE is not reported by any node, so templates matching its filter must be excluded.
        TestYamlTemplateRegistry registry = createRegistry(Map.of(FEATURE_ONE, MATCHES_COMPONENT_TWO));
        assertThat(registry.getComponentTemplateConfigs().keySet(), containsInAnyOrder("test-one@component-template"));
        // Composable templates are unaffected, since the filter only matches component template "test-two@component-template".
        assertThat(
            registry.getComposableTemplateConfigs().keySet(),
            containsInAnyOrder("test-index-one@template", "test-index-two@template")
        );
        assertThat(registry.allFeaturesSupported(), equalTo(false));
    }

    public void testFeatureAbsentFiltersMatchingComposableTemplate() {
        TestYamlTemplateRegistry registry = createRegistry(Map.of(FEATURE_ONE, MATCHES_INDEX_TWO));
        assertThat(
            registry.getComponentTemplateConfigs().keySet(),
            containsInAnyOrder("test-one@component-template", "test-two@component-template")
        );
        assertThat(registry.getComposableTemplateConfigs().keySet(), containsInAnyOrder("test-index-one@template"));
        assertThat(registry.allFeaturesSupported(), equalTo(false));
    }

    public void testOnlyUnsupportedFeaturesFilterTemplates() {
        // FEATURE_ONE is present cluster-wide so its filter is inert; FEATURE_TWO is absent so its filter applies.
        setClusterNodeFeatures(FEATURE_ONE);
        TestYamlTemplateRegistry registry = createRegistry(Map.of(FEATURE_ONE, MATCHES_COMPONENT_TWO, FEATURE_TWO, MATCHES_INDEX_TWO));
        assertThat(
            registry.getComponentTemplateConfigs().keySet(),
            containsInAnyOrder("test-one@component-template", "test-two@component-template")
        );
        assertThat(registry.getComposableTemplateConfigs().keySet(), containsInAnyOrder("test-index-one@template"));
        assertThat(registry.allFeaturesSupported(), equalTo(false));
        // Second filter is also supported
        setClusterNodeFeatures(FEATURE_ONE, FEATURE_TWO);
        assertThat(
            registry.getComponentTemplateConfigs().keySet(),
            containsInAnyOrder("test-one@component-template", "test-two@component-template")
        );
        assertThat(
            registry.getComposableTemplateConfigs().keySet(),
            containsInAnyOrder("test-index-one@template", "test-index-two@template")
        );
        assertThat(registry.allFeaturesSupported(), equalTo(true));
    }

    public void testEmptyFilterMapReturnsSameMapInstance() {
        TestYamlTemplateRegistry registry = createRegistry(Map.of());
        // No feature filters registered (the shipped default): filtering is a no-op, so the same
        // backing map is returned rather than a filtered copy.
        assertThat(registry.getComponentTemplateConfigs(), sameInstance(registry.getComponentTemplateConfigs()));
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
        // Two-node cluster: discoveryNode is the master; otherNode is a second data node.
        // Phase 1 simulates a rolling upgrade in progress: discoveryNode has upgraded and reports
        // FEATURE_ONE, but otherNode hasn't yet. The intersection across nodes is empty, so the
        // feature is not considered cluster-wide and the gated template must stay suppressed.
        DiscoveryNode otherNode = DiscoveryNodeUtils.create("other");
        DiscoveryNodes twoNodes = DiscoveryNodes.builder()
            .localNodeId(discoveryNode.getId())
            .masterNodeId(discoveryNode.getId())
            .add(discoveryNode)
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
        TestYamlTemplateRegistry registry = new TestYamlTemplateRegistry(
            Settings.EMPTY,
            clusterService,
            threadPool,
            trackingClient,
            NamedXContentRegistry.EMPTY,
            new FeatureService(List.of()),
            Map.of(FEATURE_ONE, MATCHES_INDEX_TWO)
        );
        registry.setEnabled(true);

        // Phase 1: mixed cluster — discoveryNode reports FEATURE_ONE but otherNode does not.
        // The feature intersection across all nodes is therefore empty, so test-index-two@template
        // must be suppressed. The cluster state already has both component templates installed
        // (prerequisite for composable templates) but no composable templates yet.
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state())
                .nodes(twoNodes)
                .nodeFeatures(Map.of(discoveryNode.getId(), Set.of(FEATURE_ONE.id()), otherNode.getId(), Set.of()))
                .build()
        );
        ClusterState phaseOneState = buildClusterState(
            Map.of("test-one@component-template", 1L, "test-two@component-template", 1L),
            Map.of(),
            twoNodes
        );
        registry.clusterChanged(createClusterChangedEvent(phaseOneState, twoNodes));

        assertBusy(() -> assertThat(installedIndexTemplates, containsInAnyOrder("test-index-one@template")));
        assertThat(registry.allFeaturesSupported(), equalTo(false));

        // Phase 2: otherNode has now upgraded — all nodes report FEATURE_ONE.
        // The gated template must be installed on the next cluster-changed event.
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state())
                .nodeFeatures(Map.of(discoveryNode.getId(), Set.of(FEATURE_ONE.id()), otherNode.getId(), Set.of(FEATURE_ONE.id())))
                .build()
        );

        // Cluster state reflects what was installed in phase 1: component templates and
        // test-index-one@template are present, but test-index-two@template is still absent.
        ClusterState phaseTwoState = buildClusterState(
            Map.of("test-one@component-template", 1L, "test-two@component-template", 1L),
            Map.of("test-index-one@template", 1L),
            twoNodes
        );
        registry.clusterChanged(createClusterChangedEvent(phaseTwoState, twoNodes));

        assertBusy(() -> assertThat(installedIndexTemplates, containsInAnyOrder("test-index-one@template", "test-index-two@template")));
        assertThat(registry.allFeaturesSupported(), equalTo(true));
    }

    private ClusterChangedEvent createClusterChangedEvent(ClusterState state, DiscoveryNodes nodes) {
        ClusterChangedEvent realEvent = new ClusterChangedEvent(
            "created-from-test",
            state,
            ClusterState.builder(new ClusterName("test")).build()
        );
        ClusterChangedEvent event = spy(realEvent);
        when(event.localNodeMaster()).thenReturn(nodes.isLocalNodeElectedMaster());
        return event;
    }

    /**
     * Builds a minimal cluster state containing the given component and composable index templates
     * at the specified versions. Templates not listed here are treated as absent (not yet installed).
     */
    private ClusterState buildClusterState(
        Map<String, Long> componentTemplateVersions,
        Map<String, Long> indexTemplateVersions,
        DiscoveryNodes nodes
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

        return ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().transientSettings(Settings.EMPTY).put(projectBuilder.build()).build())
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .build();
    }

    /**
     * Minimal {@link YamlTemplateRegistry} backed by the test fixtures under
     * {@code src/test/resources/resources.yaml}, used to exercise node-feature-based
     * template filtering without depending on a real plugin's resources.
     */
    static class TestYamlTemplateRegistry extends YamlTemplateRegistry {

        static final String TEST_TEMPLATE_VERSION_VARIABLE = "test.template.version";

        TestYamlTemplateRegistry(
            Settings nodeSettings,
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            NamedXContentRegistry xContentRegistry,
            FeatureService featureService,
            Map<NodeFeature, Predicate<Template>> nodeFeatureFilters
        ) {
            super(nodeSettings, clusterService, threadPool, client, xContentRegistry, ignored -> true, featureService, nodeFeatureFilters);
        }

        @Override
        public String getName() {
            return "test";
        }

        @Override
        protected String getVersionProperty() {
            return TEST_TEMPLATE_VERSION_VARIABLE;
        }

        @Override
        protected String getOrigin() {
            return "test";
        }

    }
}
