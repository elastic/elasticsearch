/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

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

    public void testDisabledRegistryIgnoresFilters() {
        TestYamlTemplateRegistry registry = createRegistry(Map.of(FEATURE_ONE, MATCHES_COMPONENT_TWO));
        registry.setEnabled(false);
        assertThat(registry.getComponentTemplateConfigs(), equalTo(Map.of()));
        assertThat(registry.getComposableTemplateConfigs(), equalTo(Map.of()));
    }

    public void testEmptyFilterMapReturnsSameMapInstance() {
        TestYamlTemplateRegistry registry = createRegistry(Map.of());
        // No feature filters registered (the shipped default): filtering is a no-op, so the same
        // backing map is returned rather than a filtered copy.
        assertThat(registry.getComponentTemplateConfigs(), sameInstance(registry.getComponentTemplateConfigs()));
        assertThat(registry.allFeaturesSupported(), equalTo(true));
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
