/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.deprecation.DeprecationInfoAction.Response.RESERVED_NAMES;
import static org.elasticsearch.xpack.deprecation.DeprecationInfoActionResponseTests.createTestDeprecationIssue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportDeprecationInfoActionTests extends ESTestCase {

    public void testCheckAndCreateResponse() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_all");
        mapping.field("enabled", false);
        mapping.endObject().endObject();

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .putMapping(Strings.toString(mapping))
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();

        DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("test", new TransportAddress(TransportAddress.META_ADDRESS, 9300));
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
        boolean clusterIssueFound = randomBoolean();
        boolean nodeIssueFound = randomBoolean();
        boolean indexIssueFound = randomBoolean();
        boolean dataStreamIssueFound = randomBoolean();
        boolean indexTemplateIssueFound = randomBoolean();
        boolean componentTemplateIssueFound = randomBoolean();
        boolean ilmPolicyIssueFound = randomBoolean();
        DeprecationIssue foundIssue = createTestDeprecationIssue();
        ClusterDeprecationChecker clusterDeprecationChecker = mock(ClusterDeprecationChecker.class);
        when(clusterDeprecationChecker.check(any(), any())).thenReturn(clusterIssueFound ? List.of(foundIssue) : List.of());
        List<ResourceDeprecationChecker> resourceCheckers = List.of(createResourceChecker("index_settings", (cs, req) -> {
            if (indexIssueFound) {
                return Map.of("test", List.of(foundIssue));
            }
            return Map.of();
        }), createResourceChecker("data_streams", (cs, req) -> {
            if (dataStreamIssueFound) {
                return Map.of("my-ds", List.of(foundIssue));
            }
            return Map.of();
        }), createResourceChecker("templates", (cs, req) -> {
            Map<String, List<DeprecationIssue>> issues = new HashMap<>();
            if (componentTemplateIssueFound) {
                issues.put("my-component-template", List.of(foundIssue));
            }
            if (indexTemplateIssueFound) {
                issues.put("my-index-template", List.of(foundIssue));
            }
            return issues;
        }), createResourceChecker("ilm_policies", (cs, req) -> {
            if (ilmPolicyIssueFound) {
                return Map.of("my-policy", List.of(foundIssue));
            }
            return Map.of();
        }));

        List<DeprecationIssue> nodeDeprecationIssues = nodeIssueFound ? List.of(foundIssue) : List.of();

        DeprecationInfoAction.Request request = new DeprecationInfoAction.Request(randomTimeValue(), Strings.EMPTY_ARRAY);
        TransportDeprecationInfoAction.PrecomputedData precomputedData = new TransportDeprecationInfoAction.PrecomputedData();
        precomputedData.setOnceTransformConfigs(List.of());
        precomputedData.setOncePluginIssues(Map.of());
        precomputedData.setOnceNodeSettingsIssues(nodeDeprecationIssues);
        DeprecationInfoAction.Response response = TransportDeprecationInfoAction.checkAndCreateResponse(
            state,
            resolver,
            request,
            List.of(),
            clusterDeprecationChecker,
            resourceCheckers,
            precomputedData
        );

        if (clusterIssueFound) {
            assertThat(response.getClusterSettingsIssues(), IsEqual.equalTo(List.of(foundIssue)));
        } else {
            assertThat(response.getClusterSettingsIssues(), empty());
        }

        if (nodeIssueFound) {
            assertThat(response.getNodeSettingsIssues(), IsEqual.equalTo(List.of(foundIssue)));
        } else {
            assertTrue(response.getNodeSettingsIssues().isEmpty());
        }

        if (indexIssueFound) {
            assertThat(response.getIndexSettingsIssues(), IsEqual.equalTo(Map.of("test", List.of(foundIssue))));
        } else {
            assertTrue(response.getIndexSettingsIssues().isEmpty());
        }
        if (dataStreamIssueFound) {
            assertThat(response.getDataStreamDeprecationIssues(), IsEqual.equalTo(Map.of("my-ds", List.of(foundIssue))));
        } else {
            assertTrue(response.getDataStreamDeprecationIssues().isEmpty());
        }
        if (ilmPolicyIssueFound) {
            assertThat(response.getIlmPolicyDeprecationIssues(), IsEqual.equalTo(Map.of("my-policy", List.of(foundIssue))));
        } else {
            assertTrue(response.getIlmPolicyDeprecationIssues().isEmpty());
        }
        if (componentTemplateIssueFound == false && indexTemplateIssueFound == false) {
            assertTrue(response.getTemplateDeprecationIssues().isEmpty());
        } else {
            if (componentTemplateIssueFound) {
                assertThat(response.getTemplateDeprecationIssues().get("my-component-template"), IsEqual.equalTo(List.of(foundIssue)));
            }
            if (indexTemplateIssueFound) {
                assertThat(response.getTemplateDeprecationIssues().get("my-index-template"), IsEqual.equalTo(List.of(foundIssue)));
            }

        }
    }

    public void testRemoveSkippedSettings() {
        Settings.Builder settingsBuilder = settings(IndexVersion.current());
        settingsBuilder.put("some.deprecated.property", "someValue1");
        settingsBuilder.put("some.other.bad.deprecated.property", "someValue2");
        settingsBuilder.put("some.undeprecated.property", "someValue3");
        settingsBuilder.putList("some.undeprecated.list.property", List.of("someValue4", "someValue5"));
        Settings inputSettings = settingsBuilder.build();
        IndexMetadata dataStreamIndexMetadata = IndexMetadata.builder("ds-test-index-1")
            .settings(inputSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ComponentTemplate componentTemplate = new ComponentTemplate(Template.builder().settings(inputSettings).build(), null, null);
        ComposableIndexTemplate indexTemplate = ComposableIndexTemplate.builder()
            .template(Template.builder().settings(inputSettings))
            .build();
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(inputSettings).numberOfShards(1).numberOfReplicas(0))
            .put(dataStreamIndexMetadata, true)
            .put(DataStream.builder("ds-test", List.of(dataStreamIndexMetadata.getIndex())).build())
            .indexTemplates(
                Map.of(
                    "my-index-template",
                    indexTemplate,
                    "empty-template",
                    ComposableIndexTemplate.builder().indexPatterns(List.of("random")).build()
                )
            )
            .componentTemplates(Map.of("my-component-template", componentTemplate))
            .persistentSettings(inputSettings)
            .build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
        AtomicReference<Settings> visibleClusterSettings = new AtomicReference<>();
        ClusterDeprecationChecker clusterDeprecationChecker = mock(ClusterDeprecationChecker.class);
        when(clusterDeprecationChecker.check(any(), any())).thenAnswer(invocationOnMock -> {
            ClusterState observedState = invocationOnMock.getArgument(0);
            visibleClusterSettings.set(observedState.getMetadata().settings());
            return List.of();
        });
        AtomicReference<Settings> visibleIndexSettings = new AtomicReference<>();
        AtomicReference<Settings> visibleComponentTemplateSettings = new AtomicReference<>();
        AtomicReference<Settings> visibleIndexTemplateSettings = new AtomicReference<>();
        AtomicInteger backingIndicesCount = new AtomicInteger(0);
        List<ResourceDeprecationChecker> resourceCheckers = List.of(createResourceChecker("index_settings", (cs, req) -> {
            for (String indexName : resolver.concreteIndexNames(cs, req)) {
                visibleIndexSettings.set(cs.metadata().getProject().index(indexName).getSettings());
            }
            return Map.of();
        }), createResourceChecker("data_streams", (cs, req) -> {
            cs.metadata().getProject().dataStreams().values().forEach(ds -> backingIndicesCount.set(ds.getIndices().size()));
            return Map.of();
        }), createResourceChecker("templates", (cs, req) -> {
            cs.metadata()
                .getProject()
                .componentTemplates()
                .values()
                .forEach(template -> visibleComponentTemplateSettings.set(template.template().settings()));
            cs.metadata().getProject().templatesV2().values().forEach(template -> {
                if (template.template() != null && template.template().settings() != null) {
                    visibleIndexTemplateSettings.set(template.template().settings());
                }
            });
            return Map.of();
        }));
        TransportDeprecationInfoAction.PrecomputedData precomputedData = new TransportDeprecationInfoAction.PrecomputedData();
        precomputedData.setOnceTransformConfigs(List.of());
        precomputedData.setOncePluginIssues(Map.of());
        precomputedData.setOnceNodeSettingsIssues(List.of());
        DeprecationInfoAction.Request request = new DeprecationInfoAction.Request(randomTimeValue(), Strings.EMPTY_ARRAY);
        TransportDeprecationInfoAction.checkAndCreateResponse(
            state,
            resolver,
            request,
            List.of("some.deprecated.property", "some.other.*.deprecated.property"),
            clusterDeprecationChecker,
            resourceCheckers,
            precomputedData
        );

        settingsBuilder = settings(IndexVersion.current());
        settingsBuilder.put("some.undeprecated.property", "someValue3");
        settingsBuilder.putList("some.undeprecated.list.property", List.of("someValue4", "someValue5"));

        Settings expectedSettings = settingsBuilder.build();
        Settings resultClusterSettings = visibleClusterSettings.get();
        Assert.assertNotNull(resultClusterSettings);
        Assert.assertEquals(expectedSettings, visibleClusterSettings.get());

        Settings resultIndexSettings = visibleIndexSettings.get();
        Assert.assertNotNull(resultIndexSettings);
        Assert.assertEquals("someValue3", resultIndexSettings.get("some.undeprecated.property"));
        Assert.assertEquals(resultIndexSettings.getAsList("some.undeprecated.list.property"), List.of("someValue4", "someValue5"));
        Assert.assertFalse(resultIndexSettings.hasValue("some.deprecated.property"));
        Assert.assertFalse(resultIndexSettings.hasValue("some.other.bad.deprecated.property"));

        assertThat(backingIndicesCount.get(), IsEqual.equalTo(1));

        Assert.assertNotNull(visibleComponentTemplateSettings.get());
        Assert.assertEquals(expectedSettings, visibleComponentTemplateSettings.get());
        Assert.assertNotNull(visibleIndexTemplateSettings.get());
        Assert.assertEquals(expectedSettings, visibleIndexTemplateSettings.get());
    }

    public void testCtorFailure() {
        Map<String, List<DeprecationIssue>> indexNames = Stream.generate(() -> randomAlphaOfLength(10))
            .limit(10)
            .collect(Collectors.toMap(Function.identity(), (_k) -> List.of()));
        Map<String, List<DeprecationIssue>> dataStreamNames = Stream.generate(() -> randomAlphaOfLength(10))
            .limit(10)
            .collect(Collectors.toMap(Function.identity(), (_k) -> List.of()));
        Set<String> shouldCauseFailure = new HashSet<>(RESERVED_NAMES);
        for (int i = 0; i < randomIntBetween(1, 100); i++) {
            Map<String, List<DeprecationIssue>> pluginSettingsIssues = randomSubsetOf(3, shouldCauseFailure).stream()
                .collect(Collectors.toMap(Function.identity(), (_k) -> List.of()));
            expectThrows(
                ElasticsearchStatusException.class,
                () -> new DeprecationInfoAction.Response(
                    List.of(),
                    List.of(),
                    Map.of("data_streams", dataStreamNames, "index_settings", indexNames),
                    pluginSettingsIssues
                )
            );
        }
    }

    public void testPluginSettingIssues() {
        DeprecationChecker.Components components = new DeprecationChecker.Components(null, Settings.EMPTY, null);
        PlainActionFuture<Map<String, List<DeprecationIssue>>> future = new PlainActionFuture<>();
        TransportDeprecationInfoAction.pluginSettingIssues(
            List.of(
                new NamedChecker("foo", List.of(), false),
                new NamedChecker(
                    "bar",
                    List.of(new DeprecationIssue(DeprecationIssue.Level.WARNING, "bar msg", "", "details", false, Map.of("key", "value"))),
                    false
                )
            ),
            components,
            future
        );
        Map<String, List<DeprecationIssue>> issueMap = future.actionGet();
        assertThat(issueMap.size(), equalTo(2));
        assertThat(issueMap.get("foo"), is(empty()));
        assertThat(issueMap.get("bar").get(0).getMessage(), equalTo("bar msg"));
        assertThat(issueMap.get("bar").get(0).getDetails(), equalTo("details"));
        assertThat(issueMap.get("bar").get(0).isResolveDuringRollingUpgrade(), is(false));
        assertThat(issueMap.get("bar").get(0).getMeta(), equalTo(Map.of("key", "value")));
    }

    public void testPluginSettingIssuesWithFailures() {
        DeprecationChecker.Components components = new DeprecationChecker.Components(null, Settings.EMPTY, null);
        PlainActionFuture<Map<String, List<DeprecationIssue>>> future = new PlainActionFuture<>();
        TransportDeprecationInfoAction.pluginSettingIssues(
            List.of(
                new NamedChecker("foo", List.of(), false),
                new NamedChecker(
                    "bar",
                    List.of(new DeprecationIssue(DeprecationIssue.Level.WARNING, "bar msg", "", null, false, null)),
                    true
                )
            ),
            components,
            future
        );
        Exception exception = expectThrows(Exception.class, future::actionGet);
        assertThat(exception.getCause().getMessage(), containsString("boom"));
    }

    private static ResourceDeprecationChecker createResourceChecker(
        String name,
        BiFunction<ClusterState, DeprecationInfoAction.Request, Map<String, List<DeprecationIssue>>> check
    ) {
        return new ResourceDeprecationChecker() {

            @Override
            public Map<String, List<DeprecationIssue>> check(
                ClusterState clusterState,
                DeprecationInfoAction.Request request,
                TransportDeprecationInfoAction.PrecomputedData precomputedData
            ) {
                return check.apply(clusterState, request);
            }

            @Override
            public String getName() {
                return name;
            }
        };
    }

    private static class NamedChecker implements DeprecationChecker {

        private final String name;
        private final List<DeprecationIssue> issues;
        private final boolean shouldFail;

        NamedChecker(String name, List<DeprecationIssue> issues, boolean shouldFail) {
            this.name = name;
            this.issues = issues;
            this.shouldFail = shouldFail;
        }

        @Override
        public boolean enabled(Settings settings) {
            return true;
        }

        @Override
        public void check(DeprecationChecker.Components components, ActionListener<CheckResult> deprecationIssueListener) {
            if (shouldFail) {
                deprecationIssueListener.onFailure(new Exception("boom"));
                return;
            }
            deprecationIssueListener.onResponse(new CheckResult(name, issues));
        }

        @Override
        public String getName() {
            return name;
        }
    }

}
