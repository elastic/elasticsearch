/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue.Level;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.IsEqual.equalTo;

public class DeprecationInfoActionResponseTests extends AbstractWireSerializingTestCase<DeprecationInfoAction.Response> {

    @Override
    protected DeprecationInfoAction.Response createTestInstance() {
        List<DeprecationIssue> clusterIssues = randomDeprecationIssues();
        List<DeprecationIssue> nodeIssues = randomDeprecationIssues();
        Map<String, List<DeprecationIssue>> indexIssues = randomMap(
            0,
            10,
            () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues())
        );
        Map<String, List<DeprecationIssue>> dataStreamIssues = randomMap(
            0,
            10,
            () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues())
        );
        Map<String, List<DeprecationIssue>> templateIssues = randomMap(
            0,
            10,
            () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues())
        );
        Map<String, List<DeprecationIssue>> ilmPolicyIssues = randomMap(
            0,
            10,
            () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues())
        );
        Map<String, List<DeprecationIssue>> pluginIssues = randomMap(
            0,
            10,
            () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues())
        );
        return new DeprecationInfoAction.Response(
            clusterIssues,
            nodeIssues,
            Map.of(
                "data_streams",
                dataStreamIssues,
                "index_settings",
                indexIssues,
                "templates",
                templateIssues,
                "ilm_policies",
                ilmPolicyIssues
            ),
            pluginIssues
        );
    }

    @Override
    protected DeprecationInfoAction.Response mutateInstance(DeprecationInfoAction.Response instance) {
        List<DeprecationIssue> clusterIssues = instance.getClusterSettingsIssues();
        List<DeprecationIssue> nodeIssues = instance.getNodeSettingsIssues();
        Map<String, List<DeprecationIssue>> indexIssues = instance.getIndexSettingsIssues();
        Map<String, List<DeprecationIssue>> dataStreamIssues = instance.getDataStreamDeprecationIssues();
        Map<String, List<DeprecationIssue>> templateIssues = instance.getTemplateDeprecationIssues();
        Map<String, List<DeprecationIssue>> ilmPolicyIssues = instance.getIlmPolicyDeprecationIssues();
        Map<String, List<DeprecationIssue>> pluginIssues = instance.getPluginSettingsIssues();
        switch (randomIntBetween(1, 7)) {
            case 1 -> clusterIssues = randomValueOtherThan(clusterIssues, DeprecationInfoActionResponseTests::randomDeprecationIssues);
            case 2 -> nodeIssues = randomValueOtherThan(nodeIssues, DeprecationInfoActionResponseTests::randomDeprecationIssues);
            case 3 -> indexIssues = randomValueOtherThan(
                indexIssues,
                () -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues()))
            );
            case 4 -> dataStreamIssues = randomValueOtherThan(
                dataStreamIssues,
                () -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues()))
            );
            case 5 -> templateIssues = randomValueOtherThan(
                templateIssues,
                () -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues()))
            );
            case 6 -> ilmPolicyIssues = randomValueOtherThan(
                ilmPolicyIssues,
                () -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues()))
            );
            case 7 -> pluginIssues = randomValueOtherThan(
                pluginIssues,
                () -> randomMap(0, 10, () -> Tuple.tuple(randomAlphaOfLength(10), randomDeprecationIssues()))
            );
        }
        return new DeprecationInfoAction.Response(
            clusterIssues,
            nodeIssues,
            Map.of(
                "data_streams",
                dataStreamIssues,
                "index_settings",
                indexIssues,
                "templates",
                templateIssues,
                "ilm_policies",
                ilmPolicyIssues
            ),
            pluginIssues
        );
    }

    @Override
    protected Writeable.Reader<DeprecationInfoAction.Response> instanceReader() {
        return DeprecationInfoAction.Response::new;
    }

    public void testFrom() throws IOException {
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
        List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks = List.of((s) -> clusterIssueFound ? foundIssue : null);
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

        NodesDeprecationCheckResponse nodeDeprecationIssues = new NodesDeprecationCheckResponse(
            new ClusterName(randomAlphaOfLength(5)),
            nodeIssueFound ? List.of(new NodesDeprecationCheckAction.NodeResponse(discoveryNode, List.of(foundIssue))) : List.of(),
            List.of()
        );

        DeprecationInfoAction.Request request = new DeprecationInfoAction.Request(randomTimeValue(), Strings.EMPTY_ARRAY);
        DeprecationInfoAction.Response response = DeprecationInfoAction.Response.from(
            state,
            resolver,
            request,
            nodeDeprecationIssues,
            clusterSettingsChecks,
            new HashMap<>(), // modified in the method to move transform deprecation issues into cluster_settings
            List.of(),
            resourceCheckers
        );

        if (clusterIssueFound) {
            assertThat(response.getClusterSettingsIssues(), equalTo(List.of(foundIssue)));
        } else {
            assertThat(response.getClusterSettingsIssues(), empty());
        }

        if (nodeIssueFound) {
            String details = foundIssue.getDetails() != null ? foundIssue.getDetails() + " " : "";
            DeprecationIssue mergedFoundIssue = new DeprecationIssue(
                foundIssue.getLevel(),
                foundIssue.getMessage(),
                foundIssue.getUrl(),
                details + "(nodes impacted: [" + discoveryNode.getName() + "])",
                foundIssue.isResolveDuringRollingUpgrade(),
                foundIssue.getMeta()
            );
            assertThat(response.getNodeSettingsIssues(), equalTo(List.of(mergedFoundIssue)));
        } else {
            assertTrue(response.getNodeSettingsIssues().isEmpty());
        }

        if (indexIssueFound) {
            assertThat(response.getIndexSettingsIssues(), equalTo(Map.of("test", List.of(foundIssue))));
        } else {
            assertTrue(response.getIndexSettingsIssues().isEmpty());
        }
        if (dataStreamIssueFound) {
            assertThat(response.getDataStreamDeprecationIssues(), equalTo(Map.of("my-ds", List.of(foundIssue))));
        } else {
            assertTrue(response.getDataStreamDeprecationIssues().isEmpty());
        }
        if (ilmPolicyIssueFound) {
            assertThat(response.getIlmPolicyDeprecationIssues(), equalTo(Map.of("my-policy", List.of(foundIssue))));
        } else {
            assertTrue(response.getIlmPolicyDeprecationIssues().isEmpty());
        }
        if (componentTemplateIssueFound == false && indexTemplateIssueFound == false) {
            assertTrue(response.getTemplateDeprecationIssues().isEmpty());
        } else {
            if (componentTemplateIssueFound) {
                assertThat(response.getTemplateDeprecationIssues().get("my-component-template"), equalTo(List.of(foundIssue)));
            }
            if (indexTemplateIssueFound) {
                assertThat(response.getTemplateDeprecationIssues().get("my-index-template"), equalTo(List.of(foundIssue)));
            }

        }
    }

    public void testFromWithMergeableNodeIssues() throws IOException {
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

        DiscoveryNode node1 = DiscoveryNodeUtils.builder("nodeId1")
            .name("node1")
            .ephemeralId("ephemeralId1")
            .address("hostName1", "hostAddress1", new TransportAddress(TransportAddress.META_ADDRESS, 9300))
            .roles(Set.of())
            .build();
        DiscoveryNode node2 = DiscoveryNodeUtils.builder("nodeId2")
            .name("node2")
            .ephemeralId("ephemeralId2")
            .address("hostName2", "hostAddress2", new TransportAddress(TransportAddress.META_ADDRESS, 9500))
            .roles(Set.of())
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
        Map<String, Object> metaMap1 = DeprecationIssue.createMetaMapForRemovableSettings(List.of("setting.1", "setting.2", "setting.3"));
        Map<String, Object> metaMap2 = DeprecationIssue.createMetaMapForRemovableSettings(List.of("setting.2", "setting.3"));
        DeprecationIssue foundIssue1 = createTestDeprecationIssue(metaMap1);
        DeprecationIssue foundIssue2 = createTestDeprecationIssue(foundIssue1, metaMap2);
        List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks = List.of();
        List<ResourceDeprecationChecker> resourceCheckers = List.of();

        NodesDeprecationCheckResponse nodeDeprecationIssues = new NodesDeprecationCheckResponse(
            new ClusterName(randomAlphaOfLength(5)),
            Arrays.asList(
                new NodesDeprecationCheckAction.NodeResponse(node1, List.of(foundIssue1)),
                new NodesDeprecationCheckAction.NodeResponse(node2, List.of(foundIssue2))
            ),
            List.of()
        );

        DeprecationInfoAction.Request request = new DeprecationInfoAction.Request(randomTimeValue(), Strings.EMPTY_ARRAY);
        DeprecationInfoAction.Response response = DeprecationInfoAction.Response.from(
            state,
            resolver,
            request,
            nodeDeprecationIssues,
            clusterSettingsChecks,
            new HashMap<>(), // modified in the method to move transform deprecation issues into cluster_settings
            List.of(),
            resourceCheckers
        );

        String details = foundIssue1.getDetails() != null ? foundIssue1.getDetails() + " " : "";
        DeprecationIssue mergedFoundIssue = new DeprecationIssue(
            foundIssue1.getLevel(),
            foundIssue1.getMessage(),
            foundIssue1.getUrl(),
            details + "(nodes impacted: [" + node1.getName() + ", " + node2.getName() + "])",
            foundIssue1.isResolveDuringRollingUpgrade(),
            foundIssue2.getMeta()
        );
        assertThat(response.getNodeSettingsIssues(), equalTo(List.of(mergedFoundIssue)));
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
        List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks = List.of((s) -> {
            visibleClusterSettings.set(s.getMetadata().settings());
            return null;
        });
        AtomicReference<Settings> visibleIndexSettings = new AtomicReference<>();
        AtomicReference<Settings> visibleComponentTemplateSettings = new AtomicReference<>();
        AtomicReference<Settings> visibleIndexTemplateSettings = new AtomicReference<>();
        AtomicInteger backingIndicesCount = new AtomicInteger(0);
        List<ResourceDeprecationChecker> resourceCheckers = List.of(createResourceChecker("index_settings", (cs, req) -> {
            for (String indexName : resolver.concreteIndexNames(cs, req)) {
                visibleIndexSettings.set(cs.metadata().index(indexName).getSettings());
            }
            return Map.of();
        }), createResourceChecker("data_streams", (cs, req) -> {
            cs.metadata().dataStreams().values().forEach(ds -> backingIndicesCount.set(ds.getIndices().size()));
            return Map.of();
        }), createResourceChecker("templates", (cs, req) -> {
            cs.metadata()
                .componentTemplates()
                .values()
                .forEach(template -> visibleComponentTemplateSettings.set(template.template().settings()));
            cs.metadata().templatesV2().values().forEach(template -> {
                if (template.template() != null && template.template().settings() != null) {
                    visibleIndexTemplateSettings.set(template.template().settings());
                }
            });
            return Map.of();
        }));

        NodesDeprecationCheckResponse nodeDeprecationIssues = new NodesDeprecationCheckResponse(
            new ClusterName(randomAlphaOfLength(5)),
            List.of(),
            List.of()
        );

        DeprecationInfoAction.Request request = new DeprecationInfoAction.Request(randomTimeValue(), Strings.EMPTY_ARRAY);
        DeprecationInfoAction.Response.from(
            state,
            resolver,
            request,
            nodeDeprecationIssues,
            clusterSettingsChecks,
            new HashMap<>(), // modified in the method to move transform deprecation issues into cluster_settings
            List.of("some.deprecated.property", "some.other.*.deprecated.property"),
            resourceCheckers
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

        assertThat(backingIndicesCount.get(), equalTo(1));

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
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
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

    private static DeprecationIssue createTestDeprecationIssue() {
        return createTestDeprecationIssue(randomMap(1, 5, () -> Tuple.tuple(randomAlphaOfLength(4), randomAlphaOfLength(4))));
    }

    private static DeprecationIssue createTestDeprecationIssue(Map<String, Object> metaMap) {
        String details = randomBoolean() ? randomAlphaOfLength(10) : null;
        return new DeprecationIssue(
            randomFrom(Level.values()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            details,
            randomBoolean(),
            metaMap
        );
    }

    private static DeprecationIssue createTestDeprecationIssue(DeprecationIssue seedIssue, Map<String, Object> metaMap) {
        return new DeprecationIssue(
            seedIssue.getLevel(),
            seedIssue.getMessage(),
            seedIssue.getUrl(),
            seedIssue.getDetails(),
            seedIssue.isResolveDuringRollingUpgrade(),
            metaMap
        );
    }

    private static List<DeprecationIssue> randomDeprecationIssues() {
        return Stream.generate(DeprecationInfoActionResponseTests::createTestDeprecationIssue)
            .limit(randomIntBetween(0, 10))
            .collect(Collectors.toList());
    }

    private static ResourceDeprecationChecker createResourceChecker(
        String name,
        BiFunction<ClusterState, DeprecationInfoAction.Request, Map<String, List<DeprecationIssue>>> check
    ) {
        return new ResourceDeprecationChecker() {

            @Override
            public Map<String, List<DeprecationIssue>> check(ClusterState clusterState, DeprecationInfoAction.Request request) {
                return check.apply(clusterState, request);
            }

            @Override
            public String getName() {
                return name;
            }
        };
    }
}
