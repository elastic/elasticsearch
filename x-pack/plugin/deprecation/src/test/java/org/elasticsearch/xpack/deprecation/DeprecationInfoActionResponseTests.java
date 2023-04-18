/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue.Level;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.deprecation.DeprecationInfoAction.Response.RESERVED_NAMES;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.IsEqual.equalTo;

public class DeprecationInfoActionResponseTests extends AbstractWireSerializingTestCase<DeprecationInfoAction.Response> {

    @Override
    protected DeprecationInfoAction.Response createTestInstance() {
        List<DeprecationIssue> clusterIssues = Stream.generate(DeprecationInfoActionResponseTests::createTestDeprecationIssue)
            .limit(randomIntBetween(0, 10))
            .collect(Collectors.toList());
        List<DeprecationIssue> nodeIssues = Stream.generate(DeprecationInfoActionResponseTests::createTestDeprecationIssue)
            .limit(randomIntBetween(0, 10))
            .collect(Collectors.toList());
        Map<String, List<DeprecationIssue>> indexIssues = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 10); i++) {
            List<DeprecationIssue> perIndexIssues = Stream.generate(DeprecationInfoActionResponseTests::createTestDeprecationIssue)
                .limit(randomIntBetween(0, 10))
                .collect(Collectors.toList());
            indexIssues.put(randomAlphaOfLength(10), perIndexIssues);
        }
        Map<String, List<DeprecationIssue>> pluginIssues = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 10); i++) {
            List<DeprecationIssue> perPluginIssues = Stream.generate(DeprecationInfoActionResponseTests::createTestDeprecationIssue)
                .limit(randomIntBetween(0, 10))
                .collect(Collectors.toList());
            pluginIssues.put(randomAlphaOfLength(10), perPluginIssues);
        }
        return new DeprecationInfoAction.Response(clusterIssues, nodeIssues, indexIssues, pluginIssues);
    }

    @Override
    protected DeprecationInfoAction.Response mutateInstance(DeprecationInfoAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();

        DiscoveryNode discoveryNode = DiscoveryNode.createLocal(
            Settings.EMPTY,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            "test"
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
        boolean clusterIssueFound = randomBoolean();
        boolean nodeIssueFound = randomBoolean();
        boolean indexIssueFound = randomBoolean();
        DeprecationIssue foundIssue = createTestDeprecationIssue();
        List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks = List.of((s) -> clusterIssueFound ? foundIssue : null);
        List<Function<IndexMetadata, DeprecationIssue>> indexSettingsChecks = List.of((idx) -> indexIssueFound ? foundIssue : null);

        NodesDeprecationCheckResponse nodeDeprecationIssues = new NodesDeprecationCheckResponse(
            new ClusterName(randomAlphaOfLength(5)),
            nodeIssueFound
                ? Collections.singletonList(
                    new NodesDeprecationCheckAction.NodeResponse(discoveryNode, Collections.singletonList(foundIssue))
                )
                : emptyList(),
            emptyList()
        );

        DeprecationInfoAction.Request request = new DeprecationInfoAction.Request(Strings.EMPTY_ARRAY);
        DeprecationInfoAction.Response response = DeprecationInfoAction.Response.from(
            state,
            resolver,
            request,
            nodeDeprecationIssues,
            indexSettingsChecks,
            clusterSettingsChecks,
            Collections.emptyMap(),
            Collections.emptyList()
        );

        if (clusterIssueFound) {
            assertThat(response.getClusterSettingsIssues(), equalTo(Collections.singletonList(foundIssue)));
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
            assertThat(response.getNodeSettingsIssues(), equalTo(Collections.singletonList(mergedFoundIssue)));
        } else {
            assertTrue(response.getNodeSettingsIssues().isEmpty());
        }

        if (indexIssueFound) {
            assertThat(response.getIndexSettingsIssues(), equalTo(Collections.singletonMap("test", Collections.singletonList(foundIssue))));
        } else {
            assertTrue(response.getIndexSettingsIssues().isEmpty());
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
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();

        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            "nodeId1",
            "ephemeralId1",
            "hostName1",
            "hostAddress1",
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            "nodeId2",
            "ephemeralId2",
            "hostName2",
            "hostAddress2",
            new TransportAddress(TransportAddress.META_ADDRESS, 9500),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
        Map<String, Object> metaMap1 = DeprecationIssue.createMetaMapForRemovableSettings(
            Collections.unmodifiableList(Arrays.asList("setting.1", "setting.2", "setting.3"))
        );
        Map<String, Object> metaMap2 = DeprecationIssue.createMetaMapForRemovableSettings(
            Collections.unmodifiableList(Arrays.asList("setting.2", "setting.3"))
        );
        DeprecationIssue foundIssue1 = createTestDeprecationIssue(metaMap1);
        DeprecationIssue foundIssue2 = createTestDeprecationIssue(foundIssue1, metaMap2);
        List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks = Collections.emptyList();
        List<Function<IndexMetadata, DeprecationIssue>> indexSettingsChecks = List.of((idx) -> null);

        NodesDeprecationCheckResponse nodeDeprecationIssues = new NodesDeprecationCheckResponse(
            new ClusterName(randomAlphaOfLength(5)),
            Arrays.asList(
                new NodesDeprecationCheckAction.NodeResponse(node1, Collections.singletonList(foundIssue1)),
                new NodesDeprecationCheckAction.NodeResponse(node2, Collections.singletonList(foundIssue2))
            ),
            emptyList()
        );

        DeprecationInfoAction.Request request = new DeprecationInfoAction.Request(Strings.EMPTY_ARRAY);
        DeprecationInfoAction.Response response = DeprecationInfoAction.Response.from(
            state,
            resolver,
            request,
            nodeDeprecationIssues,
            indexSettingsChecks,
            clusterSettingsChecks,
            Collections.emptyMap(),
            Collections.emptyList()
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
        assertThat(response.getNodeSettingsIssues(), equalTo(Collections.singletonList(mergedFoundIssue)));
    }

    public void testRemoveSkippedSettings() throws IOException {

        Settings.Builder settingsBuilder = settings(Version.CURRENT);
        settingsBuilder.put("some.deprecated.property", "someValue1");
        settingsBuilder.put("some.other.bad.deprecated.property", "someValue2");
        settingsBuilder.put("some.undeprecated.property", "someValue3");
        settingsBuilder.putList("some.undeprecated.list.property", List.of("someValue4", "someValue5"));
        Settings inputSettings = settingsBuilder.build();
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(inputSettings).numberOfShards(1).numberOfReplicas(0))
            .persistentSettings(inputSettings)
            .build();

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        IndexNameExpressionResolver resolver = TestIndexNameExpressionResolver.newInstance();
        AtomicReference<Settings> visibleClusterSettings = new AtomicReference<>();
        List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks = Collections.unmodifiableList(Arrays.asList((s) -> {
            visibleClusterSettings.set(s.getMetadata().settings());
            return null;
        }));
        AtomicReference<Settings> visibleIndexSettings = new AtomicReference<>();
        List<Function<IndexMetadata, DeprecationIssue>> indexSettingsChecks = Collections.unmodifiableList(Arrays.asList((idx) -> {
            visibleIndexSettings.set(idx.getSettings());
            return null;
        }));

        NodesDeprecationCheckResponse nodeDeprecationIssues = new NodesDeprecationCheckResponse(
            new ClusterName(randomAlphaOfLength(5)),
            emptyList(),
            emptyList()
        );

        DeprecationInfoAction.Request request = new DeprecationInfoAction.Request(Strings.EMPTY_ARRAY);
        DeprecationInfoAction.Response.from(
            state,
            resolver,
            request,
            nodeDeprecationIssues,
            indexSettingsChecks,
            clusterSettingsChecks,
            Collections.emptyMap(),
            List.of("some.deprecated.property", "some.other.*.deprecated.property")
        );

        settingsBuilder = settings(Version.CURRENT);
        settingsBuilder.put("some.undeprecated.property", "someValue3");
        settingsBuilder.putList("some.undeprecated.list.property", List.of("someValue4", "someValue5"));
        Settings expectedSettings = settingsBuilder.build();
        Settings resultClusterSettings = visibleClusterSettings.get();
        Assert.assertNotNull(resultClusterSettings);
        Assert.assertEquals(expectedSettings, visibleClusterSettings.get());
        Settings resultIndexSettings = visibleIndexSettings.get();
        Assert.assertNotNull(resultIndexSettings);
        Assert.assertTrue(resultIndexSettings.get("some.undeprecated.property").equals("someValue3"));
        Assert.assertTrue(resultIndexSettings.getAsList("some.undeprecated.list.property").equals(List.of("someValue4", "someValue5")));
        Assert.assertFalse(resultIndexSettings.hasValue("some.deprecated.property"));
        Assert.assertFalse(resultIndexSettings.hasValue("some.other.bad.deprecated.property"));
    }

    public void testCtorFailure() {
        Map<String, List<DeprecationIssue>> indexNames = Stream.generate(() -> randomAlphaOfLength(10))
            .limit(10)
            .collect(Collectors.toMap(Function.identity(), (_k) -> Collections.emptyList()));
        Set<String> shouldCauseFailure = new HashSet<>(RESERVED_NAMES);
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            Map<String, List<DeprecationIssue>> pluginSettingsIssues = randomSubsetOf(3, shouldCauseFailure).stream()
                .collect(Collectors.toMap(Function.identity(), (_k) -> Collections.emptyList()));
            expectThrows(
                ElasticsearchStatusException.class,
                () -> new DeprecationInfoAction.Response(Collections.emptyList(), Collections.emptyList(), indexNames, pluginSettingsIssues)
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
}
