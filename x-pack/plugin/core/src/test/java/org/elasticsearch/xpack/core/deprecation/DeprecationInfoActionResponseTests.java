/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.IsEqual.equalTo;

public class DeprecationInfoActionResponseTests extends AbstractWireSerializingTestCase<DeprecationInfoAction.Response> {

    @Override
    protected DeprecationInfoAction.Response createTestInstance() {
        List<DeprecationIssue> clusterIssues = Stream.generate(DeprecationIssueTests::createTestInstance)
            .limit(randomIntBetween(0, 10)).collect(Collectors.toList());
        List<DeprecationIssue> nodeIssues = Stream.generate(DeprecationIssueTests::createTestInstance)
            .limit(randomIntBetween(0, 10)).collect(Collectors.toList());
        List<DeprecationIssue> mlIssues = Stream.generate(DeprecationIssueTests::createTestInstance)
                .limit(randomIntBetween(0, 10)).collect(Collectors.toList());
        Map<String, List<DeprecationIssue>> indexIssues = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 10); i++) {
            List<DeprecationIssue> perIndexIssues = Stream.generate(DeprecationIssueTests::createTestInstance)
                .limit(randomIntBetween(0, 10)).collect(Collectors.toList());
            indexIssues.put(randomAlphaOfLength(10), perIndexIssues);
        }
        return new DeprecationInfoAction.Response(clusterIssues, nodeIssues, indexIssues, mlIssues);
    }

    @Override
    protected Writeable.Reader<DeprecationInfoAction.Response> instanceReader() {
        return DeprecationInfoAction.Response::new;
    }

    public void testFrom() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_all");
        mapping.field("enabled", false);
        mapping.endObject().endObject();

        MetaData metadata = MetaData.builder().put(IndexMetaData.builder("test")
            .putMapping(Strings.toString(mapping))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0))
            .build();

        DiscoveryNode discoveryNode = DiscoveryNode.createLocal(Settings.EMPTY,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), "test");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metaData(metadata).build();
        List<DatafeedConfig> datafeeds = Collections.singletonList(DatafeedConfigTests.createRandomizedDatafeedConfig("foo"));
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver();
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false,
            true, true);
        boolean clusterIssueFound = randomBoolean();
        boolean nodeIssueFound = randomBoolean();
        boolean indexIssueFound = randomBoolean();
        boolean mlIssueFound = randomBoolean();
        DeprecationIssue foundIssue = DeprecationIssueTests.createTestInstance();
        List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks = List.of((s) -> clusterIssueFound ? foundIssue : null);
        List<Function<IndexMetaData, DeprecationIssue>> indexSettingsChecks = List.of((idx) -> indexIssueFound ? foundIssue : null);
        List<BiFunction<DatafeedConfig, NamedXContentRegistry, DeprecationIssue>> mlSettingsChecks =
                List.of((idx, unused) -> mlIssueFound ? foundIssue : null);

        NodesDeprecationCheckResponse nodeDeprecationIssues = new NodesDeprecationCheckResponse(
            new ClusterName(randomAlphaOfLength(5)),
            nodeIssueFound
                ? Collections.singletonList(
                    new NodesDeprecationCheckAction.NodeResponse(discoveryNode, Collections.singletonList(foundIssue)))
                : emptyList(),
            emptyList());

        DeprecationInfoAction.Response response = DeprecationInfoAction.Response.from(state, NamedXContentRegistry.EMPTY,
            resolver, Strings.EMPTY_ARRAY, indicesOptions, datafeeds,
            nodeDeprecationIssues, indexSettingsChecks, clusterSettingsChecks, mlSettingsChecks);

        if (clusterIssueFound) {
            assertThat(response.getClusterSettingsIssues(), equalTo(Collections.singletonList(foundIssue)));
        } else {
            assertThat(response.getClusterSettingsIssues(), empty());
        }

        if (nodeIssueFound) {
            String details = foundIssue.getDetails() != null ? foundIssue.getDetails() + " " : "";
            DeprecationIssue mergedFoundIssue = new DeprecationIssue(foundIssue.getLevel(), foundIssue.getMessage(), foundIssue.getUrl(),
                details + "(nodes impacted: [" + discoveryNode.getName() + "])");
            assertThat(response.getNodeSettingsIssues(), equalTo(Collections.singletonList(mergedFoundIssue)));
        } else {
            assertTrue(response.getNodeSettingsIssues().isEmpty());
        }

        if (indexIssueFound) {
            assertThat(response.getIndexSettingsIssues(), equalTo(Collections.singletonMap("test",
                Collections.singletonList(foundIssue))));
        } else {
            assertTrue(response.getIndexSettingsIssues().isEmpty());
        }

        if (mlIssueFound) {
            assertThat(response.getMlSettingsIssues(), equalTo(Collections.singletonList(foundIssue)));
        } else {
            assertTrue(response.getMlSettingsIssues().isEmpty());
        }
    }
}
