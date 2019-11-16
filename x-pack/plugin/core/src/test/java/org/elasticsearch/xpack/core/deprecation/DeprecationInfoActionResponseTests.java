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
import org.elasticsearch.xpack.core.security.action.role.GetFileRolesAction;
import org.elasticsearch.xpack.core.security.action.role.GetFileRolesResponse;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
            .putMapping("testUnderscoreAll", Strings.toString(mapping))
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
        boolean rolesIssueFound = randomBoolean();
        DeprecationIssue foundIssue = DeprecationIssueTests.createTestInstance();
        // We need to be able to differentiate the roles issue, because it gets merged in with the cluster and/or node issues
        DeprecationIssue rolesIssue = randomValueOtherThan(foundIssue , DeprecationIssueTests::createTestInstance);
        List<Function<ClusterState, DeprecationIssue>> clusterSettingsChecks = List.of((s) -> clusterIssueFound ? foundIssue : null);
        List<Function<IndexMetaData, DeprecationIssue>> indexSettingsChecks = List.of((idx) -> indexIssueFound ? foundIssue : null);
        List<BiFunction<DatafeedConfig, NamedXContentRegistry, DeprecationIssue>> mlSettingsChecks =
                List.of((idx, unused) -> mlIssueFound ? foundIssue : null);
        List<Function<List<RoleDescriptor>, DeprecationIssue>> rolesChecks = List.of(roles -> rolesIssueFound ? rolesIssue : null);

        NodesDeprecationCheckResponse nodeDeprecationIssues = new NodesDeprecationCheckResponse(
            new ClusterName(randomAlphaOfLength(5)),
            nodeIssueFound
                ? Collections.singletonList(
                    new NodesDeprecationCheckAction.NodeResponse(discoveryNode, Collections.singletonList(foundIssue)))
                : emptyList(),
            emptyList());

        GetRolesResponse rolesResponse = new GetRolesResponse(new RoleDescriptor(randomAlphaOfLength(10), null, null, null));
        GetFileRolesResponse nodeRolesResponse = new GetFileRolesResponse(new ClusterName(randomAlphaOfLength(5)),
            rolesIssueFound
                ? Collections.singletonList(
                    new GetFileRolesAction.NodeResponse(discoveryNode,
                        Collections.singletonList(new RoleDescriptor(randomAlphaOfLength(10), null, null, null))))
                : emptyList(),
            emptyList());

        // Fix this this test vvv
        DeprecationInfoAction.Response response = DeprecationInfoAction.Response.from(state, NamedXContentRegistry.EMPTY,
            resolver, Strings.EMPTY_ARRAY, indicesOptions, datafeeds, rolesResponse, nodeRolesResponse, indexSettingsChecks,
            clusterSettingsChecks, mlSettingsChecks, rolesChecks, nodeDeprecationIssues);

        List<DeprecationIssue> expectedClusterIssues = new ArrayList<>();
        if (clusterIssueFound) {
            expectedClusterIssues.add(foundIssue);
        }
        if (rolesIssueFound) {
            expectedClusterIssues.add(rolesIssue);
        }
        assertThat(response.getClusterSettingsIssues(), containsInAnyOrder(expectedClusterIssues.toArray()));

        List<DeprecationIssue> expectedNodeIssues = new ArrayList<>();
        if (nodeIssueFound) {
            String details = foundIssue.getDetails() != null ? foundIssue.getDetails() + " " : "";
            expectedNodeIssues.add(new DeprecationIssue(foundIssue.getLevel(), foundIssue.getMessage(), foundIssue.getUrl(),
                details + "(nodes impacted: [" + discoveryNode.getName() + "])"));
        }
        if (rolesIssueFound) {
            String rolesIssueDetails = rolesIssue.getDetails() != null ? rolesIssue.getDetails() + " " : "";
            expectedNodeIssues.add(new DeprecationIssue(rolesIssue.getLevel(), rolesIssue.getMessage(), rolesIssue.getUrl(),
                rolesIssueDetails + "(nodes impacted: [" + discoveryNode.getName() + "])"));
        }
        assertThat(response.getNodeSettingsIssues(), containsInAnyOrder(expectedNodeIssues.toArray()));

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
