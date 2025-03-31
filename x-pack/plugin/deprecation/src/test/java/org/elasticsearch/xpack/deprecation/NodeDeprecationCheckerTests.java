/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.deprecation.DeprecationInfoActionResponseTests.createTestDeprecationIssue;
import static org.hamcrest.core.IsEqual.equalTo;

public class NodeDeprecationCheckerTests extends ESTestCase {

    public void testMergingNodeIssues() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_all");
        mapping.field("enabled", false);
        mapping.endObject().endObject();

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
        Map<String, Object> metaMap1 = DeprecationIssue.createMetaMapForRemovableSettings(List.of("setting.1", "setting.2", "setting.3"));
        Map<String, Object> metaMap2 = DeprecationIssue.createMetaMapForRemovableSettings(List.of("setting.2", "setting.3"));
        DeprecationIssue foundIssue1 = createTestDeprecationIssue(metaMap1);
        DeprecationIssue foundIssue2 = createTestDeprecationIssue(foundIssue1, metaMap2);

        NodesDeprecationCheckResponse nodeDeprecationIssues = new NodesDeprecationCheckResponse(
            new ClusterName(randomAlphaOfLength(5)),
            Arrays.asList(
                new NodesDeprecationCheckAction.NodeResponse(node1, List.of(foundIssue1)),
                new NodesDeprecationCheckAction.NodeResponse(node2, List.of(foundIssue2))
            ),
            List.of()
        );

        List<DeprecationIssue> result = NodeDeprecationChecker.reduceToDeprecationIssues(nodeDeprecationIssues);

        String details = foundIssue1.getDetails() != null ? foundIssue1.getDetails() + " " : "";
        DeprecationIssue mergedFoundIssue = new DeprecationIssue(
            foundIssue1.getLevel(),
            foundIssue1.getMessage(),
            foundIssue1.getUrl(),
            details + "(nodes impacted: [" + node1.getName() + ", " + node2.getName() + "])",
            foundIssue1.isResolveDuringRollingUpgrade(),
            foundIssue2.getMeta()
        );
        assertThat(result, equalTo(List.of(mergedFoundIssue)));
    }
}
