/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;

public class ClusterDeprecationChecksTests extends ESTestCase {

    public void testCheckClusterName() {
        final String badClusterName = randomAlphaOfLengthBetween(0, 10) + ":" + randomAlphaOfLengthBetween(0, 10);
        final ClusterState badClusterState = ClusterState.builder(new ClusterName(badClusterName)).build();

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL, "Cluster name cannot contain ':'",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                "#_literal_literal_is_no_longer_allowed_in_cluster_name",
            "This cluster is named [" + badClusterName + "], which contains the illegal character ':'.");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(badClusterState));
        assertEquals(singletonList(expected), issues);

        final String goodClusterName = randomAlphaOfLengthBetween(1,30);
        final ClusterState goodClusterState = ClusterState.builder(new ClusterName(goodClusterName)).build();
        List<DeprecationIssue> noIssues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(goodClusterState));
        assertTrue(noIssues.isEmpty());
    }

    public void testCheckShardLimit() {
        int shardsPerNode = randomIntBetween(1, 10000);
        int nodeCount = randomIntBetween(1, 10);
        int maxShardsInCluster = shardsPerNode * nodeCount;
        int currentOpenShards = maxShardsInCluster + randomIntBetween(0, 100);

        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < nodeCount; i++) {
            DiscoveryNode discoveryNode = DiscoveryNode.createLocal(Settings.builder().put("node.name", "node_check" + i).build(),
                new TransportAddress(TransportAddress.META_ADDRESS, 9200 + i), "test" + i);
            discoveryNodesBuilder.add(discoveryNode);
        }

        // verify deprecation issue is returned when number of open shards exceeds cluster soft limit
        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(currentOpenShards)
            .numberOfReplicas(0))
            .persistentSettings(settings(Version.CURRENT)
                .put(MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), String.valueOf(shardsPerNode)).build())
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData).nodes(discoveryNodesBuilder).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Number of open shards exceeds cluster soft limit",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html",
            "There are [" + currentOpenShards + "] open shards in this cluster, but the cluster is limited to [" +
                shardsPerNode + "] per data node, for [" + maxShardsInCluster + "] maximum.");
        assertEquals(singletonList(expected), issues);

        // verify no deprecation issues are returned when number of open shards is below the cluster soft limit
        MetaData goodMetaData = MetaData.builder(metaData).put(IndexMetaData.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfReplicas(0)
            .numberOfShards(maxShardsInCluster - randomIntBetween(1, 100))).build();
        ClusterState goodState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(goodMetaData).nodes(state.nodes()).build();
        issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(goodState));
        assertTrue(issues.isEmpty());
    }
}
