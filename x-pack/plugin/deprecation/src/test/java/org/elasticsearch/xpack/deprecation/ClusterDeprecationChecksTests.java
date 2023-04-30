/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.hamcrest.Matchers.equalTo;

public class ClusterDeprecationChecksTests extends ESTestCase {
    public void testCheckShards() {
        /*
         * This test sets the number of allowed shards per node to 5 and creates 2 nodes. So we have room for 10 shards, which is the
         * number of shards that checkShards() is making sure we can add. The first time there are no indices, so the check passes. The
         * next time there is an index with one shard and one replica, leaving room for 8 shards. So the check fails.
         */
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .persistentSettings(Settings.builder().put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 5).build())
                    .build()
            )
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode(UUID.randomUUID().toString(), buildNewFakeTransportAddress(), Version.CURRENT))
                    .add(new DiscoveryNode(UUID.randomUUID().toString(), buildNewFakeTransportAddress(), Version.CURRENT))
            )
            .build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));
        assertThat(0, equalTo(issues.size()));

        final ClusterState stateWithProblems = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .persistentSettings(Settings.builder().put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 4).build())
                    .put(
                        IndexMetadata.builder(randomAlphaOfLength(10))
                            .settings(settings(Version.CURRENT).put(DataTier.TIER_PREFERENCE_SETTING.getKey(), "  "))
                            .numberOfShards(1)
                            .numberOfReplicas(1)
                            .build(),
                        false
                    )
                    .build()
            )
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode(UUID.randomUUID().toString(), buildNewFakeTransportAddress(), Version.CURRENT))
                    .add(new DiscoveryNode(UUID.randomUUID().toString(), buildNewFakeTransportAddress(), Version.CURRENT))
            )
            .build();

        issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(stateWithProblems));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "The cluster has too many shards to be able to upgrade",
            "https://ela.st/es-deprecation-8-shard-limit",
            "Upgrading requires adding a small number of new shards. There is not enough room for 10 more shards. Increase the cluster"
                + ".max_shards_per_node setting, or remove indices to clear up resources.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }
}
