/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import io.netty.handler.codec.http.HttpMethod;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import static org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.THRESHOLD_SETTING;

/**
 * Tests to run before and after a full cluster restart. This is run twice,
 * one with {@code tests.is_old_cluster} set to {@code true} against a cluster
 * of an older version. The cluster is shutdown and a cluster of the new
 * version is started with the same data directories and then this is rerun
 * with {@code tests.is_old_cluster} set to {@code false}.
 */
public class FullClusterRestartArchivedSettingsIT extends ParameterizedFullClusterRestartTestCase {

    private static TemporaryFolder repoDirectory = new TemporaryFolder();

    protected static LocalClusterConfigProvider clusterConfig = c -> {};

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(Version.fromString(OLD_CLUSTER_VERSION))
        .nodes(2)
        .setting("path.repo", () -> repoDirectory.getRoot().getPath())
        .setting("xpack.security.enabled", "false")
        // some tests rely on the translog not being flushed
        .setting("indices.memory.shard_inactive_time", "60m")
        .apply(() -> clusterConfig)
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    public FullClusterRestartArchivedSettingsIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION) // this test is just about v8->v9 upgrades, remove it in v10
    public void testBalancedShardsAllocatorThreshold() throws Exception {
        assumeTrue("test only applies for v8->v9 upgrades", getOldClusterTestVersion().getMajor() == 8);

        final var chosenValue = randomFrom("0", "0.1", "0.5", "0.999");

        if (isRunningAgainstOldCluster()) {
            final var request = newXContentRequest(
                HttpMethod.PUT,
                "/_cluster/settings",
                (builder, params) -> builder.startObject("persistent").field(THRESHOLD_SETTING.getKey(), chosenValue).endObject()
            );
            request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
            assertOK(client().performRequest(request));
        }

        final var clusterSettingsResponse = ObjectPath.createFromResponse(
            client().performRequest(new Request("GET", "/_cluster/settings"))
        );

        final var settingsPath = "persistent." + THRESHOLD_SETTING.getKey();
        final var settingValue = clusterSettingsResponse.evaluate(settingsPath);

        if (isRunningAgainstOldCluster()) {
            assertEquals(chosenValue, settingValue);
        } else {
            assertNull(settingValue);
            assertNotNull(clusterSettingsResponse.<String>evaluate("persistent.archived." + THRESHOLD_SETTING.getKey()));
        }
    }
}
