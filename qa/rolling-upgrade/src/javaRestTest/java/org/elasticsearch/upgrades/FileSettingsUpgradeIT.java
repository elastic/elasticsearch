/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class FileSettingsUpgradeIT extends ParameterizedRollingUpgradeTestCase {

    private static final RunnableTestRuleAdapter versionLimit = new RunnableTestRuleAdapter(
        () -> assumeTrue("Only valid when upgrading from pre-file settings", getOldClusterTestVersion().before(new Version(8, 4, 0)))
    );

    private static final String settingsJSON = """
        {
             "metadata": {
                 "version": "1",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "cluster_settings": {
                     "indices.recovery.max_bytes_per_sec": "50mb"
                 }
             }
        }""";

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(NODE_NUM)
        .setting("path.repo", new Supplier<>() {
            @Override
            @SuppressForbidden(reason = "TemporaryFolder only has io.File methods, not nio.File")
            public String get() {
                return repoDirectory.getRoot().getPath();
            }
        })
        .setting("xpack.security.enabled", "false")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .configFile("operator/settings.json", Resource.fromString(settingsJSON))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(versionLimit).around(repoDirectory).around(cluster);

    public FileSettingsUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testFileSettingsApplied() throws IOException {
        if (isUpgradedCluster()) {
            // the nodes have all been upgraded. Check they read the file settings ok
            Map<String, Object> response = responseAsMap(adminClient().performRequest(new Request("GET", "/_cluster/settings")));
            assertThat(XContentMapValues.extractValue(response, "persistent", "indices", "recovery", "max_bytes_per_sec"), equalTo("50mb"));
        }
    }
}
