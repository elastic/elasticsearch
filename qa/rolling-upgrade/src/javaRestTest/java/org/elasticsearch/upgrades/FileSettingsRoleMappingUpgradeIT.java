/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FileSettingsRoleMappingUpgradeIT extends ParameterizedRollingUpgradeTestCase {

    private static final String settingsJSON = """
        {
             "metadata": {
                 "version": "1",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "role_mappings": {
                       "everyone_kibana": {
                          "enabled": true,
                          "roles": [ "kibana_user" ],
                          "rules": { "field": { "username": "*" } }
                       }
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
        .setting("xpack.security.enabled", "true")
        // workaround to avoid having to set up clients and authorization headers
        .setting("xpack.security.authc.anonymous.roles", "superuser")
        .configFile("operator/settings.json", Resource.fromString(settingsJSON))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    public FileSettingsRoleMappingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    @Before
    public void checkVersions() {
        assumeTrue(
            "Only relevant when upgrading from a version before role mappings were stored in cluster state",
            oldClusterHasFeature("gte_v8.4.0") && oldClusterHasFeature("gte_v8.15.0") == false
        );
    }

    public void testRoleMappingsAppliedOnUpgrade() throws IOException {
        if (isOldCluster()) {
            Request clusterStateRequest = new Request("GET", "/_cluster/state/metadata");
            List<Object> roleMappings = new XContentTestUtils.JsonMapView(entityAsMap(client().performRequest(clusterStateRequest))).get(
                "metadata.role_mappings.role_mappings"
            );
            assertThat(roleMappings, is(nullValue()));
        } else if (isUpgradedCluster()) {
            // the nodes have all been upgraded. Check they re-processed the role mappings in the settings file on
            // upgrade
            Request clusterStateRequest = new Request("GET", "/_cluster/state/metadata");
            List<Object> clusterStateRoleMappings = new XContentTestUtils.JsonMapView(
                entityAsMap(client().performRequest(clusterStateRequest))
            ).get("metadata.role_mappings.role_mappings");
            assertThat(clusterStateRoleMappings, is(not(nullValue())));
            assertThat(clusterStateRoleMappings.size(), equalTo(1));

            assertThat(
                entityAsMap(client().performRequest(new Request("GET", "/_security/role_mapping"))).keySet(),
                // TODO change this to `contains` once the clean-up migration work is merged
                hasItem("everyone_kibana-read-only-operator-mapping")
            );
        }
    }
}
