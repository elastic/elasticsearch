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

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FileSettingsRoleMappingUpgradeIT extends ParameterizedRollingUpgradeTestCase {

    private static final int ROLE_MAPPINGS_CLEANUP_MIGRATION_VERSION = 2;
    private static final String SETTING_JSON = """
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
        .configFile("operator/settings.json", Resource.fromString(SETTING_JSON))
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

    private static void waitForSecurityMigrationCompletionIfIndexExists() throws Exception {
        final Request request = new Request("GET", "_cluster/state/metadata/.security-7");
        assertBusy(() -> {
            Map<String, Object> indices = new XContentTestUtils.JsonMapView(entityAsMap(client().performRequest(request))).get(
                "metadata.indices"
            );
            assertNotNull(indices);
            // If the security index exists, migration needs to happen. There is a bug in pre cluster state role mappings code that tries
            // to write file based role mappings before security index manager state is recovered, this makes it look like the security
            // index is outdated (isIndexUpToDate == false). Because we can't rely on the index being there for old versions, this check
            // is needed.
            if (indices.containsKey(".security-7")) {
                // JsonMapView doesn't support . prefixed indices (splits on .)
                @SuppressWarnings("unchecked")
                String responseVersion = new XContentTestUtils.JsonMapView((Map<String, Object>) indices.get(".security-7")).get(
                    "migration_version.version"
                );
                assertNotNull(responseVersion);
                assertTrue(Integer.parseInt(responseVersion) >= ROLE_MAPPINGS_CLEANUP_MIGRATION_VERSION);
            }
        });
    }

    public void testRoleMappingsAppliedOnUpgrade() throws Exception {
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
            waitForSecurityMigrationCompletionIfIndexExists();
            assertThat(
                entityAsMap(client().performRequest(new Request("GET", "/_security/role_mapping"))).keySet(),
                contains("everyone_kibana-read-only-operator-mapping")
            );
        }
    }
}
