/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.RestTestLegacyFeatures;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.not;

public class SystemIndexMappingUpgradeIT extends ESRestTestCase {

    private static final String OLD_CLUSTER_VERSION = System.getProperty("tests.old_cluster_version");
    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(Version.fromString(OLD_CLUSTER_VERSION))
        .nodes(3)
        .setting("path.repo", new Supplier<>() {
            @Override
            @SuppressForbidden(reason = "TemporaryFolder only has io.File methods, not nio.File")
            public String get() {
                return repoDirectory.getRoot().getPath();
            }
        })
        .setting("xpack.security.enabled", "false")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    private String testRestCluster = cluster.getHttpAddresses();

    @Before
    public void setup() {
        assumeFalse(
            "Testing upgrades from before CompatibilityVersions held mapping versions in cluster state",
            testFeatureService.clusterHasFeature(RestTestLegacyFeatures.SYSTEM_INDICES_MAPPING_VERSION_IN_CLUSTER_STATE.id())
        );
    }

    @Override
    protected String getTestRestCluster() {
        return testRestCluster;
    }

    @Override
    protected final Settings restClientSettings() {
        return Settings.builder()
                .put(super.restClientSettings())
                // increase the timeout here to 90 seconds to handle long waits for a green
                // cluster health. the waits for green need to be longer than a minute to
                // account for delayed shards
                .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
                .build();
    }

    private static String getNodesAttribute(ObjectPath objectPath, String nodeId, String attribute) {
        try {
            return objectPath.evaluate("nodes." + nodeId + "." + attribute);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> getUpgradedNodesAddresses() throws IOException {
        Response response = client().performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        return nodesAsMap.keySet().stream()
            .filter(id -> getNodesAttribute(objectPath, id, "version").equals(Build.current().version())) //nodes on current version
            .map(id -> getNodesAttribute(objectPath, id, "http.publish_address"))
            .toList();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> fieldAsObjectList(Map<? extends String, ?> objectMap, String field) {
        return (List<Map<String, Object>>)(objectMap.get(field));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fieldAsObject(Map<? extends String, ?> objectMap, String field) {
        return (Map<String, Object>)(objectMap.get(field));
    }

    public void testGrowShrinkUpgradeUpdatesSystemIndexMapping() throws IOException {

        // Upgrade node 0 and 1 to the current version, leave node 2 to the BwC version
        logger.info("Upgrading node 0 to version {}", Version.CURRENT);
        cluster.upgradeNodeToVersion(0, Version.CURRENT);

        logger.info("Upgrading node 1 to version {}", Version.CURRENT);
        cluster.upgradeNodeToVersion(1, Version.CURRENT);

        // Query the nodes, ensure we do _not_ have node versions in the answer, or if we do, mappings are empty
        Map<String, Object> nodesVersions = entityAsMap(client().performRequest(new Request("GET", "/_cluster/state")));
        assertThat(
            nodesVersions,
            anyOf(
                not(hasKey("nodes_versions")),
                transformedMatch(
                    x -> fieldAsObjectList(x, "nodes_versions"),
                    everyItem(transformedMatch(item -> fieldAsObject(item,"mappings_versions"), anEmptyMap())))
            )
        );

        var upgradedNodes = getUpgradedNodesAddresses();

        // Stop the last "old" node
        cluster.stopNode(2, false);

        // Ensure we talk only to the 2 live, upgraded nodes
        closeClients();
        testRestCluster = String.join(",", upgradedNodes);
        initClient();

        var newNodesVersions = entityAsMap(client().performRequest(new Request("GET", "/_cluster/state")));
        assertThat(
            newNodesVersions,
            allOf(
                hasKey("nodes_versions"),
                transformedMatch(
                    x -> fieldAsObjectList(x, "nodes_versions"),
                    everyItem(transformedMatch(item -> fieldAsObject(item,"mappings_versions"), not(anEmptyMap()))))
            )
        );
    }
}
