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
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class ParameterizedRollingUpgradeTestCase extends ESRestTestCase {
    private static final Version OLD_CLUSTER_VERSION = Version.fromString(System.getProperty("tests.old_cluster_version"));
    private static int totalNodes = -1;
    private static final Set<Integer> upgradedNodes = new HashSet<>();
    private static boolean upgradeFailed = false;
    private static IndexVersion oldIndexVersion;
    private final int requestedUpgradeNode;

    protected ParameterizedRollingUpgradeTestCase(@Name("node") UpgradeNode upgradeNode) {
        if (totalNodes == -1) {
            totalNodes = upgradeNode.totalNodes;
        } else {
            assertThat("The total number of nodes has changed", upgradeNode.totalNodes, equalTo(totalNodes));
        }
        this.requestedUpgradeNode = upgradeNode.node;
    }

    public record UpgradeNode(int node, int totalNodes) {}

    protected static Iterable<Object[]> testNodes(int numNodes) {
        return IntStream.rangeClosed(0, numNodes).mapToObj(n -> new Object[] { new UpgradeNode(n, numNodes)}).toList();
    }

    @Before
    public void extractOldIndexVersion() throws Exception {
        if (upgradedNodes.isEmpty()) {
            IndexVersion indexVersion = null;   // these should all be the same version

            Response response = client().performRequest(new Request("GET", "_nodes"));
            ObjectPath objectPath = ObjectPath.createFromResponse(response);
            Map<String, Object> nodeMap = objectPath.evaluate("nodes");
            for (String id : nodeMap.keySet()) {
                String name = objectPath.evaluate("nodes." + id + ".name");

                Number ix = objectPath.evaluate("nodes." + id + ".index_version");
                IndexVersion version;
                if (ix != null) {
                    version = IndexVersion.fromId(ix.intValue());
                } else {
                    String ver = objectPath.evaluate("nodes." + id + ".version");
                    version = IndexVersion.fromId(org.elasticsearch.Version.fromString(ver).id);
                }

                if (indexVersion == null) {
                    indexVersion = version;
                } else {
                    assertThat("Node " + name + " has a different index version to other nodes", version, equalTo(indexVersion));
                }
            }

            assertThat("Index version could not be read", indexVersion, notNullValue());
            oldIndexVersion = indexVersion;
        }
    }

    @Before
    public void maybeUpgrade() throws Exception {
        // Skip remaining tests if upgrade failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);

        if (upgradedNodes.add(requestedUpgradeNode)) {
            try {
                getUpgradeCluster().upgradeNodeToVersion(requestedUpgradeNode, Version.CURRENT);
                closeClients();
                initClient();
            } catch (Exception e) {
                upgradeFailed = true;
                throw e;
            }
        }
    }

    @AfterClass
    public static void resetNodes() {
        totalNodes = -1;
        upgradedNodes.clear();
        upgradeFailed = false;
    }

    protected static org.elasticsearch.Version getOldClusterVersion() {
        return org.elasticsearch.Version.fromString(OLD_CLUSTER_VERSION.toString());
    }

    protected static IndexVersion getOldClusterIndexVersion() {
        assert oldIndexVersion != null;
        return oldIndexVersion;
    }

    protected static Version getOldClusterTestVersion() {
        return Version.fromString(OLD_CLUSTER_VERSION.toString());
    }

    protected abstract ElasticsearchCluster getUpgradeCluster();

    protected boolean isOldCluster() {
        return upgradedNodes.isEmpty();
    }

    protected boolean isMixedCluster() {
        return upgradedNodes.isEmpty() == false && upgradedNodes.size() < totalNodes;
    }

    protected boolean isUpgradedCluster() {
        return upgradedNodes.size() == totalNodes;
    }

    @Override
    protected String getTestRestCluster() {
        return getUpgradeCluster().getHttpAddresses();
    }

    @Override
    protected final boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected final boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
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

    @Override
    protected final String getEnsureGreenTimeout() {
        // increase the timeout here to 70 seconds to handle long waits for a green
        // cluster health. the waits for green need to be longer than a minute to
        // account for delayed shards
        return "70s";
    }
}
