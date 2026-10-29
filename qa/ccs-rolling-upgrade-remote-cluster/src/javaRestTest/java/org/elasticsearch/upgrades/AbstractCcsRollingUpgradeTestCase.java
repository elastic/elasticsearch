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
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Base class for the CCS rolling-upgrade suites in this project.
 * <p>
 * The local (querying) cluster consists of 2 nodes, one on the old version and one on the current version, and is
 * never upgraded during the suite. The remote cluster consists of 3 nodes, all initially on the old version, and is
 * rolled forward one node at a time across upgrade phases. Only node-0 and node-2 of the remote cluster have the
 * {@code gateway} node attribute and can accept remote connections; this creates a scenario where a query request
 * and its fetch requests can be routed through proxy nodes running different versions.
 * <p>
 * Each suite is run once per upgrade phase, from a fully old remote cluster through to a fully upgraded one, so a
 * project hosting such a suite must opt out of smart retry's individual test pruning in its {@code build.gradle}:
 * <pre>{@code smartRetry.pruneIndividualTests.set(false)}</pre>
 */
public abstract class AbstractCcsRollingUpgradeTestCase extends ESRestTestCase {

    protected static final int REMOTE_NODE_NUM = 3;

    private static final String OLD_CLUSTER_VERSION = System.getProperty("tests.upgrade_from_version");
    private static final Set<Integer> upgradedRemoteNodes = new HashSet<>();

    private static final ElasticsearchCluster remoteCluster = ElasticsearchCluster.local()
        .name("remote")
        // Nodes here start on a prior Elasticsearch version, which requires the default distribution type.
        .distribution(DistributionType.DEFAULT)
        .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
        .nodes(REMOTE_NODE_NUM)
        .setting("xpack.security.enabled", "false")
        .node(0, spec -> spec.setting("node.attr.gateway", "true"))
        .node(REMOTE_NODE_NUM - 1, spec -> spec.setting("node.attr.gateway", "true"))
        .build();

    private static final ElasticsearchCluster localCluster = ElasticsearchCluster.local()
        .name("local")
        // Node 0 starts on a prior Elasticsearch version, which requires the default distribution type.
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .setting("cluster.remote.node.attr", "gateway")
        .setting("xpack.security.enabled", "false")
        .node(0, spec -> spec.version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion()))
        .build();

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private final int requestedUpgradedNodes;

    protected AbstractCcsRollingUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes) {
        this.requestedUpgradedNodes = upgradedNodes;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return IntStream.rangeClosed(0, REMOTE_NODE_NUM).boxed().map(n -> new Object[] { n }).toList();
    }

    @Before
    public void upgradeRemoteNode() {
        if (upgradedRemoteNodes.size() < requestedUpgradedNodes) {
            for (int n = 0; n < requestedUpgradedNodes; n++) {
                if (upgradedRemoteNodes.add(n)) {
                    logger.info("Upgrading remote node {} to version {}", n, Version.CURRENT);
                    remoteCluster.upgradeNodeToVersion(n, Version.CURRENT);
                }
            }
        }
    }

    @AfterClass
    public static void resetUpgradedNodes() {
        upgradedRemoteNodes.clear();
    }

    protected static String getOldClusterVersion() {
        return OLD_CLUSTER_VERSION;
    }

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    protected static String getRemoteClusterRestAddresses() {
        return remoteCluster.getHttpAddresses();
    }

    @Override
    protected final boolean preserveClusterUponCompletion() {
        return true;
    }
}
