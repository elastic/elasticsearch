/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus.OLD;
import static org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus.UPGRADED;
import static org.hamcrest.Matchers.lessThan;

@TestCaseOrdering(FullClusterRestartTestOrdering.class)
public abstract class ParameterizedFullClusterRestartTestCase extends ESRestTestCase {
    private static final Version MINIMUM_WIRE_COMPATIBLE_VERSION = Version.fromString("7.17.0");
    private static final Version OLD_CLUSTER_VERSION = Version.fromString(System.getProperty("tests.old_cluster_version"));
    private static boolean upgradeFailed = false;
    private static boolean upgraded = false;
    private final FullClusterRestartUpgradeStatus requestedUpgradeStatus;

    public ParameterizedFullClusterRestartTestCase(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        this.requestedUpgradeStatus = upgradeStatus;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return Arrays.stream(FullClusterRestartUpgradeStatus.values()).map(v -> new Object[] { v }).toList();
    }

    @Before
    public void maybeUpgrade() throws Exception {
        if (upgraded == false && requestedUpgradeStatus == UPGRADED) {
            try {
                if (OLD_CLUSTER_VERSION.before(MINIMUM_WIRE_COMPATIBLE_VERSION)) {
                    // First upgrade to latest wire compatible version
                    getUpgradeCluster().upgradeToVersion(MINIMUM_WIRE_COMPATIBLE_VERSION);
                }
                getUpgradeCluster().upgradeToVersion(Version.CURRENT);
                closeClients();
                initClient();
            } catch (Exception e) {
                upgradeFailed = true;
                throw e;
            } finally {
                upgraded = true;
            }
        }

        // Skip remaining tests if upgrade failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);
    }

    @AfterClass
    public static void resetUpgrade() {
        upgraded = false;
        upgradeFailed = false;
    }

    public boolean isRunningAgainstOldCluster() {
        return requestedUpgradeStatus == OLD;
    }

    public static org.elasticsearch.Version getOldClusterVersion() {
        return org.elasticsearch.Version.fromString(OLD_CLUSTER_VERSION.toString());
    }

    public static IndexVersion getOldClusterIndexVersion() {
        var version = getOldClusterVersion();
        assertThat("Index version needs to be added to restart test parameters", version, lessThan(org.elasticsearch.Version.V_8_10_0));
        return IndexVersion.fromId(version.id);
    }

    public static Version getOldClusterTestVersion() {
        return Version.fromString(OLD_CLUSTER_VERSION.toString());
    }

    protected abstract ElasticsearchCluster getUpgradeCluster();

    @Override
    protected String getTestRestCluster() {
        return getUpgradeCluster().getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    protected String getRootTestName() {
        return getTestName().split(" ")[0].toLowerCase(Locale.ROOT);
    }
}
