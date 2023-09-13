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

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.util.Arrays;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@TestCaseOrdering(RollingUpgradeTestOrdering.class)
public abstract class ParameterizedRollingUpgradeTestCase extends ESRestTestCase {
    private static final Version OLD_CLUSTER_VERSION = Version.fromString(System.getProperty("tests.old_cluster_version"));
    private static RollingUpgradeStatus lastUpgradeStatus = RollingUpgradeStatus.OLD;
    private static boolean upgradeFailed = false;
    private final RollingUpgradeStatus requestedUpgradeStatus;

    public ParameterizedRollingUpgradeTestCase(@Name("cluster") RollingUpgradeStatus upgradeStatus) {
        assertThat("Test parameters are trying to downgrade a cluster", upgradeStatus.ordinal(), lessThanOrEqualTo(lastUpgradeStatus.ordinal()));
        this.requestedUpgradeStatus = upgradeStatus;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return Arrays.stream(RollingUpgradeStatus.values()).map(v -> new Object[] { v }).toList();
    }

    @Before
    public void maybeUpgrade() throws Exception {
        // Skip remaining tests if upgrade failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);

        if (lastUpgradeStatus != requestedUpgradeStatus) {
            try {
                getUpgradeCluster().upgradeNodeToVersion();
                closeClients();
                initClient();
            } catch (Exception e) {
                upgradeFailed = true;
                throw e;
            } finally {
                lastUpgradeStatus = requestedUpgradeStatus;
            }
        }
    }

    protected static IndexVersion getOldClusterIndexVersion() {
        var version = OLD_CLUSTER_VERSION;
        if (version.equals(org.elasticsearch.Version.CURRENT)) {
            return IndexVersion.current();
        } else {
            assertThat("Index version needs to be added to rolling test parameters", version, lessThan(org.elasticsearch.Version.V_8_11_0));
            return IndexVersion.fromId(version.id);
        }
    }

    protected abstract ElasticsearchCluster getUpgradeCluster();

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
