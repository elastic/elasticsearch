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

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus.OLD;
import static org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus.UPGRADED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@TestCaseOrdering(FullClusterRestartTestOrdering.class)
public abstract class ParameterizedFullClusterRestartTestCase extends ESRestTestCase {
    private static final Version MINIMUM_WIRE_COMPATIBLE_VERSION = Version.fromString("7.17.0");
    private static final Version OLD_CLUSTER_VERSION = Version.fromString(System.getProperty("tests.old_cluster_version"));
    private static IndexVersion oldIndexVersion;
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
    public void extractOldIndexVersion() throws Exception {
        if (upgraded == false) {
            IndexVersion indexVersion = null;   // these should all be the same version

            Request request = new Request("GET", "_nodes");
            request.addParameter("filter_path", "nodes.*.index_version,nodes.*.name");
            Response response = client().performRequest(request);
            ObjectPath objectPath = ObjectPath.createFromResponse(response);
            Map<String, Object> nodeMap = objectPath.evaluate("nodes");
            for (String id : nodeMap.keySet()) {
                Number ix = objectPath.evaluate("nodes." + id + ".index_version");
                IndexVersion version;
                if (ix != null) {
                    version = IndexVersion.fromId(ix.intValue());
                } else {
                    // it doesn't have index version (pre 8.11) - just infer it from the release version
                    version = IndexVersion.fromId(getOldClusterVersion().id);
                }

                if (indexVersion == null) {
                    indexVersion = version;
                } else {
                    String name = objectPath.evaluate("nodes." + id + ".name");
                    assertThat("Node " + name + " has a different index version to other nodes", version, equalTo(indexVersion));
                }
            }

            assertThat("Index version could not be read", indexVersion, notNullValue());
            oldIndexVersion = indexVersion;
        }
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
        assert oldIndexVersion != null;
        return oldIndexVersion;
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
