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
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus.OLD;
import static org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus.UPGRADED;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@TestCaseOrdering(FullClusterRestartTestOrdering.class)
public abstract class ParameterizedFullClusterRestartTestCase extends ESRestTestCase {
    private static final Version OLD_CLUSTER_VERSION = Version.fromString(System.getProperty("tests.old_cluster_version"));
    private static boolean upgradeFailed = false;
    private static boolean upgraded = false;
    private final FullClusterRestartUpgradeStatus requestedUpgradeStatus;

    public ParameterizedFullClusterRestartTestCase(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        this.requestedUpgradeStatus = upgradeStatus;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return Arrays.stream(FullClusterRestartUpgradeStatus.values()).map(v -> new Object[] { v }).collect(Collectors.toList());
    }

    @Before
    public void maybeUpgrade() throws Exception {
        if (upgraded == false && requestedUpgradeStatus == UPGRADED) {
            try {
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

    @Before
    public void init() throws IOException {
        assertThat(
            "we don't need this branch if we aren't compatible with 6.0",
            org.elasticsearch.Version.CURRENT.minimumIndexCompatibilityVersion().onOrBefore(org.elasticsearch.Version.V_6_0_0),
            equalTo(true)
        );
        if (isRunningAgainstOldCluster() && getOldClusterVersion().before(org.elasticsearch.Version.V_7_0_0)) {
            XContentBuilder template = jsonBuilder();
            template.startObject();
            {
                template.field("index_patterns", "*");
                template.field("order", "0");
                template.startObject("settings");
                template.field("number_of_shards", 5);
                template.endObject();
            }
            template.endObject();
            Request createTemplate = new Request("PUT", "/_template/template");
            createTemplate.setJsonEntity(Strings.toString(template));
            client().performRequest(createTemplate);
        }
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

    public static Version getOldClusterTestVersion() {
        return Version.fromString(OLD_CLUSTER_VERSION.toString());
    }

    /**
     * @return true if test is running against an old cluster before that last major, in this case
     * when System.getProperty("tests.is_old_cluster" == true) and oldClusterVersion is before {@link org.elasticsearch.Version#V_7_0_0}
     */
    protected final boolean isRunningAgainstAncientCluster() {
        return isRunningAgainstOldCluster() && getOldClusterVersion().before(org.elasticsearch.Version.V_7_0_0);
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
