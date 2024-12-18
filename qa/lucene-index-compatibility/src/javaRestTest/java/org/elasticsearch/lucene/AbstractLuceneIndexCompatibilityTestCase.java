/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.Comparator;
import java.util.Locale;
import java.util.stream.Stream;

import static org.elasticsearch.test.cluster.util.Version.CURRENT;
import static org.elasticsearch.test.cluster.util.Version.fromString;
import static org.elasticsearch.test.rest.ObjectPath.createFromResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test suite for Lucene indices backward compatibility with N-2 versions. The test suite creates a cluster in N-2 version, then upgrades it
 * to N-1 version and finally upgrades it to the current version. Test methods are executed after each upgrade.
 */
@TestCaseOrdering(AbstractLuceneIndexCompatibilityTestCase.TestCaseOrdering.class)
public abstract class AbstractLuceneIndexCompatibilityTestCase extends ESRestTestCase {

    protected static final Version VERSION_MINUS_2 = fromString(System.getProperty("tests.minimum.index.compatible"));
    protected static final Version VERSION_MINUS_1 = fromString(System.getProperty("tests.minimum.wire.compatible"));
    protected static final Version VERSION_CURRENT = CURRENT;

    protected static TemporaryFolder REPOSITORY_PATH = new TemporaryFolder();

    protected static LocalClusterConfigProvider clusterConfig = c -> {};
    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(VERSION_MINUS_2)
        .nodes(2)
        .setting("path.repo", () -> REPOSITORY_PATH.getRoot().getPath())
        .setting("xpack.security.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("path.repo", () -> REPOSITORY_PATH.getRoot().getPath())
        .apply(() -> clusterConfig)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(REPOSITORY_PATH).around(cluster);

    private static boolean upgradeFailed = false;

    private final Version clusterVersion;

    public AbstractLuceneIndexCompatibilityTestCase(@Name("cluster") Version clusterVersion) {
        this.clusterVersion = clusterVersion;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Stream.of(VERSION_MINUS_2, VERSION_MINUS_1, CURRENT).map(v -> new Object[] { v }).toList();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Before
    public void maybeUpgrade() throws Exception {
        // We want to use this test suite for the V9 upgrade, but we are not fully committed to necessarily having N-2 support
        // in V10, so we add a check here to ensure we'll revisit this decision once V10 exists.
        assertThat("Explicit check that N-2 version is Elasticsearch 7", VERSION_MINUS_2.getMajor(), equalTo(7));

        var currentVersion = clusterVersion();
        if (currentVersion.before(clusterVersion)) {
            try {
                cluster.upgradeToVersion(clusterVersion);
                closeClients();
                initClient();
            } catch (Exception e) {
                upgradeFailed = true;
                throw e;
            }
        }

        // Skip remaining tests if upgrade failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);
    }

    protected String suffix(String name) {
        return name + '-' + getTestName().split(" ")[0].toLowerCase(Locale.ROOT);
    }

    protected static Version clusterVersion() throws Exception {
        var response = assertOK(client().performRequest(new Request("GET", "/")));
        var responseBody = createFromResponse(response);
        var version = Version.fromString(responseBody.evaluate("version.number").toString());
        assertThat("Failed to retrieve cluster version", version, notNullValue());
        return version;
    }

    protected static Version indexLuceneVersion(String indexName) throws Exception {
        var response = assertOK(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
        int id = Integer.parseInt(createFromResponse(response).evaluate(indexName + ".settings.index.version.created"));
        return new Version((byte) ((id / 1000000) % 100), (byte) ((id / 10000) % 100), (byte) ((id / 100) % 100));
    }

    /**
     * Execute the test suite with the parameters provided by the {@link #parameters()} in version order.
     */
    public static class TestCaseOrdering implements Comparator<TestMethodAndParams> {
        @Override
        public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
            var version1 = (Version) o1.getInstanceArguments().get(0);
            var version2 = (Version) o2.getInstanceArguments().get(0);
            return version1.compareTo(version2);
        }
    }
}
