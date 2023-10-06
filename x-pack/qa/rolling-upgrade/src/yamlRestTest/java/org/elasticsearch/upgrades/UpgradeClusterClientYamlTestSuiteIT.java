/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.upgrades.AbstractUpgradeTestCase.ClusterType;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class UpgradeClusterClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    protected static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));
    protected static ClusterType CLUSTER_TYPE = ClusterType.OLD;

    private static final List<String> SECOND_RUN_ONLY_TESTS = List.of(
        "mixed_cluster/10_basic/Start scroll in mixed cluster on upgraded node that we will continue after upgrade",
        "mixed_cluster/30_ml_jobs_crud/Create a job in the mixed cluster and write some data",
        "mixed_cluster/40_ml_datafeed_crud/Put job and datafeed in mixed cluster",
        "mixed_cluster/40_ml_datafeed_crud/Put job and datafeed without aggs in mixed cluster",
        "mixed_cluster/40_ml_datafeed_crud/Put job and datafeed with aggs in mixed cluster",
        "mixed_cluster/40_ml_datafeed_crud/Put job and datafeed with composite aggs in mixed cluster",
        "mixed_cluster/80_transform_jobs_crud/Test put batch transform on mixed cluster",
        "mixed_cluster/80_transform_jobs_crud/Test put continuous transform on mixed cluster",
        "mixed_cluster/90_ml_data_frame_analytics_crud/Put an outlier_detection job on the mixed cluster",
        "mixed_cluster/110_enrich/Enrich stats query smoke test for mixed cluster",
        "mixed_cluster/120_api_key/Test API key authentication will work in a mixed cluster",
        "mixed_cluster/120_api_key/Create API key with metadata in a mixed cluster",
        "mixed_cluster/130_operator_privileges/Test operator privileges will work in the mixed cluster"
    );
    private static final int NODE_NUM = 3;
    private static final Set<Integer> upgradedNodes = new HashSet<>();
    private static boolean upgradeFailed = false;

    private final int requestedUpgradedNodes;

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(org.elasticsearch.test.cluster.util.Version.fromString(System.getProperty("tests.upgrade_from_version")))
        .nodes(NODE_NUM)
        .systemProperty("ingest.geoip.downloader.enabled.default", "true")
        .systemProperty("ingest.geoip.downloader.endpoint.default", "http://invalid.endpoint")
        .setting("ingest.geoip.downloader.endpoint", () -> "http://invalid.endpoint", s -> s.getVersion().onOrAfter("7.14.0"))
        .setting("repositories.url.allowed_urls", "http://snapshot.test*")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.token.timeout", "60m")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.audit.enabled", "true")
        .setting("xpack.security.transport.ssl.key", "testnode.pem")
        .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
        .setting("xpack.security.authc.realms.file.file1.order", () -> "0", s -> s.getVersion().onOrAfter("7.0.0"))
        .setting("xpack.security.authc.realms.native.native1.order", () -> "1", s -> s.getVersion().onOrAfter("7.0.0"))
        .setting("xpack.security.authc.realms.file1.type", () -> "file", s -> s.getVersion().before("7.0.0"))
        .setting("xpack.security.authc.realms.file1.order", () -> "0", s -> s.getVersion().before("7.0.0"))
        .setting("xpack.security.authc.realms.native1.type", () -> "native", s -> s.getVersion().before("7.0.0"))
        .setting("xpack.security.authc.realms.native1.order", () -> "1", s -> s.getVersion().before("7.0.0"))
        .setting("ccr.auto_follow.wait_for_metadata_timeout", () -> "1s", s -> s.getVersion().onOrAfter("6.6.0"))
        .setting("xpack.security.operator_privileges.enabled", () -> "true", s -> s.getVersion().onOrAfter("7.11.0"))
        .setting("xpack.watcher.encrypt_sensitive_data", "true")
        .setting("logger.org.elasticsearch.xpack.watcher", "DEBUG")
        .setting("xpack.searchable.snapshot.shared_cache.size", () -> "16MB", s -> s.getVersion().onOrAfter("7.12.0"))
        .setting("xpack.searchable.snapshot.shared_cache.region_size", () -> "256KB", s -> s.getVersion().onOrAfter("7.12.0"))
        .configFile("testnode.pem", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
        .configFile("testnode.crt", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
        .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
        .keystore("xpack.watcher.encryption_key", Resource.fromClasspath("system_key"))
        .user("test_user", "x-pack-test-password")
        .user("non_operator", "x-pack-test-password", "superuser", false)
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .build();

    public UpgradeClusterClientYamlTestSuiteIT(
        @Name("upgradedNodes") int requestedUpgradedNodes,
        @Name("test") ClientYamlTestCandidate testCandidate
    ) {
        super(testCandidate);
        this.requestedUpgradedNodes = requestedUpgradedNodes;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> parameters = new ArrayList<>();

        for (int i = 0; i <= NODE_NUM; i++) {
            ClusterType clusterType = i == 0 ? ClusterType.OLD : (i < NODE_NUM ? ClusterType.MIXED : ClusterType.UPGRADED);
            Iterable<Object[]> tests = createParameters(clusterType.name().toLowerCase() + "_cluster");
            for (Object[] test : tests) {
                if (i == 1 && SECOND_RUN_ONLY_TESTS.contains(((ClientYamlTestCandidate) test[0]).getTestPath())) {
                    // Don't bother running these tests twice
                    continue;
                }
                parameters.add(new Object[] { i, test[0] });
            }
        }

        return parameters;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void upgradeNode() throws Exception {
        // Skip remaining tests if upgrade failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);

        if (upgradedNodes.size() < requestedUpgradedNodes) {
            closeClient();
            closeClients();
            // we might be running a specific upgrade test by itself - check previous nodes too
            for (int n = 0; n < requestedUpgradedNodes; n++) {
                if (upgradedNodes.add(n)) {
                    try {
                        logger.info("Upgrading node {} to version {}", n, org.elasticsearch.test.cluster.util.Version.CURRENT);
                        cluster.upgradeNodeToVersion(n, org.elasticsearch.test.cluster.util.Version.CURRENT);
                    } catch (Exception e) {
                        upgradeFailed = true;
                        throw e;
                    }
                }
            }
            CLUSTER_TYPE = upgradedNodes.size() == NODE_NUM ? ClusterType.UPGRADED : ClusterType.MIXED;
            initClient();
            initAndResetContext();
        }

        waitForTemplates();
        waitForWatcher();
    }

    @AfterClass
    public static void resetNodes() {
        CLUSTER_TYPE = ClusterType.OLD;
        upgradedNodes.clear();
        upgradeFailed = false;
    }

    /**
     * Waits for the Machine Learning templates to be created by {@link org.elasticsearch.plugins.MetadataUpgrader}.
     * Only do this on the old cluster.  Users won't necessarily wait for templates to be upgraded during rolling
     * upgrades, so we cannot wait within the test framework, or we could miss production bugs.
     */
    public void waitForTemplates() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            try {
                boolean clusterUnderstandsComposableTemplates = UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_8_0);
                XPackRestTestHelper.waitForTemplates(
                    client(),
                    XPackRestTestConstants.ML_POST_V7120_TEMPLATES,
                    clusterUnderstandsComposableTemplates
                );
            } catch (AssertionError e) {
                throw new AssertionError("Failure in test setup: Failed to initialize ML index templates", e);
            }
        }
    }

    public void waitForWatcher() throws Exception {
        // Wait for watcher to be in started state in order to avoid errors due
        // to manually executing watches prior for watcher to be ready:
        try {
            assertBusy(() -> {
                Response response = client().performRequest(new Request("GET", "_watcher/stats"));
                Map<String, Object> responseBody = entityAsMap(response);
                List<?> stats = (List<?>) responseBody.get("stats");
                assertThat(stats.size(), greaterThanOrEqualTo(3));
                for (Object stat : stats) {
                    Map<?, ?> statAsMap = (Map<?, ?>) stat;
                    assertThat(statAsMap.get("watcher_state"), equalTo("started"));
                }
            }, 1, TimeUnit.MINUTES);
        } catch (AssertionError e) {
            throw new AssertionError("Failure in test setup: Failed to initialize at least 3 watcher nodes", e);
        }
    }

    @Override
    protected boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSearchableSnapshotsIndicesUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString(("test_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            // we increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }
}
