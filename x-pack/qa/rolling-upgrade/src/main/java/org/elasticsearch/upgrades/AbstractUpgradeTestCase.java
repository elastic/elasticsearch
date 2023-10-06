/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.test.SecuritySettingsSourceField;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class AbstractUpgradeTestCase extends ESRestTestCase {
    protected static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));
    protected static final boolean SKIP_ML_TESTS = Booleans.parseBoolean(System.getProperty("tests.ml.skip", "false"));
    protected static ClusterType CLUSTER_TYPE = ClusterType.OLD;

    private static final String BASIC_AUTH_VALUE = ESRestTestCase.basicAuthHeaderValue(
        "test_user",
        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD)
    );
    private static final int NODE_NUM = 3;
    private static final Set<Integer> upgradedNodes = new HashSet<>();
    private static boolean upgradeFailed = false;

    private final int requestedUpgradedNodes;

    protected static final TemporaryFolder repo = new TemporaryFolder();

    protected static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(org.elasticsearch.test.cluster.util.Version.fromString(UPGRADE_FROM_VERSION.toString()))
        .nodes(NODE_NUM)
        .systemProperty("ingest.geoip.downloader.enabled.default", "true")
        .systemProperty("ingest.geoip.downloader.endpoint.default", "http://invalid.endpoint")
        .setting("path.repo", () -> repo.getRoot().getPath())
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

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repo).around(cluster);

    protected AbstractUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes) {
        this.requestedUpgradedNodes = upgradedNodes;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return IntStream.rangeClosed(0, NODE_NUM).boxed().map(n -> new Object[] { n }).toList();
    }

    @Before
    public void upgradeNode() throws Exception {
        // Skip remaining tests if upgrade failed
        assumeFalse("Cluster upgrade failed", upgradeFailed);

        if (upgradedNodes.size() < requestedUpgradedNodes) {
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
        }
    }

    @AfterClass
    public static void resetNodes() {
        CLUSTER_TYPE = ClusterType.OLD;
        upgradedNodes.clear();
        upgradeFailed = false;
    }

    protected boolean isFirstRound() {
        return CLUSTER_TYPE == ClusterType.MIXED && upgradedNodes.size() == 1;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
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
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
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
    protected boolean preserveSearchableSnapshotsIndicesUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)

            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(CLIENT_SOCKET_TIMEOUT, "90s")

            .build();
    }

    protected Collection<String> templatesToWaitFor() {
        return Collections.emptyList();
    }

    @Before
    public void setupForTests() throws Exception {
        final Collection<String> expectedTemplates = templatesToWaitFor();

        if (expectedTemplates.isEmpty()) {
            return;
        }
        assertBusy(() -> {
            final Request catRequest = new Request("GET", "_cat/templates?h=n&s=n");
            final Response catResponse = adminClient().performRequest(catRequest);

            final List<String> templates = Streams.readAllLines(catResponse.getEntity().getContent());

            final List<String> missingTemplates = expectedTemplates.stream()
                .filter(each -> templates.contains(each) == false)
                .collect(Collectors.toList());

            // While it's possible to use a Hamcrest matcher for this, the failure is much less legible.
            if (missingTemplates.isEmpty() == false) {
                fail("Some expected templates are missing: " + missingTemplates + ". The templates that exist are: " + templates + "");
            }
        });
    }

    public enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;
    }
}
