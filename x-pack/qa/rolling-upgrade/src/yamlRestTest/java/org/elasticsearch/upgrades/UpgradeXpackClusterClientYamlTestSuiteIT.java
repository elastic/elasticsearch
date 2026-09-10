/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ParameterizedYamlRollingUpgradeTestCase;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.RollingUpgradePerformer.getNewClusterVersion;
import static org.elasticsearch.test.RollingUpgradePerformer.getOldClusterVersion;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class UpgradeXpackClusterClientYamlTestSuiteIT extends ParameterizedYamlRollingUpgradeTestCase {

    private static final String USER = "test_user";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static final ElasticsearchCluster cluster = buildCluster();

    private static ElasticsearchCluster buildCluster() {
        String oldClusterVersion = getOldClusterVersion();
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(oldClusterVersion, isOldClusterDetachedVersion())
            .nodes(NODE_NUM)
            .node(0, node -> node.name("node-0"))
            .node(1, node -> node.name("node-1"))
            .node(2, node -> node.name("node-2"))
            // Mark each node with node.attr.upgraded=true as soon as it is restarted on the new version.
            // Some of the tests use "index.routing.allocation.include.upgraded: true" to pin shards to already-upgraded nodes
            .setting("node.attr.upgraded", () -> "true", node -> {
                var version = node.getVersion();
                if (version.after(oldClusterVersion)) {
                    return true;
                }
                // special case: oldClusterVersion == newClusterVersion
                if (version.equals(Version.fromString(getNewClusterVersion()))) {
                    // only set that for the first node, one that is sure to be upgraded first
                    return node.getName().equals("node-0");
                }
                return false;
            })
            .setting("repositories.url.allowed_urls", "http://snapshot.test*")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.security.autoconfiguration.enabled", "false")
            .setting("xpack.security.transport.ssl.enabled", "true")
            .setting("xpack.security.transport.ssl.key", "testnode.pem")
            .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .setting("xpack.security.authc.token.timeout", "60m")
            .setting("xpack.security.authc.api_key.enabled", "true")
            .setting("xpack.security.audit.enabled", "true")
            .setting("xpack.security.authc.realms.file.file1.order", "0")
            .setting("xpack.security.authc.realms.native.native1.order", "1")
            .setting("xpack.security.operator_privileges.enabled", "true")
            .setting("xpack.watcher.encrypt_sensitive_data", "true")
            .setting("logger.org.elasticsearch.xpack.watcher", "DEBUG")
            .user(USER, PASS)
            .user("non_operator", PASS, "superuser", false)
            .configFile("testnode.pem", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .configFile("testnode.crt", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
            .keystore("xpack.watcher.encryption_key", Resource.fromClasspath("system_key"))
            .build();
    }

    public UpgradeXpackClusterClientYamlTestSuiteIT(
        @Name("upgradedNodes") int upgradedNodes,
        @Name("yaml") ClientYamlTestCandidate testCandidate
    ) {
        super(upgradedNodes, testCandidate);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    /**
     * Waits for the Machine Learning templates to be created by {@link org.elasticsearch.plugins.MetadataUpgrader}.
     * Only do this on the old cluster. Users won't necessarily wait for templates to be upgraded during rolling
     * upgrades, so we cannot wait within the test framework, or we could miss production bugs.
     */
    @Before
    public void waitForTemplates() throws Exception {
        if (rollingUpgrade.isOldCluster()) {
            try {
                XPackRestTestHelper.waitForTemplates(client(), XPackRestTestConstants.ML_POST_V7120_TEMPLATES);
            } catch (AssertionError e) {
                throw new AssertionError("Failure in test setup: Failed to initialize ML index templates", e);
            }
        }
    }

    @Before
    public void waitForWatcher() throws Exception {
        // Wait for watcher to be in started state in order to avoid errors due
        // to manually executing watches prior to watcher being ready:
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
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
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
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
