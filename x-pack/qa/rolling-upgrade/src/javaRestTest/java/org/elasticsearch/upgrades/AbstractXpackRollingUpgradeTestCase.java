/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractXpackRollingUpgradeTestCase extends ParameterizedRollingUpgradeTestCase {

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = XpackRollingUpgradeClusterConfig.buildCluster(
        getOldClusterVersion(),
        isOldClusterDetachedVersion(),
        repoDirectory
    );

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    protected AbstractXpackRollingUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(super.restClientSettings())
            .put(
                ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("test_user", new SecureString("x-pack-test-password".toCharArray()))
            )
            .build();
    }

    protected static boolean isOriginalCluster(String clusterVersion) {
        return getOldClusterVersion().equals(clusterVersion);
    }

    /**
     * Upgrade tests by design are also executed with the same version. We might want to skip some checks if that's the case, see
     * for example gh#39102.
     * @return true if the cluster version is the current version.
     */
    protected static boolean isOriginalClusterCurrent() {
        return getOldClusterVersion().equals(Build.current().version());
    }

    /**
     * ML tests may need to be skipped on hosts where the old cluster version and the installed glibc are incompatible,
     * see {@code BwcVersions#isMlCompatible} in the build logic. The build wires this into the {@code tests.ml.skip}
     * system property per BWC version.
     */
    protected static boolean skipMlTests() {
        return Booleans.parseBoolean(System.getProperty("tests.ml.skip", "false"));
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
}
