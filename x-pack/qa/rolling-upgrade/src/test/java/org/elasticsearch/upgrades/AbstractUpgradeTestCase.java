/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.test.SecuritySettingsSourceField;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractUpgradeTestCase extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "test_user",
        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD)
    );

    protected static final String UPGRADE_FROM_VERSION = System.getProperty("tests.upgrade_from_version");
    protected static final boolean SKIP_ML_TESTS = Booleans.parseBoolean(System.getProperty("tests.ml.skip", "false"));

    protected static boolean isOriginalCluster(String clusterVersion) {
        return UPGRADE_FROM_VERSION.equals(clusterVersion);
    }

    /**
     * Upgrade tests by design are also executed with the same version. We might want to skip some checks if that's the case, see
     * for example gh#39102.
     * @return true if the cluster version is the current version.
     */
    protected static boolean isOriginalClusterCurrent() {
        return UPGRADE_FROM_VERSION.equals(Build.current().version());
    }

    @Deprecated(forRemoval = true)
    @UpdateForV9
    // Tests should be reworked to rely on features from the current cluster (old, mixed or upgraded).
    // Version test against the original cluster will be removed
    protected static boolean isOriginalClusterVersionAtLeast(Version supportedVersion) {
        // Always assume non-semantic versions are OK: this method will be removed in V9, we are testing the pre-upgrade cluster version,
        // and non-semantic versions are always V8+
        return parseLegacyVersion(UPGRADE_FROM_VERSION).map(x -> x.onOrAfter(supportedVersion)).orElse(true);
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

    enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        public static ClusterType parse(String value) {
            return switch (value) {
                case "old_cluster" -> OLD;
                case "mixed_cluster" -> MIXED;
                case "upgraded_cluster" -> UPGRADED;
                default -> throw new AssertionError("unknown cluster type: " + value);
            };
        }
    }

    protected static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.suite"));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)

            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")

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
}
