/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * TODO: this is a placeholder recovered from git history as a starting point for restoring this suite onto
 * {@link AbstractXPackYamlRollingUpgradeTestCase}. It is not yet wired up to select the right YAML resource
 * directory (old_cluster/mixed_cluster/upgraded_cluster) for the requested {@code upgradedNodes} count - it will
 * be rewritten to build the proper cross product of {@code upgradedNodes} x YAML test candidate.
 */
public class UpgradeClusterClientYamlTestSuiteIT extends AbstractXPackYamlRollingUpgradeTestCase {

    public UpgradeClusterClientYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(0, testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    /**
     * Waits for the Machine Learning templates to be created by {@link org.elasticsearch.plugins.MetadataUpgrader}.
     * Only do this on the old cluster.  Users won't necessarily wait for templates to be upgraded during rolling
     * upgrades, so we cannot wait within the test framework, or we could miss production bugs.
     */
    @Before
    public void waitForTemplates() throws Exception {
        if (isOldCluster()) {
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
}
