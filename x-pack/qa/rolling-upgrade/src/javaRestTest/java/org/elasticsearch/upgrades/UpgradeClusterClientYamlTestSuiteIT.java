/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class UpgradeClusterClientYamlTestSuiteIT extends AbstractXPackYamlRollingUpgradeTestCase {

    public UpgradeClusterClientYamlTestSuiteIT(@Name("upgradedNodes") int upgradedNodes, ClientYamlTestCandidate testCandidate) {
        super(upgradedNodes, testCandidate);
    }

    /**
     * Builds the cross product of {@code upgradedNodes} (0..3, mirroring the old
     * oldClusterTest/oneThirdUpgradedTest/twoThirdsUpgradedTest/upgradedClusterTest Gradle tasks) and the YAML
     * test candidates from the corresponding resource directory.
     */
    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> parameters = new ArrayList<>();
        for (Object[] testCandidate : createParameters("old_cluster")) {
            parameters.add(new Object[] { 0, testCandidate[0] });
        }
        for (Object[] testCandidate : createParameters("mixed_cluster")) {
            parameters.add(new Object[] { 1, testCandidate[0] });
        }
        for (Object[] testCandidate : createParameters("mixed_cluster")) {
            parameters.add(new Object[] { 2, testCandidate[0] });
        }
        for (Object[] testCandidate : createParameters("upgraded_cluster")) {
            parameters.add(new Object[] { 3, testCandidate[0] });
        }
        return parameters;
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
