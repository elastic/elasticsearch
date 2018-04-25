/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.is;

@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class UpgradeClusterClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    /**
     * Waits for the Machine Learning templates to be created by {@link org.elasticsearch.plugins.MetaDataUpgrader}
     */
    @Before
    public void waitForTemplates() throws Exception {
        XPackRestTestHelper.waitForMlTemplates(client());
    }

    /**
     * Enables an HTTP exporter for monitoring so that we can test the production-level exporter (not the local exporter).
     *
     * The build.gradle file disables data collection, so the expectation is that any monitoring rest tests will use the
     * "_xpack/monitoring/_bulk" endpoint to lazily setup the templates on-demand and fill in data without worrying about
     * timing.
     */
    @Before
    public void waitForMonitoring() throws Exception {
        final String[] nodes = System.getProperty("tests.rest.cluster").split(",");
        final Map<String, Object> settings = new HashMap<>();

        settings.put("xpack.monitoring.exporters._http.enabled", true);
        // only select the last node to avoid getting the "old" node in a mixed cluster
        // if we ever randomize the order that the nodes are restarted (or add more nodes), then we need to verify which node we select
        settings.put("xpack.monitoring.exporters._http.host", nodes[nodes.length - 1]);

        assertBusy(() -> {
            final ClientYamlTestResponse response =
                    getAdminExecutionContext().callApi("cluster.put_settings",
                                                       emptyMap(),
                                                       singletonList(singletonMap("transient", settings)),
                                                       emptyMap());

            assertThat(response.evaluate("acknowledged"), is(true));
        });
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    public UpgradeClusterClientYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString(("test_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                // we increase the timeout here to 90 seconds to handle long waits for a green
                // cluster health. the waits for green need to be longer than a minute to
                // account for delayed shards
                .put(ESRestTestCase.CLIENT_RETRY_TIMEOUT, "90s")
                .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
                .build();
    }
}
