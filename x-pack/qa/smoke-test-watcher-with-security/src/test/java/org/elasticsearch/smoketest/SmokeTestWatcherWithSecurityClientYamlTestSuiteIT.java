/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.test.SecuritySettingsSourceField.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;

public class SmokeTestWatcherWithSecurityClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final String TEST_ADMIN_USERNAME = "test_admin";
    private static final String TEST_ADMIN_PASSWORD = "x-pack-test-password";

    public SmokeTestWatcherWithSecurityClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Before
    public void startWatcher() throws Exception {
        // delete the watcher history to not clutter with entries from other test
        getAdminExecutionContext().callApi("indices.delete", Collections.singletonMap("index", ".watcher-history-*"),
                emptyList(), emptyMap());

        // create one document in this index, so we can test in the YAML tests, that the index cannot be accessed
        Request request = new Request("PUT", "/index_not_allowed_to_read/_doc/1");
        request.setJsonEntity("{\"foo\":\"bar\"}");
        adminClient().performRequest(request);

        assertBusy(() -> {
            ClientYamlTestResponse response =
                    getAdminExecutionContext().callApi("watcher.stats", emptyMap(), emptyList(), emptyMap());
            String state = (String) response.evaluate("stats.0.watcher_state");

            switch (state) {
                case "stopped":
                    ClientYamlTestResponse startResponse =
                            getAdminExecutionContext().callApi("watcher.start", emptyMap(), emptyList(), emptyMap());
                    boolean isAcknowledged = (boolean) startResponse.evaluate("acknowledged");
                    assertThat(isAcknowledged, is(true));
                    throw new AssertionError("waiting until stopped state reached started state");
                case "stopping":
                    throw new AssertionError("waiting until stopping state reached stopped state to start again");
                case "starting":
                    throw new AssertionError("waiting until starting state reached started state");
                case "started":
                    // all good here, we are done
                    break;
                default:
                    throw new AssertionError("unknown state[" + state + "]");
            }
        });

        assertBusy(() -> {
            for (String template : XPackRestTestConstants.TEMPLATE_NAMES_NO_ILM) {
                ClientYamlTestResponse templateExistsResponse = getAdminExecutionContext().callApi("indices.exists_template",
                        singletonMap("name", template), emptyList(), emptyMap());
                assertThat(templateExistsResponse.getStatusCode(), is(200));
            }
        });
    }

    @After
    public void stopWatcher() throws Exception {
        assertBusy(() -> {
            ClientYamlTestResponse response =
                    getAdminExecutionContext().callApi("watcher.stats", emptyMap(), emptyList(), emptyMap());
            String state = (String) response.evaluate("stats.0.watcher_state");

            switch (state) {
                case "stopped":
                    // all good here, we are done
                    break;
                case "stopping":
                    throw new AssertionError("waiting until stopping state reached stopped state");
                case "starting":
                    throw new AssertionError("waiting until starting state reached started state to stop");
                case "started":
                    ClientYamlTestResponse stopResponse =
                            getAdminExecutionContext().callApi("watcher.stop", emptyMap(), emptyList(), emptyMap());
                    boolean isAcknowledged = (boolean) stopResponse.evaluate("acknowledged");
                    assertThat(isAcknowledged, is(true));
                    throw new AssertionError("waiting until started state reached stopped state");
                default:
                    throw new AssertionError("unknown state[" + state + "]");
            }
        }, 60, TimeUnit.SECONDS);
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("watcher_manager", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, new SecureString(TEST_ADMIN_PASSWORD.toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }
}
