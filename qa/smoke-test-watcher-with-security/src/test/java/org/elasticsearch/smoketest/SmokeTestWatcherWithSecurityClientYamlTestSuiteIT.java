/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
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
        Response resp = adminClient().performRequest("PUT", "/index_not_allowed_to_read/doc/1", Collections.emptyMap(),
                new StringEntity("{\"foo\":\"bar\"}", ContentType.APPLICATION_JSON));
        assertThat(resp.getStatusLine().getStatusCode(), is(201));

        assertBusy(() -> {
            try {
                getAdminExecutionContext().callApi("xpack.watcher.start", emptyMap(), emptyList(), emptyMap());

                for (String template : WatcherIndexTemplateRegistry.TEMPLATE_NAMES) {
                    ClientYamlTestResponse templateExistsResponse = getAdminExecutionContext().callApi("indices.exists_template",
                            singletonMap("name", template), emptyList(), emptyMap());
                    assertThat(templateExistsResponse.getStatusCode(), is(200));
                }

                ClientYamlTestResponse response =
                        getAdminExecutionContext().callApi("xpack.watcher.stats", emptyMap(), emptyList(), emptyMap());
                String state = (String) response.evaluate("stats.0.watcher_state");
                assertThat(state, is("started"));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    @After
    public void stopWatcher() throws Exception {
        assertBusy(() -> {
            try {
                getAdminExecutionContext().callApi("xpack.watcher.stop", emptyMap(), emptyList(), emptyMap());
                ClientYamlTestResponse response =
                        getAdminExecutionContext().callApi("xpack.watcher.stats", emptyMap(), emptyList(), emptyMap());
                String state = (String) response.evaluate("stats.0.watcher_state");
                assertThat(state, is("stopped"));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
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

