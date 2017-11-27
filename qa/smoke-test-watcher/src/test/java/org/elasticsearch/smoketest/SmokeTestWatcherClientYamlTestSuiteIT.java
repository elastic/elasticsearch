/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.is;

/** Runs rest tests against external cluster */
@Network // Needed to access to an external Jira server
@TestLogging("_root:TRACE")
public class SmokeTestWatcherClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public SmokeTestWatcherClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Before
    public void startWatcher() throws Exception {
        final List<String> watcherTemplates = Arrays.asList(WatcherIndexTemplateRegistry.TEMPLATE_NAMES);
        assertBusy(() -> {
            try {
                getAdminExecutionContext().callApi("xpack.watcher.start", emptyMap(), emptyList(), emptyMap());

                for (String template : watcherTemplates) {
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
}
