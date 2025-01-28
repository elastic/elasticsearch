/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

/** Runs rest tests against external cluster */
public class WatcherPagerDutyYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public WatcherPagerDutyYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Before
    public void startWatcher() throws Exception {
        final List<String> watcherTemplates = List.of(WatcherIndexTemplateRegistryField.TEMPLATE_NAMES_NO_ILM);
        assertBusy(() -> {
            try {
                getAdminExecutionContext().callApi("watcher.start", Map.of(), List.of(), Map.of());

                for (String template : watcherTemplates) {
                    ClientYamlTestResponse templateExistsResponse = getAdminExecutionContext().callApi(
                        "indices.exists_template",
                        Map.of("name", template),
                        List.of(),
                        Map.of()
                    );
                    assertThat(templateExistsResponse.getStatusCode(), is(200));
                }

                ClientYamlTestResponse response = getAdminExecutionContext().callApi("watcher.stats", Map.of(), List.of(), Map.of());
                String state = response.evaluate("stats.0.watcher_state");
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
                getAdminExecutionContext().callApi("watcher.stop", Map.of(), List.of(), Map.of());
                ClientYamlTestResponse response = getAdminExecutionContext().callApi("watcher.stats", Map.of(), List.of(), Map.of());
                String state = response.evaluate("stats.0.watcher_state");
                assertThat(state, is("stopped"));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }, 60, TimeUnit.SECONDS);
    }
}
