/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
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

public abstract class AbstractWatcherThirdPartyYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final Map<String, String> NO_ERROR_TRACE = Map.of("error_trace", "false");

    protected static LocalClusterSpecBuilder<ElasticsearchCluster> baseClusterBuilder() {
        return ElasticsearchCluster.local()
            .module("x-pack-watcher")
            .module("x-pack-ilm")
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("logger.org.elasticsearch.xpack.watcher", "DEBUG");
    }

    protected AbstractWatcherThirdPartyYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    protected abstract ElasticsearchCluster getCluster();

    @Override
    protected String getTestRestCluster() {
        return getCluster().getHttpAddresses();
    }

    @Before
    public void startWatcher() throws Exception {
        final List<String> watcherTemplates = List.of(WatcherIndexTemplateRegistryField.TEMPLATE_NAMES);
        assertBusy(() -> {
            try {
                getAdminExecutionContext().callApi("watcher.start", NO_ERROR_TRACE, List.of(), Map.of());

                for (String template : watcherTemplates) {
                    ClientYamlTestResponse templateExistsResponse = getAdminExecutionContext().callApi(
                        "indices.exists_index_template",
                        Map.of("name", template, "error_trace", "false"),
                        List.of(),
                        Map.of()
                    );
                    assertThat(templateExistsResponse.getStatusCode(), is(200));
                }

                ClientYamlTestResponse response = getAdminExecutionContext().callApi("watcher.stats", NO_ERROR_TRACE, List.of(), Map.of());
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
                getAdminExecutionContext().callApi("watcher.stop", NO_ERROR_TRACE, List.of(), Map.of());
                ClientYamlTestResponse response = getAdminExecutionContext().callApi("watcher.stats", NO_ERROR_TRACE, List.of(), Map.of());
                String state = response.evaluate("stats.0.watcher_state");
                assertThat(state, is("stopped"));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }, 60, TimeUnit.SECONDS);
    }
}
