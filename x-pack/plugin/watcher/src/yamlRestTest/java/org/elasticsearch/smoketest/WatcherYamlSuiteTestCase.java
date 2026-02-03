/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

/**
 * Parent test class for Watcher YAML based REST tests
 */
public abstract class WatcherYamlSuiteTestCase extends ESClientYamlSuiteTestCase {
    public WatcherYamlSuiteTestCase(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    static final String ADMIN_USER = "test_admin";
    static final String WATCHER_USER = "watcher_manager";
    static final String TEST_PASSWORD = "x-pack-test-password";

    static LocalClusterSpecBuilder<ElasticsearchCluster> watcherClusterSpec() {
        return ElasticsearchCluster.local()
            .module("x-pack-watcher")
            .module("x-pack-ilm")
            .module("ingest-common")
            .module("analysis-common")
            .module("lang-mustache")
            .module("lang-painless")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("logger.org.elasticsearch.xpack.watcher", "debug")
            .setting("logger.org.elasticsearch.xpack.core.watcher", "debug")
            .user(ADMIN_USER, TEST_PASSWORD, "superuser", true);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(WATCHER_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(ADMIN_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public final void startWatcher() throws Exception {
        ESTestCase.assertBusy(() -> {
            ClientYamlTestResponse response = getAdminExecutionContext().callApi("watcher.stats", Map.of(), List.of(), Map.of());
            String state = response.evaluate("stats.0.watcher_state");

            switch (state) {
                case "stopped" -> {
                    ClientYamlTestResponse startResponse = getAdminExecutionContext().callApi(
                        "watcher.start",
                        Map.of(),
                        List.of(),
                        Map.of()
                    );
                    boolean isAcknowledged = startResponse.evaluate("acknowledged");
                    assertThat(isAcknowledged, is(true));
                    throw new AssertionError("waiting until stopped state reached started state");
                }
                case "stopping" -> throw new AssertionError("waiting until stopping state reached stopped state to start again");
                case "starting" -> throw new AssertionError("waiting until starting state reached started state");
                case "started" -> {
                    int watcherCount = response.evaluate("stats.0.watch_count");
                    if (watcherCount > 0) {
                        logger.info("expected 0 active watches, but got [{}], deleting watcher indices again", watcherCount);
                        deleteAllWatcherData();
                    }
                }
                // all good here, we are done
                default -> throw new AssertionError("unknown state[" + state + "]");
            }
        });
    }

    @After
    public final void stopWatcher() throws Exception {
        ESTestCase.assertBusy(() -> {
            ClientYamlTestResponse response = getAdminExecutionContext().callApi("watcher.stats", Map.of(), List.of(), Map.of());
            String state = response.evaluate("stats.0.watcher_state");
            switch (state) {
                case "stopped":
                    // all good here, we are done
                    break;
                case "stopping":
                    throw new AssertionError("waiting until stopping state reached stopped state");
                case "starting":
                    throw new AssertionError("waiting until starting state reached started state to stop");
                case "started":
                    ClientYamlTestResponse stopResponse = getAdminExecutionContext().callApi("watcher.stop", Map.of(), List.of(), Map.of());
                    boolean isAcknowledged = stopResponse.evaluate("acknowledged");
                    assertThat(isAcknowledged, is(true));
                    throw new AssertionError("waiting until started state reached stopped state");
                default:
                    throw new AssertionError("unknown state[" + state + "]");
            }
        }, 60, TimeUnit.SECONDS);
        deleteAllWatcherData();
    }

    static void deleteAllWatcherData() throws IOException {
        var queryWatchesRequest = new Request("GET", "/_watcher/_query/watches");
        var response = ObjectPath.createFromResponse(ESRestTestCase.adminClient().performRequest(queryWatchesRequest));

        int totalCount = response.evaluate("count");
        List<Map<?, ?>> watches = response.evaluate("watches");
        assert watches.size() == totalCount : "number of watches returned is unequal to the total number of watches";
        for (Map<?, ?> watch : watches) {
            String id = (String) watch.get("_id");
            var deleteWatchRequest = new Request("DELETE", "/_watcher/watch/" + id);
            assertOK(ESRestTestCase.adminClient().performRequest(deleteWatchRequest));
        }

        var deleteWatchHistoryRequest = new Request("DELETE", ".watcher-history-*");
        deleteWatchHistoryRequest.addParameter("ignore_unavailable", "true");
        ESRestTestCase.adminClient().performRequest(deleteWatchHistoryRequest);
    }
}
