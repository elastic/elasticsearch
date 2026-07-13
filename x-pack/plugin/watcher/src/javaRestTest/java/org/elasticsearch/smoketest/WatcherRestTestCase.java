/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

/**
 * Parent test class for Watcher (not-YAML) based REST tests
 */
public abstract class WatcherRestTestCase extends ESRestTestCase {

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
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("logger.org.elasticsearch.xpack.watcher", "debug")
            .setting("logger.org.elasticsearch.xpack.core.watcher", "debug")
            .user(ADMIN_USER, TEST_PASSWORD, "superuser", true);
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
            Response response = ESRestTestCase.adminClient().performRequest(new Request("GET", "/_watcher/stats"));
            String state = ObjectPath.createFromResponse(response).evaluate("stats.0.watcher_state");

            switch (state) {
                case "stopped":
                    Response startResponse = ESRestTestCase.adminClient().performRequest(new Request("POST", "/_watcher/_start"));
                    boolean isAcknowledged = ObjectPath.createFromResponse(startResponse).evaluate("acknowledged");
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
    }

    @After
    public final void stopWatcher() throws Exception {
        ESTestCase.assertBusy(() -> {
            Response response = ESRestTestCase.adminClient().performRequest(new Request("GET", "/_watcher/stats"));
            String state = ObjectPath.createFromResponse(response).evaluate("stats.0.watcher_state");

            switch (state) {
                case "stopped":
                    // all good here, we are done
                    break;
                case "stopping":
                    throw new AssertionError("waiting until stopping state reached stopped state");
                case "starting":
                    throw new AssertionError("waiting until starting state reached started state to stop");
                case "started":
                    Response stopResponse = ESRestTestCase.adminClient().performRequest(new Request("POST", "/_watcher/_stop"));
                    boolean isAcknowledged = ObjectPath.createFromResponse(stopResponse).evaluate("acknowledged");
                    assertThat(isAcknowledged, is(true));
                    throw new AssertionError("waiting until started state reached stopped state");
                default:
                    throw new AssertionError("unknown state[" + state + "]");
            }
        }, 60, TimeUnit.SECONDS);
        deleteAllWatcherData();
    }

    public static void deleteAllWatcherData() throws IOException {
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
