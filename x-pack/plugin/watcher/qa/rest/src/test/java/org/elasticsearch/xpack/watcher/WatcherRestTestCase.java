/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

/**
 * Parent test class for Watcher (not-YAML) based REST tests
 */
public abstract class WatcherRestTestCase extends ESRestTestCase {

    @Before
    public final void startWatcher() throws Exception {
        assertBusy(() -> {
            Response response = adminClient().performRequest(new Request("GET", "/_watcher/stats"));
            String state = ObjectPath.createFromResponse(response).evaluate("stats.0.watcher_state");

            switch (state) {
                case "stopped":
                    Response startResponse = adminClient().performRequest(new Request("POST", "/_watcher/_start"));
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
        assertBusy(() -> {
            Response response = adminClient().performRequest(new Request("GET", "/_watcher/stats"));
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
                    Response stopResponse = adminClient().performRequest(new Request("POST", "/_watcher/_stop"));
                    boolean isAcknowledged = ObjectPath.createFromResponse(stopResponse).evaluate("acknowledged");
                    assertThat(isAcknowledged, is(true));
                    throw new AssertionError("waiting until started state reached stopped state");
                default:
                    throw new AssertionError("unknown state[" + state + "]");
            }
        }, 60, TimeUnit.SECONDS);

        Request deleteWatchesIndexRequest = new Request("DELETE", ".watches");
        deleteWatchesIndexRequest.addParameter("ignore_unavailable", "true");
        adminClient().performRequest(deleteWatchesIndexRequest);

        Request deleteWatchHistoryRequest = new Request("DELETE", ".watcher-history-*");
        deleteWatchHistoryRequest.addParameter("ignore_unavailable", "true");
        adminClient().performRequest(deleteWatchHistoryRequest);
    }
}
