/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class WatcherRestartIT extends AbstractUpgradeTestCase {

    public void testWatcherRestart() throws Exception {
        client().performRequest(new Request("POST", "/_watcher/_stop"));
        ensureWatcherStopped();

        client().performRequest(new Request("POST", "/_watcher/_start"));
        ensureWatcherStarted();
    }

    public void testEnsureWatcherDeletesLegacyTemplates() throws Exception {
        if (CLUSTER_TYPE.equals(ClusterType.UPGRADED)) {
            // legacy index template created in previous releases should not be present anymore
            assertBusy(() -> {
                Request request = new Request("GET", "/_template/*watch*");
                try {
                    Response response = client().performRequest(request);
                    Map<String, Object> responseLevel = entityAsMap(response);
                    assertNotNull(responseLevel);

                    assertThat(responseLevel.containsKey(".watches"), is(false));
                    assertThat(responseLevel.containsKey(".triggered_watches"), is(false));
                    assertThat(responseLevel.containsKey(".watch-history-9"), is(false));
                } catch (ResponseException e) {
                    // Not found is fine
                    assertThat(
                        "Unexpected failure getting templates: " + e.getResponse().getStatusLine(),
                        e.getResponse().getStatusLine().getStatusCode(),
                        is(404)
                    );
                }
            }, 30, TimeUnit.SECONDS);
        }
    }

    private void ensureWatcherStopped() throws Exception {
        assertBusy(() -> {
            Response stats = client().performRequest(new Request("GET", "_watcher/stats"));
            String responseBody = EntityUtils.toString(stats.getEntity(), StandardCharsets.UTF_8);
            assertThat(responseBody, containsString("\"watcher_state\":\"stopped\""));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"starting\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"started\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"stopping\"")));
        });
    }

    private void ensureWatcherStarted() throws Exception {
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "_watcher/stats"));
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            assertThat(responseBody, containsString("\"watcher_state\":\"started\""));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"starting\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"stopping\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"stopped\"")));
        });
    }
}
