/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class WatcherRestartIT extends AbstractUpgradeTestCase {
    private static final Version UPGRADE_FROM_VERSION =
        Version.fromString(System.getProperty("tests.upgrade_from_version"));

    private static final String templatePrefix = ".watch-history-";

    public void testWatcherRestart() throws Exception {
        client().performRequest(new Request("POST", "/_watcher/_stop"));
        ensureWatcherStopped();

        client().performRequest(new Request("POST", "/_watcher/_start"));
        ensureWatcherStarted();

        validateHistoryTemplate();
    }

    private void validateHistoryTemplate() throws Exception {
        // v7.7.0 contains a Watch history template (version 11) that can't be used unless all nodes in the cluster are >=7.7.0, so
        // in a mixed cluster with some nodes <7.7.0 it will install template version 10, but if all nodes are <=7.7.0 template v11
        // is used.
        final String expectedMixedClusterTemplate = templatePrefix + (UPGRADE_FROM_VERSION.before(Version.V_7_7_0) ? "10" : "11");
        final String expectedFinalTemplate = templatePrefix + "12";

        if (ClusterType.MIXED == CLUSTER_TYPE) {
            final Request request = new Request("HEAD", "/_template/" + expectedMixedClusterTemplate);
            request.addParameter("include_type_name", "false");
            RequestOptions.Builder builder = request.getOptions().toBuilder();
            builder.setWarningsHandler(WarningsHandler.PERMISSIVE);
            request.setOptions(builder);
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        } else if (ClusterType.UPGRADED == CLUSTER_TYPE) {
            Response response = client().performRequest(new Request("HEAD", "/_index_template/" + expectedFinalTemplate));
            assertThat(response.getStatusLine().getStatusCode(), is(200));
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
