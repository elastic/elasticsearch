/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HIT_AS_INT_PARAM;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SmokeTestWatcherTestSuiteIT extends ESRestTestCase {

    private static final String TEST_ADMIN_USERNAME = "test_admin";
    private static final String TEST_ADMIN_PASSWORD = "x-pack-test-password";

    @Before
    public void startWatcher() throws Exception {
        // delete the watcher history to not clutter with entries from other test
        assertOK(adminClient().performRequest(new Request("DELETE", "/.watcher-history-*")));

        assertBusy(() -> {
            Response response = adminClient().performRequest(new Request("GET", "/_xpack/watcher/stats"));
            String state = ObjectPath.createFromResponse(response).evaluate("stats.0.watcher_state");

            switch (state) {
                case "stopped":
                    Response startResponse = adminClient().performRequest(new Request("POST", "/_xpack/watcher/_start"));
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

        assertBusy(() -> {
            for (String template : WatcherIndexTemplateRegistryField.TEMPLATE_NAMES) {
                Response templateExistsResponse = adminClient().performRequest(new Request("HEAD", "/_template/" + template));
                assertThat(templateExistsResponse.getStatusLine().getStatusCode(), is(200));
            }
        });
    }

    @After
    public void stopWatcher() throws Exception {
        assertBusy(() -> {
            Response response = adminClient().performRequest(new Request("GET", "/_xpack/watcher/stats"));
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
                    Response stopResponse = adminClient().performRequest(new Request("POST", "/_xpack/watcher/_stop"));
                    boolean isAcknowledged = ObjectPath.createFromResponse(stopResponse).evaluate("acknowledged");
                    assertThat(isAcknowledged, is(true));
                    throw new AssertionError("waiting until started state reached stopped state");
                default:
                    throw new AssertionError("unknown state[" + state + "]");
            }
        });
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("watcher_manager", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, new SecureString(TEST_ADMIN_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/32299")
    public void testMonitorClusterHealth() throws Exception {
        String watchId = "cluster_health_watch";

        // get master publish address
        Response clusterStateResponse = adminClient().performRequest(new Request("GET", "/_cluster/state"));
        ObjectPath clusterState = ObjectPath.createFromResponse(clusterStateResponse);
        String masterNode = clusterState.evaluate("master_node");
        assertThat(masterNode, is(notNullValue()));

        Response statsResponse = adminClient().performRequest(new Request("GET", "/_nodes"));
        ObjectPath stats = ObjectPath.createFromResponse(statsResponse);
        String address = stats.evaluate("nodes." + masterNode + ".http.publish_address");
        assertThat(address, is(notNullValue()));
        String[] splitAddress = address.split(":", 2);
        String host = splitAddress[0];
        int port = Integer.valueOf(splitAddress[1]);

        // put watch
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            // trigger
            builder.startObject("trigger").startObject("schedule").field("interval", "1s").endObject().endObject();
            // input
            builder.startObject("input").startObject("http").startObject("request").field("host", host).field("port", port)
                .field("path", "/_cluster/health")
                .field("scheme", "http")
                .startObject("auth").startObject("basic")
                .field("username", TEST_ADMIN_USERNAME).field("password", TEST_ADMIN_PASSWORD)
                .endObject().endObject()
                .endObject().endObject().endObject();
            // condition
            builder.startObject("condition").startObject("compare").startObject("ctx.payload.number_of_data_nodes").field("lt", 10)
                .endObject().endObject().endObject();
            // actions
            builder.startObject("actions").startObject("log").startObject("logging").field("text", "executed").endObject().endObject()
                .endObject();

            builder.endObject();

            indexWatch(watchId, builder);
        }

        // check watch count
        assertWatchCount(1);

        // check watch history
        ObjectPath objectPath = getWatchHistoryEntry(watchId);
        boolean conditionMet = objectPath.evaluate("hits.hits.0._source.result.condition.met");
        assertThat(conditionMet, is(true));

        deleteWatch(watchId);
        assertWatchCount(0);
    }

    private void indexWatch(String watchId, XContentBuilder builder) throws Exception {
        Request request = new Request("PUT", "/_xpack/watcher/watch/" + watchId);
        request.setJsonEntity(Strings.toString(builder));
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = entityAsMap(response);
        assertThat(responseMap, hasEntry("_id", watchId));
    }

    private void deleteWatch(String watchId) throws IOException {
        Response response = client().performRequest(new Request("DELETE", "/_xpack/watcher/watch/" + watchId));
        assertOK(response);
        ObjectPath path = ObjectPath.createFromResponse(response);
        boolean found = path.evaluate("found");
        assertThat(found, is(true));
    }

    private ObjectPath getWatchHistoryEntry(String watchId) throws Exception {
        final AtomicReference<ObjectPath> objectPathReference = new AtomicReference<>();
        assertBusy(() -> {
            client().performRequest(new Request("POST", "/.watcher-history-*/_refresh"));

            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                builder.startObject("query").startObject("bool").startArray("must");
                builder.startObject().startObject("term").startObject("watch_id").field("value", watchId).endObject().endObject()
                    .endObject();
                builder.endArray().endObject().endObject();
                builder.startArray("sort").startObject().startObject("trigger_event.triggered_time").field("order", "desc").endObject()
                    .endObject().endArray();
                builder.endObject();

                Request searchRequest = new Request("POST", "/.watcher-history-*/_search");
                searchRequest.addParameter(TOTAL_HIT_AS_INT_PARAM, "true");
                searchRequest.setJsonEntity(Strings.toString(builder));
                Response response = client().performRequest(searchRequest);
                ObjectPath objectPath = ObjectPath.createFromResponse(response);
                int totalHits = objectPath.evaluate("hits.total");
                assertThat(totalHits, is(greaterThanOrEqualTo(1)));
                String watchid = objectPath.evaluate("hits.hits.0._source.watch_id");
                assertThat(watchid, is(watchId));
                objectPathReference.set(objectPath);
            }
        });
        return objectPathReference.get();
    }

    private void assertWatchCount(int expectedWatches) throws IOException {
        Response watcherStatsResponse = adminClient().performRequest(new Request("GET", "/_xpack/watcher/stats"));
        ObjectPath objectPath = ObjectPath.createFromResponse(watcherStatsResponse);
        int watchCount = objectPath.evaluate("stats.0.watch_count");
        assertThat(watchCount, is(expectedWatches));
    }
}
