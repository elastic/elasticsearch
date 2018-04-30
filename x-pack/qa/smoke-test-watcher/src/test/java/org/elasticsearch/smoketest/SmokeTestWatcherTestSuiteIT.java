/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
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
        assertBusy(() -> {
            adminClient().performRequest("POST", "_xpack/watcher/_start");

            for (String template : WatcherIndexTemplateRegistryField.TEMPLATE_NAMES) {
                assertOK(adminClient().performRequest("HEAD", "_template/" + template));
            }

            Response statsResponse = adminClient().performRequest("GET", "_xpack/watcher/stats");
            ObjectPath objectPath = ObjectPath.createFromResponse(statsResponse);
            String state = objectPath.evaluate("stats.0.watcher_state");
            assertThat(state, is("started"));
        });
    }

    @After
    public void stopWatcher() throws Exception {
        assertBusy(() -> {
            adminClient().performRequest("POST", "_xpack/watcher/_stop", Collections.emptyMap());
            Response statsResponse = adminClient().performRequest("GET", "_xpack/watcher/stats");
            ObjectPath objectPath = ObjectPath.createFromResponse(statsResponse);
            String state = objectPath.evaluate("stats.0.watcher_state");
            assertThat(state, is("stopped"));
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

    public void testMonitorClusterHealth() throws Exception {
        String watchId = "cluster_health_watch";

        // get master publish address
        Response clusterStateResponse = adminClient().performRequest("GET", "_cluster/state");
        ObjectPath clusterState = ObjectPath.createFromResponse(clusterStateResponse);
        String masterNode = clusterState.evaluate("master_node");
        assertThat(masterNode, is(notNullValue()));

        Response statsResponse = adminClient().performRequest("GET", "_nodes");
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
        StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);

        Response response = client().performRequest("PUT", "_xpack/watcher/watch/" + watchId, Collections.emptyMap(), entity);
        assertOK(response);
        Map<String, Object> responseMap = entityAsMap(response);
        assertThat(responseMap, hasEntry("_id", watchId));
    }

    private void deleteWatch(String watchId) throws IOException {
        Response response = client().performRequest("DELETE", "_xpack/watcher/watch/" + watchId);
        assertOK(response);
        ObjectPath path = ObjectPath.createFromResponse(response);
        boolean found = path.evaluate("found");
        assertThat(found, is(true));
    }

    private ObjectPath getWatchHistoryEntry(String watchId) throws Exception {
        final AtomicReference<ObjectPath> objectPathReference = new AtomicReference<>();
        assertBusy(() -> {
            client().performRequest("POST", ".watcher-history-*/_refresh");

            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                builder.startObject("query").startObject("bool").startArray("must");
                builder.startObject().startObject("term").startObject("watch_id").field("value", watchId).endObject().endObject()
                        .endObject();
                builder.endArray().endObject().endObject();
                builder.startArray("sort").startObject().startObject("trigger_event.triggered_time").field("order", "desc").endObject()
                        .endObject().endArray();
                builder.endObject();

                StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
                Response response = client().performRequest("POST", ".watcher-history-*/_search", Collections.emptyMap(), entity);
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
        Response watcherStatsResponse = adminClient().performRequest("GET", "_xpack/watcher/stats");
        ObjectPath objectPath = ObjectPath.createFromResponse(watcherStatsResponse);
        int watchCount = objectPath.evaluate("stats.0.watch_count");
        assertThat(watchCount, is(expectedWatches));
    }
}
