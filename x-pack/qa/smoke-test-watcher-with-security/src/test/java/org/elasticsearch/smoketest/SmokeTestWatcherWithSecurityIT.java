/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class SmokeTestWatcherWithSecurityIT extends ESRestTestCase {

    private static final String TEST_ADMIN_USERNAME = "test_admin";
    private static final String TEST_ADMIN_PASSWORD = "x-pack-test-password";

    @Before
    public void startWatcher() throws Exception {
        StringEntity entity = new StringEntity("{ \"value\" : \"15\" }", ContentType.APPLICATION_JSON);
        assertOK(adminClient().performRequest("PUT", "my_test_index/doc/1", Collections.singletonMap("refresh", "true"), entity));

        // delete the watcher history to not clutter with entries from other test
        adminClient().performRequest("DELETE", ".watcher-history-*", Collections.emptyMap());

        // create one document in this index, so we can test in the YAML tests, that the index cannot be accessed
        Response resp = adminClient().performRequest("PUT", "/index_not_allowed_to_read/doc/1", Collections.emptyMap(),
                new StringEntity("{\"foo\":\"bar\"}", ContentType.APPLICATION_JSON));
        assertThat(resp.getStatusLine().getStatusCode(), is(201));

        assertBusy(() -> {
            try {
                Response statsResponse = adminClient().performRequest("GET", "_xpack/watcher/stats");
                ObjectPath objectPath = ObjectPath.createFromResponse(statsResponse);
                String state = objectPath.evaluate("stats.0.watcher_state");

                switch (state) {
                case "stopped":
                    Response startResponse = adminClient().performRequest("POST", "_xpack/watcher/_start");
                    assertOK(startResponse);
                    String body = EntityUtils.toString(startResponse.getEntity());
                    assertThat(body, containsString("\"acknowledged\":true"));
                    break;
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
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });

        assertBusy(() -> {
            for (String template : WatcherIndexTemplateRegistryField.TEMPLATE_NAMES) {
                assertOK(adminClient().performRequest("HEAD", "_template/" + template));
            }
        });
    }

    @After
    public void stopWatcher() throws Exception {
        adminClient().performRequest("DELETE", "_xpack/watcher/watch/my_watch");
        assertOK(adminClient().performRequest("DELETE", "my_test_index"));

        assertBusy(() -> {
            try {
                Response statsResponse = adminClient().performRequest("GET", "_xpack/watcher/stats");
                ObjectPath objectPath = ObjectPath.createFromResponse(statsResponse);
                String state = objectPath.evaluate("stats.0.watcher_state");

                switch (state) {
                case "stopped":
                    // all good here, we are done
                    break;
                case "stopping":
                    throw new AssertionError("waiting until stopping state reached stopped state");
                case "starting":
                    throw new AssertionError("waiting until starting state reached started state to stop");
                case "started":
                    Response stopResponse = adminClient().performRequest("POST", "_xpack/watcher/_stop", Collections.emptyMap());
                    assertOK(stopResponse);
                    String body = EntityUtils.toString(stopResponse.getEntity());
                    assertThat(body, containsString("\"acknowledged\":true"));
                    break;
                default:
                    throw new AssertionError("unknown state[" + state + "]");
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("watcher_manager", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, new SecureString(TEST_ADMIN_PASSWORD.toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }


    public void testSearchInputHasPermissions() throws Exception {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.startObject("trigger").startObject("schedule").field("interval", "1s").endObject().endObject();
            builder.startObject("input").startObject("search").startObject("request")
                    .startArray("indices").value("my_test_index").endArray()
                    .startObject("body").startObject("query").startObject("match_all").endObject().endObject().endObject()
                    .endObject().endObject().endObject();
            builder.startObject("condition").startObject("compare").startObject("ctx.payload.hits.total").field("gte", 1)
                    .endObject().endObject().endObject();
            builder.startObject("actions").startObject("logging").startObject("logging")
                    .field("text", "successfully ran my_watch to test for search inpput").endObject().endObject().endObject();
            builder.endObject();

            indexWatch("my_watch", builder);
        }

        // check history, after watch has fired
        ObjectPath objectPath = getWatchHistoryEntry("my_watch", "executed");
        boolean conditionMet = objectPath.evaluate("hits.hits.0._source.result.condition.met");
        assertThat(conditionMet, is(true));
    }

    public void testSearchInputWithInsufficientPrivileges() throws Exception {
        String indexName = "index_not_allowed_to_read";
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.startObject("trigger").startObject("schedule").field("interval", "1s").endObject().endObject();
            builder.startObject("input").startObject("search").startObject("request")
                    .startArray("indices").value(indexName).endArray()
                    .startObject("body").startObject("query").startObject("match_all").endObject().endObject().endObject()
                    .endObject().endObject().endObject();
            builder.startObject("condition").startObject("compare").startObject("ctx.payload.hits.total").field("gte", 1)
                    .endObject().endObject().endObject();
            builder.startObject("actions").startObject("logging").startObject("logging")
                    .field("text", "this should never be logged").endObject().endObject().endObject();
            builder.endObject();

            indexWatch("my_watch", builder);
        }

        // check history, after watch has fired
        ObjectPath objectPath = getWatchHistoryEntry("my_watch");
        String state = objectPath.evaluate("hits.hits.0._source.state");
        assertThat(state, is("execution_not_needed"));
        boolean conditionMet = objectPath.evaluate("hits.hits.0._source.result.condition.met");
        assertThat(conditionMet, is(false));
    }

    public void testSearchTransformHasPermissions() throws Exception {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.startObject("trigger").startObject("schedule").field("interval", "1s").endObject().endObject();
            builder.startObject("input").startObject("simple").field("foo", "bar").endObject().endObject();
            builder.startObject("transform").startObject("search").startObject("request")
                    .startArray("indices").value("my_test_index").endArray()
                    .startObject("body").startObject("query").startObject("match_all").endObject().endObject().endObject()
                    .endObject().endObject().endObject();
            builder.startObject("actions").startObject("index").startObject("index")
                    .field("index", "my_test_index")
                    .field("doc_type", "doc")
                    .field("doc_id", "my-id")
                    .endObject().endObject().endObject();
            builder.endObject();

            indexWatch("my_watch", builder);
        }

        // check history, after watch has fired
        ObjectPath objectPath = getWatchHistoryEntry("my_watch", "executed");
        boolean conditionMet = objectPath.evaluate("hits.hits.0._source.result.condition.met");
        assertThat(conditionMet, is(true));

        ObjectPath getObjectPath = ObjectPath.createFromResponse(client().performRequest("GET", "my_test_index/doc/my-id"));
        String value = getObjectPath.evaluate("_source.hits.hits.0._source.value");
        assertThat(value, is("15"));
    }

    public void testSearchTransformInsufficientPermissions() throws Exception {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.startObject("trigger").startObject("schedule").field("interval", "1s").endObject().endObject();
            builder.startObject("input").startObject("simple").field("foo", "bar").endObject().endObject();
            builder.startObject("transform").startObject("search").startObject("request")
                    .startArray("indices").value("index_not_allowed_to_read").endArray()
                    .startObject("body").startObject("query").startObject("match_all").endObject().endObject().endObject()
                    .endObject().endObject().endObject();
            builder.startObject("condition").startObject("compare").startObject("ctx.payload.hits.total").field("gte", 1)
                    .endObject().endObject().endObject();
            builder.startObject("actions").startObject("index").startObject("index")
                    .field("index", "my_test_index")
                    .field("doc_type", "doc")
                    .field("doc_id", "some-id")
                    .endObject().endObject().endObject();
            builder.endObject();

            indexWatch("my_watch", builder);
        }

        getWatchHistoryEntry("my_watch");

        Response response = adminClient().performRequest("GET", "my_test_index/doc/some-id",
                Collections.singletonMap("ignore", "404"));
        assertThat(response.getStatusLine().getStatusCode(), is(404));
    }

    public void testIndexActionHasPermissions() throws Exception {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.startObject("trigger").startObject("schedule").field("interval", "1s").endObject().endObject();
            builder.startObject("input").startObject("simple").field("spam", "eggs").endObject().endObject();
            builder.startObject("actions").startObject("index").startObject("index")
                    .field("index", "my_test_index")
                    .field("doc_type", "doc")
                    .field("doc_id", "my-id")
                    .endObject().endObject().endObject();
            builder.endObject();

            indexWatch("my_watch", builder);
        }

        ObjectPath objectPath = getWatchHistoryEntry("my_watch", "executed");
        boolean conditionMet = objectPath.evaluate("hits.hits.0._source.result.condition.met");
        assertThat(conditionMet, is(true));

        ObjectPath getObjectPath = ObjectPath.createFromResponse(client().performRequest("GET", "my_test_index/doc/my-id"));
        String spam = getObjectPath.evaluate("_source.spam");
        assertThat(spam, is("eggs"));
    }

    public void testIndexActionInsufficientPrivileges() throws Exception {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.startObject("trigger").startObject("schedule").field("interval", "1s").endObject().endObject();
            builder.startObject("input").startObject("simple").field("spam", "eggs").endObject().endObject();
            builder.startObject("actions").startObject("index").startObject("index")
                    .field("index", "index_not_allowed_to_read")
                    .field("doc_type", "doc")
                    .field("doc_id", "my-id")
                    .endObject().endObject().endObject();
            builder.endObject();

            indexWatch("my_watch", builder);
        }

        ObjectPath objectPath = getWatchHistoryEntry("my_watch", "executed");
        boolean conditionMet = objectPath.evaluate("hits.hits.0._source.result.condition.met");
        assertThat(conditionMet, is(true));

        Response response = adminClient().performRequest("GET", "index_not_allowed_to_read/doc/my-id",
                Collections.singletonMap("ignore", "404"));
        assertThat(response.getStatusLine().getStatusCode(), is(404));
    }

    private void indexWatch(String watchId, XContentBuilder builder) throws Exception {
        StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);

        Response response = client().performRequest("PUT", "_xpack/watcher/watch/my_watch", Collections.emptyMap(), entity);
        assertOK(response);
        Map<String, Object> responseMap = entityAsMap(response);
        assertThat(responseMap, hasEntry("_id", watchId));
    }

    private ObjectPath getWatchHistoryEntry(String watchId) throws Exception {
        return getWatchHistoryEntry(watchId, null);
    }

    private ObjectPath getWatchHistoryEntry(String watchId, String state) throws Exception {
        final AtomicReference<ObjectPath> objectPathReference = new AtomicReference<>();
        assertBusy(() -> {
            client().performRequest("POST", ".watcher-history-*/_refresh");

            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                builder.startObject("query").startObject("bool").startArray("must");
                builder.startObject().startObject("term").startObject("watch_id").field("value", watchId).endObject().endObject()
                        .endObject();
                if (Strings.isNullOrEmpty(state) == false) {
                    builder.startObject().startObject("term").startObject("state").field("value", state).endObject().endObject()
                            .endObject();
                }
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
}
