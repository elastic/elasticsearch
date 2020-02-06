/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.elasticsearch.xpack.test.SecuritySettingsSourceField.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class SmokeTestWatcherWithSecurityIT extends ESRestTestCase {

    private static final String TEST_ADMIN_USERNAME = "test_admin";
    private static final String TEST_ADMIN_PASSWORD = "x-pack-test-password";

    private String watchId = randomAlphaOfLength(20);

    @Before
    public void startWatcher() throws Exception {
        Request createAllowedDoc = new Request("PUT", "/my_test_index/_doc/1");
        createAllowedDoc.setJsonEntity("{ \"value\" : \"15\" }");
        createAllowedDoc.addParameter("refresh", "true");
        adminClient().performRequest(createAllowedDoc);

        // delete the watcher history to not clutter with entries from other test
        adminClient().performRequest(new Request("DELETE", ".watcher-history-*"));

        // create one document in this index, so we can test in the YAML tests, that the index cannot be accessed
        Request createNotAllowedDoc = new Request("PUT", "/index_not_allowed_to_read/_doc/1");
        createNotAllowedDoc.setJsonEntity("{\"foo\":\"bar\"}");
        adminClient().performRequest(createNotAllowedDoc);

        assertBusy(() -> {
            try {
                Response statsResponse = adminClient().performRequest(new Request("GET", "/_watcher/stats"));
                ObjectPath objectPath = ObjectPath.createFromResponse(statsResponse);
                String state = objectPath.evaluate("stats.0.watcher_state");

                switch (state) {
                    case "stopped":
                        Response startResponse = adminClient().performRequest(new Request("POST", "/_watcher/_start"));
                        Map<String, Object> responseMap = entityAsMap(startResponse);
                        assertThat(responseMap, hasEntry("acknowledged", true));
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
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });

        assertBusy(() -> {
            for (String template : XPackRestTestConstants.TEMPLATE_NAMES_NO_ILM) {
                assertOK(adminClient().performRequest(new Request("HEAD", "_template/" + template)));
            }
        });
    }

    @After
    public void stopWatcher() throws Exception {

        assertBusy(() -> {
            try {
                Response statsResponse = adminClient().performRequest(new Request("GET", "/_watcher/stats"));
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
                    Response stopResponse = adminClient().performRequest(new Request("POST", "/_watcher/_stop"));
                    String body = EntityUtils.toString(stopResponse.getEntity());
                    assertThat(body, containsString("\"acknowledged\":true"));
                    throw new AssertionError("waiting until started state reached stopped state");
                default:
                    throw new AssertionError("unknown state[" + state + "]");
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }, 60, TimeUnit.SECONDS);

        adminClient().performRequest(new Request("DELETE", "/my_test_index"));
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
                    .field("text", "successfully ran " + watchId + "to test for search input").endObject().endObject().endObject();
            builder.endObject();

            indexWatch(watchId, builder);
        }

        // check history, after watch has fired
        ObjectPath objectPath = getWatchHistoryEntry(watchId, "executed");
        boolean conditionMet = objectPath.evaluate("hits.hits.0._source.result.condition.met");
        assertThat(conditionMet, is(true));
    }

    public void testSearchInputWithInsufficientPrivileges() throws Exception {
        String indexName = "index_not_allowed_to_read";
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.startObject("trigger").startObject("schedule").field("interval", "4s").endObject().endObject();
            builder.startObject("input").startObject("search").startObject("request")
                    .startArray("indices").value(indexName).endArray()
                    .startObject("body").startObject("query").startObject("match_all").endObject().endObject().endObject()
                    .endObject().endObject().endObject();
            builder.startObject("condition").startObject("compare").startObject("ctx.payload.hits.total").field("gte", 1)
                    .endObject().endObject().endObject();
            builder.startObject("actions").startObject("logging").startObject("logging")
                    .field("text", "this should never be logged").endObject().endObject().endObject();
            builder.endObject();

            indexWatch(watchId, builder);
        }

        // check history, after watch has fired
        ObjectPath objectPath = getWatchHistoryEntry(watchId, "execution_not_needed");
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
                    .field("doc_id", "my-id")
                    .endObject().endObject().endObject();
            builder.endObject();

            indexWatch(watchId, builder);
        }

        // check history, after watch has fired
        ObjectPath objectPath = getWatchHistoryEntry(watchId, "executed");
        boolean conditionMet = objectPath.evaluate("hits.hits.0._source.result.condition.met");
        assertThat(conditionMet, is(true));

        ObjectPath getObjectPath = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/my_test_index/_doc/my-id")));
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
                    .field("doc_id", "some-id")
                    .endObject().endObject().endObject();
            builder.endObject();

            indexWatch(watchId, builder);
        }

        getWatchHistoryEntry(watchId);

        Response response = adminClient().performRequest(new Request("HEAD", "/my_test_index/_doc/some-id"));
        assertThat(response.getStatusLine().getStatusCode(), is(404));
    }

    public void testIndexActionHasPermissions() throws Exception {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.startObject("trigger").startObject("schedule").field("interval", "1s").endObject().endObject();
            builder.startObject("input").startObject("simple").field("spam", "eggs").endObject().endObject();
            builder.startObject("actions").startObject("index").startObject("index")
                    .field("index", "my_test_index")
                    .field("doc_id", "my-id")
                    .endObject().endObject().endObject();
            builder.endObject();

            indexWatch(watchId, builder);
        }

        ObjectPath objectPath = getWatchHistoryEntry(watchId, "executed");
        boolean conditionMet = objectPath.evaluate("hits.hits.0._source.result.condition.met");
        assertThat(conditionMet, is(true));

        ObjectPath getObjectPath = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/my_test_index/_doc/my-id")));
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
                    .field("doc_id", "my-id")
                    .endObject().endObject().endObject();
            builder.endObject();

            indexWatch(watchId, builder);
        }

        ObjectPath objectPath = getWatchHistoryEntry(watchId, "executed");
        boolean conditionMet = objectPath.evaluate("hits.hits.0._source.result.condition.met");
        assertThat(conditionMet, is(true));

        Response response = adminClient().performRequest(new Request("HEAD", "/index_not_allowed_to_read/_doc/my-id"));
        assertThat(response.getStatusLine().getStatusCode(), is(404));
    }

    private void indexWatch(String watchId, XContentBuilder builder) throws Exception {
        Request request = new Request("PUT", "/_watcher/watch/" + watchId);
        request.setJsonEntity(Strings.toString(builder));
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = entityAsMap(response);
        assertThat(responseMap, hasEntry("_id", watchId));
        assertThat(responseMap, hasEntry("created", true));
        assertThat(responseMap, hasEntry("_version", 1));
    }

    private ObjectPath getWatchHistoryEntry(String watchId) throws Exception {
        return getWatchHistoryEntry(watchId, null);
    }

    private ObjectPath getWatchHistoryEntry(String watchId, String state) throws Exception {
        final AtomicReference<ObjectPath> objectPathReference = new AtomicReference<>();
        try {
            assertBusy(() -> {
                client().performRequest(new Request("POST", "/.watcher-history-*/_refresh"));

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

                    Request searchRequest = new Request("POST", "/.watcher-history-*/_search");
                    searchRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
                    searchRequest.setJsonEntity(Strings.toString(builder));
                    Response response = client().performRequest(searchRequest);
                    ObjectPath objectPath = ObjectPath.createFromResponse(response);
                    int totalHits = objectPath.evaluate("hits.total");
                    assertThat(totalHits, is(greaterThanOrEqualTo(1)));
                    String watchid = objectPath.evaluate("hits.hits.0._source.watch_id");
                    assertThat(watchid, is(watchId));
                    objectPathReference.set(objectPath);
                } catch (ResponseException e) {
                    final String err = "Failed to perform search of watcher history";
                    logger.info(err, e);
                    fail(err);
                }
            });
        } catch (AssertionError ae) {
            {
                Request request = new Request("GET", "/_watcher/stats");
                request.addParameter("metric", "_all");
                request.addParameter("pretty", "true");
                try {
                    Response response = client().performRequest(request);
                    logger.info("watcher_stats: {}", EntityUtils.toString(response.getEntity()));
                } catch (IOException e) {
                    logger.error("error while fetching watcher_stats", e);
                }
            }
            {
                Request request = new Request("GET", "/_cluster/state");
                request.addParameter("pretty", "true");
                try {
                    Response response = client().performRequest(request);
                    logger.info("cluster_state: {}", EntityUtils.toString(response.getEntity()));
                } catch (IOException e) {
                    logger.error("error while fetching cluster_state", e);
                }
            }
            {
                Request request = new Request("GET", "/.watcher-history-*/_search");
                request.addParameter("size", "100");
                request.addParameter("sort", "trigger_event.triggered_time:desc");
                request.addParameter("pretty", "true");
                try {
                    Response response = client().performRequest(request);
                    logger.info("watcher_history_snippets: {}", EntityUtils.toString(response.getEntity()));
                } catch (IOException e) {
                    logger.error("error while fetching watcher_history_snippets", e);
                }
            }
            throw ae;
        }
        return objectPathReference.get();
    }
}
