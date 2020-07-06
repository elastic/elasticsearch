/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField.RUN_AS_USER_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AsyncEqlSecurityIT extends ESRestTestCase {

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Only works on snapshot builds for now", Build.CURRENT.isSnapshot());
    }

    /**
     * All tests run as a superuser but use <code>es-security-runas-user</code> to become a less privileged user.
     */
    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Before
    public void indexDocuments() throws IOException {
        createIndex("index", Settings.EMPTY);
        index("index", "0", "event_type", "my_event", "@timestamp", "2020-04-09T12:35:48Z", "val", 0);
        refresh("index");

        createIndex("index-user1", Settings.EMPTY);
        index("index-user1", "0", "event_type", "my_event", "@timestamp", "2020-04-09T12:35:48Z", "val", 0);
        refresh("index-user1");

        createIndex("index-user2", Settings.EMPTY);
        index("index-user2", "0", "event_type", "my_event", "@timestamp", "2020-04-09T12:35:48Z", "val", 0);
        refresh("index-user2");
    }

    public void testWithUsers() throws Exception {
        testCase("user1", "user2");
        testCase("user2", "user1");
    }

    private void testCase(String user, String other) throws Exception {
        for (String indexName : new String[] {"index", "index-" + user}) {
            Response submitResp = submitAsyncEqlSearch(indexName, "my_event where val=0", TimeValue.timeValueSeconds(10), user);
            assertOK(submitResp);
            String id = extractResponseId(submitResp);
            Response getResp = getAsyncEqlSearch(id, user);
            assertOK(getResp);

            // other cannot access the result
            ResponseException exc = expectThrows(ResponseException.class, () -> getAsyncEqlSearch(id, other));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            // other cannot delete the result
            exc = expectThrows(ResponseException.class, () -> deleteAsyncEqlSearch(id, other));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            // other and user cannot access the result from direct get calls
            AsyncExecutionId searchId = AsyncExecutionId.decode(id);
            for (String runAs : new String[] {user, other}) {
                exc = expectThrows(ResponseException.class, () -> get(XPackPlugin.ASYNC_RESULTS_INDEX, searchId.getDocId(), runAs));
                assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(403));
                assertThat(exc.getMessage(), containsString("unauthorized"));
            }

             Response delResp = deleteAsyncEqlSearch(id, user);
             assertOK(delResp);
        }
        ResponseException exc = expectThrows(ResponseException.class,
            () -> submitAsyncEqlSearch("index-" + other, "*", TimeValue.timeValueSeconds(10), user));
        assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(exc.getMessage(), containsString("unauthorized"));
    }

    static String extractResponseId(Response response) throws IOException {
        Map<String, Object> map = toMap(response);
        return (String) map.get("id");
    }

    static void index(String index, String id, Object... fields) throws IOException {
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            document.field((String) fields[i], fields[i + 1]);
        }
        document.endObject();
        final Request request = new Request("POST", "/" + index + "/_doc/" + id);
        request.setJsonEntity(Strings.toString(document));
        assertOK(client().performRequest(request));
    }

    static void refresh(String index) throws IOException {
        assertOK(adminClient().performRequest(new Request("POST", "/" + index + "/_refresh")));
    }

    static Response get(String index, String id, String user) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_doc/" + id);
        setRunAsHeader(request, user);
        return client().performRequest(request);
    }

    static Response submitAsyncEqlSearch(String indexName, String query, TimeValue waitForCompletion, String user) throws IOException {
        final Request request = new Request("POST", indexName + "/_eql/search");
        setRunAsHeader(request, user);
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder()
            .startObject()
            .field("event_category_field", "event_type")
            .field("query", query)
            .endObject()));
        request.addParameter("wait_for_completion_timeout", waitForCompletion.toString());
        // we do the cleanup explicitly
        request.addParameter("keep_on_completion", "true");
        return client().performRequest(request);
    }

    static Response getAsyncEqlSearch(String id, String user) throws IOException {
        final Request request = new Request("GET",  "/_eql/search/" + id);
        setRunAsHeader(request, user);
        request.addParameter("wait_for_completion_timeout", "0ms");
        return client().performRequest(request);
    }

    static Response deleteAsyncEqlSearch(String id, String user) throws IOException {
        final Request request = new Request("DELETE",  "/_eql/search/" + id);
        setRunAsHeader(request, user);
        return client().performRequest(request);
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

    static void setRunAsHeader(Request request, String user) {
        final RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader(RUN_AS_USER_HEADER, user);
        request.setOptions(builder);
    }

}
