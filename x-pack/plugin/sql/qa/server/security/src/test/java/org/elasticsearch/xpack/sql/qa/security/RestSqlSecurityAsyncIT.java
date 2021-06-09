/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.security;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.sql.qa.rest.BaseRestSqlTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField.RUN_AS_USER_HEADER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RestSqlSecurityAsyncIT extends ESRestTestCase {

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

    @Override
    protected Settings restClientSettings() {
        return RestSqlIT.securitySettings();
    }

    @Override
    protected String getProtocol() {
        return RestSqlIT.SSL_ENABLED ? "https" : "http";
    }

    public void testWithUsers() throws Exception {
        testCase("user1", "user2");
        testCase("user2", "user1");
    }

    private void testCase(String user, String otherUser) throws Exception {
        for (String indexName : new String[] { "index", "index-" + user }) {
            Response submitResp = submitAsyncSqlSearch(
                "SELECT event_type FROM \"" + indexName + "\" WHERE val=0",
                TimeValue.timeValueSeconds(10),
                user
            );
            assertOK(submitResp);
            String id = extractResponseId(submitResp);
            Response getResp = getAsyncSqlSearch(id, user);
            assertOK(getResp);

            // other cannot access the result
            ResponseException exc = expectThrows(ResponseException.class, () -> getAsyncSqlSearch(id, otherUser));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            // other cannot delete the result
            exc = expectThrows(ResponseException.class, () -> deleteAsyncSqlSearch(id, otherUser));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            // other and user cannot access the result from direct get calls
            AsyncExecutionId searchId = AsyncExecutionId.decode(id);
            for (String runAs : new String[] { user, otherUser }) {
                exc = expectThrows(ResponseException.class, () -> get(XPackPlugin.ASYNC_RESULTS_INDEX, searchId.getDocId(), runAs));
                assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(403));
                assertThat(exc.getMessage(), containsString("unauthorized"));
            }

            Response delResp = deleteAsyncSqlSearch(id, user);
            assertOK(delResp);
        }
        ResponseException exc = expectThrows(
            ResponseException.class,
            () -> submitAsyncSqlSearch("SELECT * FROM \"index-" + otherUser + "\"", TimeValue.timeValueSeconds(10), user)
        );
        assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    // user with manage privilege can check status and delete
    public void testWithManager() throws IOException {
        Response submitResp = submitAsyncSqlSearch("SELECT event_type FROM \"index\" WHERE val=0", TimeValue.timeValueSeconds(10), "user1");
        assertOK(submitResp);
        String id = extractResponseId(submitResp);
        Response getResp = getAsyncSqlSearch(id, "user1");
        assertOK(getResp);

        Response getStatus = getAsyncSqlStatus(id, "manage_user");
        assertOK(getStatus);
        Map<String, Object> status = BaseRestSqlTestCase.toMap(getStatus, null);
        assertEquals(200, status.get("completion_status"));

        Response deleteResp = deleteAsyncSqlSearch(id, "manage_user");
        assertOK(deleteResp);
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

    static Response submitAsyncSqlSearch(String query, TimeValue waitForCompletion, String user) throws IOException {
        final Request request = new Request("POST", "/_sql");
        setRunAsHeader(request, user);
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .field("query", query)
                    .field("wait_for_completion_timeout", waitForCompletion.toString())
                    // we do the cleanup explicitly
                    .field("keep_on_completion", "true")
                    .endObject()
            )
        );
        return client().performRequest(request);
    }

    static Response getAsyncSqlSearch(String id, String user) throws IOException {
        final Request request = new Request("GET", "/_sql/async/" + id);
        setRunAsHeader(request, user);
        request.addParameter("wait_for_completion_timeout", "0ms");
        request.addParameter("format", "json");
        return client().performRequest(request);
    }

    static Response getAsyncSqlStatus(String id, String user) throws IOException {
        final Request request = new Request("GET", "/_sql/async/status/" + id);
        setRunAsHeader(request, user);
        request.addParameter("format", "json");
        return client().performRequest(request);
    }

    static Response deleteAsyncSqlSearch(String id, String user) throws IOException {
        final Request request = new Request("DELETE", "/_sql/async/delete/" + id);
        setRunAsHeader(request, user);
        return client().performRequest(request);
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

    /**
     * Use <code>es-security-runas-user</code> to become a less privileged user.
     */
    static void setRunAsHeader(Request request, String user) {
        final RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader(RUN_AS_USER_HEADER, user);
        request.setOptions(builder);
    }
}
