/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.eql.SecurityUtils.secureClientSettings;
import static org.elasticsearch.xpack.eql.SecurityUtils.setRunAsHeader;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AsyncEqlSecurityIT extends ESRestTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = EqlSecurityTestCluster.getCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * All tests run as a superuser but use <code>es-security-runas-user</code> to become a less privileged user.
     */
    @Override
    protected Settings restClientSettings() {
        return secureClientSettings();
    }

    @Before
    public void indexDocuments() throws IOException {
        createIndex("index", Settings.EMPTY);
        index("index", "0", "event_type", "my_event", "@timestamp", "2020-04-09T12:35:48Z", "val", 0);
        refresh(adminClient(), "index");

        createIndex("index-user1", Settings.EMPTY);
        index("index-user1", "0", "event_type", "my_event", "@timestamp", "2020-04-09T12:35:48Z", "val", 0);
        refresh(adminClient(), "index-user1");

        createIndex("index-user2", Settings.EMPTY);
        index("index-user2", "0", "event_type", "my_event", "@timestamp", "2020-04-09T12:35:48Z", "val", 0);
        refresh(adminClient(), "index-user2");
    }

    public void testWithUsers() throws Exception {
        testCase("user1", "user2");
        testCase("user2", "user1");
    }

    private void testCase(String user, String other) throws Exception {
        for (String indexName : new String[] { "index", "index-" + user }) {
            Response submitResp = submitAsyncEqlSearch(indexName, "my_event where val==0", TimeValue.timeValueSeconds(10), user);
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
            for (String runAs : new String[] { user, other }) {
                exc = expectThrows(ResponseException.class, () -> get(XPackPlugin.ASYNC_RESULTS_INDEX, searchId.getDocId(), runAs));
                assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(403));
                assertThat(exc.getMessage(), containsString("unauthorized"));
            }

            Response delResp = deleteAsyncEqlSearch(id, user);
            assertOK(delResp);
        }
        ResponseException exc = expectThrows(
            ResponseException.class,
            () -> submitAsyncEqlSearch("index-" + other, "*", TimeValue.timeValueSeconds(10), user)
        );
        assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    static String extractResponseId(Response response) throws IOException {
        var map = responseAsMap(response);
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

    static Response get(String index, String id, String user) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_doc/" + id);
        setRunAsHeader(request, user);
        return client().performRequest(request);
    }

    static Response submitAsyncEqlSearch(String indexName, String query, TimeValue waitForCompletion, String user) throws IOException {
        final Request request = new Request("POST", indexName + "/_eql/search");
        setRunAsHeader(request, user);
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder().startObject().field("event_category_field", "event_type").field("query", query).endObject()
            )
        );
        request.addParameter("wait_for_completion_timeout", waitForCompletion.toString());
        // we do the cleanup explicitly
        request.addParameter("keep_on_completion", "true");
        return client().performRequest(request);
    }

    static Response getAsyncEqlSearch(String id, String user) throws IOException {
        final Request request = new Request("GET", "/_eql/search/" + id);
        setRunAsHeader(request, user);
        request.addParameter("wait_for_completion_timeout", "0ms");
        return client().performRequest(request);
    }

    static Response deleteAsyncEqlSearch(String id, String user) throws IOException {
        final Request request = new Request("DELETE", "/_eql/search/" + id);
        setRunAsHeader(request, user);
        return client().performRequest(request);
    }

}
