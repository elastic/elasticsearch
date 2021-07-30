/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.asyncsearch.AsyncSearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.XPackPlugin.ASYNC_RESULTS_INDEX;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField.RUN_AS_USER_HEADER;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class AsyncSearchSecurityIT extends ESRestTestCase {
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
        index("index", "0", "foo", "bar");
        index("index", "1", "bar", "baz");
        index("index", "2", "baz", "boo");
        refresh("index");

        createIndex("index-user1", Settings.EMPTY);
        index("index-user1", "0", "foo", "bar");
        refresh("index-user1");

        createIndex("index-user2", Settings.EMPTY);
        index("index-user2", "0", "foo", "bar");
        index("index-user2", "1", "bar", "baz");
        refresh("index-user2");
    }

    public void testWithDlsAndFls() throws Exception {
        Response submitResp = submitAsyncSearch("*", "*", TimeValue.timeValueSeconds(10), "user-dls");
        assertOK(submitResp);
        SearchHit[] hits = getSearchHits(extractResponseId(submitResp), "user-dls");
        assertThat(hits, arrayContainingInAnyOrder(
                new CustomMatcher<SearchHit>("\"index\" doc 1 matcher") {
                    @Override
                    public boolean matches(Object actual) {
                        SearchHit hit = (SearchHit) actual;
                        return "index".equals(hit.getIndex()) &&
                                "1".equals(hit.getId()) &&
                                hit.getSourceAsMap().isEmpty();
                    }
                },
                new CustomMatcher<SearchHit>("\"index\" doc 2 matcher") {
                    @Override
                    public boolean matches(Object actual) {
                        SearchHit hit = (SearchHit) actual;
                        return "index".equals(hit.getIndex()) &&
                                "2".equals(hit.getId()) &&
                                "boo".equals(hit.getSourceAsMap().get("baz"));
                    }
                },
                new CustomMatcher<SearchHit>("\"index-user2\" doc 1 matcher") {
                    @Override
                    public boolean matches(Object actual) {
                        SearchHit hit = (SearchHit) actual;
                        return "index-user2".equals(hit.getIndex()) &&
                                "1".equals(hit.getId()) &&
                                hit.getSourceAsMap().isEmpty();
                    }
                }));
    }

    public void testWithUsers() throws Exception {
        testCase("user1", "user2");
        testCase("user2", "user1");
    }

    private void testCase(String user, String other) throws Exception {
        for (String indexName : new String[] {"index", "index-" + user}) {
            Response submitResp = submitAsyncSearch(indexName, "foo:bar", TimeValue.timeValueSeconds(10), user);
            assertOK(submitResp);
            String id = extractResponseId(submitResp);
            Response getResp = getAsyncSearch(id, user);
            assertOK(getResp);

            // other cannot access the result
            ResponseException exc = expectThrows(ResponseException.class, () -> getAsyncSearch(id, other));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            // user-cancel cannot access the result
            exc = expectThrows(ResponseException.class, () -> getAsyncSearch(id, "user-cancel"));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            // other cannot delete the result
            exc = expectThrows(ResponseException.class, () -> deleteAsyncSearch(id, other));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            // other and user cannot access the result from direct get calls
            AsyncExecutionId searchId = AsyncExecutionId.decode(id);
            for (String runAs : new String[] {user, other}) {
                exc = expectThrows(ResponseException.class, () -> get(ASYNC_RESULTS_INDEX, searchId.getDocId(), runAs));
                assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(403));
                assertThat(exc.getMessage(), containsString("unauthorized"));
            }

            Response delResp = deleteAsyncSearch(id, user);
            assertOK(delResp);

            // check that users with the 'cancel_task' privilege can delete an async
            // search submitted by a different user.
            for (String runAs : new String[] { "user-cancel", "test_kibana_user" }) {
                Response newResp = submitAsyncSearch(indexName, "foo:bar", TimeValue.timeValueSeconds(10), user);
                assertOK(newResp);
                String newId = extractResponseId(newResp);
                exc = expectThrows(ResponseException.class, () -> getAsyncSearch(id, runAs));
                assertThat(exc.getResponse().getStatusLine().getStatusCode(), greaterThan(400));
                delResp = deleteAsyncSearch(newId, runAs);
                assertOK(delResp);
            }
        }
        ResponseException exc = expectThrows(ResponseException.class,
            () -> submitAsyncSearch("index-" + other, "*", TimeValue.timeValueSeconds(10), user));
        assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(exc.getMessage(), containsString("unauthorized"));
    }

    private SearchHit[] getSearchHits(String asyncId, String user) throws IOException {
        final Response resp = getAsyncSearch(asyncId, user);
        assertOK(resp);
        AsyncSearchResponse searchResponse = AsyncSearchResponse.fromXContent(XContentHelper.createParser(NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            new BytesArray(EntityUtils.toByteArray(resp.getEntity())),
            XContentType.JSON));
        return searchResponse.getSearchResponse().getHits().getHits();
    }

    public void testAuthorizationOfPointInTime() throws Exception {
        String authorizedUser = randomFrom("user1", "user2");
        final Matcher<SearchHit> hitMatcher = new CustomMatcher<>("hit") {
            @Override
            public boolean matches(Object actual) {
                SearchHit hit = (SearchHit) actual;
                return hit.getIndex().equals("index-" + authorizedUser) && hit.getId().equals("0");
            }
        };
        final String pitId = openPointInTime(new String[]{"index-" + authorizedUser}, authorizedUser);
        try {
            Response submit = submitAsyncSearchWithPIT(pitId, "foo:bar", TimeValue.timeValueSeconds(10), authorizedUser);
            assertOK(submit);
            final Response resp = getAsyncSearch(extractResponseId(submit), authorizedUser);
            assertOK(resp);
            assertThat(getSearchHits(extractResponseId(resp), authorizedUser), arrayContainingInAnyOrder(hitMatcher));

            String unauthorizedUser = randomValueOtherThan(authorizedUser, () -> randomFrom("user1", "user2"));
            ResponseException exc = expectThrows(ResponseException.class,
                () -> submitAsyncSearchWithPIT(pitId, "*:*", TimeValue.timeValueSeconds(10), unauthorizedUser));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(exc.getMessage(), containsString("unauthorized"));

        } finally {
            closePointInTime(pitId, authorizedUser);
        }
    }

    public void testRejectPointInTimeWithIndices() throws Exception {
        String authorizedUser = randomFrom("user1", "user2");
        final String pitId = openPointInTime(new String[]{"index-" + authorizedUser}, authorizedUser);
        try {
            final Request request = new Request("POST", "/_async_search");
            setRunAsHeader(request, authorizedUser);
            request.addParameter("wait_for_completion_timeout", "true");
            request.addParameter("keep_on_completion", "true");
            if (randomBoolean()) {
                request.addParameter("index", "index-" + authorizedUser);
            } else {
                request.addParameter("index", "*");
            }
            final XContentBuilder requestBody = JsonXContent.contentBuilder()
                .startObject()
                .startObject("pit")
                .field("id", pitId)
                .field("keep_alive", "1m")
                .endObject()
                .endObject();
            request.setJsonEntity(Strings.toString(requestBody));
            final ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(exc.getMessage(), containsString("[indices] cannot be used with point in time. Do not specify any index with point in time."));
        } finally {
            closePointInTime(pitId, authorizedUser);
        }
    }

    public void testSharingPointInTime() throws Exception {
        final Matcher<SearchHit> hitMatcher = new CustomMatcher<>("index") {
            @Override
            public boolean matches(Object actual) {
                SearchHit hit = (SearchHit) actual;
                return hit.getIndex().equals("index") && hit.getId().equals("0");
            }
        };
        String firstUser = randomFrom("user1", "user2");
        final String pitId = openPointInTime(new String[]{"index"}, firstUser);
        try {
            {
                Response firstSubmit = submitAsyncSearchWithPIT(pitId, "foo:bar", TimeValue.timeValueSeconds(10), firstUser);
                assertOK(firstSubmit);
                final Response firstResp = getAsyncSearch(extractResponseId(firstSubmit), firstUser);
                assertOK(firstResp);
                final SearchHit[] firstHits = getSearchHits(extractResponseId(firstResp), firstUser);
                assertThat(firstHits, arrayContainingInAnyOrder(hitMatcher));
            }
            {
                String secondUser = randomValueOtherThan(firstUser, () -> randomFrom("user1", "user2"));
                Response secondSubmit = submitAsyncSearchWithPIT(pitId, "foo:bar", TimeValue.timeValueSeconds(10), secondUser);
                assertOK(secondSubmit);
                final Response secondResp = getAsyncSearch(extractResponseId(secondSubmit), secondUser);
                assertOK(secondResp);
                final SearchHit[] secondHits = getSearchHits(extractResponseId(secondResp), secondUser);
                assertThat(secondHits, arrayContainingInAnyOrder(hitMatcher));
            }
        } finally {
            closePointInTime(pitId, firstUser);
        }
    }

    public void testWithDLSPointInTime() throws Exception {
        final String pitId = openPointInTime(new String[]{"index"}, "user1");
        try {
            Response userResp = submitAsyncSearchWithPIT(pitId, "*", TimeValue.timeValueSeconds(10), "user1");
            assertOK(userResp);
            assertThat(getSearchHits(extractResponseId(userResp), "user1"), arrayWithSize(3));

            Response dlsResp = submitAsyncSearchWithPIT(pitId, "*", TimeValue.timeValueSeconds(10), "user-dls");
            assertOK(dlsResp);
            assertThat(getSearchHits(extractResponseId(dlsResp), "user-dls"), arrayContainingInAnyOrder(
                new CustomMatcher<SearchHit>("\"index\" doc 1 matcher") {
                    @Override
                    public boolean matches(Object actual) {
                        SearchHit hit = (SearchHit) actual;
                        return "index".equals(hit.getIndex()) &&
                            "1".equals(hit.getId()) &&
                            hit.getSourceAsMap().isEmpty();
                    }
                },
                new CustomMatcher<SearchHit>("\"index\" doc 2 matcher") {
                    @Override
                    public boolean matches(Object actual) {
                        SearchHit hit = (SearchHit) actual;
                        return "index".equals(hit.getIndex()) &&
                            "2".equals(hit.getId()) &&
                            "boo".equals(hit.getSourceAsMap().get("baz"));
                    }
                }));
        } finally {
            closePointInTime(pitId, "user1");
        }
    }

    static String extractResponseId(Response response) throws IOException {
        Map<String, Object> map = toMap(response);
        return (String) map.get("id");
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Map<String, Object>>> extractHits(Map<String, Object> respMap) {
        Map<String, Object> response = ((Map<String, Object>) respMap.get("response"));
        return ((List<Map<String, Map<String, Object>>>)((Map<String, Object>) response.get("hits")).get("hits"));
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

    static Response submitAsyncSearch(String indexName, String query, TimeValue waitForCompletion, String user) throws IOException {
        final Request request = new Request("POST", indexName + "/_async_search");
        setRunAsHeader(request, user);
        request.addParameter("q", query);
        request.addParameter("wait_for_completion_timeout", waitForCompletion.toString());
        // we do the cleanup explicitly
        request.addParameter("keep_on_completion", "true");
        return client().performRequest(request);
    }

    static Response getAsyncSearch(String id, String user) throws IOException {
        final Request request = new Request("GET",  "/_async_search/" + id);
        setRunAsHeader(request, user);
        request.addParameter("wait_for_completion_timeout", "0ms");
        return client().performRequest(request);
    }

    static Response deleteAsyncSearch(String id, String user) throws IOException {
        final Request request = new Request("DELETE",  "/_async_search/" + id);
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

    private String openPointInTime(String[] indexNames, String user) throws IOException {
        final Request request = new Request("POST", "/_pit");
        request.addParameter("index", String.join(",", indexNames));
        setRunAsHeader(request, user);
        request.addParameter("keep_alive", between(1, 5) + "m");
        final Response response = client().performRequest(request);
        assertOK(response);
        return (String) toMap(response).get("id");
    }

    static Response submitAsyncSearchWithPIT(String pit, String query, TimeValue waitForCompletion, String user) throws IOException {
        final Request request = new Request("POST", "/_async_search");
        setRunAsHeader(request, user);
        request.addParameter("wait_for_completion_timeout", waitForCompletion.toString());
        request.addParameter("q", query);
        request.addParameter("keep_on_completion", "true");
        final XContentBuilder requestBody = JsonXContent.contentBuilder()
            .startObject()
                .startObject("pit")
                    .field("id", pit)
                    .field("keep_alive", "1m")
                .endObject()
            .endObject();
        request.setJsonEntity(Strings.toString(requestBody));
        return client().performRequest(request);
    }

    private void closePointInTime(String pitId, String user) throws IOException {
        final Request request = new Request("DELETE", "/_pit");
        setRunAsHeader(request, user);
        final XContentBuilder requestBody = JsonXContent.contentBuilder()
            .startObject()
                .field("id", pitId)
            .endObject();
        request.setJsonEntity(Strings.toString(requestBody));
        assertOK(client().performRequest(request));
    }
}
