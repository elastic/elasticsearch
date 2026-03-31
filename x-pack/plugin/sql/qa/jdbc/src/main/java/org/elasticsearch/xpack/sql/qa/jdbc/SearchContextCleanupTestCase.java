/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public abstract class SearchContextCleanupTestCase extends JdbcIntegrationTestCase {

    public void testAssertNoSearchContextsWaitsForAsyncCleanup() throws Exception {
        String index = "test_async_cleanup";
        index(index, builder -> builder.field("foo", "bar"));

        Request scrollSearch = new Request("POST", "/" + index + "/_search");
        scrollSearch.addParameter("scroll", "1m");
        scrollSearch.setJsonEntity("{\"size\":1,\"query\":{\"match_all\":{}}}");
        Response scrollSearchResponse = client().performRequest(scrollSearch);
        String scrollId = extractScrollId(scrollSearchResponse);
        assertThat(scrollId, notNullValue());

        assertBusy(() -> assertThat(openSearchContexts(index), greaterThan(0)));

        AtomicReference<Exception> clearScrollError = new AtomicReference<>();
        Thread clearScrollThread = new Thread(() -> {
            try {
                Thread.sleep(250);
                clearScroll(scrollId);
            } catch (Exception e) {
                clearScrollError.set(e);
            }
        }, "clear-scroll-context");
        clearScrollThread.start();

        assertNoSearchContexts();

        clearScrollThread.join(TimeUnit.SECONDS.toMillis(30));
        if (clearScrollThread.isAlive()) {
            clearScrollThread.interrupt();
            fail("timed out waiting for clear scroll thread");
        }
        if (clearScrollError.get() != null) {
            throw clearScrollError.get();
        }
    }

    @SuppressWarnings("unchecked")
    private static String extractScrollId(Response response) throws Exception {
        try (InputStream content = response.getEntity().getContent()) {
            Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
            return (String) map.get("_scroll_id");
        }
    }

    @SuppressWarnings("unchecked")
    private static int openSearchContexts(String index) throws Exception {
        Response response = client().performRequest(new Request("GET", "/_stats/search"));
        try (InputStream content = response.getEntity().getContent()) {
            Map<String, Object> stats = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
            Map<String, Object> indices = (Map<String, Object>) stats.get("indices");
            Map<String, Object> indexStats = (Map<String, Object>) indices.get(index);
            Map<String, Object> totalStats = (Map<String, Object>) indexStats.get("total");
            Map<String, Object> searchStats = (Map<String, Object>) totalStats.get("search");
            return (Integer) searchStats.get("open_contexts");
        }
    }

    private static void clearScroll(String scrollId) throws Exception {
        Request request = new Request("DELETE", "/_search/scroll");
        XContentBuilder builder = JsonXContent.contentBuilder().startObject().field("scroll_id", scrollId).endObject();
        request.setJsonEntity(Strings.toString(builder));
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }
}
