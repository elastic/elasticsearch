/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.hamcrest.Matchers.greaterThan;

public class JdbcAssertNoSearchContextsIT extends JdbcIntegrationTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = singleNodeCluster();

    @Override
    public ElasticsearchCluster getCluster() {
        return cluster;
    }

    public void testAssertNoSearchContextsWaitsForAsyncClear() throws Exception {
        String index = "test_scroll_cleanup_" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        Request createIndex = new Request("PUT", "/" + index);
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "f": { "type": "keyword" }
                }
              }
            }
            """);
        assertEquals(200, client().performRequest(createIndex).getStatusLine().getStatusCode());

        index(index, b -> b.field("f", "v"));

        Request searchRequest = new Request("GET", "/" + index + "/_search");
        searchRequest.setJsonEntity("""
            { "size" : 1, "query" : { "match_all" : {} } }
            """);
        searchRequest.addParameter("scroll", "1m");
        Response searchResponse = client().performRequest(searchRequest);
        assertEquals(200, searchResponse.getStatusLine().getStatusCode());

        @SuppressWarnings("unchecked")
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(searchResponse.getEntity()), false);
        String scrollId = (String) map.get("_scroll_id");
        assertNotNull(scrollId);

        // Ensure search stats have observed the open context before we assert on cleanup.
        assertBusy(() -> assertThat(getOpenContexts(searchStats(), index), greaterThan(0)));

        AtomicReference<Exception> clearFailure = new AtomicReference<>();
        CountDownLatch cleared = new CountDownLatch(1);
        Thread clearScrollThread = new Thread(() -> {
            try {
                Thread.sleep(2_000L);
                Request clearScrollRequest = new Request("DELETE", "/_search/scroll");
                clearScrollRequest.addParameter("scroll_id", scrollId);
                client().performRequest(clearScrollRequest);
            } catch (Exception e) {
                clearFailure.set(e);
            } finally {
                cleared.countDown();
            }
        }, "clear-scroll-id");

        clearScrollThread.start();

        try {
            // Without the assertBusy retry in JdbcIntegrationTestCase.assertNoSearchContexts(), this would fail deterministically
            // because the scroll context is still open when we enter the check.
            assertNoSearchContexts();
        } finally {
            cleared.await(30, TimeUnit.SECONDS);
            if (clearFailure.get() != null) {
                throw clearFailure.get();
            }
        }
    }

    private static Map<String, Object> searchStats() throws Exception {
        Response response = client().performRequest(new Request("GET", "/_stats/search"));
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

    @SuppressWarnings("unchecked")
    private static int getOpenContexts(Map<String, Object> stats, String index) {
        Map<String, Object> indices = (Map<String, Object>) stats.get("indices");
        Map<String, Object> indexStats = (Map<String, Object>) indices.get(index);
        Map<String, Object> total = (Map<String, Object>) indexStats.get("total");
        Map<String, Object> search = (Map<String, Object>) total.get("search");
        return (Integer) search.get("open_contexts");
    }
}

