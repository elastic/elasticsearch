/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ManyRequestsIT extends HeapAttackTestCase {

    /**
     * Submit as many requests as possible, then the cluster should reject instead of OOM
     */
    public void testManyRequests() throws Exception {
        createPausableIndex();
        blockPauseField();
        int queueSize = getSearchQueueSize();
        long queryLength = ByteSizeValue.ofMb(Clusters.HEAP_SIZE_IN_MB).getBytes() / queueSize;
        List<String> asyncIds = new ArrayList<>();
        logger.info("queue size {}", queueSize);
        try {
            int totalRequests = queueSize * 5;
            for (int i = 0; i < totalRequests; i++) {
                String comment = "q" + i + " ";
                String pad = comment.repeat(Math.toIntExact(queryLength / comment.length()));
                String query = query(pad);
                try {
                    String id = submitAsyncQuery(query);
                    logger.info("--> submitted query={} length={} id={}", i, query.length(), id);
                    asyncIds.add(id);
                } catch (ResponseException ex) {
                    int statusCode = ex.getResponse().getStatusLine().getStatusCode();
                    String resp = EntityUtils.toString(ex.getResponse().getEntity());
                    logger.info("--> failed to submit query {} response {} status {}", i, resp, statusCode);
                    assertThat(resp, statusCode, equalTo(HttpStatus.SC_TOO_MANY_REQUESTS));
                    break;
                }
            }
        } finally {
            unblockPauseField();
            for (int i = 0; i < asyncIds.size(); i++) {
                String id = asyncIds.get(i);
                logger.info("--> stopping query={} id={}", i, id);
                deleteAsyncQuery(id);
            }
            for (String id : asyncIds) {
                ensureAsyncQueryCompleted(id);
            }
        }
    }

    /**
     * Cancelling tail requests should not have to wait for requests ahead in the queue.
     */
    public void testCancelTailRequest() throws Exception {
        createPausableIndex();
        blockPauseField();
        int batchSize = between(100, 200);
        List<String> asyncIds = new ArrayList<>();
        try {
            for (int i = 0; i < batchSize; i++) {
                String query = query("q-" + i);
                String id = submitAsyncQuery(query);
                asyncIds.add(id);
            }
            // cancelling requests in the queue should not wait for the running requests or those ahead in the queue
            int tailRequests = between(10, 20);
            for (int i = batchSize - tailRequests; i < batchSize; i++) {
                String id = asyncIds.get(i);
                logger.info("--> cancelling query={} id={}", i, id);
                deleteAsyncQuery(id);
            }
            for (int i = batchSize - tailRequests; i < batchSize; i++) {
                String id = asyncIds.get(i);
                ensureAsyncQueryCompleted(id);
            }
            batchSize -= tailRequests;
        } finally {
            unblockPauseField();
            for (int i = 0; i < batchSize; i++) {
                String id = asyncIds.get(i);
                logger.info("--> stopping query={} id={}", i, id);
                deleteAsyncQuery(id);
            }
            for (int i = 0; i < batchSize; i++) {
                String id = asyncIds.get(i);
                ensureAsyncQueryCompleted(id);
            }
        }
    }

    private void createPausableIndex() throws IOException {
        Request createIndex = new Request("PUT", "/pause_test");
        createIndex.setJsonEntity("""
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "runtime": {
                        "pause_long_field": {
                            "type": "long",
                            "script": {
                                "source": "",
                                "lang": "pause"
                            }
                        }
                    },
                    "properties": {
                        "tag": {
                            "type": "keyword"
                        }
                    }
                }
            }""");
        client().performRequest(createIndex);
        Request bulk = new Request("POST", "/pause_test/_bulk?refresh=true");
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            body.append("{\"index\":{}}\n").append("{\"tag\":\"tag-").append(i).append("\"}\n");
        }
        bulk.setJsonEntity(body.toString());
        client().performRequest(bulk);
    }

    private String query(String comment) {
        return "FROM pause_test | STATS SUM(pause_long_field) | LIMIT 1 /* " + comment + " */";
    }

    private String submitAsyncQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query/async");
        request.setJsonEntity("{\"query\": \"" + query + "\", \"wait_for_completion_timeout\": \"10ms\", \"keep_on_completion\": false}");
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = entityAsMap(response);
        return (String) responseMap.get("id");
    }

    private void deleteAsyncQuery(String id) throws Exception {
        assertBusy(() -> {
            try {
                client().performRequest(new Request("DELETE", "/_query/async/" + id));
            } catch (ResponseException ex) {
                int statusCode = ex.getResponse().getStatusLine().getStatusCode();
                if (statusCode != HttpStatus.SC_NOT_FOUND) {
                    throw ex;
                }
            }
        });
    }

    private void ensureAsyncQueryCompleted(String id) throws Exception {
        assertBusy(() -> {
            try {
                client().performRequest(new Request("GET", "/_query/async/" + id));
                fail("expected async query [" + id + "] to be gone");
            } catch (ResponseException ex) {
                int statusCode = ex.getResponse().getStatusLine().getStatusCode();
                // the request might have been rejected (429) or already deleted
                if (statusCode != HttpStatus.SC_TOO_MANY_REQUESTS && statusCode != HttpStatus.SC_NOT_FOUND) {
                    throw ex;
                }
            }
        });
    }

    private void blockPauseField() throws IOException {
        client().performRequest(new Request("POST", "/_pause_field/block"));
    }

    private void unblockPauseField() throws IOException {
        client().performRequest(new Request("POST", "/_pause_field/unblock"));
    }

    @SuppressWarnings("unchecked")
    private int getSearchQueueSize() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_nodes/thread_pool"));
        Map<String, Object> nodes = (Map<String, Object>) entityAsMap(response).get("nodes");
        Map<String, Object> firstNode = (Map<String, Object>) nodes.values().iterator().next();
        return (int) XContentMapValues.extractValue("thread_pool.search.queue_size", firstNode);
    }
}
