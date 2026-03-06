/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class AsyncSearchBytesReadIT extends AsyncSearchIntegTestCase {

    @Before
    public void ensureDirectoryMetricsEnabled() {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    public void testBytesReadHeaderOnSyncCompletion() throws InterruptedException {
        String indexName = setupTestIndex();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .sort("id", SortOrder.ASC);
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchSourceBuilder, indexName);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMinutes(1));
        request.setKeepOnCompletion(false);

        long bytesRead = assertBytesReadHeaderOnAsyncSubmit(request);
        assertThat(bytesRead, greaterThan(0L));
    }

    public void testBytesReadHeaderOnAsyncGet() throws Exception {
        String indexName = setupTestIndex();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
            .sort("id", SortOrder.ASC);
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchSourceBuilder, indexName);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMinutes(1));
        request.setKeepOnCompletion(true);

        SetOnce<String> asyncSearchId = new SetOnce<>();
        assertBytesReadHeaderOnAsyncSubmit(request, response -> asyncSearchId.set(response.getId()));

        String searchId = asyncSearchId.get();
        assertThat(searchId, notNullValue());
        try {
            long bytesReadOnGet = assertBytesReadHeaderOnAsyncGet(searchId);
            assertThat(bytesReadOnGet, greaterThan(0L));
        } finally {
            deleteAsyncSearch(searchId);
        }
    }

    private String setupTestIndex() {
        String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        int numDocs = atLeast(200);
        List<IndexRequestBuilder> builders = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            builders.add(prepareIndex(indexName).setSource("field", "value value" + i, "id", i).setId(i + ""));
        }
        indexRandom(true, false, true, false, builders);
        flushAndRefresh(indexName);
        return indexName;
    }

    private long assertBytesReadHeaderOnAsyncSubmit(SubmitAsyncSearchRequest request) throws InterruptedException {
        return assertBytesReadHeaderOnAsyncSubmit(request, response -> {});
    }

    private long assertBytesReadHeaderOnAsyncSubmit(
        SubmitAsyncSearchRequest request,
        java.util.function.Consumer<AsyncSearchResponse> consumer
    ) throws InterruptedException {
        SetOnce<Long> bytesRead = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);

        final Client client = client();
        client.execute(SubmitAsyncSearchAction.INSTANCE, request, ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(AsyncSearchResponse response) {
                try {
                    Map<String, List<String>> responseHeaders = client.threadPool().getThreadContext().getResponseHeaders();
                    assertThat(responseHeaders, hasKey("X-Elasticsearch-Bytes-Read"));
                    assertThat(responseHeaders.get("X-Elasticsearch-Bytes-Read"), hasSize(1));
                    Long actual = Long.valueOf(responseHeaders.get("X-Elasticsearch-Bytes-Read").get(0));
                    assertThat(actual, greaterThan(0L));
                    bytesRead.set(actual);
                    consumer.accept(response);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
                fail("no error expected");
            }
        }));
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertThat(bytesRead.get(), notNullValue());
        return bytesRead.get();
    }

    private long assertBytesReadHeaderOnAsyncGet(String searchId) throws InterruptedException {
        SetOnce<Long> bytesRead = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);

        final Client client = client();
        GetAsyncResultRequest getRequest = new GetAsyncResultRequest(searchId);
        client.execute(GetAsyncSearchAction.INSTANCE, getRequest, ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(AsyncSearchResponse response) {
                try {
                    Map<String, List<String>> responseHeaders = client.threadPool().getThreadContext().getResponseHeaders();
                    assertThat(responseHeaders, hasKey("X-Elasticsearch-Bytes-Read"));
                    assertThat(responseHeaders.get("X-Elasticsearch-Bytes-Read"), hasSize(1));
                    Long actual = Long.valueOf(responseHeaders.get("X-Elasticsearch-Bytes-Read").get(0));
                    assertThat(actual, greaterThan(0L));
                    bytesRead.set(actual);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
                fail("no error expected");
            }
        }));
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertThat(bytesRead.get(), notNullValue());
        return bytesRead.get();
    }
}
