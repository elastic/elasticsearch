/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.asyncsearch.AsyncSearchResponse;
import org.elasticsearch.client.asyncsearch.DeleteAsyncSearchRequest;
import org.elasticsearch.client.asyncsearch.GetAsyncSearchRequest;
import org.elasticsearch.client.asyncsearch.SubmitAsyncSearchRequest;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Documentation for Async Search APIs in the high level java client.
 * Code wrapped in {@code tag} and {@code end} tags is included in the docs.
 */
public class AsyncSearchDocumentationIT extends ESRestHighLevelClientTestCase {

    @Before void setUpIndex() throws IOException {
        CreateIndexResponse createIndexResponse = highLevelClient().indices().create(new CreateIndexRequest("my-index"),
            RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());
    }

    public void testSubmitAsyncSearch() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // tag::asyncsearch-submit-request
        SearchSourceBuilder searchSource = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery()); // <1>
        String[] indices = new String[] { "my-index" }; // <2>
        SubmitAsyncSearchRequest request
                = new SubmitAsyncSearchRequest(searchSource, indices);
        // end::asyncsearch-submit-request

        // tag::asyncsearch-submit-request-arguments
        request.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(30)); // <1>
        request.setKeepAlive(TimeValue.timeValueMinutes(15)); // <2>
        request.setKeepOnCompletion(false); // <3>
        // end::asyncsearch-submit-request-arguments

        // tag::asyncsearch-submit-execute
        AsyncSearchResponse response = client.asyncSearch()
                .submit(request, RequestOptions.DEFAULT); // <1>
        // end::asyncsearch-submit-execute

        assertNotNull(response);
        assertNull(response.getFailure());

        // tag::asyncsearch-submit-response
        response.getSearchResponse(); // <1>
        response.getId(); // <2>
        response.isPartial(); // <3>
        response.isRunning(); // <4>
        response.getStartTime(); // <5>
        response.getExpirationTime(); // <6>
        response.getFailure(); // <7>
        // end::asyncsearch-submit-response


        // tag::asyncsearch-submit-listener
        ActionListener<AsyncSearchResponse> listener =
            new ActionListener<AsyncSearchResponse>() {
                @Override
                public void onResponse(AsyncSearchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::asyncsearch-submit-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::asyncsearch-submit-execute-async
        client.asyncSearch()
            .submitAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::asyncsearch-submit-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetAsyncSearch() throws Exception {
        RestHighLevelClient client = highLevelClient();
        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        String[] indices = new String[] { "my-index" };
        SubmitAsyncSearchRequest submitRequest = new SubmitAsyncSearchRequest(searchSource, indices);
        submitRequest.setKeepOnCompletion(true);
        AsyncSearchResponse submitResponse = client.asyncSearch().submit(submitRequest, RequestOptions.DEFAULT);
        String id = submitResponse.getId();

        // tag::asyncsearch-get-request
        GetAsyncSearchRequest request = new GetAsyncSearchRequest(id);
        // end::asyncsearch-get-request

        // tag::asyncsearch-get-request-arguments
        request.setWaitForCompletion(TimeValue.timeValueSeconds(30)); // <1>
        request.setKeepAlive(TimeValue.timeValueMinutes(15)); // <2>
        // end::asyncsearch-get-request-arguments

        // tag::asyncsearch-get-execute
        AsyncSearchResponse response = client.asyncSearch()
                .get(request, RequestOptions.DEFAULT); // <1>
        // end::asyncsearch-get-execute

        assertNotNull(response);
        assertNull(response.getFailure());

        // tag::asyncsearch-get-response
        response.getSearchResponse(); // <1>
        response.getId(); // <2>
        response.isPartial(); // <3>
        response.isRunning(); // <4>
        response.getStartTime(); // <5>
        response.getExpirationTime(); // <6>
        response.getFailure(); // <7>
        // end::asyncsearch-get-response


        // tag::asyncsearch-get-listener
        ActionListener<AsyncSearchResponse> listener =
            new ActionListener<AsyncSearchResponse>() {
                @Override
                public void onResponse(AsyncSearchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::asyncsearch-get-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::asyncsearch-get-execute-async
        client.asyncSearch()
            .getAsync(request, RequestOptions.DEFAULT, listener);  // <1>
        // end::asyncsearch-get-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
        client.asyncSearch().delete(new DeleteAsyncSearchRequest(id), RequestOptions.DEFAULT);
    }

    @SuppressWarnings("unused")
    public void testDeleteAsyncSearch() throws Exception {
        RestHighLevelClient client = highLevelClient();
        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        String[] indices = new String[] { "my-index" };
        SubmitAsyncSearchRequest submitRequest = new SubmitAsyncSearchRequest(searchSource, indices);
        submitRequest.setKeepOnCompletion(true);
        AsyncSearchResponse submitResponse = client.asyncSearch().submit(submitRequest, RequestOptions.DEFAULT);
        String id = submitResponse.getId();

        // tag::asyncsearch-delete-request
        DeleteAsyncSearchRequest request = new DeleteAsyncSearchRequest(id);
        // end::asyncsearch-delete-request

        // tag::asyncsearch-delete-execute
        AcknowledgedResponse response = client.asyncSearch() // <1>
                .delete(new DeleteAsyncSearchRequest(id),
                        RequestOptions.DEFAULT);
        // end::asyncsearch-delete-execute

        assertNotNull(response);
        assertTrue(response.isAcknowledged());

        // tag::asyncsearch-delete-response
        response.isAcknowledged(); // <1>
        // end::asyncsearch-delete-response


        // tag::asyncsearch-delete-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::asyncsearch-delete-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::asyncsearch-delete-execute-async
        client.asyncSearch()
            .deleteAsync(request, RequestOptions.DEFAULT, listener);  // <1>
        // end::asyncsearch-delete-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));

    }
}
