/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.asyncsearch;

import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class AsyncSearchIT extends ESRestHighLevelClientTestCase {

    public void testAsyncSearch() throws IOException {
        String index = "test-index";
        createIndex(index, Settings.EMPTY);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        SubmitAsyncSearchRequest submitRequest = new SubmitAsyncSearchRequest(sourceBuilder, index);
        submitRequest.setKeepOnCompletion(true);
        AsyncSearchResponse submitResponse = highLevelClient().asyncSearch().submit(submitRequest, RequestOptions.DEFAULT);
        assertNotNull(submitResponse.getId());
        assertFalse(submitResponse.isPartial());
        assertTrue(submitResponse.getStartTime() > 0);
        assertTrue(submitResponse.getExpirationTime() > 0);
        assertNotNull(submitResponse.getSearchResponse());
        if (submitResponse.isRunning() == false) {
            assertFalse(submitResponse.isPartial());
        } else {
            assertTrue(submitResponse.isPartial());
        }

        GetAsyncSearchRequest getRequest = new GetAsyncSearchRequest(submitResponse.getId());
        AsyncSearchResponse getResponse = highLevelClient().asyncSearch().get(getRequest, RequestOptions.DEFAULT);
        while (getResponse.isRunning()) {
            getResponse = highLevelClient().asyncSearch().get(getRequest, RequestOptions.DEFAULT);
        }

        assertFalse(getResponse.isRunning());
        assertFalse(getResponse.isPartial());
        assertTrue(getResponse.getStartTime() > 0);
        assertTrue(getResponse.getExpirationTime() > 0);
        assertNotNull(getResponse.getSearchResponse());

        DeleteAsyncSearchRequest deleteRequest = new DeleteAsyncSearchRequest(submitResponse.getId());
        AcknowledgedResponse deleteAsyncSearchResponse = highLevelClient().asyncSearch().delete(deleteRequest,
                RequestOptions.DEFAULT);
        assertNotNull(deleteAsyncSearchResponse);
        assertNotNull(deleteAsyncSearchResponse.isAcknowledged());
    }
}
