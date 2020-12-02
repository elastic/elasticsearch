/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        assertNotNull(submitResponse.Id());
        assertFalse(submitResponse.isPartial());
        assertTrue(submitResponse.StartTime() > 0);
        assertTrue(submitResponse.ExpirationTime() > 0);
        assertNotNull(submitResponse.SearchResponse());
        if (submitResponse.isRunning() == false) {
            assertFalse(submitResponse.isPartial());
        } else {
            assertTrue(submitResponse.isPartial());
        }

        GetAsyncSearchRequest getRequest = new GetAsyncSearchRequest(submitResponse.Id());
        AsyncSearchResponse getResponse = highLevelClient().asyncSearch().get(getRequest, RequestOptions.DEFAULT);
        while (getResponse.isRunning()) {
            getResponse = highLevelClient().asyncSearch().get(getRequest, RequestOptions.DEFAULT);
        }

        assertFalse(getResponse.isRunning());
        assertFalse(getResponse.isPartial());
        assertTrue(getResponse.StartTime() > 0);
        assertTrue(getResponse.ExpirationTime() > 0);
        assertNotNull(getResponse.SearchResponse());

        DeleteAsyncSearchRequest deleteRequest = new DeleteAsyncSearchRequest(submitResponse.Id());
        AcknowledgedResponse deleteAsyncSearchResponse = highLevelClient().asyncSearch().delete(deleteRequest,
                RequestOptions.DEFAULT);
        assertNotNull(deleteAsyncSearchResponse);
        assertNotNull(deleteAsyncSearchResponse.isAcknowledged());
    }
}
