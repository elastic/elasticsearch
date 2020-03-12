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

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AsyncSearchIT extends ESRestHighLevelClientTestCase {

    public void testSubmitAsyncSearchRequest() throws IOException {
        String index = "test-index";
        createIndex(index, Settings.EMPTY);

        SearchRequest searchRequest = new SearchRequest(index);
        // TODO either check that in client request validation already or set it automatically?
        searchRequest.setCcsMinimizeRoundtrips(false);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        searchRequest.source(sourceBuilder);
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        // 2 sec should be enough to make sure we always complete right away
        request.setWaitForCompletion(new TimeValue(2, TimeUnit.SECONDS));
        SubmitAsyncSearchResponse response = highLevelClient().asyncSearch().submitAsyncSearch(request, RequestOptions.DEFAULT);
        assertNull(response.getId());
        assertTrue(response.getVersion() >= 0);
        assertFalse(response.isRunning());
        assertFalse(response.isPartial());
        assertTrue(response.getStartTime() > 0);
        assertTrue(response.getExpirationTime() > 0);
        assertNotNull(response.getSearchResponse());

        // try very short wait to force partial response
        request.setWaitForCompletion(new TimeValue(1, TimeUnit.MILLISECONDS));
        response = highLevelClient().asyncSearch().submitAsyncSearch(request, RequestOptions.DEFAULT);
        assertNotNull(response.getId());
        assertTrue(response.getVersion() >= 0);
        assertTrue(response.isRunning());
        assertTrue(response.isPartial());
        assertTrue(response.getStartTime() > 0);
        assertTrue(response.getExpirationTime() > 0);
        assertNotNull(response.getSearchResponse());
    }

}
