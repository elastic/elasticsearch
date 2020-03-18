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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

import static org.elasticsearch.client.RequestConvertersTests.createTestSearchSourceBuilder;
import static org.elasticsearch.client.RequestConvertersTests.setRandomIndicesOptions;

public class AsyncSearchRequestConvertersTests extends ESTestCase {

    public void testSubmitAsyncSearch() throws Exception {
        String[] indices = RequestConvertersTests.randomIndicesNames(0, 5);
        Map<String, String> expectedParams = new HashMap<>();
        SearchSourceBuilder searchSourceBuilder = createTestSearchSourceBuilder();
        SubmitAsyncSearchRequest submitRequest = new SubmitAsyncSearchRequest(searchSourceBuilder, indices);

        // the following parameters might be overwritten by random ones later,
        // but we need to set these since they are the default we send over http
        expectedParams.put("request_cache", "true");
        expectedParams.put("batched_reduce_size", "5");
        SearchRequest searchRequest = submitRequest.getSearchRequest();
        setRandomSearchParams(searchRequest, expectedParams);
        setRandomIndicesOptions(searchRequest::indicesOptions, searchRequest::indicesOptions, expectedParams);

        if (randomBoolean()) {
            boolean cleanOnCompletion = randomBoolean();
            submitRequest.setCleanOnCompletion(cleanOnCompletion);
            expectedParams.put("clean_on_completion", Boolean.toString(cleanOnCompletion));
        }
        if (randomBoolean()) {
            TimeValue keepAlive = TimeValue.parseTimeValue(randomTimeValue(), "test");
            submitRequest.setKeepAlive(keepAlive);
            expectedParams.put("keep_alive", keepAlive.getStringRep());
        }
        if (randomBoolean()) {
            TimeValue waitForCompletion = TimeValue.parseTimeValue(randomTimeValue(), "test");
            submitRequest.setWaitForCompletion(waitForCompletion);
            expectedParams.put("wait_for_completion", waitForCompletion.getStringRep());
        }

        Request request = AsyncSearchRequestConverters.submitAsyncSearch(submitRequest);
        StringJoiner endpoint = new StringJoiner("/", "/", "");
        String index = String.join(",", indices);
        if (Strings.hasLength(index)) {
            endpoint.add(index);
        }
        endpoint.add("_async_search");
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(endpoint.toString(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        RequestConvertersTests.assertToXContentBody(searchSourceBuilder, request.getEntity());
    }

    private static void setRandomSearchParams(SearchRequest request, Map<String, String> expectedParams) {
        expectedParams.put(RestSearchAction.TYPED_KEYS_PARAM, "true");
        if (randomBoolean()) {
            request.routing(randomAlphaOfLengthBetween(3, 10));
            expectedParams.put("routing", request.routing());
        }
        if (randomBoolean()) {
            request.preference(randomAlphaOfLengthBetween(3, 10));
            expectedParams.put("preference", request.preference());
        }
        if (randomBoolean()) {
            request.searchType(randomFrom(SearchType.CURRENTLY_SUPPORTED));
        }
        expectedParams.put("search_type", request.searchType().name().toLowerCase(Locale.ROOT));
        if (randomBoolean()) {
            request.allowPartialSearchResults(randomBoolean());
            expectedParams.put("allow_partial_search_results", Boolean.toString(request.allowPartialSearchResults()));
        }
        if (randomBoolean()) {
            request.requestCache(randomBoolean());
            expectedParams.put("request_cache", Boolean.toString(request.requestCache()));
        }
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, Integer.MAX_VALUE));
        }
        expectedParams.put("batched_reduce_size", Integer.toString(request.getBatchedReduceSize()));
    }

}
