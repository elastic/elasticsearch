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
import org.elasticsearch.client.asyncsearch.SubmitAsyncSearchRequest;
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

        setRandomSearchParams(submitRequest.getSearchRequest(), expectedParams);
        setRandomIndicesOptions(submitRequest.getSearchRequest()::indicesOptions,
                submitRequest.getSearchRequest()::indicesOptions, expectedParams);
        // some properties of theSearchRequest are always overwritten in the submit request,
        // so we need to adapt the expected parameters accordingly for tests to pass
        expectedParams.put("ccs_minimize_roundtrips", "false");
        expectedParams.put("request_cache", "true");
        expectedParams.put("batched_reduce_size", "5");
        expectedParams.put("pre_filter_shard_size", "1");

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

    private static void setRandomSearchParams(SearchRequest searchRequest, Map<String, String> expectedParams) {
        expectedParams.put(RestSearchAction.TYPED_KEYS_PARAM, "true");
        if (randomBoolean()) {
            searchRequest.routing(randomAlphaOfLengthBetween(3, 10));
            expectedParams.put("routing", searchRequest.routing());
        }
        if (randomBoolean()) {
            searchRequest.preference(randomAlphaOfLengthBetween(3, 10));
            expectedParams.put("preference", searchRequest.preference());
        }
        if (randomBoolean()) {
            searchRequest.searchType(randomFrom(SearchType.CURRENTLY_SUPPORTED));
        }
        expectedParams.put("search_type", searchRequest.searchType().name().toLowerCase(Locale.ROOT));
        if (randomBoolean()) {
            searchRequest.allowPartialSearchResults(randomBoolean());
            expectedParams.put("allow_partial_search_results", Boolean.toString(searchRequest.allowPartialSearchResults()));
        }
    }

}
