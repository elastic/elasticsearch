/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

public final class RestCountActionTests extends RestActionTestCase {
    private RestCountAction action;

    @Before
    public void setUpAction() {
        action = new RestCountAction();
        controller().registerHandler(action);
    }

    /**
     * Test that the stats parameter is properly parsed and set on the search request
     */
    public void testStatsParameter() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("stats", "tag1,tag2,tag3");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/some_index/_count")
            .withParams(params)
            .build();

        AtomicReference<SearchRequest> searchRequestRef = new AtomicReference<>();

        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            searchRequestRef.set((SearchRequest) actionRequest);
            SearchHits hits = new SearchHits(new SearchHit[0], null, 0);
            SearchResponseSections searchResponseSections = new SearchResponseSections(
                hits,
                InternalAggregations.EMPTY,
                null,
                false,
                false,
                null,
                1
            );
            return new SearchResponse(
                searchResponseSections,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY,
                null
            );
        });

        action.handleRequest(request, new FakeRestChannel(request, randomBoolean(), 1), verifyingClient);

        SearchRequest capturedRequest = searchRequestRef.get();
        assertNotNull(capturedRequest.source());
        assertNotNull(capturedRequest.source().stats());
        assertThat(capturedRequest.source().stats(), hasItems("tag1", "tag2", "tag3"));
    }

    /**
     * Test count request without stats parameter (should not have stats set)
     */
    public void testNoStatsParameter() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/some_index/_count")
            .build();

        AtomicReference<SearchRequest> searchRequestRef = new AtomicReference<>();

        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            searchRequestRef.set((SearchRequest) actionRequest);
            SearchHits hits = new SearchHits(new SearchHit[0], null, 0);
            SearchResponseSections searchResponseSections = new SearchResponseSections(
                hits,
                InternalAggregations.EMPTY,
                null,
                false,
                false,
                null,
                1
            );
            return new SearchResponse(
                searchResponseSections,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY,
                null
            );
        });

        action.handleRequest(request, new FakeRestChannel(request, randomBoolean(), 1), verifyingClient);

        SearchRequest capturedRequest = searchRequestRef.get();
        assertNotNull(capturedRequest.source());
        assertNull(capturedRequest.source().stats());
    }

    /**
     * Test single stats tag
     */
    public void testSingleStatsTag() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("stats", "single_tag");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_count")
            .withParams(params)
            .build();

        AtomicReference<SearchRequest> searchRequestRef = new AtomicReference<>();

        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            searchRequestRef.set((SearchRequest) actionRequest);
            SearchHits hits = new SearchHits(new SearchHit[0], null, 0);
            SearchResponseSections searchResponseSections = new SearchResponseSections(
                hits,
                InternalAggregations.EMPTY,
                null,
                false,
                false,
                null,
                1
            );
            return new SearchResponse(
                searchResponseSections,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY,
                null
            );
        });

        action.handleRequest(request, new FakeRestChannel(request, randomBoolean(), 1), verifyingClient);

        SearchRequest capturedRequest = searchRequestRef.get();
        assertNotNull(capturedRequest.source());
        assertNotNull(capturedRequest.source().stats());
        assertThat(capturedRequest.source().stats().size(), equalTo(1));
        assertThat(capturedRequest.source().stats(), hasItems("single_tag"));
    }

    /**
     * Test empty stats parameter (should still set empty list)
     */
    public void testEmptyStatsParameter() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("stats", "");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_count")
            .withParams(params)
            .build();

        AtomicReference<SearchRequest> searchRequestRef = new AtomicReference<>();

        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            searchRequestRef.set((SearchRequest) actionRequest);
            SearchHits hits = new SearchHits(new SearchHit[0], null, 0);
            SearchResponseSections searchResponseSections = new SearchResponseSections(
                hits,
                InternalAggregations.EMPTY,
                null,
                false,
                false,
                null,
                1
            );
            return new SearchResponse(
                searchResponseSections,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY,
                null
            );
        });

        action.handleRequest(request, new FakeRestChannel(request, randomBoolean(), 1), verifyingClient);

        SearchRequest capturedRequest = searchRequestRef.get();
        assertNotNull(capturedRequest.source());
        assertNotNull(capturedRequest.source().stats());
        assertThat(capturedRequest.source().stats().size(), equalTo(1));
        assertThat(capturedRequest.source().stats().get(0), equalTo(""));
    }
}
