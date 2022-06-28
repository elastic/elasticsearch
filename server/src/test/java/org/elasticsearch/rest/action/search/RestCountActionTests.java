/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class RestCountActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestCountAction());
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(Method.POST)
            .withPath("/some_index/some_type/_count")
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier(
            (arg1, arg2) -> new SearchResponse(
                new SearchResponseSections(
                    new SearchHits(new SearchHit[0], new TotalHits(5, TotalHits.Relation.EQUAL_TO), 0.0f),
                    null,
                    null,
                    false,
                    null,
                    null,
                    1
                ),
                null,
                1,
                1,
                0,
                10,
                new ShardSearchFailure[0],
                null
            )
        );

        dispatchRequest(request);
        assertWarnings(RestCountAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeParameter() {
        Map<String, String> params = new HashMap<>();
        params.put("type", "some_type");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(Method.GET)
            .withPath("/some_index/_count")
            .withParams(params)
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier(
            (arg1, arg2) -> new SearchResponse(
                new SearchResponseSections(
                    new SearchHits(new SearchHit[0], new TotalHits(5, TotalHits.Relation.EQUAL_TO), 0.0f),
                    null,
                    null,
                    false,
                    null,
                    null,
                    1
                ),
                null,
                1,
                1,
                0,
                10,
                new ShardSearchFailure[0],
                null
            )
        );

        dispatchRequest(request);
        assertWarnings(RestCountAction.TYPES_DEPRECATION_MESSAGE);
    }
}
