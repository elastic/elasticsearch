/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.graph.rest.action;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.protocol.xpack.graph.GraphExploreResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.HashMap;

public class RestGraphActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGraphAction());
    }

    public void testTypeInPath() {
        for (Tuple<RestRequest.Method, String> methodAndPath : org.elasticsearch.core.List.of(
            Tuple.tuple(RestRequest.Method.GET, "/some_index/some_type/_graph/explore"),
            Tuple.tuple(RestRequest.Method.POST, "/some_index/some_type/_graph/explore"),
            Tuple.tuple(RestRequest.Method.GET, "/some_index/some_type/_xpack/graph/_explore"),
            Tuple.tuple(RestRequest.Method.POST, "/some_index/some_type/_xpack/graph/_explore")
        )) {

            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(methodAndPath.v1())
                .withPath(methodAndPath.v2())
                .withContent(new BytesArray("{}"), XContentType.JSON)
                .build();
            // We're not actually testing anything to do with the client,
            // but need to set this so it doesn't fail the test for being unset.
            verifyingClient.setExecuteVerifier(
                (arg1, arg2) -> new GraphExploreResponse(
                    0,
                    false,
                    new ShardOperationFailedException[0],
                    new HashMap<>(),
                    new HashMap<>(),
                    false
                )
            );

            dispatchRequest(request);
            assertWarnings(RestGraphAction.TYPES_DEPRECATION_MESSAGE);
        }
    }
}
