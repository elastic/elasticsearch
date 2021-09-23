/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.graph.rest.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.protocol.xpack.graph.GraphExploreRequest;
import org.elasticsearch.protocol.xpack.graph.GraphExploreResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class RestGraphActionTests extends RestActionTestCase {
    private final List<String> compatibleMediaType = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGraphAction());
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(GraphExploreRequest.class));
            return Mockito.mock(GraphExploreResponse.class);
        });
    }

    public void testTypeInPath() {
        for (Tuple<RestRequest.Method, String> methodAndPath :
            List.of(
                Tuple.tuple(RestRequest.Method.GET, "/some_index/some_type/_graph/explore"),
                Tuple.tuple(RestRequest.Method.POST, "/some_index/some_type/_graph/explore"),
                Tuple.tuple(RestRequest.Method.GET, "/some_index/some_type/_xpack/graph/_explore"),
                Tuple.tuple(RestRequest.Method.POST, "/some_index/some_type/_xpack/graph/_explore")
            )) {

            RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
                .withHeaders(Map.of("Accept", compatibleMediaType, "Content-Type",
                    Collections.singletonList(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7))))
                .withMethod(methodAndPath.v1())
                .withPath(methodAndPath.v2())
                .withContent(new BytesArray("{}"), null)
                .build();

            dispatchRequest(request);
            assertWarnings(RestGraphAction.TYPES_DEPRECATION_MESSAGE);
        }
    }
}
