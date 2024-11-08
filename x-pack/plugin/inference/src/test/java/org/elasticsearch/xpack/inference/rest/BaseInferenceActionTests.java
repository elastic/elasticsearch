/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingByteResults;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BaseInferenceActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new BaseInferenceAction() {
            @Override
            protected ActionListener<InferenceAction.Response> listener(RestChannel channel) {
                return new RestChunkedToXContentListener<>(channel);
            }

            @Override
            public String getName() {
                return "base_inference_action";
            }

            @Override
            public List<Route> routes() {
                return List.of(new Route(POST, route("{task_type_or_id}")));
            }
        });
    }

    private static String route(String param) {
        return "_route/" + param;
    }

    public void testUsesDefaultTimeout() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(InferenceAction.Request.class));

            var request = (InferenceAction.Request) actionRequest;
            assertThat(request.getInferenceTimeout(), is(InferenceAction.Request.DEFAULT_TIMEOUT));

            executeCalled.set(true);
            return createResponse();
        }));

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(route("test"))
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testUses3SecondTimeoutFromParams() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(InferenceAction.Request.class));

            var request = (InferenceAction.Request) actionRequest;
            assertThat(request.getInferenceTimeout(), is(TimeValue.timeValueSeconds(3)));

            executeCalled.set(true);
            return createResponse();
        }));

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(route("test"))
            .withParams(new HashMap<>(Map.of("timeout", "3s")))
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    static InferenceAction.Response createResponse() {
        return new InferenceAction.Response(
            new InferenceTextEmbeddingByteResults(
                List.of(new InferenceTextEmbeddingByteResults.InferenceByteEmbedding(new byte[] { (byte) -1 }))
            )
        );
    }
}
