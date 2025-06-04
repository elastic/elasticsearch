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
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestTests;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.InferenceActionProxy;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.inference.rest.BaseInferenceAction.parseParams;
import static org.elasticsearch.xpack.inference.rest.BaseInferenceAction.parseTimeout;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_OR_INFERENCE_ID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BaseInferenceActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new BaseInferenceAction() {
            @Override
            protected boolean shouldStream() {
                return false;
            }

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

    public void testParseParams_ExtractsInferenceIdAndTaskType() {
        var params = parseParams(
            RestRequestTests.contentRestRequest("{}", Map.of(INFERENCE_ID, "id", TASK_TYPE_OR_INFERENCE_ID, TaskType.COMPLETION.toString()))
        );
        assertThat(params, is(new BaseInferenceAction.Params("id", TaskType.COMPLETION)));
    }

    public void testParseParams_DefaultsToTaskTypeAny_WhenInferenceId_IsMissing() {
        var params = parseParams(
            RestRequestTests.contentRestRequest("{}", Map.of(TASK_TYPE_OR_INFERENCE_ID, TaskType.COMPLETION.toString()))
        );
        assertThat(params, is(new BaseInferenceAction.Params("completion", TaskType.ANY)));
    }

    public void testParseParams_ThrowsStatusException_WhenTaskTypeIsMissing() {
        var e = expectThrows(
            ElasticsearchStatusException.class,
            () -> parseParams(RestRequestTests.contentRestRequest("{}", Map.of(INFERENCE_ID, "id")))
        );
        assertThat(e.getMessage(), is("Task type must not be null"));
    }

    public void testParseTimeout_ReturnsTimeout() {
        var timeout = parseTimeout(
            RestRequestTests.contentRestRequest("{}", Map.of(InferenceAction.Request.TIMEOUT.getPreferredName(), "4s"))
        );

        assertThat(timeout, is(TimeValue.timeValueSeconds(4)));
    }

    public void testParseTimeout_ReturnsDefaultTimeout() {
        var timeout = parseTimeout(RestRequestTests.contentRestRequest("{}", Map.of()));

        assertThat(timeout, is(TimeValue.timeValueSeconds(30)));
    }

    public void testUsesDefaultTimeout() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(InferenceActionProxy.Request.class));

            var request = (InferenceActionProxy.Request) actionRequest;
            assertThat(request.getTimeout(), is(InferenceAction.Request.DEFAULT_TIMEOUT));

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
            assertThat(actionRequest, instanceOf(InferenceActionProxy.Request.class));

            var request = (InferenceActionProxy.Request) actionRequest;
            assertThat(request.getTimeout(), is(TimeValue.timeValueSeconds(3)));

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

    public void testExtractProductUseCase() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        String productUseCase = "product-use-case";

        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(InferenceActionProxy.Request.class));

            var request = (InferenceActionProxy.Request) actionRequest;
            InferenceContext context = request.getContext();
            assertNotNull(context);
            assertThat(context.productUseCase(), equalTo(productUseCase));

            executeCalled.set(true);
            return createResponse();
        }));

        // Create a request with the product use case header
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, List.of(productUseCase));

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(route("test"))
            .withHeaders(headers)
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();

        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testExtractProductUseCase_EmptyWhenHeaderMissing() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();

        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(InferenceActionProxy.Request.class));

            var request = (InferenceActionProxy.Request) actionRequest;
            InferenceContext context = request.getContext();
            assertNotNull(context);
            assertThat(context.productUseCase(), equalTo(""));

            executeCalled.set(true);
            return createResponse();
        }));

        // Create a request without the product use case header
        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(route("test"))
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();

        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testExtractProductUseCase_EmptyWhenHeaderValueEmpty() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();

        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(InferenceActionProxy.Request.class));

            var request = (InferenceActionProxy.Request) actionRequest;
            InferenceContext context = request.getContext();
            assertNotNull(context);
            assertThat(context.productUseCase(), equalTo(""));

            executeCalled.set(true);
            return createResponse();
        }));

        // Create a request with an empty product use case header value
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER, List.of(""));

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(route("test"))
            .withHeaders(headers)
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();

        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    static InferenceAction.Response createResponse() {
        return new InferenceAction.Response(
            new TextEmbeddingByteResults(List.of(new TextEmbeddingByteResults.Embedding(new byte[] { (byte) -1 })))
        );
    }
}
