/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceActionProxy;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.xpack.inference.rest.BaseInferenceActionTests.createResponse;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestStreamInferenceActionTests extends RestActionTestCase {
    private final SetOnce<ThreadPool> threadPool = new SetOnce<>();

    @Before
    public void setUpAction() {
        threadPool.set(new TestThreadPool(getTestName()));
        controller().registerHandler(new RestStreamInferenceAction(threadPool));
    }

    @After
    public void tearDownAction() {
        terminate(threadPool.get());

    }

    public void testStreamIsTrue() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(InferenceActionProxy.Request.class));

            var request = (InferenceActionProxy.Request) actionRequest;
            assertThat(request.isStreaming(), is(true));

            executeCalled.set(true);
            return createResponse();
        }));

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("_inference/test/_stream")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testStreamIsTrue_ChatCompletion() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(InferenceActionProxy.Request.class));

            var request = (InferenceActionProxy.Request) actionRequest;
            assertThat(request.isStreaming(), is(true));

            executeCalled.set(true);
            return createResponse();
        }));

        var requestBody = """
            {
              "messages": [
                {
                  "content": "abc",
                  "role": "user"
                }
              ]
            }
            """;

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("_inference/chat_completion/test/_stream")
            .withContent(new BytesArray(requestBody), XContentType.JSON)
            .build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        dispatchRequest(inferenceRequest, new AbstractRestChannel(inferenceRequest, true) {
            @Override
            public void sendResponse(RestResponse response) {
                responseSetOnce.set(response);
            }
        });

        // the response content will be null when there is no error
        assertNull(responseSetOnce.get().content());
        assertThat(executeCalled.get(), equalTo(true));
    }

    private void dispatchRequest(final RestRequest request, final RestChannel channel) {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        controller().dispatchRequest(request, channel, threadContext);
    }
}
