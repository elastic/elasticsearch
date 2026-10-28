/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceActionProxy;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.rest.RestResponseUtils.getBodyContent;
import static org.elasticsearch.xpack.inference.rest.BaseInferenceActionTests.createResponse;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestInferenceActionTests extends RestActionTestCase {
    private static final String ERROR_MESSAGE = "inference endpoint [does-not-exist] not found";
    private static final String ERROR_TYPE = "resource_not_found_exception";
    private static final String ERROR_CODE = "not_found";
    private static final String OPENAI_ERROR_SHAPE = Strings.format("""
        {
          "error": {
            "code": "%s",
            "message": "%s",
            "type": "%s"
          }
        }
        """, ERROR_CODE, ERROR_MESSAGE, ERROR_TYPE);

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestInferenceAction());
    }

    public void testStreamIsFalse() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(InferenceActionProxy.Request.class));

            var request = (InferenceActionProxy.Request) actionRequest;
            assertFalse(request.isStreaming());

            executeCalled.set(true);
            return createResponse();
        }));

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("_inference/test")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    /**
     * A {@link UnifiedChatCompletionException} must render its own OpenAI-compatible body here, exactly as it does on the
     * {@code _stream} route, rather than the standard error envelope that {@code RestActionListener#onFailure} would produce.
     */
    public void testFailure_UnifiedChatCompletionException_RendersOpenAiErrorShape() throws IOException {
        var response = failureResponse(new UnifiedChatCompletionException(RestStatus.NOT_FOUND, ERROR_MESSAGE, ERROR_TYPE, ERROR_CODE));

        assertThat(response.status(), is(RestStatus.NOT_FOUND));
        assertThat(getBodyContent(response).utf8ToString(), is(XContentHelper.stripWhitespace(OPENAI_ERROR_SHAPE)));
    }

    /**
     * Transport wrapping must not hide the formatted exception, since {@code ExceptionsHelper#unwrapCause} is what finds it.
     */
    public void testFailure_WrappedUnifiedChatCompletionException_RendersOpenAiErrorShape() throws IOException {
        var response = failureResponse(
            new RemoteTransportException(
                "wrapper",
                new UnifiedChatCompletionException(RestStatus.NOT_FOUND, ERROR_MESSAGE, ERROR_TYPE, ERROR_CODE)
            )
        );

        assertThat(response.status(), is(RestStatus.NOT_FOUND));
        assertThat(getBodyContent(response).utf8ToString(), is(XContentHelper.stripWhitespace(OPENAI_ERROR_SHAPE)));
    }

    /**
     * Every task type other than {@code chat_completion} keeps the standard error envelope.
     */
    public void testFailure_PlainException_RendersStandardErrorEnvelope() throws IOException {
        var response = failureResponse(new ElasticsearchStatusException(ERROR_MESSAGE, RestStatus.BAD_REQUEST));

        assertThat(response.status(), is(RestStatus.BAD_REQUEST));
        assertThat(getBodyContent(response).utf8ToString(), is(XContentHelper.stripWhitespace(Strings.format("""
            {
              "error": {
                "root_cause": [
                  {
                    "type": "status_exception",
                    "reason": "%s"
                  }
                ],
                "type": "status_exception",
                "reason": "%s"
              },
              "status": 400
            }
            """, ERROR_MESSAGE, ERROR_MESSAGE))));
    }

    private RestResponse failureResponse(Exception e) {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("_inference/chat_completion/test")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        var channel = new FakeRestChannel(request, true);

        new RestInferenceAction().listener(channel).onFailure(e);

        var response = channel.capturedResponse();
        assertNotNull(response);
        return response;
    }
}
