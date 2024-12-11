/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.googleaistudio;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseParser;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.response.googleaistudio.GoogleAiStudioErrorResponseEntity;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventProcessor;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.IOException;
import java.util.concurrent.Flow;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.HttpUtils.checkForEmptyBody;

public class GoogleAiStudioResponseHandler extends BaseResponseHandler {

    static final String GOOGLE_AI_STUDIO_UNAVAILABLE = "The Google AI Studio service may be temporarily overloaded or down";
    private final boolean canHandleStreamingResponses;
    private final CheckedFunction<XContentParser, String, IOException> content;

    public GoogleAiStudioResponseHandler(String requestType, ResponseParser parseFunction) {
        this(requestType, parseFunction, false, xContentParser -> {
            assert false : "do not call this";
            return "";
        });
    }

    public GoogleAiStudioResponseHandler(
        String requestType,
        ResponseParser parseFunction,
        boolean canHandleStreamingResponses,
        CheckedFunction<XContentParser, String, IOException> content
    ) {
        super(requestType, parseFunction, GoogleAiStudioErrorResponseEntity::fromResponse);
        this.canHandleStreamingResponses = canHandleStreamingResponses;
        this.content = content;
    }

    @Override
    public void validateResponse(ThrottlerManager throttlerManager, Logger logger, Request request, HttpResult result)
        throws RetryException {
        checkForFailureStatusCode(request, result);
        checkForEmptyBody(throttlerManager, logger, request, result);
    }

    /**
     * Validates the status code and throws a RetryException if not in the range [200, 300).
     *
     * The Google AI Studio error codes are documented <a href="https://ai.google.dev/gemini-api/docs/troubleshooting">here</a>.
     * @param request The originating request
     * @param result The http response and body
     * @throws RetryException Throws if status code is {@code >= 300 or < 200 }
     */
    void checkForFailureStatusCode(Request request, HttpResult result) throws RetryException {
        if (result.isSuccessfulResponse()) {
            return;
        }

        // handle error codes
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 500) {
            throw new RetryException(true, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 503) {
            throw new RetryException(true, buildError(GOOGLE_AI_STUDIO_UNAVAILABLE, request, result));
        } else if (statusCode > 500) {
            throw new RetryException(false, buildError(SERVER_ERROR, request, result));
        } else if (statusCode == 429) {
            throw new RetryException(true, buildError(RATE_LIMIT, request, result));
        } else if (statusCode == 404) {
            throw new RetryException(false, buildError(resourceNotFoundError(request), request, result));
        } else if (statusCode == 403) {
            throw new RetryException(false, buildError(PERMISSION_DENIED, request, result));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, request, result));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, request, result));
        }
    }

    private static String resourceNotFoundError(Request request) {
        return format("Resource not found at [%s]", request.getURI());
    }

    @Override
    public boolean canHandleStreamingResponses() {
        return canHandleStreamingResponses;
    }

    @Override
    public InferenceServiceResults parseResult(Request request, Flow.Publisher<HttpResult> flow) {
        var serverSentEventProcessor = new ServerSentEventProcessor(new ServerSentEventParser());
        var googleAiProcessor = new GoogleAiStudioStreamingProcessor(content);
        flow.subscribe(serverSentEventProcessor);
        serverSentEventProcessor.subscribe(googleAiProcessor);
        return new StreamingChatCompletionResults(googleAiProcessor);
    }

}
