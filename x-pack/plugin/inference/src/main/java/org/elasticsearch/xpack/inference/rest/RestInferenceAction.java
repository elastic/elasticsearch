/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.core.inference.results.XContentFormattedException.X_CONTENT_PARAM;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_ID_PATH;
import static org.elasticsearch.xpack.inference.rest.Paths.TASK_TYPE_INFERENCE_ID_PATH;

@ServerlessScope(Scope.PUBLIC)
public class RestInferenceAction extends BaseInferenceAction {
    private static final Logger logger = LogManager.getLogger(RestInferenceAction.class);

    @Override
    public String getName() {
        return "inference_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, INFERENCE_ID_PATH), new Route(POST, TASK_TYPE_INFERENCE_ID_PATH));
    }

    @Override
    protected boolean shouldStream() {
        return false;
    }

    /**
     * Successful responses are rendered by {@link RestChunkedToXContentListener}. Failures only go through it when the inference layer
     * did not already format them: {@link RestChunkedToXContentListener} inherits a {@code final}
     * {@link RestActionListener#onFailure} that always writes the standard error envelope, which would drop
     * the OpenAI-compatible body of a {@link UnifiedChatCompletionException}. That would
     * make the error shape of {@code chat_completion} depend on whether the caller asked for a stream, so pre-formatted errors are sent
     * here instead. See {@link InferenceErrorFormat}.
     */
    @Override
    protected ActionListener<InferenceAction.Response> listener(RestChannel channel) {
        var params = new ToXContent.DelegatingMapParams(
            Map.of(X_CONTENT_PARAM, String.valueOf(channel.detailedErrorsEnabled())),
            channel.request()
        );
        var delegate = new RestChunkedToXContentListener<InferenceAction.Response>(channel, params);
        return ActionListener.wrap(delegate::onResponse, e -> sendFailure(channel, params, delegate, e));
    }

    private static void sendFailure(
        RestChannel channel,
        ToXContent.Params params,
        ActionListener<InferenceAction.Response> delegate,
        Exception e
    ) {
        var formattedException = InferenceErrorFormat.formattedException(e);
        if (formattedException == null) {
            delegate.onFailure(e);
            return;
        }

        final ChunkedRestResponseBodyPart bodyPart;
        try {
            bodyPart = ChunkedRestResponseBodyPart.fromXContent(formattedException, params, channel);
        } catch (Exception inner) {
            inner.addSuppressed(e);
            // nothing has been sent yet, so fall back to the standard error envelope rather than leaving the channel hanging
            delegate.onFailure(inner);
            return;
        }

        try {
            channel.sendResponse(RestResponse.chunked(formattedException.status(), bodyPart, null));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.warn("failed to send failure response", inner);
        }
    }
}
