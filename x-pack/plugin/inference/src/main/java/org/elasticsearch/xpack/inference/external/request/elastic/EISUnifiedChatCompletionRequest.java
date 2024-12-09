/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiRequest;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class EISUnifiedChatCompletionRequest implements OpenAiRequest {

    private final ElasticInferenceServiceCompletionModel model;
    private final UnifiedChatInput unifiedChatInput;
    private final URI uri;
    private final TraceContext traceContext;

    public EISUnifiedChatCompletionRequest(
        UnifiedChatInput unifiedChatInput,
        ElasticInferenceServiceCompletionModel model,
        TraceContext traceContext
    ) {
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
        this.model = Objects.requireNonNull(model);
        this.uri = model.uri();
        this.traceContext = traceContext;

    }

    @Override
    public HttpRequest createHttpRequest() {
        var httpPost = new HttpPost(uri);
        var requestEntity = Strings.toString(new EISUnifiedChatCompletionRequestEntity(unifiedChatInput));

        ByteArrayEntity byteEntity = new ByteArrayEntity(requestEntity.getBytes(StandardCharsets.UTF_8));
        httpPost.setEntity(byteEntity);

        if (traceContext != null) {
            propagateTraceContext(httpPost);
        }

        httpPost.setHeader(new BasicHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate() {
        // No truncation for OpenAI chat completions
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        // No truncation for OpenAI chat completions
        return null;
    }

    @Override
    public String getInferenceEntityId() {
        return model.getInferenceEntityId();
    }

    @Override
    public boolean isStreaming() {
        return true;
    }

    public TraceContext getTraceContext() {
        return traceContext;
    }

    private void propagateTraceContext(HttpPost httpPost) {
        var traceParent = traceContext.traceParent();
        var traceState = traceContext.traceState();

        if (traceParent != null) {
            httpPost.setHeader(Task.TRACE_PARENT_HTTP_HEADER, traceParent);
        }

        if (traceState != null) {
            httpPost.setHeader(Task.TRACE_STATE, traceState);
        }
    }
}
